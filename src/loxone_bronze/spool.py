from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterable

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=FULL;
PRAGMA foreign_keys=ON;
PRAGMA busy_timeout=5000;

CREATE TABLE IF NOT EXISTS ws_messages (
    message_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    run_id TEXT NOT NULL,
    received_at TEXT NOT NULL,
    message_type INTEGER NOT NULL,
    payload_format TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    collector_version TEXT NOT NULL,
    uploaded_at TEXT,
    upload_attempts INTEGER NOT NULL DEFAULT 0,
    last_upload_error TEXT
);

CREATE INDEX IF NOT EXISTS ix_ws_messages_pending
ON ws_messages(uploaded_at, received_at);

CREATE TABLE IF NOT EXISTS structures (
    structure_id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL,
    captured_at TEXT NOT NULL,
    last_modified TEXT,
    payload_json TEXT NOT NULL,
    payload_sha256 TEXT NOT NULL,
    collector_version TEXT NOT NULL,
    uploaded_at TEXT,
    upload_attempts INTEGER NOT NULL DEFAULT 0,
    last_upload_error TEXT
);

CREATE INDEX IF NOT EXISTS ix_structures_pending
ON structures(uploaded_at, captured_at);
"""


class Spool:
    def __init__(self, path: str):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.connect() as con:
            con.executescript(SCHEMA)

    @contextmanager
    def connect(self):
        con = sqlite3.connect(self.path, timeout=10)
        con.row_factory = sqlite3.Row
        try:
            con.execute("PRAGMA busy_timeout=5000")
            yield con
            con.commit()
        finally:
            con.close()

    def insert_message(
        self,
        *,
        message_id: str,
        source_id: str,
        run_id: str,
        received_at: str,
        message_type: int,
        payload_format: str,
        payload_json: str,
        payload_sha256: str,
        collector_version: str,
    ) -> None:
        with self.connect() as con:
            con.execute(
                """
                INSERT OR IGNORE INTO ws_messages (
                    message_id, source_id, run_id, received_at, message_type,
                    payload_format, payload_json, payload_sha256, collector_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    message_id, source_id, run_id, received_at, message_type,
                    payload_format, payload_json, payload_sha256, collector_version,
                ),
            )

    def insert_structure(
        self,
        *,
        structure_id: str,
        source_id: str,
        captured_at: str,
        last_modified: str | None,
        payload_json: str,
        payload_sha256: str,
        collector_version: str,
    ) -> None:
        with self.connect() as con:
            con.execute(
                """
                INSERT OR IGNORE INTO structures (
                    structure_id, source_id, captured_at, last_modified,
                    payload_json, payload_sha256, collector_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    structure_id, source_id, captured_at, last_modified,
                    payload_json, payload_sha256, collector_version,
                ),
            )

    def latest_structure_version(self, source_id: str) -> str | None:
        with self.connect() as con:
            row = con.execute(
                """
                SELECT last_modified
                FROM structures
                WHERE source_id = ?
                ORDER BY captured_at DESC
                LIMIT 1
                """,
                (source_id,),
            ).fetchone()
        return row["last_modified"] if row else None

    def pending_messages(self, limit: int):
        with self.connect() as con:
            return con.execute(
                """
                SELECT message_id, source_id, run_id, received_at, message_type,
                       payload_format, payload_json, payload_sha256, collector_version
                FROM ws_messages
                WHERE uploaded_at IS NULL
                ORDER BY received_at
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

    def pending_structures(self, limit: int):
        with self.connect() as con:
            return con.execute(
                """
                SELECT structure_id, source_id, captured_at, last_modified,
                       payload_json, payload_sha256, collector_version
                FROM structures
                WHERE uploaded_at IS NULL
                ORDER BY captured_at
                LIMIT ?
                """,
                (limit,),
            ).fetchall()

    def mark_messages_uploaded(self, ids: Iterable[str], uploaded_at: str) -> None:
        ids = list(ids)
        if not ids:
            return
        with self.connect() as con:
            con.executemany(
                """
                UPDATE ws_messages
                SET uploaded_at = ?, last_upload_error = NULL
                WHERE message_id = ?
                """,
                [(uploaded_at, item_id) for item_id in ids],
            )

    def mark_structures_uploaded(self, ids: Iterable[str], uploaded_at: str) -> None:
        ids = list(ids)
        if not ids:
            return
        with self.connect() as con:
            con.executemany(
                """
                UPDATE structures
                SET uploaded_at = ?, last_upload_error = NULL
                WHERE structure_id = ?
                """,
                [(uploaded_at, item_id) for item_id in ids],
            )

    def mark_message_failure(self, ids: Iterable[str], error: str) -> None:
        ids = list(ids)
        if not ids:
            return
        with self.connect() as con:
            con.executemany(
                """
                UPDATE ws_messages
                SET upload_attempts = upload_attempts + 1,
                    last_upload_error = ?
                WHERE message_id = ?
                """,
                [(error[:2000], item_id) for item_id in ids],
            )

    def mark_structure_failure(self, ids: Iterable[str], error: str) -> None:
        ids = list(ids)
        if not ids:
            return
        with self.connect() as con:
            con.executemany(
                """
                UPDATE structures
                SET upload_attempts = upload_attempts + 1,
                    last_upload_error = ?
                WHERE structure_id = ?
                """,
                [(error[:2000], item_id) for item_id in ids],
            )

    def prune_uploaded(self, retention_days: int) -> None:
        modifier = f"-{int(retention_days)} days"
        with self.connect() as con:
            con.execute(
                """
                DELETE FROM ws_messages
                WHERE uploaded_at IS NOT NULL
                  AND datetime(uploaded_at) < datetime('now', ?)
                """,
                (modifier,),
            )
            con.execute(
                """
                DELETE FROM structures
                WHERE uploaded_at IS NOT NULL
                  AND datetime(uploaded_at) < datetime('now', ?)
                """,
                (modifier,),
            )

    def status(self) -> dict:
        with self.connect() as con:
            msg = con.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN uploaded_at IS NULL THEN 1 ELSE 0 END) AS pending,
                    MIN(CASE WHEN uploaded_at IS NULL THEN received_at END) AS oldest_pending,
                    MAX(received_at) AS latest
                FROM ws_messages
                """
            ).fetchone()
            structures = con.execute(
                """
                SELECT
                    COUNT(*) AS total,
                    SUM(CASE WHEN uploaded_at IS NULL THEN 1 ELSE 0 END) AS pending,
                    MAX(captured_at) AS latest
                FROM structures
                """
            ).fetchone()

        return {
            "messages": dict(msg),
            "structures": dict(structures),
            "path": str(self.path),
        }
