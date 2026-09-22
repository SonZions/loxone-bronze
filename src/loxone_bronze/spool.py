from __future__ import annotations

import sqlite3
import time
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Iterable

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA foreign_keys=ON;
PRAGMA busy_timeout=10000;

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

-- The pending index cannot efficiently answer MAX(received_at) globally.
CREATE INDEX IF NOT EXISTS ix_ws_messages_received
ON ws_messages(received_at);

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
        self._write(lambda con: con.executescript(SCHEMA))

    @contextmanager
    def connect(self):
        con = sqlite3.connect(self.path, timeout=10)
        con.row_factory = sqlite3.Row
        try:
            con.execute("PRAGMA busy_timeout=10000")
            con.execute("PRAGMA foreign_keys=ON")
            con.execute("PRAGMA synchronous=NORMAL")
            yield con
            con.commit()
        finally:
            con.close()

    def _write(self, operation):
        """Run one SQLite write and retry temporary writer contention."""
        for attempt in range(6):
            try:
                with self.connect() as con:
                    return operation(con)
            except sqlite3.OperationalError as exc:
                message = str(exc).lower()
                if "locked" not in message and "busy" not in message:
                    raise
                if attempt == 5:
                    raise
                time.sleep(0.25 * (attempt + 1))

    def _mark(self, statement: str, rows) -> None:
        if not rows:
            return
        def mark(con):
            for start in range(0, len(rows), 250):
                con.executemany(statement, rows[start:start + 250])
        self._write(mark)

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
        self._write(
            lambda con: con.execute(
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
        self._write(
            lambda con: con.execute(
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
        self._mark(
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
        self._mark(
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
        self._mark(
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
        self._mark(
            """
            UPDATE structures
            SET upload_attempts = upload_attempts + 1,
                last_upload_error = ?
            WHERE structure_id = ?
            """,
            [(error[:2000], item_id) for item_id in ids],
        )

    def prune_uploaded(self, retention_days: int, limit: int = 5000) -> None:
        # uploaded_at is always UTC ISO text emitted by this application. Use the
        # existing index and bound deletions so retention cannot monopolize WAL.
        cutoff = (datetime.now(timezone.utc) - timedelta(days=retention_days)).isoformat(
            timespec="microseconds"
        )
        for table in ("ws_messages", "structures"):
            self._write(lambda con, table=table: con.execute(
                f"DELETE FROM {table} WHERE rowid IN ("
                f"SELECT rowid FROM {table} WHERE uploaded_at < ? "
                "ORDER BY uploaded_at LIMIT ?)", (cutoff, limit),
            ))

    def status(self, include_totals: bool = True) -> dict:
        result = {"path": str(self.path)}
        with self.connect() as con:
            # One WAL read snapshot; no writer lock. Runtime/health skip totals.
            con.execute("BEGIN")
            for key, table, timestamp in (
                ("messages", "ws_messages", "received_at"),
                ("structures", "structures", "captured_at"),
            ):
                pending = con.execute(
                    f"SELECT COUNT(*) FROM {table} WHERE uploaded_at IS NULL"
                ).fetchone()[0]
                oldest = con.execute(
                    f"SELECT {timestamp} FROM {table} WHERE uploaded_at IS NULL "
                    f"ORDER BY {timestamp} LIMIT 1"
                ).fetchone()
                latest = con.execute(f"SELECT MAX({timestamp}) FROM {table}").fetchone()[0]
                result[key] = {
                    "pending": pending,
                    "oldest_pending": oldest[0] if oldest else None,
                    "latest": latest,
                }
                if include_totals:
                    result[key]["total"] = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        return result
