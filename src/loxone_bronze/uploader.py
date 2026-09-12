from __future__ import annotations

import logging
import os
import sys

import duckdb

from .common import env_required, utc_now_iso
from .spool import Spool

LOG = logging.getLogger("loxone_bronze.uploader")


def upload_messages(md, spool: Spool, limit: int) -> int:
    rows = spool.pending_messages(limit)
    if not rows:
        return 0

    ids = [row["message_id"] for row in rows]
    try:
        md.execute("DROP TABLE IF EXISTS temp._loxone_messages")
        md.execute(
            """
            CREATE TEMP TABLE _loxone_messages (
                message_id VARCHAR,
                source_id VARCHAR,
                run_id VARCHAR,
                received_at TIMESTAMPTZ,
                message_type INTEGER,
                payload_format VARCHAR,
                payload_json JSON,
                payload_sha256 VARCHAR,
                collector_version VARCHAR
            )
            """
        )
        md.executemany(
            """
            INSERT INTO _loxone_messages VALUES (?, ?, ?, ?::TIMESTAMPTZ, ?, ?, ?::JSON, ?, ?)
            """,
            [
                (
                    row["message_id"], row["source_id"], row["run_id"], row["received_at"],
                    row["message_type"], row["payload_format"], row["payload_json"],
                    row["payload_sha256"], row["collector_version"],
                )
                for row in rows
            ],
        )
        md.execute(
            """
            INSERT OR IGNORE INTO loxone_bronze.ws_messages (
                message_id, source_id, run_id, received_at, message_type,
                payload_format, payload_json, payload_sha256, collector_version
            )
            SELECT
                message_id, source_id, run_id, received_at, message_type,
                payload_format, payload_json, payload_sha256, collector_version
            FROM _loxone_messages
            """
        )
        uploaded_at = utc_now_iso()
        spool.mark_messages_uploaded(ids, uploaded_at)
        return len(rows)
    except Exception as exc:
        spool.mark_message_failure(ids, repr(exc))
        raise


def upload_structures(md, spool: Spool, limit: int) -> int:
    rows = spool.pending_structures(limit)
    if not rows:
        return 0

    ids = [row["structure_id"] for row in rows]
    try:
        md.execute("DROP TABLE IF EXISTS temp._loxone_structures")
        md.execute(
            """
            CREATE TEMP TABLE _loxone_structures (
                structure_id VARCHAR,
                source_id VARCHAR,
                captured_at TIMESTAMPTZ,
                last_modified VARCHAR,
                payload_json JSON,
                payload_sha256 VARCHAR,
                collector_version VARCHAR
            )
            """
        )
        md.executemany(
            """
            INSERT INTO _loxone_structures VALUES (?, ?, ?::TIMESTAMPTZ, ?, ?::JSON, ?, ?)
            """,
            [
                (
                    row["structure_id"], row["source_id"], row["captured_at"],
                    row["last_modified"], row["payload_json"], row["payload_sha256"],
                    row["collector_version"],
                )
                for row in rows
            ],
        )
        md.execute(
            """
            INSERT OR IGNORE INTO loxone_bronze.structures (
                structure_id, source_id, captured_at, last_modified,
                payload_json, payload_sha256, collector_version
            )
            SELECT
                structure_id, source_id, captured_at, last_modified,
                payload_json, payload_sha256, collector_version
            FROM _loxone_structures
            """
        )
        uploaded_at = utc_now_iso()
        spool.mark_structures_uploaded(ids, uploaded_at)
        return len(rows)
    except Exception as exc:
        spool.mark_structure_failure(ids, repr(exc))
        raise


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    # Force a clear error before DuckDB tries interactive/browser auth.
    env_required("MOTHERDUCK_TOKEN")

    spool = Spool(os.environ.get("SPOOL_DB", "/var/lib/loxone-bronze/spool.sqlite3"))
    batch_size = int(os.environ.get("UPLOAD_BATCH_SIZE", "5000"))
    retention_days = int(os.environ.get("LOCAL_RETENTION_DAYS", "7"))
    database = os.environ.get("MOTHERDUCK_DATABASE", "my_db").strip()

    md = duckdb.connect(f"md:{database}")
    try:
        # Fail fast if the explicitly provisioned Bronze schema is missing.
        md.execute("SELECT 1 FROM loxone_bronze.ws_messages LIMIT 0")
        md.execute("SELECT 1 FROM loxone_bronze.structures LIMIT 0")

        structures = upload_structures(md, spool, min(batch_size, 500))
        messages = upload_messages(md, spool, batch_size)
        spool.prune_uploaded(retention_days)

        LOG.info("Upload complete: %s structures, %s messages", structures, messages)
    finally:
        md.close()


if __name__ == "__main__":
    main()
