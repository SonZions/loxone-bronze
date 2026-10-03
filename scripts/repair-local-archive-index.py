#!/usr/bin/env python3
"""One-time, lossless rebuild of the local Bronze working table's PK index.

Run only with both local Silver timers stopped and a verified, separate copy of
the DuckDB file. The original table is retained after the atomic name swap.
"""
from __future__ import annotations

import argparse
from pathlib import Path
import re

import duckdb

from loxone_bronze.local_silver import lock


SCHEMA = "loxone_bronze"
SOURCE = f"{SCHEMA}.archive_messages"
SHADOW = f"{SCHEMA}.archive_messages_rebuilt"
OLD = f"{SCHEMA}.archive_messages_before_pk_repair"
COLUMNS = (
    "message_id", "source_id", "received_at", "message_type", "ingested_at",
    "payload_json", "run_id", "payload_format", "payload_sha256",
    "collector_version",
)


def rebuild(database: Path, backup: Path, expected_rows: int,
            probe_source_id: str, probe_message_id: str) -> None:
    if not database.is_file() or not backup.is_file() or database.samefile(backup):
        raise ValueError("Database and separate backup must both exist")
    if database.stat().st_size != backup.stat().st_size:
        raise ValueError("Backup and database sizes differ")

    with lock(str(database) + ".lock"), lock(str(database.parent / "pipeline.lock")):
        c = duckdb.connect(str(database), config={"threads": 1, "memory_limit": "192MB"})
        try:
            names = tuple(row[0] for row in c.execute(f"DESCRIBE {SOURCE}").fetchall())
            if names != COLUMNS:
                raise RuntimeError(f"Unexpected working archive columns: {names}")
            existing = c.execute(
                "SELECT table_name FROM duckdb_tables() WHERE schema_name=? "
                "AND table_name IN ('archive_messages_rebuilt', "
                "'archive_messages_before_pk_repair')", [SCHEMA]
            ).fetchall()
            if existing:
                raise RuntimeError(f"Prior repair tables exist: {existing}")
            count = c.execute(f"SELECT count(*) FROM {SOURCE}").fetchone()[0]
            if count != expected_rows:
                raise RuntimeError(f"Archive row count changed: {count} != {expected_rows}")
            if c.execute(
                f"SELECT 1 FROM {SOURCE} WHERE source_id=? AND message_id=?",
                [probe_source_id, probe_message_id],
            ).fetchone():
                raise RuntimeError("Probe key is already visible; index repair is not indicated")
            view = c.execute(
                "SELECT sql FROM duckdb_views() WHERE schema_name=? AND view_name='ws_messages'",
                [SCHEMA],
            ).fetchone()
            if not view or not re.match(r"^CREATE VIEW\s", view[0], re.I):
                raise RuntimeError("Working Bronze view definition missing")
            view_sql = re.sub(r"^CREATE VIEW\s", "CREATE OR REPLACE VIEW ", view[0], count=1, flags=re.I)

            print(f"rebuild_start rows={count}", flush=True)
            c.execute(f"""CREATE TABLE {SHADOW} (
                message_id VARCHAR, source_id VARCHAR,
                received_at TIMESTAMPTZ, message_type INTEGER,
                ingested_at TIMESTAMPTZ, payload_json JSON,
                run_id VARCHAR, payload_format VARCHAR,
                payload_sha256 VARCHAR, collector_version VARCHAR,
                PRIMARY KEY (source_id, message_id))""")
            c.execute(f"INSERT INTO {SHADOW} SELECT * FROM {SOURCE}")
            rebuilt = c.execute(f"SELECT count(*) FROM {SHADOW}").fetchone()[0]
            if rebuilt != count:
                raise RuntimeError(f"Copied row count differs: {rebuilt} != {count}")

            # A rolled-back probe proves the new ART index accepts the exact key
            # blocked by the old index without adding a row to either table.
            c.execute("BEGIN")
            try:
                c.execute(f"""INSERT INTO {SHADOW}
                    (source_id,message_id,received_at,message_type,ingested_at,payload_json)
                    VALUES (?,?,now(),2,now(),'{{}}'::JSON)""",
                    [probe_source_id, probe_message_id])
            finally:
                c.execute("ROLLBACK")
            if c.execute(f"SELECT count(*) FROM {SHADOW}").fetchone()[0] != count:
                raise RuntimeError("Probe rollback did not preserve the row count")

            print(f"rebuild_validated rows={rebuilt}", flush=True)
            c.execute("BEGIN")
            try:
                c.execute(f"ALTER TABLE {SOURCE} RENAME TO archive_messages_before_pk_repair")
                c.execute(f"ALTER TABLE {SHADOW} RENAME TO archive_messages")
                c.execute(view_sql)
                if c.execute(f"SELECT count(*) FROM {SOURCE}").fetchone()[0] != count:
                    raise RuntimeError("Swapped archive row count differs")
                c.execute("COMMIT")
            except BaseException:
                c.execute("ROLLBACK")
                raise
            print(f"rebuild_complete rows={count} old_table={OLD}", flush=True)
        finally:
            c.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, required=True)
    parser.add_argument("--backup", type=Path, required=True)
    parser.add_argument("--expected-rows", type=int, required=True)
    parser.add_argument("--probe-source-id", required=True)
    parser.add_argument("--probe-message-id", required=True)
    args = parser.parse_args()
    rebuild(args.database, args.backup, args.expected_rows,
            args.probe_source_id, args.probe_message_id)


if __name__ == "__main__":
    main()
