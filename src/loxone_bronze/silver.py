"""Raspberry scheduler/client; all persistent data and SQL work tables are remote."""
from __future__ import annotations

import argparse
import fcntl
import json
import logging
import os
import re
import time
import uuid
from dataclasses import dataclass
from pathlib import Path

import duckdb

LOG = logging.getLogger("loxone_bronze.silver")
SQL_DIR = Path(__file__).with_name("silver_sql")


def identifier(value):
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value):
        raise ValueError("Invalid database/schema identifier")
    return '"' + value + '"'


@dataclass(frozen=True)
class SilverConfig:
    source_database: str = "loxone_ingest"
    target_database: str = "my_db"
    source_schema: str = "loxone_bronze"
    target_schema: str = "loxone_silver"
    batch_size: int = 5000
    max_batches: int = 12
    max_runtime_seconds: int = 600

    def __post_init__(self):
        for name in (self.source_database, self.target_database, self.source_schema, self.target_schema):
            identifier(name)
        if self.source_database == self.target_database and self.source_schema == self.target_schema:
            raise ValueError("Source and target must differ")
        if not 1 <= self.batch_size <= 50000 or not 1 <= self.max_batches <= 1000 or not 1 <= self.max_runtime_seconds <= 7200:
            raise ValueError("Silver work limits out of range")

    @property
    def bronze(self):
        return f"{identifier(self.source_database)}.{identifier(self.source_schema)}"

    @property
    def silver(self):
        return f"{identifier(self.target_database)}.{identifier(self.target_schema)}"

    @classmethod
    def from_env(cls):
        return cls(**{name: type(default)(os.environ.get("SILVER_" + name.upper(), default))
                      for name, default in cls().__dict__.items()})

    def sql(self, name):
        return ((SQL_DIR / name).read_text().replace("${bronze}", self.bronze)
                .replace("${silver}", self.silver).replace("${batch_size}", str(self.batch_size)))


def preflight(con, cfg):
    # Read only; no automatic database creation or fallback to empty placeholders.
    for table in ("ws_messages", "structures"):
        con.execute(f"SELECT * FROM {cfg.bronze}.{table} LIMIT 0")
    if not con.execute("SELECT 1 FROM duckdb_databases() WHERE database_name=?", [cfg.target_database]).fetchone():
        raise ValueError("Target database is not attached")


def initialize(con, cfg):
    con.execute(cfg.sql("schema.sql"))
    s = cfg.silver
    con.execute(f"CREATE TABLE IF NOT EXISTS {s}.processed_messages ("
                "source_id VARCHAR, message_id VARCHAR, structure_id VARCHAR, "
                "processed_at TIMESTAMPTZ DEFAULT now(), PRIMARY KEY(source_id,message_id))")
    con.execute(f"CREATE TABLE IF NOT EXISTS {s}.refresh_guard (id INTEGER PRIMARY KEY, revision BIGINT)")
    con.execute(f"INSERT OR IGNORE INTO {s}.refresh_guard VALUES (1,0)")


def begin(con, cfg):
    con.execute("BEGIN TRANSACTION")
    # Concurrent writers in different clients conflict on this row. Local flock
    # handles normal Pi invocations; this also protects remote scratch tables.
    con.execute(f"UPDATE {cfg.silver}.refresh_guard SET revision=revision+1 WHERE id=1")


def rollback(con):
    try:
        con.execute("ROLLBACK")
    except Exception:
        pass


def sync_structures(con, cfg, run_id=None):
    try:
        begin(con, cfg)
        con.execute(cfg.sql("structures.sql"))
        count = con.execute(f"SELECT count(*) FROM {cfg.silver}._refresh_structures").fetchone()[0]
        if run_id:
            con.execute(f"UPDATE {cfg.silver}.load_runs SET structures_loaded=structures_loaded+?, "
                        f"mappings_loaded=mappings_loaded+(SELECT coalesce(sum(state_mapping_count),0) "
                        f"FROM {cfg.silver}.structure_versions WHERE structure_id IN "
                        f"(SELECT structure_id FROM {cfg.silver}._refresh_structures)) WHERE load_run_id=?", [count,run_id])
        con.execute(f"DROP TABLE {cfg.silver}._refresh_controls")
        con.execute(f"DROP TABLE {cfg.silver}._refresh_structures")
        con.execute("COMMIT")
        return count
    except Exception:
        rollback(con)
        raise


def process_batch(con, cfg, run_id=None):
    s = cfg.silver
    try:
        begin(con, cfg)
        con.execute(cfg.sql("batch.sql"))
        messages, latest = con.execute(f"SELECT count(*), max(ingested_at) FROM {s}._refresh_batch").fetchone()
        events = con.execute(f"SELECT count(*) FROM {s}._refresh_events").fetchone()[0]
        if run_id:
            con.execute(f"UPDATE {s}.load_runs SET messages_processed=messages_processed+?, "
                        "events_inserted=events_inserted+?, "
                        "source_max_ingested_at=greatest(source_max_ingested_at, ?) WHERE load_run_id=?",
                        [messages, events, latest, run_id])
        con.execute(f"DROP TABLE {s}._refresh_events")
        con.execute(f"DROP TABLE {s}._refresh_batch")
        con.execute("COMMIT")
        return {"messages": messages, "events": events}
    except Exception:
        rollback(con)
        raise


def refresh(con, cfg):
    started = time.monotonic()
    preflight(con, cfg)
    initialize(con, cfg)
    run_id = "raspberry-silver-" + str(uuid.uuid4())
    con.execute(f"INSERT INTO {cfg.silver}.load_runs (load_run_id,started_at,status) VALUES (?,now(),'running')", [run_id])
    try:
        structures = 0
        mapping_ready = False
        reason = "structure_limit"
        for _ in range(cfg.max_batches):
            if time.monotonic() - started >= cfg.max_runtime_seconds:
                reason = "time_limit"
                break
            count = sync_structures(con, cfg, run_id)
            structures += count
            if count == 0:
                mapping_ready = True
                break
        if mapping_ready:
            reason = "batch_limit"
        for _ in range(cfg.max_batches if mapping_ready else 0):
            if time.monotonic() - started >= cfg.max_runtime_seconds:
                reason = "time_limit"
                break
            result = process_batch(con, cfg, run_id)
            LOG.info("silver_batch %s", json.dumps(result))
            if not result["messages"]:
                reason = "caught_up"
                break
        status = "succeeded" if reason == "caught_up" else "partial"
        con.execute(f"UPDATE {cfg.silver}.load_runs SET status=?,finished_at=now() WHERE load_run_id=?", [status,run_id])
        LOG.info("silver_summary %s", json.dumps({"status":status,"reason":reason,"structures":structures,
                                                  "elapsed_seconds":round(time.monotonic()-started,2)}))
    except Exception as exc:
        rollback(con)
        try:
            con.execute(f"UPDATE {cfg.silver}.load_runs SET status='failed',finished_at=now(),error_message=? WHERE load_run_id=?",
                        [type(exc).__name__,run_id])
        except Exception:
            pass
        raise


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true", help="Read-only source/target access check")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,format="%(asctime)s %(levelname)s %(message)s")
    try:
        cfg = SilverConfig.from_env()
        if not os.environ.get("MOTHERDUCK_TOKEN", "").strip():
            raise ValueError("MOTHERDUCK_TOKEN is required")
        lock_path = Path(os.environ.get("SILVER_LOCK_PATH", "/var/lib/loxone-silver/refresh.lock"))
        with lock_path.open("a") as lock:
            try:
                fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
            except BlockingIOError:
                LOG.info("silver_skipped reason=already_running")
                return
            with duckdb.connect("md:" + cfg.target_database) as con:
                con.execute("SET TimeZone='UTC'")
                con.execute("SET threads=1")
                con.execute("SET memory_limit='256MB'")
                if args.check:
                    preflight(con,cfg)
                    LOG.info("silver_check source_and_target_readable=true write_access_not_tested=true")
                else:
                    refresh(con,cfg)
    except Exception as exc:
        # Connection errors may embed tokens; never log the exception text.
        LOG.error("silver_error type=%s",type(exc).__name__)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
