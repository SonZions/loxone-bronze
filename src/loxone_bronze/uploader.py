from __future__ import annotations

import fcntl
import json
import logging
import os
import tempfile
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

import duckdb

from .common import env_required, utc_now_iso
from .spool import Spool

LOG = logging.getLogger("loxone_bronze.uploader")
MESSAGE_COLUMNS = {
    "message_id": "VARCHAR", "source_id": "VARCHAR", "run_id": "VARCHAR",
    "received_at": "TIMESTAMPTZ", "message_type": "INTEGER",
    "payload_format": "VARCHAR", "payload_json": "VARCHAR",
    "payload_sha256": "VARCHAR", "collector_version": "VARCHAR",
}
STRUCTURE_COLUMNS = {
    "structure_id": "VARCHAR", "source_id": "VARCHAR", "captured_at": "TIMESTAMPTZ",
    "last_modified": "VARCHAR", "payload_json": "VARCHAR",
    "payload_sha256": "VARCHAR", "collector_version": "VARCHAR",
}


@dataclass(frozen=True)
class UploadConfig:
    batch_size: int = 5000
    max_batches_per_run: int = 12
    max_runtime_seconds: int = 240
    retention_days: int = 7

    @classmethod
    def from_env(cls):
        values = {}
        for field, name, default in (
            ("batch_size", "UPLOAD_BATCH_SIZE", 5000),
            ("max_batches_per_run", "UPLOAD_MAX_BATCHES_PER_RUN", 12),
            ("max_runtime_seconds", "UPLOAD_MAX_RUNTIME_SECONDS", 240),
            ("retention_days", "LOCAL_RETENTION_DAYS", 7),
        ):
            try:
                value = int(os.environ.get(name, str(default)))
            except ValueError:
                raise ValueError(f"{name} must be a positive integer") from None
            if value <= 0:
                raise ValueError(f"{name} must be a positive integer")
            values[field] = value
        return cls(**values)


def bulk_insert(md, table: str, columns: dict, rows) -> None:
    """One atomic INSERT ... SELECT over a local file; no per-row remote calls.

    payload_json is an opaque string in the envelope. DuckDB validates/casts it
    to JSON without parsing and reserializing its original contents in Python.
    The explicit schema also preserves SQL NULL vs an empty last_modified.
    """
    names = ", ".join(columns)
    projection = ", ".join(
        "payload_json::JSON" if name == "payload_json" else name for name in columns
    )
    types = ", ".join(f"'{name}': '{kind}'" for name, kind in columns.items())
    with tempfile.TemporaryDirectory(prefix="loxone-upload-") as directory:
        path = Path(directory) / "batch.jsonl"
        max_object_size = 16777216
        with path.open("w", encoding="utf-8") as out:
            for row in rows:
                line = json.dumps({name: row[name] for name in columns}, ensure_ascii=True)
                max_object_size = max(max_object_size, len(line) + 1)
                out.write(line + "\n")
        md.execute(
            f"INSERT OR IGNORE INTO loxone_bronze.{table} ({names}) "
            f"SELECT {projection} FROM read_json(?, columns={{{types}}}, "
            f"format='newline_delimited', maximum_object_size={max_object_size})",
            [str(path)],
        )


def upload_messages(md, spool: Spool, limit: int) -> int:
    rows = spool.pending_messages(limit)
    if not rows:
        return 0
    ids = [row["message_id"] for row in rows]
    try:
        bulk_insert(md, "ws_messages", MESSAGE_COLUMNS, rows)
        spool.mark_messages_uploaded(ids, utc_now_iso())
        return len(rows)
    except Exception as exc:
        # Driver exceptions may contain credentials, URLs or payloads. Persist
        # only the exception class, never repr(exc), str(exc) or a traceback.
        spool.mark_message_failure(ids, type(exc).__name__)
        raise


def upload_structures(md, spool: Spool, limit: int) -> int:
    rows = spool.pending_structures(limit)
    if not rows:
        return 0
    ids = [row["structure_id"] for row in rows]
    try:
        bulk_insert(md, "structures", STRUCTURE_COLUMNS, rows)
        spool.mark_structures_uploaded(ids, utc_now_iso())
        return len(rows)
    except Exception as exc:
        spool.mark_structure_failure(ids, type(exc).__name__)
        raise


def oldest_age_seconds(status: dict) -> float | None:
    oldest = status["messages"]["oldest_pending"]
    if oldest is None:
        return None
    return max(0.0, (datetime.now(timezone.utc) - datetime.fromisoformat(oldest)).total_seconds())


def run_upload(spool, config, connect, clock=time.monotonic) -> dict:
    """Runtime is a deadline for starting work, not cancellation of a commit.

    An in-flight batch finishes and is acknowledged before checking the deadline.
    A hard systemd timeout is safe too: the unacknowledged batch is retried.
    """
    start = clock()
    result = {"batches": 0, "messages": 0, "structures": 0, "reason": "error"}
    md = None
    LOG.info("upload_config %s", json.dumps(asdict(config), sort_keys=True))
    try:
        before = spool.status(include_totals=False)
        result["pending_before"] = before["messages"]["pending"]
        LOG.info("upload_start %s", json.dumps(before, sort_keys=True))
        md = connect()
        md.execute("SELECT 1 FROM loxone_bronze.ws_messages LIMIT 0")
        md.execute("SELECT 1 FROM loxone_bronze.structures LIMIT 0")
        if clock() - start < config.max_runtime_seconds:
            # Structure snapshots can be large; keep their batch much smaller.
            result["structures"] = upload_structures(md, spool, min(config.batch_size, 10))
        while True:
            if clock() - start >= config.max_runtime_seconds:
                result["reason"] = "time_limit"
                break
            if result["batches"] >= config.max_batches_per_run:
                result["reason"] = "batch_limit"
                break
            batch_start = clock()
            count = upload_messages(md, spool, config.batch_size)
            if not count:
                result["reason"] = "empty"
                break
            result["messages"] += count
            result["batches"] += 1
            LOG.info("upload_batch %s", json.dumps({
                "batch": result["batches"], "messages": count,
                "elapsed_seconds": round(clock() - batch_start, 3),
            }))
        if clock() - start < config.max_runtime_seconds:
            spool.prune_uploaded(config.retention_days, limit=config.batch_size)
    except Exception as exc:
        result["reason"] = "error"
        result["error_type"] = type(exc).__name__
        LOG.error("upload_error type=%s", type(exc).__name__)
    finally:
        if md is not None:
            try:
                md.close()
            except Exception as exc:
                result["reason"] = "error"
                LOG.error("upload_close_error type=%s", type(exc).__name__)
        try:
            after = spool.status(include_totals=False)
            result["pending_after"] = after["messages"]["pending"]
            result["oldest_pending_after"] = after["messages"]["oldest_pending"]
            result["oldest_pending_age_seconds"] = oldest_age_seconds(after)
            result["latest_local_after"] = after["messages"]["latest"]
        except Exception as exc:
            result["reason"] = "error"
            LOG.error("upload_status_error type=%s", type(exc).__name__)
        elapsed = max(0.0, clock() - start)
        result["elapsed_seconds"] = round(elapsed, 3)
        result["messages_per_second"] = round(result["messages"] / elapsed, 3) if elapsed else 0
        LOG.info("upload_summary %s", json.dumps(result, sort_keys=True))
    return result


def main() -> None:
    logging.basicConfig(
        level=os.environ.get("LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        config = UploadConfig.from_env()
        env_required("MOTHERDUCK_TOKEN")
        path = Path(os.environ.get("SPOOL_DB", "/var/lib/loxone-bronze/spool.sqlite3"))
        # systemd serializes the oneshot; flock also excludes manual invocations.
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.with_suffix(path.suffix + ".uploader.lock").open("a") as lock:
            try:
                fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                LOG.info("upload_skipped reason=already_running")
                return
            spool = Spool(str(path))
            database = os.environ.get("MOTHERDUCK_DATABASE", "my_db").strip()
            result = run_upload(spool, config, lambda: duckdb.connect(f"md:{database}"))
        raise SystemExit(1 if result["reason"] == "error" else 0)
    except Exception as exc:
        LOG.error("upload_startup_error type=%s", type(exc).__name__)
        raise SystemExit(1) from None


if __name__ == "__main__":
    main()
