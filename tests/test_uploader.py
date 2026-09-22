from __future__ import annotations

import os
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb

from loxone_bronze.spool import Spool
from loxone_bronze.uploader import UploadConfig, run_upload, upload_messages, upload_structures


class Remote:
    """Real DuckDB SQL plus fault injection; executemany is forbidden."""
    def __init__(self, con):
        self.con = con
        self.inserts = 0
        self.fail_on = None
        self.paths = []
        self.after_insert = None

    def execute(self, sql, params=None):
        if sql.startswith("INSERT"):
            self.inserts += 1
            self.paths.append(Path(params[0]))
            if self.inserts == self.fail_on:
                raise RuntimeError("driver details must not escape")
        value = self.con.execute(sql, params) if params else self.con.execute(sql)
        if sql.startswith("INSERT") and self.after_insert:
            self.after_insert()
        return value

    def executemany(self, *args):
        raise AssertionError("Remote executemany is forbidden")

    def close(self):
        pass  # keep the real database open for assertions


class UploaderTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.spool = Spool(str(Path(self.tmp.name) / "spool.sqlite3"))
        self.con = duckdb.connect()
        self.con.execute(Path("sql/001_motherduck_bronze.sql").read_text())
        self.remote = Remote(self.con)

    def tearDown(self):
        self.con.close()
        self.tmp.cleanup()

    def add(self, count, payload='{"value": 1}'):
        for i in range(count):
            self.spool.insert_message(
                message_id=f"message-{i:04}", source_id="test", run_id="run",
                received_at=f"2026-09-22T00:00:{i % 60:02}+00:00", message_type=2,
                payload_format="test", payload_json=payload,
                payload_sha256="fixture-hash", collector_version="test",
            )

    def run_upload(self, **kwargs):
        return run_upload(self.spool, UploadConfig(batch_size=2, **kwargs), lambda: self.remote)

    def pending(self):
        return self.spool.status()["messages"]["pending"]

    def test_multiple_batches_then_empty(self):
        self.add(5)
        result = self.run_upload()
        self.assertEqual((result["messages"], result["batches"], result["reason"]), (5, 3, "empty"))
        self.assertEqual(self.pending(), 0)
        self.assertEqual(self.remote.inserts, 3)
        self.assertTrue(all(not path.exists() for path in self.remote.paths))

    def test_empty_queue(self):
        result = self.run_upload()
        self.assertEqual((result["messages"], result["batches"], result["reason"]), (0, 0, "empty"))
        self.assertEqual(self.remote.inserts, 0)

    def test_batch_limit(self):
        self.add(5)
        result = self.run_upload(max_batches_per_run=2)
        self.assertEqual((result["messages"], result["reason"], self.pending()), (4, "batch_limit", 1))

    def test_time_limit_finishes_current_batch(self):
        self.add(5)
        ticks = [0]
        self.remote.after_insert = lambda: ticks.__setitem__(0, 241)
        result = run_upload(self.spool, UploadConfig(batch_size=2), lambda: self.remote, clock=lambda: ticks[0])
        self.assertEqual((result["messages"], result["reason"], self.pending()), (2, "time_limit", 3))

    def test_setup_counts_toward_time_limit(self):
        ticks = [0]
        def connect():
            ticks[0] = 240
            return self.remote
        self.add(1)
        result = run_upload(self.spool, UploadConfig(), connect, clock=lambda: ticks[0])
        self.assertEqual(result["reason"], "time_limit")
        self.assertEqual(self.remote.inserts, 0)

    def test_later_failure_keeps_earlier_batch_marked(self):
        self.add(5)
        self.remote.fail_on = 2
        result = self.run_upload()
        self.assertEqual((result["messages"], result["batches"], result["reason"]), (2, 1, "error"))
        self.assertEqual(self.pending(), 3)
        with self.spool.connect() as con:
            rows = con.execute("SELECT uploaded_at, upload_attempts FROM ws_messages ORDER BY message_id").fetchall()
        self.assertTrue(all(row[0] is not None for row in rows[:2]))
        self.assertEqual([row[1] for row in rows], [0, 0, 1, 1, 0])
        self.assertTrue(all(not path.exists() for path in self.remote.paths))

    def test_first_failure_does_not_mark_any_messages(self):
        self.add(3)
        self.remote.fail_on = 1
        result = self.run_upload()
        self.assertEqual(result["messages"], 0)
        self.assertEqual(self.pending(), 3)

    def test_retry_after_remote_commit_before_local_ack_is_idempotent(self):
        self.add(2)
        with patch.object(self.spool, "mark_messages_uploaded", side_effect=KeyboardInterrupt):
            with self.assertRaises(KeyboardInterrupt):
                upload_messages(self.remote, self.spool, 2)
        self.assertEqual(self.pending(), 2)
        self.assertEqual(upload_messages(self.remote, self.spool, 2), 2)
        self.assertEqual(self.pending(), 0)
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM loxone_bronze.ws_messages").fetchone()[0], 2)
        self.assertTrue(all(not path.exists() for path in self.remote.paths))

    def test_bulk_preserves_json_exactly(self):
        payload = '{\n "text": "Grüße, \\"Welt\\"\\n\\t", "big": 12345678901234567890, "n": null }'
        self.add(3, payload)
        self.assertEqual(upload_messages(self.remote, self.spool, 3), 3)
        values = self.con.execute("SELECT payload_json::VARCHAR FROM loxone_bronze.ws_messages").fetchall()
        self.assertEqual(values, [(payload,)] * 3)
        self.assertEqual(self.remote.inserts, 1)

    def test_invalid_json_rolls_back_whole_batch(self):
        self.add(2)
        with self.spool.connect() as con:
            con.execute("UPDATE ws_messages SET payload_json='invalid' WHERE message_id='message-0001'")
        result = self.run_upload()
        self.assertEqual(result["reason"], "error")
        self.assertEqual(self.pending(), 2)
        self.assertEqual(self.con.execute("SELECT COUNT(*) FROM loxone_bronze.ws_messages").fetchone()[0], 0)

    def test_structures_bulk_preserves_null_and_empty_string(self):
        for i, modified in enumerate((None, "", "version")):
            self.spool.insert_structure(
                structure_id=str(i), source_id="test", captured_at="2026-09-22T00:00:00+00:00",
                last_modified=modified, payload_json=' {"controls": {}} ',
                payload_sha256="fixture-hash", collector_version="test",
            )
        self.assertEqual(upload_structures(self.remote, self.spool, 3), 3)
        self.assertEqual(self.con.execute("SELECT last_modified FROM loxone_bronze.structures ORDER BY structure_id").fetchall(), [(None,), ("",), ("version",)])
        self.assertEqual(self.spool.status()["structures"]["pending"], 0)
        self.assertEqual(self.remote.inserts, 1)

    def test_collector_can_write_during_remote_upload(self):
        self.add(2)
        errors = []
        def collector():
            try:
                self.add(3)
            except Exception as exc:
                errors.append(exc)
        def while_uploading():
            thread = threading.Thread(target=collector)
            thread.start()
            thread.join(timeout=2)
            self.assertFalse(thread.is_alive(), "Collector blocked by upload transaction")
        self.remote.after_insert = while_uploading
        upload_messages(self.remote, self.spool, 2)
        self.assertEqual(errors, [])
        self.assertEqual(self.pending(), 1)

    def test_error_text_not_logged_or_persisted(self):
        self.add(2)
        # Synthetic marker, deliberately not a credential or credential-shaped value.
        marker = "private-driver-detail"
        with patch.object(self.remote, "execute", side_effect=RuntimeError(marker)):
            with self.assertLogs("loxone_bronze.uploader") as captured:
                result = self.run_upload()
        self.assertEqual(result["reason"], "error")
        self.assertNotIn(marker, "\n".join(captured.output))
        with patch.object(self.remote, "execute", side_effect=RuntimeError(marker)):
            with self.assertRaises(RuntimeError):
                upload_messages(self.remote, self.spool, 2)
        with self.spool.connect() as con:
            self.assertEqual(con.execute("SELECT DISTINCT last_upload_error FROM ws_messages").fetchone()[0], "RuntimeError")

    def test_effective_config_and_summary_logged(self):
        self.add(1)
        with self.assertLogs("loxone_bronze.uploader") as captured:
            self.run_upload()
        output = "\n".join(captured.output)
        for field in ('"batch_size": 2', '"pending_before": 1', '"pending_after": 0',
                      '"oldest_pending_age_seconds"', '"latest_local_after"', '"messages_per_second"'):
            self.assertIn(field, output)

    def test_configuration_defaults_and_overrides(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(UploadConfig.from_env(), UploadConfig())
        with patch.dict(os.environ, {"UPLOAD_BATCH_SIZE": "1000", "UPLOAD_MAX_BATCHES_PER_RUN": "4", "UPLOAD_MAX_RUNTIME_SECONDS": "25"}):
            config = UploadConfig.from_env()
            self.assertEqual((config.batch_size, config.max_batches_per_run, config.max_runtime_seconds), (1000, 4, 25))
        for value in ("0", "-1", "invalid"):
            with patch.dict(os.environ, {"UPLOAD_BATCH_SIZE": value}):
                with self.assertRaises(ValueError):
                    UploadConfig.from_env()

    def test_main_excludes_a_concurrent_uploader(self):
        import fcntl
        from loxone_bronze.uploader import main
        lock_path = self.spool.path.with_suffix(".sqlite3.uploader.lock")
        with lock_path.open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with patch.dict(os.environ, {"SPOOL_DB": str(self.spool.path)}):
                with patch("loxone_bronze.uploader.env_required"), patch("logging.basicConfig"):
                    with patch("loxone_bronze.uploader.duckdb.connect") as connect:
                        with self.assertLogs("loxone_bronze.uploader") as captured:
                            main()
            connect.assert_not_called()
            self.assertIn("already_running", "".join(captured.output))

    def test_connection_failure_still_logs_summary(self):
        def fail_connect():
            raise RuntimeError("private-driver-detail")
        with self.assertLogs("loxone_bronze.uploader") as captured:
            result = run_upload(self.spool, UploadConfig(), fail_connect)
        self.assertEqual(result["reason"], "error")
        self.assertEqual(result["pending_after"], 0)
        self.assertIn("upload_summary", "".join(captured.output))
        self.assertNotIn("private-driver-detail", "".join(captured.output))


if __name__ == "__main__":
    unittest.main()
