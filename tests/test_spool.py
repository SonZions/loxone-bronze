from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from loxone_bronze.spool import Spool


class SpoolTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.spool = Spool(str(Path(self.tmp.name) / "spool.sqlite3"))

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def insert_message(self, message_id: str) -> None:
        self.spool.insert_message(
            message_id=message_id,
            source_id="test-source",
            run_id="test-run",
            received_at="2026-01-01T00:00:00+00:00",
            message_type=2,
            payload_format="test",
            payload_json='{"value": 1}',
            payload_sha256="sha256",
            collector_version="test",
        )

    def test_message_is_idempotent_and_marked_uploaded(self) -> None:
        self.insert_message("message-1")
        self.insert_message("message-1")

        pending = self.spool.pending_messages(10)
        self.assertEqual([row["message_id"] for row in pending], ["message-1"])

        self.spool.mark_messages_uploaded(
            ["message-1"], "2026-01-01T00:01:00+00:00"
        )

        status = self.spool.status()
        self.assertEqual(status["messages"]["total"], 1)
        self.assertEqual(status["messages"]["pending"], 0)

    def test_failure_is_recorded(self) -> None:
        self.insert_message("message-2")
        self.spool.mark_message_failure(["message-2"], "network unavailable")

        with self.spool.connect() as con:
            row = con.execute(
                "SELECT upload_attempts, last_upload_error "
                "FROM ws_messages WHERE message_id = ?",
                ("message-2",),
            ).fetchone()

        self.assertEqual(row["upload_attempts"], 1)
        self.assertEqual(row["last_upload_error"], "network unavailable")

    def test_structure_version_uses_latest_capture(self) -> None:
        self.spool.insert_structure(
            structure_id="structure-1",
            source_id="test-source",
            captured_at="2026-01-01T00:00:00+00:00",
            last_modified="first",
            payload_json='{"controls": {}}',
            payload_sha256="first",
            collector_version="test",
        )
        self.spool.insert_structure(
            structure_id="structure-2",
            source_id="test-source",
            captured_at="2026-01-01T00:01:00+00:00",
            last_modified="second",
            payload_json='{"controls": {}}',
            payload_sha256="second",
            collector_version="test",
        )

        self.assertEqual(
            self.spool.latest_structure_version("test-source"),
            "second",
        )

    def test_mark_uses_one_write_for_more_than_250_ids(self):
        from unittest.mock import patch
        ids = [f"message-{i}" for i in range(501)]
        for item in ids:
            self.insert_message(item)
        with patch.object(self.spool, "_write", wraps=self.spool._write) as write:
            self.spool.mark_messages_uploaded(ids, "2026-09-22T00:00:00+00:00")
        self.assertEqual(write.call_count, 1)
        self.assertEqual(self.spool.status()["messages"]["pending"], 0)

    def test_mark_rolls_back_all_chunks_on_failure(self):
        from unittest.mock import patch
        for i in range(501):
            self.insert_message(str(i))
        with self.spool.connect() as con:
            con.execute("CREATE TRIGGER fail_mark BEFORE UPDATE ON ws_messages "
                        "WHEN NEW.message_id = '300' BEGIN SELECT RAISE(ABORT, 'test failure'); END")
        import sqlite3
        with self.assertRaises(sqlite3.IntegrityError):
            self.spool.mark_messages_uploaded([str(i) for i in range(501)], "2026-09-22T00:00:00+00:00")
        self.assertEqual(self.spool.status()["messages"]["pending"], 501)

    def test_status_queries_use_indexes(self):
        with self.spool.connect() as con:
            for sql, index in (
                ("SELECT COUNT(*) FROM ws_messages WHERE uploaded_at IS NULL", "ix_ws_messages_pending"),
                ("SELECT received_at FROM ws_messages WHERE uploaded_at IS NULL ORDER BY received_at LIMIT 1", "ix_ws_messages_pending"),
                ("SELECT MAX(received_at) FROM ws_messages", "ix_ws_messages_received"),
            ):
                plan = str([tuple(row) for row in con.execute("EXPLAIN QUERY PLAN " + sql)])
                self.assertIn(index, plan)
        status = self.spool.status(include_totals=False)
        self.assertNotIn("total", status["messages"])
        self.assertEqual(status["messages"]["pending"], 0)

    def test_pruning_is_bounded_and_keeps_pending(self):
        for i in range(6):
            self.insert_message(str(i))
        self.spool.mark_messages_uploaded([str(i) for i in range(5)], "2020-01-01T00:00:00+00:00")
        self.spool.prune_uploaded(7, limit=2)
        status = self.spool.status()["messages"]
        self.assertEqual((status["total"], status["pending"]), (4, 1))

    def test_retry_writer_contention(self):
        import sqlite3
        from unittest.mock import patch
        with patch.object(self.spool, "connect", side_effect=[sqlite3.OperationalError("database is locked"), self.spool.connect()]):
            with patch("loxone_bronze.spool.time.sleep") as sleep:
                self.insert_message("retried")
        sleep.assert_called_once()
        self.assertEqual(self.spool.status()["messages"]["pending"], 1)


if __name__ == "__main__":
    unittest.main()
