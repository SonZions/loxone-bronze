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


if __name__ == "__main__":
    unittest.main()
