import importlib.util
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

from loxone_bronze.spool import Spool

spec = importlib.util.spec_from_file_location("upload_report", "scripts/upload-report.py")
upload_report = importlib.util.module_from_spec(spec)
spec.loader.exec_module(upload_report)


class ReportTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.spool = Spool(str(Path(self.tmp.name) / "spool.sqlite3"))
        self.now = datetime(2026, 9, 22, 12, tzinfo=timezone.utc)
        with self.spool.connect() as con:
            for i in range(120):
                con.execute("INSERT INTO ws_messages VALUES (?, 's', 'r', ?, 2, 'test', '{}', 'hash', 'test', ?, 0, NULL)",
                            (str(i), (self.now - timedelta(minutes=30)).isoformat(timespec="microseconds"),
                             (self.now - timedelta(minutes=5)).isoformat(timespec="microseconds") if i < 100 else None))

    def report(self, ingress=0):
        with patch.object(upload_report, "datetime") as clock:
            clock.now.return_value = self.now
            clock.fromisoformat.side_effect = datetime.fromisoformat
            return upload_report.report(str(self.spool.path), 10, ingress)

    def test_eta_includes_pauses_and_measured_arrivals(self):
        result = self.report()
        self.assertEqual(result["pending"], 20)
        self.assertEqual(result["uploaded_in_window"], 100)
        self.assertEqual(result["received_in_window"], 0)
        self.assertEqual(result["oldest_pending_age_hours"], 0.5)
        self.assertEqual(result["wall_clock_upload_messages_per_second"], 0.167)
        self.assertEqual(result["eta_hours"], 0.03)
        self.assertTrue(result["catches_up_within_12h"])

    def test_insufficient_rate_has_no_finite_eta(self):
        result = self.report(323000)
        self.assertIsNone(result["eta_hours"])
        self.assertFalse(result["catches_up_within_12h"])

    def test_empty_backlog_has_zero_eta(self):
        with self.spool.connect() as con:
            con.execute("UPDATE ws_messages SET uploaded_at=?", (self.now.isoformat(timespec="microseconds"),))
        result = self.report(323000)
        self.assertEqual(result["eta_hours"], 0)
        self.assertIsNone(result["oldest_pending_age_hours"])
        self.assertTrue(result["catches_up_within_12h"])
