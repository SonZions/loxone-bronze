import os
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

from loxone_bronze.health import health_problems


class HealthTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 9, 22, 12, 0, tzinfo=timezone.utc)
        self.status = {"messages": {"latest": "2026-09-22T11:55:00+00:00", "pending": 100,
                                    "oldest_pending": "2026-09-22T11:40:00+00:00"}}
        self.env = patch.dict(os.environ, {}, clear=True)
        self.env.start()
        self.addCleanup(self.env.stop)

    def problems(self):
        return health_problems(self.status, self.now)

    def test_healthy_defaults(self):
        self.assertEqual(self.problems(), [])

    def test_configurable_collector_age(self):
        os.environ["HEALTH_MAX_EVENT_AGE_MINUTES"] = "4"
        self.assertIn("collector_stale", self.problems()[0])

    def test_configurable_pending_count(self):
        os.environ["HEALTH_MAX_PENDING_MESSAGES"] = "99"
        self.assertIn("upload_backlog", self.problems()[0])

    def test_configurable_pending_age(self):
        os.environ["HEALTH_MAX_PENDING_AGE_MINUTES"] = "19"
        self.assertIn("oldest pending", self.problems()[0])

    def test_backlog_disabled_does_not_disable_collector_check(self):
        os.environ.update(HEALTH_CHECK_BACKLOG="false", HEALTH_MAX_PENDING_MESSAGES="0",
                          HEALTH_MAX_PENDING_AGE_MINUTES="0", HEALTH_MAX_EVENT_AGE_MINUTES="0")
        self.assertEqual(len(self.problems()), 1)
        self.assertIn("collector_stale", self.problems()[0])

    def test_empty_queue_and_no_events(self):
        self.status["messages"].update(latest=None, pending=0, oldest_pending=None)
        self.assertEqual(len(self.problems()), 1)
        self.assertIn("collector_stale", self.problems()[0])

    def test_threshold_equality_is_healthy(self):
        os.environ.update(HEALTH_MAX_EVENT_AGE_MINUTES="5", HEALTH_MAX_PENDING_MESSAGES="100",
                          HEALTH_MAX_PENDING_AGE_MINUTES="20")
        self.assertEqual(self.problems(), [])

    def test_invalid_boolean_or_negative_threshold_rejected(self):
        for key, value in (("HEALTH_CHECK_BACKLOG", "typo"), ("HEALTH_MAX_PENDING_MESSAGES", "-1")):
            with patch.dict(os.environ, {key: value}):
                with self.assertRaises(ValueError):
                    self.problems()
