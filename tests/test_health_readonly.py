import contextlib
import io
import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from loxone_bronze.health import main
from loxone_bronze.spool import Spool


class ReadOnlyHealthTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.path = Path(self.tmp.name) / "spool.sqlite3"
        self.spool = Spool(str(self.path))
        self.spool.insert_message(
            message_id="committed", source_id="test", run_id="test",
            received_at=datetime.now(timezone.utc).isoformat(), message_type=2,
            payload_format="test", payload_json="{}", payload_sha256="test",
            collector_version="test",
        )

    def health(self, path=None):
        stdout, stderr = io.StringIO(), io.StringIO()
        with patch.dict(os.environ, {
            "SPOOL_DB": str(path or self.path), "HEALTH_CHECK_BACKLOG": "false",
            "HEALTH_MAX_EVENT_AGE_MINUTES": "10",
        }):
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                with self.assertRaises(SystemExit) as exit_result:
                    main()
        return exit_result.exception.code, stdout.getvalue(), stderr.getvalue()

    def test_health_succeeds_while_writer_holds_lock_and_index_is_missing(self):
        # Reproduce the pre-migration production database, not just a freshly
        # initialized schema. Keep a real write transaction open throughout.
        writer = sqlite3.connect(self.path)
        self.addCleanup(writer.close)
        writer.execute("DROP INDEX ix_ws_messages_received")
        writer.commit()
        writer.execute("BEGIN IMMEDIATE")
        writer.execute("UPDATE ws_messages SET received_at='2000-01-01T00:00:00+00:00'")
        env = dict(os.environ, SPOOL_DB=str(self.path), HEALTH_CHECK_BACKLOG="false",
                   HEALTH_MAX_EVENT_AGE_MINUTES="10")
        result = subprocess.run(
            [sys.executable, "-m", "loxone_bronze.health"], env=env,
            capture_output=True, text=True, timeout=3,
        )
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotEqual(json.loads(result.stdout)["messages"]["latest"],
                            "2000-01-01T00:00:00+00:00")
        self.assertTrue(writer.in_transaction)
        self.assertIsNone(writer.execute(
            "SELECT name FROM sqlite_master WHERE name='ix_ws_messages_received'"
        ).fetchone())
        writer.commit()
        self.assertEqual(self.health()[0], 1)  # next read observes the committed WAL

    def test_health_emits_no_schema_writes_or_pragmas(self):
        statements = []
        original_connect = sqlite3.connect
        def connect(*args, **kwargs):
            con = original_connect(*args, **kwargs)
            con.set_trace_callback(statements.append)
            return con
        with patch("loxone_bronze.spool.sqlite3.connect", side_effect=connect):
            self.assertEqual(self.health()[0], 0)
        self.assertTrue(statements)
        for sql in statements:
            self.assertIn(sql.strip().split()[0].upper(), {"SELECT", "BEGIN"}, sql)

    def test_readonly_handle_rejects_actual_writes(self):
        reader = Spool(str(self.path), read_only=True)
        with reader.connect() as con:
            with self.assertRaises(sqlite3.OperationalError):
                con.execute("DELETE FROM ws_messages")
        with self.assertRaises(sqlite3.OperationalError):
            reader.mark_messages_uploaded(["committed"], "2026-01-01T00:00:00+00:00")
        self.assertEqual(reader.status()["messages"]["total"], 1)

    def test_missing_database_does_not_create_directory_or_file(self):
        path = Path(self.tmp.name) / "absent" / "missing.sqlite3"
        code, _, error = self.health(path)
        self.assertEqual(code, 1)
        self.assertIn("spool_unavailable", error)
        self.assertNotIn("Traceback", error)
        self.assertFalse(path.parent.exists())

    def test_empty_database_is_not_initialized_by_health(self):
        self.path = Path(self.tmp.name) / "empty.sqlite3"
        self.path.touch()
        self.assertEqual(self.health()[0], 1)
        con = sqlite3.connect(self.path)
        try:
            self.assertEqual(con.execute("SELECT count(*) FROM sqlite_master").fetchone()[0], 0)
            self.assertEqual(con.execute("PRAGMA journal_mode").fetchone()[0], "delete")
        finally:
            con.close()

    def test_explicit_migration_creates_index_then_health_only_reads(self):
        with self.spool.connect() as con:
            con.execute("DROP INDEX ix_ws_messages_received")
        Spool(str(self.path))  # explicit deployment migration while writers stopped
        with self.spool.connect() as con:
            self.assertIsNotNone(con.execute(
                "SELECT name FROM sqlite_master WHERE name='ix_ws_messages_received'"
            ).fetchone())
        self.assertEqual(self.health()[0], 0)


class DeployContractTests(unittest.TestCase):
    def test_bridge_version_is_a_side_effect_free_capability_check(self):
        result = subprocess.run(["bash", "scripts/deploy-from-github.sh", "--version"],
                                capture_output=True, text=True, timeout=3)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stdout.strip(), "loxone-bronze-deploy-v2")

    def test_deploy_order_and_workflow_preflight(self):
        script = Path("scripts/deploy-from-github.sh").read_text()
        # Scope to the normal deployment, excluding the rollback function.
        script = script[script.index('echo "Deploying Loxone Bronze commit'):]
        stop = script.index('systemctl stop "$collector"')
        migrate = script.index("from loxone_bronze.spool import Spool; Spool(")
        restart = script.index('systemctl restart "$collector"')
        health = script.index('"$venv/bin/loxone-bronze-health"')
        timer = script.index('systemctl start "$timer"')
        self.assertEqual(sorted([stop, migrate, restart, health, timer]),
                         [stop, migrate, restart, health, timer])
        workflow = Path(".github/workflows/deploy.yml").read_text()
        self.assertLess(workflow.index("--version"), workflow.index("sudo -n"))
        self.assertIn("loxone-bronze-deploy-v2", workflow)
