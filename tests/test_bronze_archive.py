import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from loxone_bronze.bronze_archive import ArchiveConfig, run
from loxone_bronze.local_silver import enable_retention_guard, sha256
from loxone_bronze.spool import Spool


class BronzeArchiveTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.spool = Spool(str(self.root / 'spool.sqlite3'))
        enable_retention_guard(str(self.spool.path))
        self.cfg = ArchiveConfig(str(self.spool.path), str(self.root / 'bronze'),
                                 archive_size=2, max_batches=3, min_free_mb=0)

    def message(self, message_id):
        self.spool.insert_message(
            message_id=message_id, source_id='home', run_id='r',
            received_at='2026-10-03T00:00:00+00:00', message_type=2,
            payload_format='json', payload_json=json.dumps({'parsed': {'state': 1}}),
            payload_sha256='h', collector_version='test')

    def test_archives_without_opening_silver_or_pruning_spool(self):
        for message_id in ('m1', 'm2', 'm3'):
            self.message(message_id)
        result = run(self.cfg)
        self.assertEqual(result['archived']['ws_messages'], 3)
        self.assertFalse((self.root / 'silver.duckdb').exists())
        with self.spool.connect() as src:
            self.assertEqual(src.execute('SELECT count(*) FROM ws_messages').fetchone()[0], 3)
            self.assertEqual(src.execute("SELECT rowid_highwater FROM local_silver_checkpoints WHERE table_name='ws_messages'").fetchone()[0], 0)
            self.assertEqual(src.execute("SELECT rowid_highwater FROM parquet_archive_checkpoints WHERE table_name='ws_messages'").fetchone()[0], 3)
        manifests = list((self.root / 'bronze').glob('ws_messages/date=*/*.manifest.json'))
        self.assertEqual(len(manifests), 2)
        for path in manifests:
            manifest = json.loads(path.read_text())
            self.assertEqual(sha256(path.parent / manifest['file']), manifest['sha256'])
        self.assertEqual(run(self.cfg)['archived']['ws_messages'], 0)
        self.assertEqual(len(list((self.root / 'bronze').glob('ws_messages/date=*/*.manifest.json'))), 2)

    def test_failed_manifest_does_not_advance_checkpoint(self):
        self.message('m1')
        with patch('loxone_bronze.local_silver._write_manifest', side_effect=OSError('test failure')):
            with self.assertRaises(OSError):
                run(self.cfg)
        with self.spool.connect() as src:
            self.assertEqual(src.execute("SELECT rowid_highwater FROM parquet_archive_checkpoints WHERE table_name='ws_messages'").fetchone()[0], 0)
        self.assertEqual(run(self.cfg)['archived']['ws_messages'], 1)
        with self.spool.connect() as src:
            self.assertEqual(src.execute("SELECT rowid_highwater FROM parquet_archive_checkpoints WHERE table_name='ws_messages'").fetchone()[0], 1)


if __name__ == '__main__':
    unittest.main()
