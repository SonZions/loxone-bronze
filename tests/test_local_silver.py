import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import duckdb
from loxone_bronze.spool import Spool
from loxone_bronze.local_silver import (LocalConfig,enable_retention_guard,connect_local,
    archive_spool,import_spool,run,finalize_outbox,process_local_batch,enqueue_remapping,
    prune_local_archived,prune_working_archive,sha256)
from loxone_bronze.silver import sync_structures
from loxone_bronze.silver_publish import publish_batch,validate_batch,run as publish_run
from loxone_bronze.local_mapping import sync_local_structure

STATE='12345678-1234-1234-1234567890abcdef'

class LocalSilverTests(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name); self.spool=Spool(str(self.root/'spool.sqlite3'))
        self.cfg=LocalConfig(spool=str(self.spool.path),database=str(self.root/'silver.duckdb'),
          outbox=str(self.root/'outbox'),bronze_archive=str(self.root/'bronze'),batch_size=2,
          import_size=3,archive_size=3,max_batches=4,min_free_mb=0)
        enable_retention_guard(self.cfg.spool)
    def message(self,mid='m1',value=1,at='2026-09-01T00:00:00+00:00'):
        self.spool.insert_message(message_id=mid,source_id='home',run_id='r',received_at=at,
          message_type=2,payload_format='json',payload_json=json.dumps({'parsed':{STATE:value}}),
          payload_sha256='h',collector_version='test')
    def structure(self,sid='s1',at='2026-09-01',name='Meter'):
        data={'controls':{'meter':{'name':name,'type':'Meter','states':{'actual':STATE}}},'padding':'x'*1100}
        self.spool.insert_structure(structure_id=sid,source_id='home',captured_at=at,last_modified=None,
          payload_json=json.dumps(data),payload_sha256='h',collector_version='test')
    def test_offline_replay_and_materialized_results(self):
        self.structure();self.message();self.message('m2',3,'2026-09-01T00:20:00+00:00')
        run(self.cfg);run(self.cfg)
        with duckdb.connect(self.cfg.database) as c:
            self.assertEqual(c.execute('SELECT count(*) FROM loxone_silver.state_events').fetchone()[0],2)
            self.assertEqual(c.execute('SELECT value_numeric FROM latest_events').fetchone()[0],3)
        dirs=sorted(Path(self.cfg.outbox).glob('*/manifest.json'))
        self.assertEqual(len(dirs),1)
        remote=duckdb.connect(str(self.root/'cloud.duckdb'))
        directory=dirs[0].parent;m=validate_batch(directory)
        publish_batch(remote,directory,m,'cloud.loxone_silver_local')
        publish_batch(remote,directory,m,'cloud.loxone_silver_local')
        self.assertEqual(remote.execute('SELECT count(*) FROM loxone_silver_local.state_events').fetchone()[0],2)
        self.assertEqual(remote.execute('SELECT numeric_avg FROM loxone_silver_local.hourly_state').fetchone()[0],2)
        remote.close()
    def test_prune_requires_durable_archive_ack_and_keeps_last_row(self):
        self.message();self.message('m2');self.message('m3')
        self.spool.mark_messages_uploaded(['m1','m2','m3'],'2020-01-01T00:00:00+00:00')
        self.spool.prune_uploaded(1)
        self.assertEqual(self.spool.status()['messages']['total'],3)
        c,sc=connect_local(self.cfg);import_spool(c,self.cfg,'ws_messages');c.close()
        self.spool.prune_uploaded(1)
        self.assertEqual(self.spool.status()['messages']['total'],1)
        self.message('m4')
        with self.spool.connect() as src:
            self.assertEqual(src.execute("SELECT rowid FROM ws_messages WHERE message_id='m4'").fetchone()[0],4)
    def test_local_only_prune_requires_archive_ack_and_preserves_cloud_status(self):
        self.message();self.message('m2');self.message('m3')
        self.assertEqual(prune_local_archived(self.cfg)['ws_messages'],0)
        c,sc=connect_local(self.cfg);import_spool(c,self.cfg,'ws_messages')
        self.assertEqual(prune_local_archived(self.cfg)['ws_messages'],0)
        archive_spool(c,self.cfg,'ws_messages');c.close()
        self.assertEqual(prune_local_archived(self.cfg)['ws_messages'],2)
        with self.spool.connect() as src:
            self.assertEqual(src.execute('SELECT count(*) FROM ws_messages').fetchone()[0],1)
            self.assertIsNone(src.execute('SELECT uploaded_at FROM ws_messages').fetchone()[0])
        self.message('m4')
        with self.spool.connect() as src:
            self.assertEqual(src.execute("SELECT rowid FROM ws_messages WHERE message_id='m4'").fetchone()[0],4)
    def test_missing_archive_after_pruning_fails_closed(self):
        self.message();run(self.cfg)
        Path(self.cfg.database).unlink()
        c,sc=connect_local(self.cfg)
        try:
            with self.assertRaises(RuntimeError):import_spool(c,self.cfg,'ws_messages')
        finally:c.close()
    def test_late_structure_replaces_existing_published_mapping(self):
        self.message(at='2026-09-03');run(self.cfg)
        self.structure();run(self.cfg)
        self.structure('s2','2026-09-02','New meter');run(self.cfg)
        remote=duckdb.connect(str(self.root/'cloud.duckdb'))
        for path in sorted(Path(self.cfg.outbox).glob('*/manifest.json')):
            publish_batch(remote,path.parent,validate_batch(path.parent),'cloud.loxone_silver_local')
        self.assertEqual(remote.execute('SELECT control_name FROM loxone_silver_local.state_events').fetchall(),[('New meter',)])
        self.assertEqual(remote.execute('SELECT control_name FROM loxone_silver_local.current_state').fetchall(),[('New meter',)])
        remote.close()
    def test_failed_publication_rolls_back_events_and_ledger(self):
        self.message();run(self.cfg)
        path=next(Path(self.cfg.outbox).glob('*/manifest.json')).parent
        m=validate_batch(path)
        remote=duckdb.connect(str(self.root/'cloud.duckdb'))
        remote.execute('CREATE SCHEMA loxone_silver_local')
        remote.execute('CREATE TABLE loxone_silver_local.hourly_state(wrong_column INT)')
        with self.assertRaises(duckdb.Error):publish_batch(remote,path,m,'cloud.loxone_silver_local')
        self.assertEqual(remote.execute('SELECT count(*) FROM loxone_silver_local.published_batches').fetchone()[0],0)
        self.assertEqual(remote.execute('SELECT count(*) FROM loxone_silver_local.state_events').fetchone()[0],0)
        remote.close()
    def test_grouped_publish_retry_after_partial_ack_does_not_regress(self):
        self.message(value=1);run(self.cfg)
        self.message('m2',5,'2026-09-01T01:00:00+00:00');run(self.cfg)
        cloud=str(self.root/'cloud.duckdb')
        publish_run(self.cfg.outbox,'cloud',connect=lambda:duckdb.connect(cloud))
        paths=sorted(Path(self.cfg.outbox).glob('*/ACK'))
        paths[0].unlink()  # crash after cloud commit and only some local acknowledgements
        publish_run(self.cfg.outbox,'cloud',connect=lambda:duckdb.connect(cloud))
        with duckdb.connect(cloud) as c:
            self.assertEqual(c.execute('SELECT value_numeric FROM loxone_silver_local.current_state').fetchone()[0],5)
            self.assertEqual(c.execute('SELECT count(*) FROM loxone_silver_local.state_events').fetchone()[0],2)
            self.assertEqual(c.execute('SELECT count(*) FROM loxone_silver_local.published_batches').fetchone()[0],2)
    def test_low_memory_mapper_matches_sql(self):
        payload={'controls':{'root':{'name':'Parent','type':'Meter','room':'r','cat':'c',
          'uuidAction':STATE,'states':{'actual':STATE},'details':{'nested':[STATE,'not-a-uuid']},
          'subControls':{'root/AI5':{'name':'Child','states':{'value':STATE}}}}},
          'rooms':{'r':{'name':'Room'}},'cats':{'c':{'name':'Category'}},'padding':'x'*1100}
        self.spool.insert_structure(structure_id='s1',source_id='home',captured_at='2026-09-01',last_modified=None,
          payload_json=json.dumps(payload),payload_sha256='h',collector_version='test')
        c,sc=connect_local(self.cfg)
        try:
            import_spool(c,self.cfg,'structures');sync_structures(c,sc)
            query='SELECT * EXCLUDE(loaded_at) FROM loxone_silver.state_uuid_map ORDER BY ALL'
            expected=c.execute(query).fetchall()
            c.execute('DELETE FROM loxone_silver.state_uuid_map');c.execute('DELETE FROM loxone_silver.structure_versions')
            sync_local_structure(c,sc,enqueue_remapping)
            self.assertEqual(c.execute(query).fetchall(),expected)
        finally:c.close()
    def test_failed_export_rolls_back_and_retry_has_no_duplicate(self):
        self.structure();self.message()
        real_sha256=sha256
        def fail_outbox(path):
            if str(self.root/'outbox') in str(path):raise OSError('simulated export failure')
            return real_sha256(path)
        with patch('loxone_bronze.local_silver.sha256',side_effect=fail_outbox):
            with self.assertRaises(OSError):run(self.cfg)
        with duckdb.connect(self.cfg.database) as c:
            self.assertEqual(c.execute('SELECT count(*) FROM loxone_silver.state_events').fetchone()[0],0)
            self.assertEqual(c.execute('SELECT count(*) FROM process_queue').fetchone()[0],1)
        self.assertFalse(list(Path(self.cfg.outbox).glob('*/manifest.json')))
        run(self.cfg)
        with duckdb.connect(self.cfg.database) as c:
            self.assertEqual(c.execute('SELECT count(*) FROM loxone_silver.state_events').fetchone()[0],1)
    def test_missing_manifest_is_recovered_from_committed_queue(self):
        self.message();run(self.cfg)
        manifest=next(Path(self.cfg.outbox).glob('*/manifest.json'));manifest.unlink()
        run(self.cfg)
        self.assertTrue(manifest.exists())
        validate_batch(manifest.parent)
    def test_corrupt_transport_copy_is_rejected(self):
        self.message();run(self.cfg)
        path=next(Path(self.cfg.outbox).glob('*/manifest.json')).parent
        (path/'state_events.parquet').write_bytes(b'corrupt')
        with self.assertRaises(ValueError):validate_batch(path)

    def test_parquet_archive_is_partitioned_checksummed_and_queryable(self):
        self.structure();self.message();self.message('m2',at='2026-09-02T00:00:00+00:00')
        c,sc=connect_local(self.cfg)
        try:
            self.assertEqual(archive_spool(c,self.cfg,'structures'),1)
            self.assertEqual(archive_spool(c,self.cfg,'ws_messages'),2)
            self.assertEqual(archive_spool(c,self.cfg,'ws_messages'),0)
            self.assertEqual(c.execute('SELECT count(*) FROM loxone_bronze.parquet_ws_messages').fetchone()[0],2)
            self.assertEqual(c.execute('SELECT count(*) FROM loxone_bronze.parquet_structures').fetchone()[0],1)
        finally:c.close()
        manifests=list((self.root/'bronze'/'ws_messages').glob('date=*/*.manifest.json'))
        self.assertEqual(len(manifests),2)
        for path in manifests:
            manifest=json.loads(path.read_text())
            self.assertEqual(sha256(path.parent/manifest['file']),manifest['sha256'])

    def test_parquet_bulk_archive_preserves_raw_metadata(self):
        self.message();c,sc=connect_local(self.cfg)
        try:
            archive_spool(c,self.cfg,'ws_messages')
            row=c.execute('''SELECT run_id,payload_format,payload_sha256,collector_version
              FROM loxone_bronze.parquet_ws_messages''').fetchone()
            self.assertEqual(row,('r','json','h','test'))
        finally:c.close()

    def test_working_bronze_is_pruned_only_after_processing_and_parquet(self):
        self.cfg.working_retention_days=1
        self.message(at='2020-01-01T00:00:00+00:00');run(self.cfg)
        with duckdb.connect(self.cfg.database) as c:
            self.assertEqual(c.execute('SELECT count(*) FROM loxone_bronze.archive_messages').fetchone()[0],0)
            self.assertEqual(c.execute('SELECT count(*) FROM loxone_bronze.parquet_ws_messages').fetchone()[0],1)

if __name__=='__main__':unittest.main()
