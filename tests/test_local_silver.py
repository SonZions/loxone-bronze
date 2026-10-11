import json
from datetime import datetime,timedelta,timezone
from pathlib import Path
import runpy
import shutil
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import duckdb
from loxone_bronze.spool import Spool
from loxone_bronze.local_silver import (LocalConfig,enable_retention_guard,connect_local,
    archive_spool,import_spool,run,finalize_outbox,process_local_batch,enqueue_remapping,
    prune_local_archived,prune_working_archive,sha256,lock)
from loxone_bronze.silver import sync_structures
from loxone_bronze.silver_publish import publish_batch,validate_batch,run as publish_run
from loxone_bronze.local_mapping import sync_local_structure
from loxone_bronze.bronze_archive import ArchiveConfig,run as archive_run

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

    def test_hourly_refresh_includes_prior_batch_with_out_of_order_events(self):
        self.structure()
        self.cfg.batch_size=1;self.cfg.max_batches=1
        self.message('m1',3,'2026-09-01T00:20:00+00:00')
        self.message('m2',1,'2026-09-01T00:10:00+00:00')
        self.message('m3',9,'2026-09-01T01:00:00+00:00')
        run(self.cfg);run(self.cfg)
        directories=sorted(Path(self.cfg.outbox).glob('*/manifest.json'))
        c=duckdb.connect()
        try:
            row=c.execute('SELECT event_count,numeric_min,numeric_max,numeric_avg,numeric_first,numeric_last FROM read_parquet(?)',
                [str(directories[-1].parent/'hourly_state.parquet')]).fetchone()
            self.assertEqual(row,(2,1,3,2,1,3))
        finally:c.close()

    def test_queue_processes_oldest_receipt_and_acknowledges_same_slice(self):
        self.structure();self.cfg.batch_size=1;self.cfg.max_batches=1
        self.message('a-new',3,'2026-09-02T00:00:00+00:00')
        self.message('z-old',1,'2026-09-01T00:00:00+00:00')
        run(self.cfg)
        with duckdb.connect(self.cfg.database) as c:
            self.assertEqual(c.execute('SELECT message_id FROM process_queue').fetchall(),[('a-new',)])
            self.assertEqual(c.execute('SELECT message_id FROM loxone_silver.processed_messages').fetchall(),[('z-old',)])
        run(self.cfg)
        with duckdb.connect(self.cfg.database) as c:
            self.assertEqual(c.execute('SELECT count(*) FROM process_queue').fetchone()[0],0)
            self.assertEqual(c.execute('SELECT value_numeric FROM latest_events').fetchone()[0],3)

    def test_reopen_failure_after_import_resumes_without_losing_or_duplicating_rows(self):
        self.structure();self.message('m1');self.message('m2',2)
        connections=0
        def reopen(cfg):
            nonlocal connections
            connections+=1
            if connections==2:raise RuntimeError('Simulated reopen failure')
            return connect_local(cfg)
        with patch('loxone_bronze.local_silver.connect_local',side_effect=reopen):
            with self.assertRaisesRegex(RuntimeError,'Simulated reopen failure'):run(self.cfg)
        with duckdb.connect(self.cfg.database) as c:
            self.assertEqual(c.execute('SELECT count(*) FROM process_queue').fetchone()[0],2)
            self.assertEqual(c.execute('SELECT count(*) FROM loxone_silver.state_events').fetchone()[0],0)
        with self.spool.connect() as c:
            self.assertEqual(c.execute("SELECT rowid_highwater FROM local_silver_checkpoints WHERE table_name='ws_messages'").fetchone()[0],2)
        run(self.cfg);run(self.cfg)
        with duckdb.connect(self.cfg.database) as c:
            self.assertEqual(c.execute('SELECT count(*) FROM loxone_bronze.archive_messages').fetchone()[0],2)
            self.assertEqual(c.execute('SELECT count(*) FROM loxone_silver.state_events').fetchone()[0],2)
            self.assertEqual(c.execute('SELECT count(*) FROM process_queue').fetchone()[0],0)

    def test_bounded_ledger_replays_late_structure_and_ignores_other_source(self):
        self.structure();self.message('m1',7,'2026-09-03T00:00:00+00:00')
        run(self.cfg)
        with duckdb.connect(self.cfg.database) as c:
            c.execute("INSERT INTO loxone_silver.processed_messages(source_id,message_id,structure_id) VALUES ('other','m2','s1')")
        self.structure('s2','2026-09-02','Updated meter')
        self.message('m2',9,'2026-09-03T00:10:00+00:00')
        run(self.cfg);run(self.cfg)
        with duckdb.connect(self.cfg.database) as c:
            self.assertEqual(c.execute('SELECT message_id,control_name,value_numeric FROM loxone_silver.state_events ORDER BY message_id').fetchall(),
                [('m1','Updated meter',7),('m2','Updated meter',9)])
            self.assertEqual(c.execute("SELECT message_id,structure_id FROM loxone_silver.processed_messages WHERE source_id='home' ORDER BY message_id").fetchall(),
                [('m1','s2'),('m2','s2')])
            self.assertEqual(c.execute('SELECT count(*) FROM process_queue').fetchone()[0],0)

    def test_index_retirement_preserves_rows_cursors_and_replay(self):
        self.structure();self.message('m1',7)
        c,sc=connect_local(self.cfg)
        try:
            c.execute('ALTER TABLE loxone_bronze.archive_messages ADD PRIMARY KEY(source_id,message_id)')
            import_spool(c,self.cfg,'structures');import_spool(c,self.cfg,'ws_messages')
            before=c.execute('SELECT * FROM loxone_bronze.archive_messages ORDER BY ALL').fetchall()
            cursors=c.execute('SELECT * FROM import_progress ORDER BY table_name').fetchall()
        finally:c.close()
        script=runpy.run_path(str(Path(__file__).resolve().parents[1]/'scripts/retire-local-archive-index.py'))
        script['retire'](Path(self.cfg.database),1)
        c,sc=connect_local(self.cfg)
        try:
            self.assertEqual(c.execute("SELECT count(*) FROM duckdb_constraints() WHERE schema_name='loxone_bronze' AND table_name='archive_messages' AND constraint_type='PRIMARY KEY'").fetchone()[0],0)
            self.assertEqual(c.execute('SELECT * FROM loxone_bronze.archive_messages ORDER BY ALL').fetchall(),before)
            self.assertEqual(c.execute('SELECT * FROM loxone_bronze.archive_messages_before_index_retirement ORDER BY ALL').fetchall(),before)
            self.assertEqual(c.execute('SELECT * FROM import_progress ORDER BY table_name').fetchall(),cursors)
            c.execute("UPDATE import_progress SET rowid_highwater=0 WHERE table_name='ws_messages'")
            with self.spool.connect() as src:src.execute("UPDATE local_silver_checkpoints SET rowid_highwater=0 WHERE table_name='ws_messages'")
            import_spool(c,self.cfg,'ws_messages')
            self.assertEqual(c.execute('SELECT count(*) FROM loxone_bronze.archive_messages').fetchone()[0],1)
        finally:c.close()
        run(self.cfg)
        with duckdb.connect(self.cfg.database) as c:
            self.assertEqual(c.execute('SELECT value_numeric FROM loxone_silver.state_events').fetchall(),[(7,)])

    def test_failed_commit_preserves_cause_and_does_not_acknowledge_spool(self):
        self.message()
        c,sc=connect_local(self.cfg)
        class FailedCommit:
            def execute(self,sql,*args):
                if sql=='COMMIT':
                    c.execute('ROLLBACK')
                    raise duckdb.TransactionException('Original commit failure')
                return c.execute(sql,*args)
        try:
            with self.assertRaisesRegex(duckdb.TransactionException,'Original commit failure'):
                import_spool(FailedCommit(),self.cfg,'ws_messages')
            self.assertEqual(c.execute('SELECT count(*) FROM loxone_bronze.archive_messages').fetchone()[0],0)
            self.assertEqual(c.execute('SELECT count(*) FROM import_progress').fetchone()[0],0)
            with self.spool.connect() as src:
                self.assertEqual(src.execute("SELECT rowid_highwater FROM local_silver_checkpoints WHERE table_name='ws_messages'").fetchone()[0],0)
        finally:c.close()
    def test_import_skips_message_already_restored_from_cloud_history(self):
        self.message('old',value=7)
        self.message('new',value=8)
        c,sc=connect_local(self.cfg)
        try:
            c.execute('''INSERT INTO loxone_bronze.archive_messages VALUES
              ('old','home','2026-09-01T00:00:00+00:00',2,
               '2026-09-01T00:00:00+00:00','{"parsed":{}}')''')
            self.assertEqual(import_spool(c,self.cfg,'ws_messages'),2)
            self.assertEqual(c.execute('SELECT count(*) FROM loxone_bronze.archive_messages').fetchone()[0],2)
            self.assertEqual(c.execute("SELECT payload_json FROM loxone_bronze.archive_messages WHERE message_id='old'").fetchone()[0],'{"parsed":{}}')
            self.assertEqual(c.execute('SELECT message_id FROM process_queue').fetchall(),[('new',)])
            self.assertEqual(c.execute("SELECT rowid_highwater FROM import_progress WHERE table_name='ws_messages'").fetchone()[0],2)
        finally:c.close()
    def test_publisher_can_wait_for_active_local_worker_lock(self):
        acquired=[]
        path=self.root/'pipeline.lock'
        def publisher():
            with lock(path,wait_seconds=2): acquired.append(True)
        with lock(path):
            thread=threading.Thread(target=publisher)
            thread.start()
            time.sleep(0.1)
            self.assertEqual(acquired,[])
        thread.join(timeout=2)
        self.assertFalse(thread.is_alive())
        self.assertEqual(acquired,[True])
    def test_archive_index_rebuild_keeps_original_and_repairs_view(self):
        c,sc=connect_local(self.cfg)
        try:
            for name in ('run_id','payload_format','payload_sha256','collector_version'):
                c.execute(f'ALTER TABLE loxone_bronze.archive_messages ADD COLUMN {name} VARCHAR')
            c.execute("INSERT INTO loxone_bronze.archive_messages(message_id,source_id,payload_json) VALUES ('old','home','{}')")
        finally:c.close()
        backup=self.root/'before-repair.duckdb'
        shutil.copy2(self.cfg.database,backup)
        script=runpy.run_path(str(Path(__file__).resolve().parents[1]/'scripts/repair-local-archive-index.py'))
        script['rebuild'](Path(self.cfg.database),backup,1,'home','blocked')
        with duckdb.connect(self.cfg.database) as c:
            self.assertEqual(c.execute('SELECT message_id FROM loxone_bronze.archive_messages').fetchall(),[('old',)])
            self.assertEqual(c.execute('SELECT message_id FROM loxone_bronze.archive_messages_before_pk_repair').fetchall(),[('old',)])
            c.execute("INSERT INTO process_queue VALUES ('home','old')")
            self.assertEqual(c.execute('SELECT message_id FROM loxone_bronze.ws_messages').fetchall(),[('old',)])
            c.execute("INSERT INTO loxone_bronze.archive_messages(message_id,source_id,payload_json) VALUES ('blocked','home','{}')")
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
        now=datetime.now(timezone.utc)
        self.message(at=(now-timedelta(days=1)).isoformat());run(self.cfg)
        self.structure(at=(now-timedelta(days=3)).isoformat());run(self.cfg)
        self.structure('s2',(now-timedelta(days=2)).isoformat(),'New meter');run(self.cfg)
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

    def test_reconnect_reuses_archive_views_and_sees_new_partitions(self):
        self.structure();self.message()
        archive_run(ArchiveConfig(self.cfg.spool,self.cfg.bronze_archive,min_free_mb=0))
        c,_=connect_local(self.cfg);c.close()
        self.message('m2',at='2026-09-02T00:00:00+00:00')
        archive_run(ArchiveConfig(self.cfg.spool,self.cfg.bronze_archive,min_free_mb=0))
        with patch.object(Path,'glob',side_effect=AssertionError('Worker startup enumerated the archive')):
            c,_=connect_local(self.cfg)
        try:
            self.assertEqual(c.execute('SELECT count(*) FROM loxone_bronze.parquet_ws_messages').fetchone()[0],2)
            self.assertEqual(c.execute('SELECT count(*) FROM loxone_bronze.parquet_structures').fetchone()[0],1)
        finally:c.close()

    def test_working_bronze_is_pruned_only_after_processing_and_parquet(self):
        self.cfg.working_retention_days=1
        self.message(at='2020-01-01T00:00:00+00:00')
        archive_run(ArchiveConfig(self.cfg.spool,self.cfg.bronze_archive,min_free_mb=0))
        run(self.cfg)
        with duckdb.connect(self.cfg.database) as c:
            self.assertEqual(c.execute('SELECT count(*) FROM loxone_bronze.archive_messages').fetchone()[0],0)
            self.assertEqual(c.execute('SELECT count(*) FROM loxone_bronze.parquet_ws_messages').fetchone()[0],1)

if __name__=='__main__':unittest.main()
