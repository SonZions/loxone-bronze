import json,sqlite3,tempfile,unittest
from pathlib import Path
from dataclasses import replace
from unittest.mock import patch
import duckdb
from loxone_bronze.spool import Spool
from loxone_bronze.local_silver import enable_retention_guard
from loxone_bronze.cloud_edge import EdgeConfig,run,open_state,read_chunk,initialize_edge,refresh_derived,remap_history
from loxone_bronze.silver import process_batch,sync_structures
STATE='12345678-1234-1234-1234567890abcdef'
class EdgeTests(unittest.TestCase):
 def setUp(self):
  self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup);self.root=Path(self.tmp.name)
  self.spool=Spool(str(self.root/'spool.db'));enable_retention_guard(str(self.spool.path))
  self.cfg=EdgeConfig(spool=str(self.spool.path),state=str(self.root/'edge.db'),database='target',raw_schema='raw',silver_schema='silver',chunk_size=2,max_chunks=4,min_messages=0)
  self.cloud=str(self.root/'target.duckdb')
 def connection(self):return duckdb.connect(self.cloud)
 def structure(self,sid='s1',at='2026-09-01',name='Old'):
  self.spool.insert_structure(structure_id=sid,source_id='home',captured_at=at,last_modified=None,payload_json=json.dumps({'controls':{'meter':{'name':name,'type':'Meter','states':{'actual':STATE}}},'padding':'x'*1100}),payload_sha256=sid,collector_version='test')
 def message(self,mid='m1',at='2026-09-03',value=3):
  self.spool.insert_message(message_id=mid,source_id='home',run_id='r',received_at=at,message_type=2,payload_format='json',payload_json=json.dumps({'parsed':{STATE:value}}),payload_sha256=mid,collector_version='test')
 def archive(self):
  with self.spool.connect() as c:
   for t in ('structures','ws_messages'):c.execute('UPDATE parquet_archive_checkpoints SET rowid_highwater=(SELECT coalesce(max(rowid),0) FROM '+t+') WHERE table_name=?',[t])
 def connect(self):return duckdb.connect(self.cloud)
 def query(self,sql):
  with self.connect() as c:return c.execute(sql).fetchall()
 def test_archive_gate_byte_bound_and_no_source_ack(self):
  self.structure();self.message();self.message('m2',value=4)
  self.assertEqual(read_chunk(self.cfg,0)[0],[])
  self.archive();result=run(self.cfg,connect=self.connect,initial_cursor=0)
  self.assertEqual(result['messages'],2)
  self.assertEqual(self.query('SELECT count(*) FROM target.silver.state_events'),[(2,)])
  self.assertEqual(self.query('SELECT event_count,numeric_avg FROM target.silver.hourly_state'),[(2,3.5)])
  self.assertEqual(self.query('SELECT count(*) FROM target.raw.ws_messages'),[(0,)])
  with self.spool.connect() as c:self.assertEqual(c.execute("SELECT rowid_highwater FROM local_silver_checkpoints WHERE table_name='ws_messages'").fetchone()[0],0)
 def test_lost_local_ack_recovers_cloud_commit_without_duplicates(self):
  self.structure();self.message();self.archive();run(self.cfg,connect=self.connect,initial_cursor=0)
  with sqlite3.connect(self.cfg.state) as c:c.execute('UPDATE progress SET cursor=0')
  result=run(self.cfg,connect=self.connect)
  self.assertEqual(result['recovered_acks'],1)
  self.assertEqual(self.query('SELECT count(*) FROM target.silver.state_events'),[(1,)])
 def test_late_upload_keeps_latest_and_recomputes_hour(self):
  self.structure();self.message('new','2026-09-03T00:30:00+00:00',9);self.archive();run(self.cfg,connect=self.connect,initial_cursor=0)
  self.message('old','2026-09-03T00:10:00+00:00',1);self.archive();run(self.cfg,connect=self.connect)
  self.assertEqual(self.query('SELECT value_numeric FROM target.silver.current_state'),[(9.0,)])
  self.assertEqual(self.query('SELECT event_count,numeric_first,numeric_last FROM target.silver.hourly_state'),[(2,1.0,9.0)])
 def test_late_structure_replays_acknowledged_history_without_raw_staging(self):
  self.structure();self.message();self.message('m2','2026-09-05',5);self.archive();run(self.cfg,connect=self.connect,initial_cursor=0)
  self.structure('s2','2026-09-04','New');self.archive();run(self.cfg,connect=self.connect)
  self.assertEqual(self.query('SELECT message_id,control_name,value_numeric FROM target.silver.state_events ORDER BY message_id'),[('m1','Old',3.0),('m2','New',5.0)])
  self.assertEqual(self.query('SELECT control_name FROM target.silver.current_state'),[('New',)])
 def test_derived_failure_rolls_back_events_and_ack(self):
  self.structure();self.message();self.archive()
  def fail(c,sc):raise RuntimeError('simulated derived failure')
  with patch('loxone_bronze.cloud_edge.refresh_derived',side_effect=fail):
   with self.assertRaises(RuntimeError):run(self.cfg,connect=self.connect,initial_cursor=0)
  self.assertEqual(self.query('SELECT count(*) FROM target.silver.state_events'),[(0,)])
  self.assertEqual(self.query('SELECT count(*) FROM target.raw.chunks'),[(0,)])
  run(self.cfg,connect=self.connect)
  self.assertEqual(self.query('SELECT count(*) FROM target.silver.state_events'),[(1,)])
 def test_other_pipeline_cannot_reuse_staging(self):
  self.structure();self.message();self.archive();run(self.cfg,connect=self.connect,initial_cursor=0)
  with sqlite3.connect(self.cfg.state) as c:c.execute("UPDATE progress SET pipeline_id='other'")
  self.message('next');self.archive()
  with self.assertRaises(RuntimeError):run(self.cfg,connect=self.connect)
 def test_explicit_initial_cursor_and_bounds(self):
  with self.assertRaises(RuntimeError):open_state(self.root/'missing.db')
  with self.assertRaises(ValueError):replace(self.cfg,chunk_size=5001).validate()
  self.message();self.archive()
  with self.assertRaises(RuntimeError):read_chunk(replace(self.cfg,chunk_bytes=1),0)
 def test_large_snapshot_and_exact_equivalence_to_local_outputs(self):
  from loxone_bronze.local_silver import LocalConfig,run as local_run
  self.structure()
  values={f'unknown-{i}':i for i in range(1850)};values[STATE]=7
  self.spool.insert_message(message_id='snapshot',source_id='home',run_id='r',received_at='2026-09-03',message_type=2,payload_format='json',payload_json=json.dumps({'parsed':values}),payload_sha256='snapshot',collector_version='test')
  self.message('later','2026-09-03T00:30:00+00:00',9);self.archive()
  run(self.cfg,connect=self.connect,initial_cursor=0)
  local=LocalConfig(spool=str(self.spool.path),database=str(self.root/'local.duckdb'),outbox=str(self.root/'outbox'),bronze_archive=str(self.root/'bronze'),batch_size=2,import_size=2,max_batches=4,min_free_mb=0)
  local_run(local)
  with duckdb.connect(local.database) as c:
   cols=[r[0] for r in c.execute('DESCRIBE loxone_silver.state_events').fetchall() if r[0] not in ('loaded_at','bronze_ingested_at')]
   projection=','.join(cols)
   expected=c.execute('SELECT '+projection+' FROM loxone_silver.state_events ORDER BY source_id,message_id,state_uuid').fetchall()
   self.assertEqual(self.query('SELECT '+projection+' FROM target.silver.state_events ORDER BY source_id,message_id,state_uuid'),expected)
   self.assertEqual(self.query('SELECT count(*) FROM target.silver.current_state'),[(1851,)])
 def test_unsupported_record_is_raw_acknowledged_without_silver_event(self):
  self.structure();self.spool.insert_message(message_id='unknown',source_id='home',run_id='r',received_at='2026-09-03',message_type=7,payload_format='json',payload_json='{"parsed":null}',payload_sha256='h',collector_version='test');self.archive()
  result=run(self.cfg,connect=self.connect,initial_cursor=0)
  self.assertEqual((result['messages'],result['events'],result['cursor']),(1,0,1))
 def test_remap_and_replay_preserve_text_nested_json(self):
  self.structure();self.message(value={'nested':[1,'z',True]});self.archive();run(self.cfg,connect=self.connect,initial_cursor=0)
  before=self.query('SELECT value_json FROM target.silver.state_events')
  self.structure('s2','2026-09-02','New');self.archive();run(self.cfg,connect=self.connect)
  self.assertEqual(self.query('SELECT value_json FROM target.silver.state_events'),before)
  self.assertEqual(self.query('SELECT control_name FROM target.silver.state_events'),[('New',)])

 def test_idle_avoids_cloud_connection_and_destination_change_fails(self):
  self.structure();self.message();self.archive();run(self.cfg,connect=self.connect,initial_cursor=0)
  with patch.object(self,'connect',side_effect=AssertionError('Idle must not connect')):
   result=run(self.cfg,connect=self.connect)
  self.assertEqual(result['reason'],'waiting_for_batch')
  with self.assertRaises(RuntimeError):run(replace(self.cfg,silver_schema='other'),connect=self.connect)
 def test_crash_after_event_commit_before_chunk_ack_replays_safely(self):
  import loxone_bronze.cloud_edge as module
  self.structure();self.message();self.archive();original=module.process_batch;calls=0
  def interrupt(*a,**kw):
   nonlocal calls
   calls+=1
   if calls==2:raise RuntimeError('simulated lost chunk acknowledgement')
   return original(*a,**kw)
  with patch.object(module,'process_batch',side_effect=interrupt):
   with self.assertRaises(RuntimeError):run(self.cfg,connect=self.connect,initial_cursor=0)
  self.assertEqual(self.query('SELECT count(*) FROM target.silver.state_events'),[(1,)])
  self.assertEqual(self.query('SELECT count(*) FROM target.raw.chunks'),[(0,)])
  run(self.cfg,connect=self.connect)
  self.assertEqual(self.query('SELECT count(*),sum(events) FROM target.silver.published_batches'),[(1,1)])
  self.assertEqual(self.query('SELECT count(*) FROM target.silver.state_events'),[(1,)])

 def test_late_structure_does_not_erase_mapping_of_another_source(self):
  self.structure();self.message();self.archive();run(self.cfg,connect=self.connect,initial_cursor=0)
  with self.connect() as c:
   c.execute("INSERT INTO target.silver.state_events SELECT * REPLACE('other' AS source_id,'historic' AS structure_id) FROM target.silver.state_events")
   c.execute("INSERT OR REPLACE INTO target.raw.control VALUES('remap_pending','1')")
   self.assertEqual(remap_history(c,self.cfg),0)
  self.assertEqual(self.query("SELECT structure_id FROM target.silver.state_events WHERE source_id='other'"),[('historic',)])

 def test_error_backoff_never_opens_cloud(self):
  import time
  self.structure();self.message();self.archive()
  with open_state(self.cfg.state,initial_cursor=0) as c:c.execute("INSERT INTO metadata VALUES('retry_after',?)",[str(time.time()+300)])
  with patch.object(self,'connect',side_effect=AssertionError('Backoff must not connect')):
   self.assertEqual(run(self.cfg,connect=self.connect)['reason'],'error_backoff')
