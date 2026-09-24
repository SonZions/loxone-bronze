import json
import unittest
import tempfile
from pathlib import Path
from dataclasses import replace
from unittest.mock import patch

import duckdb

from loxone_bronze.silver import SilverConfig, initialize, process_batch, sync_structures, preflight, refresh, begin, rollback

UUID = '12345678-1234-1234-1234567890abcdef'
ACTION = '87654321-1234-1234-1234567890abcdef'
ROOM = '11111111-1234-1234-1234567890abcdef'


class SilverTests(unittest.TestCase):
    def setUp(self):
        self.con = duckdb.connect()
        self.con.execute("ATTACH ':memory:' AS bronze")
        self.con.execute("ATTACH ':memory:' AS target")
        self.cfg = SilverConfig(source_database='bronze', target_database='target',batch_size=2)
        self.con.execute('CREATE SCHEMA bronze.loxone_bronze')
        self.con.execute('''CREATE TABLE bronze.loxone_bronze.ws_messages (
            message_id VARCHAR,source_id VARCHAR,received_at TIMESTAMPTZ,
            message_type INTEGER,ingested_at TIMESTAMPTZ,payload_json JSON)''')
        self.con.execute('''CREATE TABLE bronze.loxone_bronze.structures (
            structure_id VARCHAR,source_id VARCHAR,captured_at TIMESTAMPTZ,
            last_modified VARCHAR,payload_json JSON,payload_sha256 VARCHAR,
            collector_version VARCHAR,ingested_at TIMESTAMPTZ)''')
        initialize(self.con,self.cfg)

    def tearDown(self):
        self.con.close()

    def structure(self, sid='s1', at='2026-09-02', name='Meter', state=UUID, conflict=False, source='home'):
        control = {'name':name,'type':'Meter','uuidAction':ACTION,'room':ROOM,
                   'states':{'actual':state},
                   'subControls':{'named-child':{'name':'Child','type':'Switch','states':{'active':ACTION}}}}
        controls = {ACTION:control}
        if conflict:
            controls['other']={'name':'Other','uuidAction':ROOM,'states':{'different':state}}
        data={'controls':controls,'rooms':{ROOM:{'name':'Plant'}},'cats':{},'padding':'x'*1100}
        self.con.execute('INSERT INTO bronze.loxone_bronze.structures VALUES (?,?,?,NULL,?,?,?,?)',
                         [sid,source,at,json.dumps(data),'hash','test',at])

    def message(self, mid='m1', at='2026-09-03', value=1, source='home', ingested=None):
        self.con.execute('INSERT INTO bronze.loxone_bronze.ws_messages VALUES (?,?,?,2,?,?)',
                         [mid,source,at,ingested or at,json.dumps({'parsed':{UUID:value}})])

    def rows(self):
        return self.con.execute('SELECT message_id,value_numeric,control_name,structure_resolution,resolution_status FROM target.loxone_silver.state_events ORDER BY message_id').fetchall()

    def test_preflight_read_only_and_missing_source(self):
        preflight(self.con,self.cfg)
        with self.assertRaises(duckdb.Error):
            preflight(self.con,SilverConfig(source_database='missing',target_database='target'))

    def test_mapping_temporal_fallback_and_idempotency(self):
        self.structure()
        self.message('m1','2026-09-01')
        self.message('m2','2026-09-03',2)
        self.assertEqual(sync_structures(self.con,self.cfg),1)
        self.assertEqual(sync_structures(self.con,self.cfg),0)
        self.assertEqual(process_batch(self.con,self.cfg),{'messages':2,'events':2})
        self.assertEqual(self.rows(),[('m1',1.0,'Meter','fallback_earliest','resolved_state'),
                                     ('m2',2.0,'Meter','asof','resolved_state')])
        self.assertEqual(process_batch(self.con,self.cfg)['messages'],0)
        child=self.con.execute("SELECT room_name FROM target.loxone_silver.state_uuid_map WHERE control_name='Child' AND mapping_source='state'").fetchall()
        self.assertEqual(child,[('Plant',)])

    def test_late_structure_remaps_and_late_upload_is_not_lost(self):
        self.message('m1','2026-09-03')
        self.message('m2','2026-09-05',2)
        process_batch(self.con,self.cfg)
        self.assertEqual(self.rows()[0][-1],'unresolved')
        self.structure()
        sync_structures(self.con,self.cfg)
        self.assertEqual(process_batch(self.con,self.cfg)['messages'],2)
        self.structure('s2','2026-09-04','New meter')
        sync_structures(self.con,self.cfg)
        self.assertEqual(process_batch(self.con,self.cfg)['messages'],1)
        self.assertEqual(self.rows()[1][2],'New meter')
        self.message('m3','2026-09-01',3,ingested='2026-09-06')
        self.assertEqual(process_batch(self.con,self.cfg)['messages'],1)
        self.assertEqual(self.rows()[2][3],'fallback_earliest')

    def test_conflicts_text_json_invalid_structure_and_other_source(self):
        self.structure(conflict=True)
        self.con.execute("INSERT INTO bronze.loxone_bronze.structures VALUES ('bad','home','2026-09-04',NULL,'{}','h','t','2026-09-04')")
        self.message(value='ü \\" \\n')
        self.message('m2',value={'nested':[1,2]},source='other')
        self.assertEqual(sync_structures(self.con,self.cfg),1)
        process_batch(self.con,self.cfg)
        self.assertEqual(self.rows()[0][-1],'ambiguous')
        self.assertEqual(self.rows()[1][-1],'unresolved')
        rows=self.con.execute('SELECT value_text,value_json FROM target.loxone_silver.state_events ORDER BY message_id').fetchall()
        self.assertEqual(rows[0][0],'ü \\" \\n')
        self.assertEqual(json.loads(rows[1][1]),{'nested':[1,2]})

    def test_failed_transaction_preserves_events_and_ack_then_retry(self):
        self.structure()
        self.message()
        sync_structures(self.con,self.cfg)
        process_batch(self.con,self.cfg)
        self.structure('s2','2026-09-01','Earlier')
        sync_structures(self.con,self.cfg)
        # Force a replay of the existing message to exercise DELETE rollback.
        self.con.execute('DELETE FROM target.loxone_silver.processed_messages')
        original=SilverConfig.sql
        def broken(cfg,name):
            sql=original(cfg,name)
            if name=='batch.sql':
                sql=sql.replace('INSERT INTO "target"."loxone_silver".state_events',
                                'INSERT INTO "target"."loxone_silver".missing_events')
            return sql
        before=self.rows()
        with patch.object(SilverConfig,'sql',broken):
            with self.assertRaises(duckdb.Error):
                process_batch(self.con,self.cfg)
        self.assertEqual(self.rows(),before)
        self.assertEqual(self.con.execute('SELECT count(*) FROM target.loxone_silver.processed_messages').fetchone()[0],0)
        self.assertEqual(process_batch(self.con,self.cfg)['messages'],1)
        self.assertEqual(len(self.rows()),1)

    def test_run_counters_and_resume(self):
        self.cfg = replace(self.cfg, max_batches=2)
        self.structure()
        for i in range(5):
            self.message('m'+str(i))
        refresh(self.con,self.cfg)
        first=self.con.execute("SELECT status,messages_processed,events_inserted,structures_loaded,mappings_loaded FROM target.loxone_silver.load_runs").fetchone()
        self.assertEqual(first[:4],('partial',4,4,1))
        self.assertGreater(first[4],0)
        refresh(self.con,self.cfg)
        last=self.con.execute("SELECT status,messages_processed,events_inserted FROM target.loxone_silver.load_runs ORDER BY started_at DESC LIMIT 1").fetchone()
        self.assertEqual(last,('succeeded',1,1))
        self.assertEqual(len(self.rows()),5)

    def test_guard_conflicts_between_independent_clients(self):
        with tempfile.TemporaryDirectory() as directory:
            database=str(Path(directory)/'cloud_target.duckdb')
            a=duckdb.connect(database)
            b=duckdb.connect(database)
            cfg=SilverConfig(target_database='cloud_target')
            a.execute('CREATE SCHEMA cloud_target.loxone_silver')
            a.execute('CREATE TABLE cloud_target.loxone_silver.refresh_guard(id INTEGER PRIMARY KEY,revision BIGINT)')
            a.execute('INSERT INTO cloud_target.loxone_silver.refresh_guard VALUES (1,0)')
            try:
                begin(a,cfg)
                with self.assertRaises(duckdb.TransactionException):
                    begin(b,cfg)
                rollback(b)
                a.execute('COMMIT')
                begin(b,cfg)
                b.execute('COMMIT')
            finally:
                a.close()
                b.close()

    def test_bounds_and_duplicate_existing_events_repaired(self):
        for i in range(5):
            self.message('m'+str(i))
        self.assertEqual(process_batch(self.con,self.cfg)['messages'],2)
        self.assertEqual(process_batch(self.con,self.cfg)['messages'],2)
        self.assertEqual(process_batch(self.con,self.cfg)['messages'],1)
        self.assertEqual(process_batch(self.con,self.cfg)['messages'],0)
        self.con.execute('INSERT INTO target.loxone_silver.state_events SELECT * FROM target.loxone_silver.state_events')
        self.con.execute('DELETE FROM target.loxone_silver.processed_messages')
        for _ in range(3):
            process_batch(self.con,self.cfg)
        self.assertEqual(len(self.rows()),5)
        with self.assertRaises(ValueError):
            SilverConfig(target_database='oops;DROP')
        with self.assertRaises(ValueError):
            SilverConfig(batch_size=0)


if __name__=='__main__':
    unittest.main()
