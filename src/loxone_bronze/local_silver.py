"""Bounded, entirely offline Silver processing; immutable Parquet publication queue.

Only the publisher imports MotherDuck. This worker uses the existing collector
UID to read/acknowledge the spool without exposing its environment or credentials.
"""
from __future__ import annotations
import argparse
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import fcntl
import hashlib
import json
import logging
import os
from pathlib import Path
import shutil
import sqlite3
import time
import tempfile
import uuid

import duckdb
from .silver import SilverConfig, initialize, sync_structures
from .local_mapping import sync_local_structure

LOG = logging.getLogger(__name__)
TABLES = ('structures', 'ws_messages')

@dataclass
class LocalConfig:
    spool: str = '/var/lib/loxone-bronze/spool.sqlite3'
    database: str = '/srv/raspi-data/loxone-silver/silver.duckdb'
    outbox: str = '/srv/raspi-data/loxone-silver/outbox'
    bronze_archive: str = '/srv/raspi-data/loxone-silver/motherduck-bronze'
    batch_size: int = 50
    import_size: int = 250
    archive_size: int = 5000
    max_batches: int = 5
    max_seconds: int = 90
    memory_mb: int = 96
    min_free_mb: int = 2048
    spool_retention_days: int = 7
    working_retention_days: int = 30
    prune_local_only: int = 0

    @classmethod
    def from_env(cls):
        return cls(**{k:type(v)(os.environ.get('LOCAL_SILVER_'+k.upper(),v))
                     for k,v in cls().__dict__.items()})

    def validate(self):
        if not 1 <= self.batch_size <= 1000 or not 1 <= self.import_size <= 10000 or not 1 <= self.archive_size <= 50000:
            raise ValueError('Invalid batch size')
        if not 1 <= self.max_batches <= 1000 or not 1 <= self.max_seconds <= 3600 or not 32 <= self.memory_mb <= 4096:
            raise ValueError('Invalid resource limits')
        if self.database.startswith('md:'):
            raise ValueError('Local database required')
        if self.spool_retention_days < 1 or self.working_retention_days < 1 or self.prune_local_only not in (0, 1):
            raise ValueError('Invalid local retention settings')

@contextmanager
def lock(path, wait_seconds=0):
    with Path(path).open('a') as f:
        deadline=time.monotonic()+wait_seconds
        while True:
            try:
                fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if time.monotonic()>=deadline: raise
                time.sleep(min(0.5,deadline-time.monotonic()))
        yield


def spool_connection(path):
    # Must not create a missing collector database.
    c=sqlite3.connect(Path(path).resolve().as_uri()+'?mode=rw',uri=True,timeout=2)
    c.row_factory=sqlite3.Row
    return c


def enable_retention_guard(path):
    with spool_connection(path) as c:
        c.execute('CREATE TABLE IF NOT EXISTS local_silver_checkpoints(table_name TEXT PRIMARY KEY,rowid_highwater INTEGER NOT NULL)')
        c.execute('CREATE TABLE IF NOT EXISTS parquet_archive_checkpoints(table_name TEXT PRIMARY KEY,rowid_highwater INTEGER NOT NULL)')
        c.executemany('INSERT OR IGNORE INTO local_silver_checkpoints VALUES (?,0)',[(t,) for t in TABLES])
        c.executemany('INSERT OR IGNORE INTO parquet_archive_checkpoints VALUES (?,0)',[(t,) for t in TABLES])


def prune_local_archived(cfg,limit=5000):
    """Bound spool retention by the durable local archive when cloud Bronze is off."""
    cutoff=(datetime.now(timezone.utc)-timedelta(days=cfg.spool_retention_days)).isoformat(timespec='microseconds')
    timestamps={'ws_messages':'received_at','structures':'captured_at'}
    deleted={}
    with spool_connection(cfg.spool) as src:
        for table,timestamp in timestamps.items():
            silver=src.execute('SELECT rowid_highwater FROM local_silver_checkpoints WHERE table_name=?',(table,)).fetchone()
            parquet=src.execute('SELECT rowid_highwater FROM parquet_archive_checkpoints WHERE table_name=?',(table,)).fetchone()
            checkpoint=min(silver[0] if silver else 0,parquet[0] if parquet else 0)
            before=src.total_changes
            # Retain the last row so SQLite rowids stay monotonic across restarts.
            src.execute(f'''DELETE FROM {table} WHERE rowid IN (
              SELECT rowid FROM {table} WHERE {timestamp} < ? AND rowid <= ?
              AND rowid < (SELECT max(rowid) FROM {table}) ORDER BY {timestamp} LIMIT ?)''',
              (cutoff,checkpoint,limit))
            deleted[table]=src.total_changes-before
    return deleted


def connect_local(cfg):
    cfg.validate()
    Path(cfg.database).parent.mkdir(parents=True,exist_ok=True)
    out=Path(cfg.outbox); out.mkdir(parents=True,exist_ok=True)
    if shutil.disk_usage(out).free < cfg.min_free_mb*1024**2:
        raise RuntimeError('Insufficient free space')
    c=duckdb.connect(cfg.database,config={'threads':1,'memory_limit':f'{cfg.memory_mb}MB'})
    c.execute("SET TimeZone='UTC'")
    c.execute("SET preserve_insertion_order=false")
    c.execute("SET max_temp_directory_size='2GB'")
    c.execute('CREATE SCHEMA IF NOT EXISTS loxone_bronze')
    c.execute('''CREATE TABLE IF NOT EXISTS loxone_bronze.archive_messages(
      message_id VARCHAR,source_id VARCHAR,received_at TIMESTAMPTZ,message_type INTEGER,
      ingested_at TIMESTAMPTZ,payload_json JSON,PRIMARY KEY(source_id,message_id))''')
    c.execute('''CREATE TABLE IF NOT EXISTS loxone_bronze.structures(
      structure_id VARCHAR PRIMARY KEY,source_id VARCHAR,captured_at TIMESTAMPTZ,
      last_modified VARCHAR,payload_json JSON,payload_sha256 VARCHAR,
      collector_version VARCHAR,ingested_at TIMESTAMPTZ)''')
    c.execute('CREATE TABLE IF NOT EXISTS import_progress(table_name VARCHAR PRIMARY KEY,rowid_highwater BIGINT)')
    c.execute('CREATE TABLE IF NOT EXISTS process_queue(source_id VARCHAR,message_id VARCHAR,PRIMARY KEY(source_id,message_id))')
    c.execute('CREATE TABLE IF NOT EXISTS local_metadata(key VARCHAR PRIMARY KEY,value VARCHAR)')
    c.execute("INSERT OR IGNORE INTO local_metadata VALUES ('pipeline_id',?)",[str(uuid.uuid4())])
    c.execute('CREATE TABLE IF NOT EXISTS publication_queue(sequence BIGINT PRIMARY KEY,status VARCHAR,manifest VARCHAR)')
    # Only a bounded queue slice participates in event transformation.
    c.execute(f'''CREATE OR REPLACE VIEW loxone_bronze.ws_messages AS SELECT a.*
      FROM loxone_bronze.archive_messages a JOIN
      (SELECT * FROM process_queue ORDER BY source_id,message_id LIMIT {cfg.batch_size}) q
      USING(source_id,message_id)''')
    db=c.execute('SELECT current_database()').fetchone()[0]
    sc=SilverConfig(source_database=db,target_database=db,batch_size=cfg.batch_size)
    initialize(c,sc)
    c.execute('CREATE TABLE IF NOT EXISTS latest_events AS SELECT * FROM loxone_silver.state_events LIMIT 0')
    refresh_archive_views(c,cfg)
    return c,sc


ARCHIVE_COLUMNS={
    'ws_messages':('message_id','source_id','received_at','message_type','ingested_at','payload_json',
                   'run_id','payload_format','payload_sha256','collector_version'),
    'structures':('structure_id','source_id','captured_at','last_modified','payload_json',
                  'payload_sha256','collector_version','ingested_at'),
}


def _archive_types(columns):
    return {name:('TIMESTAMPTZ' if name in ('received_at','captured_at','ingested_at')
                  else 'INTEGER' if name=='message_type' else 'VARCHAR') for name in columns}


def _write_manifest(path,manifest):
    temporary=path.with_suffix(path.suffix+'.tmp')
    with temporary.open('w') as output:
        json.dump(manifest,output,sort_keys=True)
        output.write('\n');output.flush();os.fsync(output.fileno())
    os.replace(temporary,path)
    _fsync_directory(path.parent)


def _fsync_directory(path):
    descriptor=os.open(path,os.O_RDONLY|getattr(os,'O_DIRECTORY',0))
    try:os.fsync(descriptor)
    finally:os.close(descriptor)


def _archive_partition(c,cfg,table,rows,partition):
    root=Path(cfg.bronze_archive)/table
    if table=='ws_messages': root=root/('date='+partition)
    root.mkdir(parents=True,exist_ok=True,mode=0o700)
    first,last=rows[0]['spool_rowid'],rows[-1]['spool_rowid']
    stem=f'local-{first:020d}-{last:020d}'
    final=root/(stem+'.parquet'); temporary=root/(stem+'.parquet.tmp')
    columns=ARCHIVE_COLUMNS[table]
    types=_archive_types(columns)
    schema=','.join("'"+name+"':'"+kind+"'" for name,kind in types.items())
    now=datetime.now(timezone.utc).isoformat();max_record=1024*1024
    with tempfile.TemporaryDirectory(prefix='parquet-archive-') as tmp:
        source=Path(tmp)/'rows.jsonl'
        with source.open('w') as output:
            for row in rows:
                record={name:(now if name=='ingested_at' else row[name]) for name in columns}
                line=json.dumps(record)+'\n';max_record=max(max_record,len(line)+1);output.write(line)
        c.execute(f"CREATE OR REPLACE TEMP TABLE archive_rows AS SELECT * FROM read_json(?,columns={{{schema}}},format='newline_delimited',maximum_object_size={max_record})",[str(source)])
    if temporary.exists(): temporary.unlink()
    c.execute('COPY archive_rows TO ? (FORMAT PARQUET,COMPRESSION ZSTD)',[str(temporary)])
    with temporary.open('rb') as stream: os.fsync(stream.fileno())
    os.replace(temporary,final)
    _fsync_directory(root)
    manifest={'version':1,'table':table,'file':final.name,'sha256':sha256(final),'rows':len(rows),
      'spool_rowid_first':first,'spool_rowid_last':last,
      'minimum_timestamp':min(row['received_at' if table=='ws_messages' else 'captured_at'] for row in rows),
      'maximum_timestamp':max(row['received_at' if table=='ws_messages' else 'captured_at'] for row in rows)}
    _write_manifest(root/(stem+'.manifest.json'),manifest)
    return final


def refresh_archive_views(c,cfg):
    root=str(Path(cfg.bronze_archive)).replace("'","''")
    messages=Path(cfg.bronze_archive)/'ws_messages'
    structures=Path(cfg.bronze_archive)/'structures'
    if any(messages.glob('date=*/*.parquet')):
        columns=','.join(ARCHIVE_COLUMNS['ws_messages'])
        c.execute(f"CREATE OR REPLACE VIEW loxone_bronze.parquet_ws_messages_raw AS SELECT {columns} FROM read_parquet('{root}/ws_messages/date=*/*.parquet',union_by_name=true,hive_partitioning=true)")
        c.execute(f'''CREATE OR REPLACE VIEW loxone_bronze.parquet_ws_messages AS
          SELECT {columns} FROM loxone_bronze.parquet_ws_messages_raw
          QUALIFY row_number() OVER(PARTITION BY source_id,message_id ORDER BY ingested_at DESC)=1''')
    if any(structures.glob('*.parquet')):
        columns=','.join(ARCHIVE_COLUMNS['structures'])
        c.execute(f"CREATE OR REPLACE VIEW loxone_bronze.parquet_structures_raw AS SELECT {columns} FROM read_parquet('{root}/structures/*.parquet',union_by_name=true)")
        c.execute(f'''CREATE OR REPLACE VIEW loxone_bronze.parquet_structures AS
          SELECT {columns} FROM loxone_bronze.parquet_structures_raw
          QUALIFY row_number() OVER(PARTITION BY structure_id ORDER BY captured_at DESC,ingested_at DESC)=1''')


def archive_spool(c,cfg,table):
    """Persist a bounded SQLite slice as immutable Parquet, then acknowledge it."""
    if table not in TABLES: raise ValueError('Invalid source table')
    with spool_connection(cfg.spool) as src:
        row=src.execute('SELECT rowid_highwater FROM parquet_archive_checkpoints WHERE table_name=?',(table,)).fetchone()
        cursor=row[0] if row else 0
        selected=src.execute(f'SELECT rowid AS spool_rowid,* FROM {table} WHERE rowid>? ORDER BY rowid LIMIT ?',
                             (cursor,cfg.archive_size if table=='ws_messages' else min(cfg.archive_size,100)))
        rows=[];payload_bytes=0
        for record in selected:
            rows.append(record);payload_bytes+=len(record['payload_json'].encode())
            if payload_bytes>=32*1024*1024: break
    if not rows:return 0
    groups={}
    for row in rows:
        partition=row['received_at'][:10] if table=='ws_messages' else 'structures'
        groups.setdefault(partition,[]).append(row)
    for partition,group in groups.items():_archive_partition(c,cfg,table,group,partition)
    cursor=rows[-1]['spool_rowid']
    # Files and manifests are durable before the pruning gate advances.
    with spool_connection(cfg.spool) as src:
        src.execute('INSERT OR REPLACE INTO parquet_archive_checkpoints VALUES (?,?)',(table,cursor))
    refresh_archive_views(c,cfg)
    return len(rows)


def prune_working_archive(c,cfg,limit=5000):
    """Bound DuckDB raw working data only after Parquet has caught up."""
    imported=c.execute("SELECT rowid_highwater FROM import_progress WHERE table_name='ws_messages'").fetchone()
    with spool_connection(cfg.spool) as src:
        parquet=src.execute("SELECT rowid_highwater FROM parquet_archive_checkpoints WHERE table_name='ws_messages'").fetchone()
    if not imported or not parquet or parquet[0] < imported[0]:return 0
    cutoff=datetime.now(timezone.utc)-timedelta(days=cfg.working_retention_days)
    c.execute('''CREATE OR REPLACE TEMP TABLE expired_working_messages AS
      SELECT a.source_id,a.message_id FROM loxone_bronze.archive_messages a
      SEMI JOIN loxone_silver.processed_messages p USING(source_id,message_id)
      ANTI JOIN process_queue q USING(source_id,message_id)
      WHERE a.received_at < ? ORDER BY a.received_at LIMIT ?''',[cutoff,limit])
    count=c.execute('SELECT count(*) FROM expired_working_messages').fetchone()[0]
    if count:c.execute('DELETE FROM loxone_bronze.archive_messages USING expired_working_messages e WHERE archive_messages.source_id=e.source_id AND archive_messages.message_id=e.message_id')
    return count


def import_spool(c,cfg,table):
    if table not in TABLES: raise ValueError('Invalid source table')
    row=c.execute('SELECT rowid_highwater FROM import_progress WHERE table_name=?',[table]).fetchone()
    cursor=row[0] if row else 0
    with spool_connection(cfg.spool) as src:
        ack=src.execute('SELECT rowid_highwater FROM local_silver_checkpoints WHERE table_name=?',(table,)).fetchone()
        if ack is None or ack[0]>cursor:
            raise RuntimeError('Archive missing or older than acknowledged spool; restore required')
        if ack[0] < cursor:
            src.execute('UPDATE local_silver_checkpoints SET rowid_highwater=? WHERE table_name=?',(cursor,table))
        selected=src.execute(f'SELECT rowid AS spool_rowid,* FROM {table} WHERE rowid>? ORDER BY rowid LIMIT ?',
                             (cursor,1 if table=='structures' else cfg.import_size))
        rows=[]; payload_bytes=0
        for record in selected:
            rows.append(record); payload_bytes+=len(record['payload_json'].encode())
            if payload_bytes>=2*1024*1024: break
    if not rows: return 0
    c.execute('BEGIN')
    try:
        columns=(('message_id','source_id','received_at','message_type','ingested_at','payload_json') if table=='ws_messages'
          else ('structure_id','source_id','captured_at','last_modified','payload_json','payload_sha256','collector_version','ingested_at'))
        types={name:('TIMESTAMPTZ' if name in ('received_at','captured_at','ingested_at') else 'INTEGER' if name=='message_type' else 'VARCHAR') for name in columns}
        schema=','.join("'"+name+"':'"+kind+"'" for name,kind in types.items())
        now=datetime.now(timezone.utc).isoformat()
        with tempfile.TemporaryDirectory(prefix='local-import-') as tmp:
            path=Path(tmp)/'rows.jsonl'
            max_record=1024*1024
            with path.open('w') as output:
                for r in rows:
                    record={name:(now if name=='ingested_at' else r[name]) for name in columns}
                    line=json.dumps(record)+'\n'
                    max_record=max(max_record,len(line)+1)
                    output.write(line)
            c.execute(f"CREATE OR REPLACE TEMP TABLE import_rows AS SELECT * FROM read_json(?,columns={{{schema}}},format='newline_delimited',maximum_object_size={max_record})",[str(path)])
        if table=='ws_messages':
            c.execute('INSERT OR IGNORE INTO process_queue SELECT source_id,message_id FROM import_rows r WHERE NOT EXISTS(SELECT 1 FROM loxone_bronze.archive_messages a WHERE a.source_id=r.source_id AND a.message_id=r.message_id)')
        # Cloud-history backfills can already have the same message in the working
        # archive while the SQLite rowid cursor is still behind it. Filter those
        # rows before the insert: DuckDB's conflict handler is not reliable for
        # every bulk INSERT .. SELECT against a primary-key table.
        target='archive_messages' if table=='ws_messages' else 'structures'
        keys=('source_id','message_id') if table=='ws_messages' else ('structure_id',)
        condition=' AND '.join(f'a.{key}=r.{key}' for key in keys)
        c.execute(f'''INSERT INTO loxone_bronze.{target} BY NAME
          SELECT * REPLACE(payload_json::JSON AS payload_json) FROM import_rows r
          WHERE NOT EXISTS (SELECT 1 FROM loxone_bronze.{target} a WHERE {condition})''')
        c.execute('DROP TABLE import_rows')
        cursor=rows[-1]['spool_rowid']
        c.execute('INSERT OR REPLACE INTO import_progress VALUES (?,?)',[table,cursor])
        c.execute('COMMIT')
    except BaseException:
        c.execute('ROLLBACK'); raise
    # A failure here safely repeats the idempotent import. Never acknowledge first.
    with spool_connection(cfg.spool) as src:
        src.execute('UPDATE local_silver_checkpoints SET rowid_highwater=? WHERE table_name=?',(cursor,table))
    return len(rows)


def enqueue_remapping(c,sc):
    # Run only after a new structure was mapped. Unchanged historical events stay put.
    c.execute(f'''INSERT OR IGNORE INTO process_queue
    WITH v AS (SELECT *,row_number() OVER(PARTITION BY source_id ORDER BY structure_captured_at,structure_id) rn,
      lead(structure_captured_at) OVER(PARTITION BY source_id ORDER BY structure_captured_at,structure_id) valid_to
      FROM {sc.silver}.structure_versions)
    SELECT b.source_id,b.message_id FROM loxone_bronze.archive_messages b
    JOIN {sc.silver}.processed_messages p USING(source_id,message_id)
    LEFT JOIN v ON v.source_id=b.source_id AND (v.rn=1 OR b.received_at>=v.structure_captured_at)
      AND (v.valid_to IS NULL OR b.received_at<v.valid_to)
    WHERE p.structure_id IS DISTINCT FROM v.structure_id''')


def sha256(path):
    h=hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda:f.read(1024*1024),b''): h.update(chunk)
    return h.hexdigest()


def finalize_outbox(c,cfg):
    root=Path(cfg.outbox)
    for seq,status,manifest in c.execute("SELECT * FROM publication_queue WHERE status='pending' ORDER BY sequence").fetchall():
        directory=root/f'{seq:020d}'
        if not directory.is_dir(): raise RuntimeError('Committed publication files missing')
        ready=directory/'manifest.json'
        if not ready.exists():
            tmp=directory/'manifest.tmp'; tmp.write_text(manifest); os.replace(tmp,ready)
        if (directory/'ACK').exists():
            c.execute("UPDATE publication_queue SET status='published' WHERE sequence=?",[seq])
            # Keep audit metadata; discard only acknowledged transport copies.
            for path in directory.glob('*.parquet'): path.unlink()


def process_local_batch(c,sc,cfg):
    c.execute(f'CREATE OR REPLACE TEMP TABLE chosen AS SELECT * FROM process_queue ORDER BY source_id,message_id LIMIT {cfg.batch_size}')
    selected=c.execute('SELECT count(*) FROM chosen').fetchone()[0]
    if not selected: return None
    seq=c.execute('SELECT coalesce(max(sequence),0)+1 FROM publication_queue').fetchone()[0]
    directory=Path(cfg.outbox)/f'{seq:020d}'
    # An uncommitted attempt can leave files; no manifest is ever exposed for it.
    if directory.exists():
        if (directory/'manifest.json').exists(): raise RuntimeError('Unexpected published sequence')
        shutil.rmtree(directory)
    directory.mkdir(mode=0o700)
    c.execute('BEGIN')
    try:
        sql=sc.sql('batch.sql')
        c.execute(sql)
        events=c.execute(f'SELECT count(*) FROM {sc.silver}._refresh_events').fetchone()[0]
        c.execute(f'''CREATE OR REPLACE TEMP TABLE next_latest AS
          SELECT * FROM (SELECT * FROM latest_events UNION ALL
            SELECT * FROM {sc.silver}.state_events e SEMI JOIN {sc.silver}._refresh_batch b USING(source_id,message_id))
          QUALIFY row_number() OVER(PARTITION BY source_id,state_uuid ORDER BY received_at DESC,message_id DESC,loaded_at DESC)=1''')
        c.execute('DELETE FROM latest_events')
        c.execute('INSERT INTO latest_events SELECT * FROM next_latest')
        c.execute('DELETE FROM process_queue USING chosen q WHERE process_queue.source_id=q.source_id AND process_queue.message_id=q.message_id')
        queries={
          'messages':f'SELECT source_id,message_id FROM {sc.silver}._refresh_batch',
          'state_events':f'SELECT e.* FROM {sc.silver}.state_events e SEMI JOIN {sc.silver}._refresh_batch b USING(source_id,message_id)',
          'current_state':f'''SELECT source_id,state_uuid,state_name,control_uuid,control_name,control_type,room_uuid,room_name,
            category_uuid,category_name,received_at AS last_received_at,received_at_berlin AS last_received_at_berlin,
            value_numeric,value_text,value_json,structure_id,resolution_status FROM latest_events
            SEMI JOIN (SELECT DISTINCT source_id,state_uuid FROM {sc.silver}._refresh_events) k USING(source_id,state_uuid)''',
          'hourly_state':f'''SELECT source_id,state_uuid,date_trunc('hour',received_at) AS hour_utc,
            count(*) AS event_count,count(value_numeric) AS numeric_count,min(value_numeric) AS numeric_min,
            max(value_numeric) AS numeric_max,avg(value_numeric) AS numeric_avg,
            arg_min(value_numeric,received_at) AS numeric_first,arg_max(value_numeric,received_at) AS numeric_last
            FROM {sc.silver}.state_events e SEMI JOIN
            (SELECT DISTINCT source_id,state_uuid,date_trunc('hour',received_at) AS hour_utc FROM {sc.silver}._refresh_events) k
            ON e.source_id=k.source_id AND e.state_uuid=k.state_uuid AND date_trunc('hour',e.received_at)=k.hour_utc
            GROUP BY e.source_id,e.state_uuid,date_trunc('hour',received_at)'''}
        files={}
        for name,query in queries.items():
            path=directory/(name+'.parquet')
            c.execute(f'COPY ({query}) TO ? (FORMAT PARQUET, COMPRESSION ZSTD)',[str(path)])
            with path.open('rb') as f: os.fsync(f.fileno())
            files[path.name]=sha256(path)
        pipeline=c.execute("SELECT value FROM local_metadata WHERE key='pipeline_id'").fetchone()[0]
        manifest=json.dumps({'version':1,'pipeline_id':pipeline,'sequence':seq,'files':files,
          'messages':selected,'events':events,'created_at':datetime.now(timezone.utc).isoformat()})
        c.execute("INSERT INTO publication_queue VALUES (?,'pending',?)",[seq,manifest])
        c.execute(f'DROP TABLE {sc.silver}._refresh_batch')
        c.execute(f'DROP TABLE {sc.silver}._refresh_events')
        c.execute('COMMIT')
    except BaseException:
        c.execute('ROLLBACK'); raise
    finalize_outbox(c,cfg)
    return {'messages':selected,'events':events,'sequence':seq}


def run(cfg):
    cfg.validate()
    Path(cfg.database).parent.mkdir(parents=True,exist_ok=True)
    with lock(cfg.database+'.lock'), lock(str(Path(cfg.outbox).parent/'pipeline.lock')):
        c,sc=connect_local(cfg)
        try:
            finalize_outbox(c,cfg)
            start=time.monotonic(); imported=0; batches=[]
            archived={table:archive_spool(c,cfg,table) for table in TABLES}
            for _ in range(cfg.max_batches):
                if time.monotonic()-start>=cfg.max_seconds: break
                structures=import_spool(c,cfg,'structures')
                sync_local_structure(c,sc,enqueue_remapping)
                # Structures must catch up before event mappings can be trusted.
                with spool_connection(cfg.spool) as src:
                    cursor=c.execute("SELECT rowid_highwater FROM import_progress WHERE table_name='structures'").fetchone()
                    more=src.execute('SELECT 1 FROM structures WHERE rowid>? LIMIT 1',(cursor[0] if cursor else 0,)).fetchone()
                unmapped=c.execute('SELECT 1 FROM loxone_bronze.structures b WHERE NOT EXISTS(SELECT 1 FROM mapped_structure_inputs m WHERE m.structure_id=b.structure_id) LIMIT 1').fetchone()
                if more or unmapped: continue
                pending=c.execute('SELECT count(*) FROM process_queue').fetchone()[0]
                if pending < cfg.import_size: imported+=import_spool(c,cfg,'ws_messages')
                result=process_local_batch(c,sc,cfg)
                if result: batches.append(result); LOG.info('local_batch %s',json.dumps(result))
                else: break
            pruned=prune_local_archived(cfg) if cfg.prune_local_only else {}
            working_pruned=prune_working_archive(c,cfg)
            summary={'archived':archived,'imported':imported,'batches':len(batches),'events':sum(b['events'] for b in batches),
              'pending_messages':c.execute('SELECT count(*) FROM process_queue').fetchone()[0],
              'pending_publications':c.execute("SELECT count(*) FROM publication_queue WHERE status='pending'").fetchone()[0],
              'pruned_spool_rows':sum(pruned.values()),'pruned_working_messages':working_pruned,
              'elapsed_seconds':round(time.monotonic()-start,2)}
            LOG.info('local_summary %s',json.dumps(summary)); return summary
        finally: c.close()


def main():
    p=argparse.ArgumentParser(); p.add_argument('--enable-retention-guard',action='store_true'); args=p.parse_args()
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
    try:
        cfg=LocalConfig.from_env()
        if args.enable_retention_guard: enable_retention_guard(cfg.spool)
        else: run(cfg)
    except BlockingIOError: LOG.info('local_skipped already_running=true')
    except Exception as exc:
        LOG.error('local_error type=%s',type(exc).__name__); raise SystemExit(1) from None

if __name__=='__main__': main()
