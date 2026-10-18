"""Flight-free, archive-gated edge upload and remote Silver computation.

Raw staging is bounded to one chunk. Source rows and the immutable SSD archive
are never pruned here. Cloud chunk acknowledgement follows Silver/derived commits;
a lost local acknowledgement is recovered from the cloud ledger on retry.
"""
from dataclasses import dataclass
from datetime import datetime,timezone
import hashlib
import json
import logging
import os
from pathlib import Path
import sqlite3
import tempfile
import time
import uuid

import duckdb
from .local_silver import lock
from .silver import SilverConfig,initialize,sync_structures,process_batch,identifier
from .uploader import MESSAGE_COLUMNS,STRUCTURE_COLUMNS

LOG=logging.getLogger(__name__)

@dataclass(frozen=True)
class EdgeConfig:
    spool: str = '/srv/raspi-data/loxone-bronze/spool.sqlite3'
    state: str = '/srv/raspi-data/loxone-silver/cloud-edge.sqlite3'
    database: str = 'my_db'
    raw_schema: str = 'loxone_bronze_edge'
    silver_schema: str = 'loxone_silver_local'
    chunk_size: int = 1000
    chunk_bytes: int = 4*1024*1024
    max_chunks: int = 5
    max_seconds: int = 180
    history_directory: str = ''
    min_messages: int = 1000
    max_wait_seconds: int = 300

    def validate(self):
        for s in (self.database,self.raw_schema,self.silver_schema):identifier(s)
        if self.raw_schema==self.silver_schema:raise ValueError('Separate raw schema required')
        if not 1<=self.chunk_size<=5000 or not 1024<=self.chunk_bytes<=8*1024*1024:raise ValueError('Invalid chunk bounds')
        if not 0<=self.min_messages<=self.chunk_size or not 0<=self.max_wait_seconds<=3600:raise ValueError('Invalid scheduling bounds')
        if not 1<=self.max_chunks<=100 or not 1<=self.max_seconds<=600:raise ValueError('Invalid run bounds')

    @property
    def raw(self):return identifier(self.database)+'.'+identifier(self.raw_schema)
    @property
    def silver(self):return identifier(self.database)+'.'+identifier(self.silver_schema)
    @property
    def transform(self):return SilverConfig(source_database=self.database,target_database=self.database,source_schema=self.raw_schema,target_schema=self.silver_schema,batch_size=self.chunk_size)

    @classmethod
    def from_env(cls):return cls(**{k:type(v)(os.environ.get('EDGE_'+k.upper(),v)) for k,v in cls().__dict__.items()})


def open_state(path,initial_cursor=None):
    p=Path(path);p.parent.mkdir(parents=True,exist_ok=True)
    if not p.exists() and initial_cursor is None:raise RuntimeError('Explicit cutover cursor required for new state')
    c=sqlite3.connect(p,timeout=2)
    c.execute('PRAGMA synchronous=FULL')
    c.execute('CREATE TABLE IF NOT EXISTS progress(id INTEGER PRIMARY KEY CHECK(id=1),pipeline_id TEXT NOT NULL,cursor INTEGER NOT NULL)')
    if c.execute('SELECT 1 FROM progress').fetchone() is None:
        if initial_cursor is None or initial_cursor<0:raise ValueError('Invalid initial cursor')
        c.execute('INSERT INTO progress VALUES(1,?,?)',[str(uuid.uuid4()),initial_cursor]);c.commit()
        fd=os.open(p.parent,os.O_RDONLY)
        try:os.fsync(fd)
        finally:os.close(fd)
    c.execute('CREATE TABLE IF NOT EXISTS metadata(key TEXT PRIMARY KEY,value TEXT NOT NULL)');c.commit()
    return c


def source_connection(path):
    c=sqlite3.connect(Path(path).resolve().as_uri()+'?mode=ro',uri=True,timeout=2);c.row_factory=sqlite3.Row;return c


def read_chunk(cfg,cursor):
    with source_connection(cfg.spool) as c:
        cap=c.execute("SELECT rowid_highwater FROM parquet_archive_checkpoints WHERE table_name='ws_messages'").fetchone()
        if cap is None:raise RuntimeError('Durable raw archive checkpoint missing')
        records=[];size=0
        for r in c.execute('SELECT rowid AS spool_rowid,* FROM ws_messages WHERE rowid>? AND rowid<=? ORDER BY rowid LIMIT ?',[cursor,cap[0],cfg.chunk_size]):
            n=len(r['payload_json'].encode())
            if n>cfg.chunk_bytes:raise RuntimeError('Single message exceeds configured byte bound')
            if records and size+n>cfg.chunk_bytes:break
            records.append(dict(r));size+=n
        return records,cap[0],size


def read_structures(cfg):
    with source_connection(cfg.spool) as c:
        cap=c.execute("SELECT rowid_highwater FROM parquet_archive_checkpoints WHERE table_name='structures'").fetchone()
        if cap is None:raise RuntimeError('Durable structure archive checkpoint missing')
        # Structure payloads are staged one at a time; never collect all JSON in RAM.
        keys=[r[0] for r in c.execute('SELECT rowid FROM structures WHERE rowid<=? ORDER BY rowid',[cap[0]])]
    return keys


def stage_rows(c,target,table,columns,rows,root):
    path=Path(root)/(table+'.jsonl')
    with path.open('w') as f:
        for r in rows:f.write(json.dumps({k:r[k] for k in columns},ensure_ascii=True)+'\n')
    types=','.join("'"+k+"':'"+v+"'" for k,v in columns.items())
    projection=','.join('payload_json::JSON' if k=='payload_json' else '"'+k+'"' for k in columns)
    c.execute(f'INSERT OR IGNORE INTO {target}.{table} ({",".join(columns)}) SELECT {projection} FROM read_json(?,columns={{{types}}},format=\'newline_delimited\',maximum_object_size=16777216)',[str(path)])


def initialize_edge(c,cfg,pipeline):
    c.execute(f'CREATE SCHEMA IF NOT EXISTS {cfg.raw}')
    c.execute(f'CREATE TABLE IF NOT EXISTS {cfg.raw}.owner(id INTEGER PRIMARY KEY,pipeline_id VARCHAR NOT NULL)')
    owner=c.execute(f'SELECT pipeline_id FROM {cfg.raw}.owner WHERE id=1').fetchone()
    if owner and owner[0]!=pipeline:raise RuntimeError('Raw staging belongs to a different pipeline')
    c.execute(f'INSERT OR IGNORE INTO {cfg.raw}.owner VALUES(1,?)',[pipeline])
    for name,columns,key in [('ws_messages',MESSAGE_COLUMNS,'source_id,message_id'),('structures',STRUCTURE_COLUMNS,'structure_id')]:
        defs=','.join('"'+k+'" '+('JSON' if k=='payload_json' else kind) for k,kind in columns.items())
        c.execute(f'CREATE TABLE IF NOT EXISTS {cfg.raw}.{name}({defs},ingested_at TIMESTAMPTZ DEFAULT now(),PRIMARY KEY({key}))')
    c.execute(f'CREATE TABLE IF NOT EXISTS {cfg.raw}.control(key VARCHAR PRIMARY KEY,value VARCHAR)')
    c.execute(f'CREATE TABLE IF NOT EXISTS {cfg.raw}.chunks(pipeline_id VARCHAR,from_cursor BIGINT,to_cursor BIGINT,messages BIGINT,payload_sha256 VARCHAR,committed_at TIMESTAMPTZ DEFAULT now(),PRIMARY KEY(pipeline_id,from_cursor))')
    c.execute(f'CREATE SCHEMA IF NOT EXISTS {cfg.silver}')
    c.execute(f'''CREATE TABLE IF NOT EXISTS {cfg.silver}.current_state(
        source_id VARCHAR,state_uuid VARCHAR,state_name VARCHAR,control_uuid VARCHAR,control_name VARCHAR,control_type VARCHAR,
        room_uuid VARCHAR,room_name VARCHAR,category_uuid VARCHAR,category_name VARCHAR,last_received_at TIMESTAMPTZ,
        last_received_at_berlin TIMESTAMP,value_numeric DOUBLE,value_text VARCHAR,value_json JSON,structure_id VARCHAR,resolution_status VARCHAR)''')
    c.execute(f'''CREATE TABLE IF NOT EXISTS {cfg.silver}.hourly_state(source_id VARCHAR,state_uuid VARCHAR,hour_utc TIMESTAMPTZ,
        event_count BIGINT,numeric_count BIGINT,numeric_min DOUBLE,numeric_max DOUBLE,numeric_avg DOUBLE,numeric_first DOUBLE,numeric_last DOUBLE)''')
    initialize(c,cfg.transform)
    seed_latest(c,cfg)
    c.execute(f'CREATE TABLE IF NOT EXISTS {cfg.silver}.published_batches(batch_id VARCHAR PRIMARY KEY,created_at TIMESTAMPTZ,published_at TIMESTAMPTZ,messages BIGINT,events BIGINT)')



def bootstrap_history(c,cfg):
    if not cfg.history_directory:return
    root=Path(cfg.history_directory);manifest=root/'manifest.json'
    data=manifest.read_bytes();digest=hashlib.sha256(data).hexdigest();meta=json.loads(data)
    expected={'structures.parquet','structure_versions.parquet','state_uuid_map.parquet'}
    if meta.get('version')!=1 or set(meta.get('files',{}))!=expected:raise ValueError('Invalid history manifest')
    marker=c.execute(f"SELECT value FROM {cfg.raw}.control WHERE key='history_bootstrap'").fetchone()
    if marker==(digest,):return
    if marker:raise RuntimeError('History bootstrap changed; explicit reconciliation required')
    for name,info in meta['files'].items():
        h=hashlib.sha256()
        with (root/name).open('rb') as f:
            for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
        if h.hexdigest()!=info['sha256']:raise RuntimeError('History file checksum mismatch')
        table=name.removesuffix('.parquet')
        c.execute(f'CREATE OR REPLACE TABLE {cfg.raw}.history_{table} AS SELECT * FROM read_parquet(?)',[str(root/name)])
        if c.execute(f'SELECT count(*) FROM {cfg.raw}.history_{table}').fetchone()[0]!=info['rows']:raise RuntimeError('History row count mismatch')
    c.execute('BEGIN')
    try:
        c.execute(f'INSERT OR IGNORE INTO {cfg.raw}.structures BY NAME SELECT * FROM {cfg.raw}.history_structures')
        c.execute(f'INSERT OR IGNORE INTO {cfg.silver}.structure_versions BY NAME SELECT * FROM {cfg.raw}.history_structure_versions')
        c.execute(f'''INSERT INTO {cfg.silver}.state_uuid_map BY NAME SELECT b.* FROM {cfg.raw}.history_state_uuid_map b
            WHERE NOT EXISTS(SELECT 1 FROM {cfg.silver}.state_uuid_map m WHERE m.structure_id=b.structure_id)''')
        bad=c.execute(f'''SELECT 1 FROM {cfg.raw}.history_structure_versions v WHERE v.state_mapping_count !=
            (SELECT count(*) FROM {cfg.silver}.state_uuid_map m WHERE m.structure_id=v.structure_id) LIMIT 1''').fetchone()
        if bad:raise RuntimeError('Historical mapping count mismatch')
        unknown=c.execute(f'''SELECT 1 FROM {cfg.silver}.state_events e WHERE e.structure_id IS NOT NULL
            AND NOT EXISTS(SELECT 1 FROM {cfg.silver}.structure_versions v WHERE v.structure_id=e.structure_id) LIMIT 1''').fetchone()
        if unknown:raise RuntimeError('Historical event references missing structure version')
        c.execute(f"INSERT INTO {cfg.raw}.control VALUES('history_bootstrap',?)",[digest])
        for table in ['structures','structure_versions','state_uuid_map']:c.execute(f'DROP TABLE {cfg.raw}.history_{table}')
        c.execute('COMMIT')
    except BaseException:
        try:c.execute('ROLLBACK')
        except duckdb.TransactionException:pass
        raise
    LOG.info('edge_history_bootstrapped %s',json.dumps({name:info['rows'] for name,info in meta['files'].items()}))


def current_projection():
    return ('source_id,state_uuid,state_name,control_uuid,control_name,control_type,room_uuid,room_name,'
            'category_uuid,category_name,received_at,received_at_berlin,value_numeric,value_text,value_json,structure_id,resolution_status')


def seed_latest(c,cfg):
    s=cfg.silver
    exists=c.execute('SELECT 1 FROM duckdb_tables() WHERE database_name=? AND schema_name=? AND table_name=?',
        [cfg.database,cfg.silver_schema,'edge_latest_events']).fetchone()
    if exists:return
    columns=[r[0] for r in c.execute(f'DESCRIBE {s}.state_events').fetchall()]
    values=','.join(identifier(k)+' := '+identifier(k) for k in columns if k not in ('source_id','state_uuid'))
    projection=','.join('g.'+identifier(k) if k in ('source_id','state_uuid') else 'g.latest.'+identifier(k) for k in columns)
    c.execute('BEGIN')
    try:
        c.execute(f'''CREATE TABLE {s}.edge_latest_events AS WITH grouped AS (
            SELECT source_id,state_uuid,arg_max(struct_pack({values}),struct_pack(t := received_at,m := message_id,l := loaded_at)) latest
            FROM {s}.state_events GROUP BY source_id,state_uuid)
            SELECT {projection} FROM grouped g''')
        c.execute(f'DELETE FROM {s}.current_state')
        c.execute(f'INSERT INTO {s}.current_state SELECT {current_projection()} FROM {s}.edge_latest_events')
        c.execute('COMMIT')
    except BaseException:
        try:c.execute('ROLLBACK')
        except duckdb.TransactionException:pass
        raise
    LOG.info('edge_latest_seeded')


def refresh_derived(c,sc, *, include_hourly=True):
    s=sc.silver
    # Only the compact latest-state table and this batch enter the window.
    # Historical events are scanned once when the cloud pipeline takes ownership.
    c.execute(f'''CREATE OR REPLACE TABLE {s}._edge_next_latest AS
        SELECT * FROM (SELECT * FROM {s}.edge_latest_events UNION ALL SELECT * FROM {s}._refresh_events)
        QUALIFY row_number() OVER(PARTITION BY source_id,state_uuid ORDER BY received_at DESC,message_id DESC,loaded_at DESC)=1''')
    c.execute(f'DELETE FROM {s}.edge_latest_events')
    c.execute(f'INSERT INTO {s}.edge_latest_events SELECT * FROM {s}._edge_next_latest')
    c.execute(f'DROP TABLE {s}._edge_next_latest')
    c.execute(f'DELETE FROM {s}.current_state')
    c.execute(f'INSERT INTO {s}.current_state SELECT {current_projection()} FROM {s}.edge_latest_events')
    if not include_hourly:return
    keys=f"SELECT DISTINCT source_id,state_uuid,date_trunc('hour',received_at) AS hour_utc FROM {s}._refresh_events"
    c.execute(f'''DELETE FROM {s}.hourly_state t USING ({keys}) k
        WHERE t.source_id=k.source_id AND t.state_uuid=k.state_uuid AND t.hour_utc=k.hour_utc''')
    c.execute(f'''INSERT INTO {s}.hourly_state SELECT e.source_id,e.state_uuid,date_trunc('hour',e.received_at),
        count(*),count(value_numeric),min(value_numeric),max(value_numeric),avg(value_numeric),
        arg_min(value_numeric,e.received_at),arg_max(value_numeric,e.received_at)
        FROM {s}.state_events e SEMI JOIN ({keys}) k ON e.source_id=k.source_id AND e.state_uuid=k.state_uuid AND date_trunc('hour',e.received_at)=k.hour_utc
        WHERE e.received_at >= (SELECT min(date_trunc('hour',received_at)) FROM {s}._refresh_events)
          AND e.received_at < (SELECT max(date_trunc('hour',received_at))+INTERVAL '1 hour' FROM {s}._refresh_events)
        GROUP BY e.source_id,e.state_uuid,date_trunc('hour',e.received_at)''')



def remap_history(c,cfg):
    """Replay metadata from retained events when a late structure changes as-of mapping.

    Values are already losslessly available in state_events; raw staging can stay
    bounded even after the original upload is acknowledged. Work remains message-
    bounded and its flag is durable before new mappings commit.
    """
    if c.execute(f"SELECT value FROM {cfg.raw}.control WHERE key='remap_pending'").fetchone()!=('1',):return 0
    c.execute(f'''CREATE OR REPLACE TABLE {cfg.raw}.remap_messages AS
      WITH versions AS (
        SELECT *,row_number() OVER(PARTITION BY source_id ORDER BY structure_captured_at,structure_id) rn,
          lead(structure_captured_at) OVER(PARTITION BY source_id ORDER BY structure_captured_at,structure_id) valid_to
        FROM {cfg.silver}.structure_versions
      ), chosen AS (
        SELECT e.source_id,e.message_id,min(e.received_at) received_at FROM {cfg.silver}.state_events e
        LEFT JOIN versions v ON v.source_id=e.source_id AND (v.rn=1 OR e.received_at>=v.structure_captured_at)
          AND (v.valid_to IS NULL OR e.received_at<v.valid_to)
        WHERE v.structure_id IS NOT NULL AND e.structure_id IS DISTINCT FROM v.structure_id
        GROUP BY e.source_id,e.message_id ORDER BY min(e.received_at),e.source_id,e.message_id LIMIT {cfg.chunk_size}
      )
      SELECT e.source_id,e.message_id,min(e.received_at) received_at,min(e.message_type) message_type,
        min(e.bronze_ingested_at) ingested_at,json_object('parsed',json_group_object(e.state_uuid,e.value_json)) payload_json
      FROM {cfg.silver}.state_events e SEMI JOIN chosen q USING(source_id,message_id)
      GROUP BY e.source_id,e.message_id''')
    count=c.execute(f'SELECT count(*) FROM {cfg.raw}.remap_messages').fetchone()[0]
    if count:
        result=process_batch(c,cfg.transform,after_transform=lambda db,sc:refresh_derived(db,sc,include_hourly=False),
            ws_messages=cfg.database+'.'+cfg.raw_schema+'.remap_messages')
        if result['messages']!=count:raise RuntimeError('Remapping did not acknowledge selected messages')
    else:c.execute(f"INSERT OR REPLACE INTO {cfg.raw}.control VALUES('remap_pending','0')")
    c.execute(f'DROP TABLE {cfg.raw}.remap_messages')
    LOG.info('edge_remap %s',json.dumps({'messages':count}));return count


def run(cfg,connect=None,initial_cursor=None):
    cfg.validate();started=time.monotonic();messages=chunks=recovered=events=0
    with lock(cfg.state+'.lock'):
        state=open_state(cfg.state,initial_cursor)
        try:
            pipeline,cursor=state.execute('SELECT pipeline_id,cursor FROM progress WHERE id=1').fetchone()
            destination=json.dumps([cfg.database,cfg.raw_schema,cfg.silver_schema])
            old=state.execute("SELECT value FROM metadata WHERE key='destination'").fetchone()
            if old and old[0]!=destination:raise RuntimeError('State belongs to a different destination')
            state.execute("INSERT OR IGNORE INTO metadata VALUES('destination',?)",[destination]);state.commit()
            retry=state.execute("SELECT value FROM metadata WHERE key='retry_after'").fetchone()
            if retry and float(retry[0])>time.time():
                result={'messages':0,'events':0,'chunks':0,'cursor':cursor,'reason':'error_backoff'}
                LOG.info('edge_idle %s',json.dumps(result));return result
            ready,_,_=read_chunk(cfg,cursor)
            with source_connection(cfg.spool) as src:structure_cap=src.execute("SELECT rowid_highwater FROM parquet_archive_checkpoints WHERE table_name='structures'").fetchone()[0]
            meta=dict(state.execute('SELECT key,value FROM metadata'))
            structure_changed=structure_cap>int(meta.get('structures_cursor','0'))
            received=datetime.fromisoformat(ready[0]['received_at']) if ready else datetime.now(timezone.utc)
            if received.tzinfo is None:received=received.replace(tzinfo=timezone.utc)
            age=(datetime.now(timezone.utc)-received).total_seconds()
            if not structure_changed and meta.get('remap_pending','0')=='0' and (not ready or (len(ready)<cfg.min_messages and age<cfg.max_wait_seconds)):
                result={'messages':0,'events':0,'chunks':0,'recovered_acks':0,'cursor':cursor,'elapsed_seconds':round(time.monotonic()-started,3),'reason':'waiting_for_batch'}
                LOG.info('edge_idle %s',json.dumps(result));return result
            LOG.info('edge_start %s',json.dumps({'cursor':cursor,'chunk_size':cfg.chunk_size,'chunk_bytes':cfg.chunk_bytes,'max_chunks':cfg.max_chunks,'max_seconds':cfg.max_seconds}))
            c=(connect or (lambda:duckdb.connect('md:'+cfg.database,config={'threads':1,'memory_limit':'96MB','extension_directory':str(Path(cfg.state).parent/'extensions')})))()
            try:
                LOG.info('edge_connected elapsed_seconds=%.3f',time.monotonic()-started)
                c.execute("SET TimeZone='UTC'");initialize_edge(c,cfg,pipeline)
                bootstrap_history(c,cfg)
                with tempfile.TemporaryDirectory(prefix='cloud-edge-',dir=Path(cfg.state).parent) as tmp:
                    for rowid in read_structures(cfg):
                        if time.monotonic()-started>=cfg.max_seconds:break
                        with source_connection(cfg.spool) as src:r=src.execute('SELECT * FROM structures WHERE rowid=?',[rowid]).fetchone()
                        if not c.execute(f'SELECT 1 FROM {cfg.raw}.structures WHERE structure_id=?',[r['structure_id']]).fetchone():
                            stage_rows(c,cfg.raw,'structures',STRUCTURE_COLUMNS,[r],tmp)
                    # Do not transform until every archived structure has been uploaded.
                    with source_connection(cfg.spool) as src:
                        local_ids={r['structure_id'] for r in src.execute('SELECT structure_id FROM structures')}
                    remote_ids={r[0] for r in c.execute(f'SELECT structure_id FROM {cfg.raw}.structures').fetchall()}
                    if not local_ids<=remote_ids:raise RuntimeError('Structure upload incomplete')
                    unmapped=c.execute(f"SELECT 1 FROM {cfg.raw}.structures b WHERE length(cast(payload_json AS VARCHAR))>1000 AND json_type(payload_json,'$.controls')='OBJECT' AND len(json_keys(payload_json,'$.controls'))>0 AND NOT EXISTS(SELECT 1 FROM {cfg.silver}.structure_versions v WHERE v.structure_id=b.structure_id) LIMIT 1").fetchone()
                    if unmapped:c.execute(f"INSERT OR REPLACE INTO {cfg.raw}.control VALUES('remap_pending','1')")
                    for _ in range(len(remote_ids)+1):
                        if not sync_structures(c,cfg.transform):break
                    else:raise RuntimeError('Structure mapping did not converge')
                    LOG.info('edge_structures_ready count=%s',len(remote_ids))
                    remap_history(c,cfg)
                    pending=c.execute(f"SELECT value FROM {cfg.raw}.control WHERE key='remap_pending'").fetchone()
                    state.execute("INSERT OR REPLACE INTO metadata VALUES('structures_cursor',?)",[str(structure_cap)])
                    state.execute("INSERT OR REPLACE INTO metadata VALUES('remap_pending',?)",[pending[0] if pending else '0']);state.commit()
                    for _ in range(cfg.max_chunks):
                        if time.monotonic()-started>=cfg.max_seconds:break
                        ack=c.execute(f'SELECT to_cursor,messages FROM {cfg.raw}.chunks WHERE pipeline_id=? AND from_cursor=?',[pipeline,cursor]).fetchone()
                        if ack:
                            _,cap,_=read_chunk(cfg,cursor)
                            if ack[0]>cap or ack[0]<=cursor:raise RuntimeError('Cloud acknowledgement outside durable archive')
                            cursor=ack[0];state.execute('UPDATE progress SET cursor=? WHERE id=1',[cursor]);state.commit();recovered+=1;continue
                        rows,cap,nbytes=read_chunk(cfg,cursor)
                        if not rows:break
                        t=time.monotonic();end=rows[-1]['spool_rowid']
                        digest=hashlib.sha256(''.join(str(r['spool_rowid'])+':'+r['payload_sha256']+'\n' for r in rows).encode()).hexdigest()
                        c.execute('BEGIN')
                        try:
                            c.execute(f'DELETE FROM {cfg.raw}.ws_messages')
                            stage_rows(c,cfg.raw,'ws_messages',MESSAGE_COLUMNS,rows,tmp)
                            c.execute('COMMIT')
                        except BaseException:c.execute('ROLLBACK');raise
                        LOG.info('edge_transform_start cursor=%s rows=%s',cursor,len(rows))
                        result=process_batch(c,cfg.transform,after_transform=refresh_derived)
                        # Every supported record must be acknowledged before the raw chunk advances.
                        remaining=process_batch(c,cfg.transform,after_transform=refresh_derived)
                        if remaining['messages']:raise RuntimeError('Chunk transform did not converge')
                        event_count=result['events']
                        if not result['messages']:
                            event_count=c.execute(f'SELECT count(*) FROM {cfg.silver}.state_events e SEMI JOIN {cfg.raw}.ws_messages b USING(source_id,message_id) WHERE e.received_at >= (SELECT min(received_at) FROM {cfg.raw}.ws_messages) AND e.received_at <= (SELECT max(received_at) FROM {cfg.raw}.ws_messages)').fetchone()[0]
                        c.execute('BEGIN')
                        try:
                            c.execute(f'INSERT INTO {cfg.silver}.published_batches VALUES(?,now(),now(),?,?)',['edge:'+pipeline+':'+str(cursor),len(rows),event_count])
                            c.execute(f'INSERT INTO {cfg.raw}.chunks(pipeline_id,from_cursor,to_cursor,messages,payload_sha256) VALUES(?,?,?,?,?)',[pipeline,cursor,end,len(rows),digest])
                            c.execute(f'DELETE FROM {cfg.raw}.ws_messages')
                            c.execute('COMMIT')
                        except BaseException:c.execute('ROLLBACK');raise
                        cursor=end;state.execute('UPDATE progress SET cursor=? WHERE id=1',[cursor]);state.commit()
                        messages+=len(rows);chunks+=1;events+=result['events']
                        LOG.info('edge_batch %s',json.dumps({'messages':len(rows),'events':result['events'],'cursor':cursor,'archive_highwater':cap,'bytes':nbytes,'seconds':round(time.monotonic()-t,3)}))
                result={'messages':messages,'events':events,'chunks':chunks,'recovered_acks':recovered,'cursor':cursor,'elapsed_seconds':round(time.monotonic()-started,3)}
                LOG.info('edge_summary %s',json.dumps(result));return result
            finally:c.close()
        finally:state.close()


def main():
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
    cfg=None
    try:
        cfg=EdgeConfig.from_env()
        if not os.environ.get('MOTHERDUCK_TOKEN','').strip():raise ValueError('Existing token required')
        run(cfg)
    except BlockingIOError:LOG.info('edge_skipped already_running=true')
    except Exception as exc:
        frame=exc.__traceback__
        while frame and frame.tb_next:frame=frame.tb_next
        origin=f'{frame.tb_frame.f_code.co_name}:{frame.tb_lineno}' if frame else 'unknown'
        LOG.error('edge_error type=%s origin=%s',type(exc).__name__,origin)
        if cfg is not None:
            try:
                with sqlite3.connect(Path(cfg.state).resolve().as_uri()+'?mode=rw',uri=True,timeout=1) as state:
                    state.execute('PRAGMA synchronous=FULL')
                    state.execute("INSERT OR REPLACE INTO metadata VALUES('retry_after',?)",[str(time.time()+300)])
            except Exception:pass
        raise SystemExit(1) from None

if __name__=='__main__':main()
