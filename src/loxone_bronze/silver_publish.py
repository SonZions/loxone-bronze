"""Publish already transformed Parquet batches. No raw JSON transformation in cloud."""
import argparse
import json
import logging
import os
from pathlib import Path
import re
import time
import tempfile
import duckdb
from .local_silver import lock,sha256
from .silver import identifier

LOG=logging.getLogger(__name__)
FILES={'messages.parquet','state_events.parquet','current_state.parquet','hourly_state.parquet'}

def validate_batch(directory):
    manifest=json.loads((directory/'manifest.json').read_text())
    if manifest['version']!=1 or set(manifest['files'])!=FILES:
        raise ValueError('Invalid publication manifest')
    if not re.fullmatch(r'[a-f0-9-]{36}',manifest['pipeline_id']): raise ValueError('Invalid pipeline ID')
    if f"{int(manifest['sequence']):020d}"!=directory.name: raise ValueError('Invalid sequence')
    for name,digest in manifest['files'].items():
        if sha256(directory/name)!=digest: raise ValueError('Publication checksum mismatch')
    return manifest


def publish_batch(c,directory,manifest,target):
    batch_id=manifest['pipeline_id']+':'+str(manifest['sequence'])
    members=manifest.get('members',[{'batch_id':batch_id,**manifest}])
    c.execute(f'CREATE SCHEMA IF NOT EXISTS {target}')
    c.execute(f'''CREATE TABLE IF NOT EXISTS {target}.published_batches(
      batch_id VARCHAR PRIMARY KEY,created_at TIMESTAMPTZ,published_at TIMESTAMPTZ,
      messages BIGINT,events BIGINT)''')
    # Retry after a committed cloud transaction requires only an acknowledgement.
    if c.execute(f'SELECT 1 FROM {target}.published_batches WHERE batch_id=?',[batch_id]).fetchone(): return
    for name in ('state_events','current_state','hourly_state'):
        c.execute(f'CREATE TABLE IF NOT EXISTS {target}.{name} AS SELECT * FROM read_parquet(?) LIMIT 0',
                  [str(directory/(name+'.parquet'))])
    c.execute('BEGIN')
    try:
        c.execute(f'''DELETE FROM {target}.state_events e USING read_parquet(?) b
          WHERE e.source_id=b.source_id AND e.message_id=b.message_id''',[str(directory/'messages.parquet')])
        c.execute(f'INSERT INTO {target}.state_events BY NAME SELECT * FROM read_parquet(?)',[str(directory/'state_events.parquet')])
        for name,keys in [('current_state',('source_id','state_uuid')),('hourly_state',('source_id','state_uuid','hour_utc'))]:
            condition=' AND '.join('t.'+key+'=b.'+key for key in keys)
            c.execute(f'DELETE FROM {target}.{name} t USING read_parquet(?) b WHERE {condition}',[str(directory/(name+'.parquet'))])
            c.execute(f'INSERT INTO {target}.{name} BY NAME SELECT * FROM read_parquet(?)',[str(directory/(name+'.parquet'))])
        values=','.join('(?,?,now(),?,?)' for _ in members)
        args=[value for m in members for value in (m['batch_id'],m['created_at'],m['messages'],m['events'])]
        c.execute(f'INSERT INTO {target}.published_batches VALUES '+values,args)
        c.execute('COMMIT')
    except BaseException:
        c.execute('ROLLBACK'); raise


def acknowledge(directory):
    ack=directory/'ACK.tmp'
    with ack.open('w') as f:
        f.write('committed\n');f.flush();os.fsync(f.fileno())
    os.replace(ack,directory/'ACK')


def combine_batches(batches,destination):
    """Resolve replays locally and send one cloud transaction for many batches."""
    c=duckdb.connect(config={'threads':1,'memory_limit':'96MB'})
    try:
        files=lambda name:[str(directory/(name+'.parquet')) for directory,_ in batches]
        c.execute("CREATE TABLE message_versions AS SELECT source_id,message_id,max(regexp_extract(filename,'([0-9]{20})/[^/]+$',1)) seq FROM read_parquet(?,filename=true) GROUP BY source_id,message_id",[files('messages')])
        c.execute('COPY (SELECT source_id,message_id FROM message_versions) TO ? (FORMAT PARQUET,COMPRESSION ZSTD)',[str(destination/'messages.parquet')])
        c.execute("COPY (SELECT e.* EXCLUDE(filename) FROM read_parquet($files,filename=true) e JOIN message_versions m USING(source_id,message_id) WHERE regexp_extract(filename,'([0-9]{20})/[^/]+$',1)=m.seq) TO $dest (FORMAT PARQUET,COMPRESSION ZSTD)",{'files':files('state_events'),'dest':str(destination/'state_events.parquet')})
        for name,keys in [('current_state','source_id,state_uuid'),('hourly_state','source_id,state_uuid,hour_utc')]:
            c.execute(f'COPY (SELECT * EXCLUDE(filename) FROM read_parquet($files,filename=true) QUALIFY row_number() OVER(PARTITION BY {keys} ORDER BY filename DESC)=1) TO $dest (FORMAT PARQUET,COMPRESSION ZSTD)',{'files':files(name),'dest':str(destination/(name+'.parquet'))})
    finally:c.close()
    manifest=dict(batches[-1][1])
    manifest['members']=[{'batch_id':m['pipeline_id']+':'+str(m['sequence']),**m} for _,m in batches]
    return manifest


def run(outbox,database='my_db',schema='loxone_silver_local',max_batches=100,max_seconds=180,connect=None):
    target=identifier(database)+'.'+identifier(schema)
    root=Path(outbox);root.mkdir(parents=True,exist_ok=True)
    with lock(root.parent/'pipeline.lock'),lock(root/'publish.lock'):
        directories=sorted(p.parent for p in root.glob('*/manifest.json') if not (p.parent/'ACK').exists())
        if not directories:return {'published':0}
        start=time.monotonic();batches=[];size=0
        for directory in directories[:max_batches]:
            if time.monotonic()-start>=max_seconds:break
            m=validate_batch(directory);batches.append((directory,m))
            size+=sum((directory/name).stat().st_size for name in FILES)
            if size>=16*1024*1024:break
        if not batches:return {'published':0}
        # Cloud connection exists only in this publisher, never in the local worker.
        c=(connect or (lambda:duckdb.connect('md:'+database,config={'threads':1,'memory_limit':'96MB','extension_directory':str(root.parent/'extensions')})))()
        try:
            c.execute(f'CREATE SCHEMA IF NOT EXISTS {target}')
            c.execute(f'CREATE TABLE IF NOT EXISTS {target}.published_batches(batch_id VARCHAR PRIMARY KEY,created_at TIMESTAMPTZ,published_at TIMESTAMPTZ,messages BIGINT,events BIGINT)')
            ids=[m['pipeline_id']+':'+str(m['sequence']) for _,m in batches]
            known={r[0] for r in c.execute(f'SELECT batch_id FROM {target}.published_batches WHERE batch_id IN (SELECT unnest(?))',[ids]).fetchall()}
            fresh=[]
            for directory,m in batches:
                if m['pipeline_id']+':'+str(m['sequence']) in known:acknowledge(directory)
                else:fresh.append((directory,m))
            if fresh:
                with tempfile.TemporaryDirectory(prefix='.publish-',dir=root) as tmp:
                    manifest=combine_batches(fresh,Path(tmp))
                    publish_batch(c,Path(tmp),manifest,target)
                for directory,_ in fresh:acknowledge(directory)
        finally:c.close()
        result={'published':len(batches),'pending':len(directories)-len(batches),'elapsed_seconds':round(time.monotonic()-start,2)}
        LOG.info('publish_summary %s',json.dumps(result));return result


def main():
    argparse.ArgumentParser(description=__doc__).parse_args()
    logging.basicConfig(level=logging.INFO,format='%(asctime)s %(levelname)s %(message)s')
    try:
        if not os.environ.get('MOTHERDUCK_TOKEN','').strip(): raise ValueError('Missing token')
        run(os.environ.get('LOCAL_SILVER_OUTBOX','/srv/raspi-data/loxone-silver/outbox'),
            os.environ.get('PUBLISH_DATABASE','my_db'),os.environ.get('PUBLISH_SCHEMA','loxone_silver_local'),
            int(os.environ.get('PUBLISH_MAX_BATCHES','10')),int(os.environ.get('PUBLISH_MAX_SECONDS','120')))
    except BlockingIOError: LOG.info('publish_skipped already_running=true')
    except Exception as exc:
        LOG.error('publish_error type=%s',type(exc).__name__);raise SystemExit(1) from None

if __name__=='__main__':main()
