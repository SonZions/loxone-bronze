#!/usr/bin/env python3
"""Offline, locked migration of local queue/ledger; preserve original tables.

Stop the worker and publisher timers first. Run with effective CPU/RAM/I/O
limits. Shadow copies are verified before the transactional name swap.
"""
import argparse
from pathlib import Path
import duckdb
from loxone_bronze.local_silver import lock


def migrate(database, expected_queue, expected_ledger):
    database=Path(database)
    if not database.is_file():raise ValueError('Existing database required')
    with lock(str(database)+'.lock'),lock(str(database.parent/'pipeline.lock')):
        # WAL recovery can load the old ART indexes. Finish and checkpoint it
        # with the existing worker budget, then reopen without those buffers.
        recovery=duckdb.connect(str(database),config={'threads':1,'memory_limit':'96MB'})
        try:recovery.execute('CHECKPOINT')
        finally:recovery.close()
        print('recovery_checkpoint_complete',flush=True)
        c=duckdb.connect(str(database),config={'threads':1,'memory_limit':'64MB'})
        try:
            c.execute('SET preserve_insertion_order=false')
            c.execute("SET max_temp_directory_size='2GB'")
            if c.execute("SELECT 1 FROM duckdb_tables() WHERE table_name IN ('process_queue_before_work_migration','processed_messages_before_work_migration','process_queue_shadow','processed_messages_shadow')").fetchone():
                raise RuntimeError('Previous migration tables exist; inspect before retrying')
            if c.execute('SELECT count(*) FROM process_queue').fetchone()[0]!=expected_queue:
                raise RuntimeError('Queue count changed')
            if c.execute('SELECT count(*) FROM loxone_silver.processed_messages').fetchone()[0]!=expected_ledger:
                raise RuntimeError('Ledger count changed')
            cursors=c.execute('SELECT * FROM import_progress ORDER BY table_name').fetchall()
            metadata=c.execute('SELECT * FROM local_metadata ORDER BY key').fetchall()
            publications=c.execute('SELECT * FROM publication_queue ORDER BY sequence').fetchall()
            print('migration_start',flush=True)
            c.execute('''CREATE TABLE process_queue_shadow AS
              SELECT q.source_id,q.message_id,a.received_at FROM process_queue q
              JOIN loxone_bronze.archive_messages a USING(source_id,message_id)
              ORDER BY a.received_at,q.source_id,q.message_id''')
            if c.execute('SELECT count(*) FROM process_queue_shadow').fetchone()[0]!=expected_queue:
                raise RuntimeError('Missing or duplicated archive key for queue')
            for left,right in [('process_queue','process_queue_shadow'),('process_queue_shadow','process_queue')]:
                if c.execute(f'SELECT 1 FROM (SELECT source_id,message_id FROM {left} EXCEPT ALL SELECT source_id,message_id FROM {right}) LIMIT 1').fetchone():
                    raise RuntimeError('Queue keys differ')
            print('queue_verified',flush=True)
            c.execute('''CREATE TABLE loxone_silver.processed_messages_shadow(
              source_id VARCHAR,message_id VARCHAR,structure_id VARCHAR,
              processed_at TIMESTAMPTZ DEFAULT now())''')
            c.execute('INSERT INTO loxone_silver.processed_messages_shadow SELECT * FROM loxone_silver.processed_messages')
            for left,right in [('processed_messages','processed_messages_shadow'),('processed_messages_shadow','processed_messages')]:
                if c.execute(f'SELECT 1 FROM (SELECT * FROM loxone_silver.{left} EXCEPT ALL SELECT * FROM loxone_silver.{right}) LIMIT 1').fetchone():
                    raise RuntimeError('Ledger rows differ')
            print('ledger_verified',flush=True)
            c.execute('BEGIN')
            try:
                c.execute('ALTER TABLE process_queue RENAME TO process_queue_before_work_migration')
                c.execute('ALTER TABLE process_queue_shadow RENAME TO process_queue')
                c.execute('ALTER TABLE loxone_silver.processed_messages RENAME TO processed_messages_before_work_migration')
                c.execute('ALTER TABLE loxone_silver.processed_messages_shadow RENAME TO processed_messages')
                c.execute('''CREATE OR REPLACE VIEW loxone_bronze.ws_messages AS
                  SELECT a.* FROM loxone_bronze.archive_messages a JOIN
                  (SELECT source_id,message_id FROM process_queue ORDER BY received_at,source_id,message_id LIMIT 250) q
                  USING(source_id,message_id)''')
                if c.execute('SELECT * FROM import_progress ORDER BY table_name').fetchall()!=cursors:
                    raise RuntimeError('Import cursors changed')
                if c.execute('SELECT * FROM local_metadata ORDER BY key').fetchall()!=metadata:
                    raise RuntimeError('Metadata changed')
                if c.execute('SELECT * FROM publication_queue ORDER BY sequence').fetchall()!=publications:
                    raise RuntimeError('Publications changed')
                c.execute('COMMIT')
            except BaseException:
                try:c.execute('ROLLBACK')
                except duckdb.TransactionException:pass
                raise
            print(f'migration_complete queue={expected_queue} ledger={expected_ledger} originals_preserved=true',flush=True)
        finally:c.close()


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',required=True)
    p.add_argument('--expected-queue',type=int,required=True)
    p.add_argument('--expected-ledger',type=int,required=True)
    args=p.parse_args();migrate(args.database,args.expected_queue,args.expected_ledger)
