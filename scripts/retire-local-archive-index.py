#!/usr/bin/env python3
"""Retire the local raw archive's memory-heavy ART index, keeping the old table.

Stop both Silver timers first. Run under effective CPU, RAM and I/O limits.
The worker's locks and transactional NOT EXISTS import guard provide uniqueness.
No source rows, spool cursors, event rows or publication metadata are deleted.
"""
import argparse
from pathlib import Path
import re
import duckdb
from loxone_bronze.local_silver import lock

SOURCE='loxone_bronze.archive_messages'
SHADOW='loxone_bronze.archive_messages_without_index'
OLD='archive_messages_before_index_retirement'


def retire(database: Path, expected_rows: int):
    if not database.is_file():raise ValueError('Existing database required')
    with lock(str(database)+'.lock'),lock(str(database.parent/'pipeline.lock')):
        c=duckdb.connect(str(database),config={'threads':1,'memory_limit':'48MB'})
        try:
            existing=c.execute("SELECT table_name FROM duckdb_tables() WHERE schema_name='loxone_bronze' AND table_name IN ('archive_messages_without_index',?)",[OLD]).fetchall()
            if existing:raise RuntimeError('Prior index retirement tables exist')
            count=c.execute(f'SELECT count(*) FROM {SOURCE}').fetchone()[0]
            if count!=expected_rows:raise RuntimeError('Archive row count changed')
            view=c.execute("SELECT sql FROM duckdb_views() WHERE schema_name='loxone_bronze' AND view_name='ws_messages'").fetchone()
            if not view or not re.match(r'^CREATE VIEW\s',view[0],re.I):raise RuntimeError('Working archive view missing')
            view_sql=re.sub(r'^CREATE VIEW\s','CREATE OR REPLACE VIEW ',view[0],count=1,flags=re.I)
            cursors=c.execute('SELECT * FROM import_progress ORDER BY table_name').fetchall()
            print(f'retirement_start rows={count}',flush=True)
            # CTAS streams column data and creates no ART index. The original
            # remains intact even if this copy or the following swap fails.
            c.execute(f'CREATE TABLE {SHADOW} AS SELECT * FROM {SOURCE}')
            copied=c.execute(f'SELECT count(*) FROM {SHADOW}').fetchone()[0]
            if copied!=count:raise RuntimeError('Copied row count differs')
            if c.execute(f'DESCRIBE {SOURCE}').fetchall()!=c.execute(f'DESCRIBE {SHADOW}').fetchall():
                # CTAS deliberately removes the key constraint; compare names
                # and types rather than constraint/nullability metadata.
                source=[r[:2] for r in c.execute(f'DESCRIBE {SOURCE}').fetchall()]
                shadow=[r[:2] for r in c.execute(f'DESCRIBE {SHADOW}').fetchall()]
                if source!=shadow:raise RuntimeError('Copied schema differs')
            c.execute('BEGIN')
            try:
                c.execute(f'ALTER TABLE {SOURCE} RENAME TO {OLD}')
                c.execute(f'ALTER TABLE {SHADOW} RENAME TO archive_messages')
                c.execute(view_sql)
                if c.execute(f'SELECT count(*) FROM {SOURCE}').fetchone()[0]!=count:raise RuntimeError('Swapped row count differs')
                if c.execute('SELECT * FROM import_progress ORDER BY table_name').fetchall()!=cursors:raise RuntimeError('Import cursor changed')
                c.execute('COMMIT')
            except BaseException:
                try:c.execute('ROLLBACK')
                except duckdb.TransactionException:pass
                raise
            print(f'retirement_complete rows={count} original=loxone_bronze.{OLD}',flush=True)
        finally:c.close()


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--database',type=Path,required=True)
    p.add_argument('--expected-rows',type=int,required=True)
    args=p.parse_args();retire(args.database,args.expected_rows)
