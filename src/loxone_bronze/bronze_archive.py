"""Archive collector spool rows to durable Parquet without opening Silver DuckDB."""
from __future__ import annotations

from dataclasses import dataclass
import json
import logging
import os
from pathlib import Path
import shutil
import time

import duckdb

from .local_silver import LocalConfig, TABLES, archive_spool, lock


LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class ArchiveConfig:
    spool: str
    bronze_archive: str
    archive_size: int = 1000
    max_batches: int = 4
    max_seconds: int = 45
    memory_mb: int = 96
    min_free_mb: int = 2048

    @classmethod
    def from_env(cls):
        local = LocalConfig.from_env()
        return cls(
            spool=local.spool,
            bronze_archive=local.bronze_archive,
            archive_size=int(os.environ.get('BRONZE_ARCHIVE_CHUNK_SIZE', 1000)),
            max_batches=int(os.environ.get('BRONZE_ARCHIVE_MAX_BATCHES', 4)),
            max_seconds=int(os.environ.get('BRONZE_ARCHIVE_MAX_SECONDS', 45)),
            memory_mb=int(os.environ.get('BRONZE_ARCHIVE_MEMORY_MB', 96)),
            min_free_mb=local.min_free_mb,
        )

    def validate(self):
        if not 1 <= self.archive_size <= 5000 or not 1 <= self.max_batches <= 100:
            raise ValueError('Invalid archive work limit')
        if not 1 <= self.max_seconds <= 90 or not 32 <= self.memory_mb <= 128:
            raise ValueError('Invalid archive resource limit')
        if self.min_free_mb < 0:
            raise ValueError('Invalid minimum free space')


def run(cfg: ArchiveConfig):
    cfg.validate()
    root = Path(cfg.bronze_archive)
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    with lock(str(root / '.archive.lock')):
        c = duckdb.connect(config={'threads': 1, 'memory_limit': f'{cfg.memory_mb}MB'})
        try:
            c.execute('SET preserve_insertion_order=false')
            started = time.monotonic()
            archived = dict.fromkeys(TABLES, 0)
            for _ in range(cfg.max_batches):
                if time.monotonic() - started >= cfg.max_seconds:
                    break
                if shutil.disk_usage(root).free < cfg.min_free_mb * 1024**2:
                    raise RuntimeError('Insufficient free space for Bronze archive')
                counts = {table: archive_spool(c, cfg, table, refresh_views=False)
                          for table in TABLES}
                for table, count in counts.items():
                    archived[table] += count
                if not any(counts.values()):
                    break
            result = {'archived': archived,
                      'elapsed_seconds': round(time.monotonic() - started, 2)}
            LOG.info('archive_summary %s', json.dumps(result))
            return result
        finally:
            c.close()


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    try:
        run(ArchiveConfig.from_env())
    except BlockingIOError:
        LOG.info('archive_skipped already_running=true')
    except Exception as exc:
        LOG.error('archive_error type=%s', type(exc).__name__)
        raise SystemExit(1) from None


if __name__ == '__main__':
    main()
