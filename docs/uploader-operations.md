# Upload backlog recovery

## Cause and implementation

The old one-batch timer can acknowledge at most 1,000 × 288 = 288,000
messages/day before accounting for failures. That cannot keep up with
300,000–323,000 arrivals/day. `executemany` adds per-row staging overhead.
The old marking code also opens 20 transactions per 5,000-row batch.

Tracked `build/lib` and egg-info artifacts were removed so packages are built
from `src/`. Deployment removes any old excluded build directory before packaging.

The uploader now drains bounded batches using defaults:

| Parameter | Default |
| --- | ---: |
| `UPLOAD_BATCH_SIZE` | 5000 |
| `UPLOAD_MAX_BATCHES_PER_RUN` | 12 |
| `UPLOAD_MAX_RUNTIME_SECONDS` | 240 |
| `LOCAL_RETENTION_DAYS` | 7 |

Only one batch is read into Python at a time. Each becomes a private temporary
JSONL file, consumed by one `INSERT OR IGNORE ... SELECT ... FROM read_json(...)`.
There are no per-row remote inserts, temporary remote tables or extra dependencies.
The envelope treats `payload_json` as a string, so whitespace, Unicode, escapes,
large integers and SQL NULL versus empty strings survive. An explicit DuckDB cast
validates JSON. The UUID primary key retains retry idempotency.

The INSERT runs in autocommit. Only its successful return permits the local ack.
One `_write` transaction marks the whole batch (internal 250-row chunks). Failure
rolls the local transaction back. A crash after remote commit retries those UUIDs;
previously acknowledged batches stay acknowledged. Failure details record only
the exception class, since driver exception text can contain secrets or payloads.

The time budget covers connection/setup and the drain. It is checked between
batches, including before the first one, so an in-flight batch can finish safely.
The oneshot hard timeout is 270 seconds plus at most 15 seconds to stop a hung
process. A hard kill may omit `upload_summary`; systemd records the timeout and
unacknowledged IDs are safe to retry. If you increase the 240-second budget, also
review the service timeout and timer interval together.

The five-minute `OnUnitActiveSec` timer and `AccuracySec=15s` are unchanged.
The same systemd unit never runs in parallel; a per-spool `flock` also excludes
manual uploader invocations. Do not run the uploader under different spool paths
pointing at the same database. Normal cleanup uses `TemporaryDirectory`; systemd
`PrivateTmp=true` also cleans private temporary files after service termination.
A manually SIGKILLed run outside systemd can leave a `loxone-upload-*` directory
in its temporary directory; remove only after verifying no uploader uses it.

SQLite read connections close before network I/O. WAL, 10-second busy timeout,
and bounded contention retries remain. Acknowledgement briefly takes the writer
lock for one batch. Retention deletion now uses the existing upload index and a
bounded batch per table, instead of one potentially huge delete transaction.

Status skips historical totals for uploader/health. Exact pending counts use the
existing `(uploaded_at, received_at)` covering index; oldest pending is an indexed
lookup. A new `received_at` index supports global latest-event lookups and the
verification report's incoming-rate count. Counts still cost O(pending rows),
but do not read payloads or scan all retained messages. Index creation is a
one-time operation that needs disk space and the SQLite writer lock. The deployment
bridge creates it while the collector is stopped, before the collector health
check. Allow a maintenance window for the existing multi-million-row spool.

## Existing production configuration: explicit one-time change

Neither `install.sh` nor the deployment bridge overwrites existing environment
files. The old deploy drop-in also contained `UPLOAD_BATCH_SIZE=1000`; its defaults
are updated, but **EnvironmentFile takes precedence over Environment**. Therefore
updating the example or the drop-in alone does not change an existing env value.

Run this on the Raspberry. It changes exactly three keys, preserves all other
lines byte-for-byte, preserves ownership/mode, writes atomically, and prints no
file contents or secrets. Stop the timer and wait for any active upload to finish
before the deployment step below; the command affects subsequent processes.

```bash
sudo python3 - <<'PY'
import os
import re
import stat
import tempfile
from pathlib import Path

path = Path('/etc/loxone-bronze/motherduck.env')
values = {b'UPLOAD_BATCH_SIZE': b'5000',
          b'UPLOAD_MAX_BATCHES_PER_RUN': b'12',
          b'UPLOAD_MAX_RUNTIME_SECONDS': b'240'}
info = path.stat()
original = path.read_bytes()
lines = original.splitlines(keepends=True)
pattern = re.compile(rb'^\s*(UPLOAD_BATCH_SIZE|UPLOAD_MAX_BATCHES_PER_RUN|UPLOAD_MAX_RUNTIME_SECONDS)\s*=')
kept = [line for line in lines if not pattern.match(line)]
updated = b''.join(kept)
if updated and not updated.endswith(b'\n'):
    updated += b'\n'
updated += b''.join(key + b'=' + value + b'\n' for key, value in values.items())
fd, name = tempfile.mkstemp(prefix='.motherduck.env.', dir=path.parent)
try:
    with os.fdopen(fd, 'wb') as out:
        os.fchown(out.fileno(), info.st_uid, info.st_gid)
        os.fchmod(out.fileno(), stat.S_IMODE(info.st_mode))
        out.write(updated)
        out.flush()
        os.fsync(out.fileno())
    os.replace(name, path)
    directory = os.open(path.parent, os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
finally:
    if os.path.exists(name):
        os.unlink(name)
print('Uploader parameters set: batch=5000, batches=12, runtime=240s')
PY
```

Optional health settings go into the same file (use `sudoedit`, never `cat` or
`systemctl show -p Environment` on credential-bearing units):

```ini
HEALTH_MAX_EVENT_AGE_MINUTES=10
HEALTH_MAX_PENDING_MESSAGES=100000
HEALTH_MAX_PENDING_AGE_MINUTES=30
HEALTH_CHECK_BACKLOG=true
```

Health reports `collector_stale` independently of `upload_backlog`. All limits are
configurable; equality is healthy. `HEALTH_CHECK_BACKLOG=false` disables only the
backlog checks, as required by deployment recovery. During catch-up, a backlog
health failure is expected and does not prove the collector is broken.

## Deployment

### Recovery from the locked-healthcheck deployment failure

Health now opens an existing spool with SQLite `mode=ro`, without running schema
DDL, setting journal mode or creating missing files/directories. It can read the
last committed WAL snapshot while another connection holds a write transaction,
including on an older spool without the latest-event index. A missing/unreadable
spool or schema reports `spool_unavailable` and exits nonzero without a traceback.
Do not use `immutable=1` for this live database: committed WAL updates must remain
visible. Read-only refers to database contents; SQLite may still use WAL/SHM
coordination files. The service account already owns the spool directory.

Before redeploying, merge this correction and refresh the separately installed
bridge using the commands in step 4 below. Confirm:

```bash
/usr/local/sbin/loxone-bronze-deploy --version
```

Expected: `loxone-bronze-deploy-v3`. The workflow now refuses an old bridge before
stopping any services. It does not grant the runner extra sudo rights or replace
the root-owned bridge automatically. The bridge stops all writers, performs the
schema migration, starts the collector, checks its health, and only then resumes
the upload timer. Existing environment files/secrets remain untouched.

1. Review/merge the PR into `main`. The change does not deploy merely by pushing.
   The workflow is still manual, with the existing production environment gate.
2. Apply the three production parameters above. Do not display environment files.
3. Run the **Tests** workflow and confirm green. **Deploy Loxone Bronze** now also
   runs the suite on a hosted runner before touching the ARM64 production runner.
4. Dispatch **Deploy Loxone Bronze** against the exact current `main`. The bridge
   stops timer/uploader/collector, installs the package and both unit definitions,
   builds the status index, restarts the collector, then checks fresh events with
   backlog excluded. It restores the last known good revision on failure. Existing
   secrets survive. The updated bridge must be installed at
   `/usr/local/sbin/loxone-bronze-deploy` before this deployment, because that path
   is a separately installed script, not a live reference to the repository:

   ```bash
   sudo -u loxberry git -C /srv/raspi-data/repos/loxone-bronze fetch origin main
   bridge_review_dir="$(mktemp -d)"
   sudo -u loxberry git -C /srv/raspi-data/repos/loxone-bronze show origin/main:scripts/deploy-from-github.sh > "$bridge_review_dir/deploy.sh"
   less "$bridge_review_dir/deploy.sh"
   sudo install -o root -g root -m 0755 "$bridge_review_dir/deploy.sh" /usr/local/sbin/loxone-bronze-deploy
   rm "$bridge_review_dir/deploy.sh"
   rmdir "$bridge_review_dir"
   ```

   A trusted admin should run this after reviewing the merged revision. The bridge
   still refuses anything other than the exact approved `origin/main` SHA. The
   workflow timeout is thirty minutes, including the separate Silver phase; a very large first index build may need
   a longer planned maintenance window. `install.sh` is for initial installation;
   do not run it over a live deployment instead of the controlled bridge.
5. Enable the timer if it was previously disabled, and trigger one run:

   ```bash
   sudo systemctl enable --now loxone-bronze-uploader.timer
   sudo systemctl start loxone-bronze-uploader.service
   ```

## Verification commands on the Raspberry

Effective configuration from the actual process, not just from the example:

```bash
sudo journalctl -u loxone-bronze-uploader.service --since '30 minutes ago' --no-pager -o cat | grep 'upload_config '
```

Expect `batch_size: 5000`, `max_batches_per_run: 12`, `max_runtime_seconds: 240`.
Only safe numeric settings are logged. A value of 1000 means an active configuration
override still exists. Do not dump the complete environment to investigate it.

Pending count, oldest age, latest local event, real wall-clock rate and ETA:

```bash
sudo -u loxonebronze /opt/loxone-bronze/venv/bin/python /opt/loxone-bronze/app/scripts/upload-report.py --window-minutes 60
sudo bash /opt/loxone-bronze/app/scripts/healthcheck.sh
```

Use a **full post-deployment window**, ideally one hour / twelve timer cycles,
while backlog is nonempty. A window containing pre-deployment time underestimates
new throughput. The report counts local acknowledgements, including successful
idempotent retries, rather than counting only new remote rows. It includes timer
pauses, failures and startup time. `eta_hours: null` with pending rows means the
measured rate cannot catch up. Incoming rate for the estimate is the larger of
measured arrivals and the conservative 323,000/day baseline; override the latter
with `--ingress-per-day` when justified. After the queue empties the observed rate
tracks demand and no longer estimates maximum upload capacity.

Per-run/batch throughput and stop reason:

```bash
sudo journalctl -u loxone-bronze-uploader.service --since '1 hour ago' --no-pager -o cat | grep -E 'upload_(config|start|batch|summary|error|startup_error|close_error|status_error|skipped) '
systemctl is-active loxone-bronze-collector.service loxone-bronze-uploader.timer
systemctl show loxone-bronze-uploader.service -p Result -p ExecMainStatus -p TimeoutStartUSec
systemctl list-timers --all loxone-bronze-uploader.timer
sudo journalctl -u loxone-bronze-collector.service -u loxone-bronze-uploader.service --since '1 hour ago' -p warning --no-pager
```

An idle oneshot is normally `inactive`; check `Result=success`, timer state and
summaries, not continuous uploader activity. Watch collector latest timestamps
continue moving during uploads. `messages_per_second` in a run summary excludes
inter-run pauses; use the report's `wall_clock_upload_messages_per_second` for ETA.
A hard timeout has no guaranteed summary and appears in systemd logs.

Run this read-only SQL in MotherDuck (adjust the database name if necessary):

```sql
SELECT current_timestamp AS checked_at,
       max(received_at) AS newest_received,
       max(ingested_at) AS newest_ingest,
       date_diff('second', max(received_at), current_timestamp) AS source_lag_seconds
FROM my_db.loxone_bronze.ws_messages;

SELECT count(*) AS newly_inserted_last_hour,
       quantile_cont(date_diff('second', received_at, ingested_at), 0.5) AS median_lag_seconds,
       quantile_cont(date_diff('second', received_at, ingested_at), 0.95) AS p95_lag_seconds
FROM my_db.loxone_bronze.ws_messages
WHERE ingested_at >= current_timestamp - INTERVAL '1 hour';
```

Repeat after an hour. Newest remote receive time should advance faster than wall
clock while draining; pending count and oldest pending age should fall. Quantiles
of historical ingests remain high until the analysis window has moved past recovery.
The aggregate remote queries can scan historical data and cost compute; use for
post-deploy checks, not for each batch. Local acknowledgements alone are not a
substitute for this remote freshness check.

## Capacity and twelve-hour acceptance gate

For 430,000 pending and 323,000 new messages/day:

```
arrival = 323000 / 86400 = 3.7384 messages/s
required = arrival + 430000 / (12 * 3600) = 13.6921 messages/s
ETA hours = pending / (measured wall-clock upload rate - arrival) / 3600
```

About **1.183 million uploads/day**, or **4,108 per five-minute period**, is needed
for the worst-case twelve-hour goal. A single full 5,000-row batch every 315s
(including the timer's 15s accuracy allowance) yields 15.87/s and about 9.84h ETA.
That is a capacity scenario, **not a production measurement**. Twelve such batches
per run add headroom, subject to the runtime, memory, network and MotherDuck limits.

| Measured wall-clock upload rate | ETA for 430,000 pending at 323,000 arrivals/day |
| --- | ---: |
| 10/s | 19.08 h (fails) |
| 14/s | 11.64 h |
| 20/s | 7.35 h |
| 50/s | 2.58 h |

Do not claim twelve-hour recovery until the Raspberry report measures sufficient
sustained rate and the MotherDuck freshness check confirms progress. Local DuckDB
unit tests validate SQL/atomicity, not MotherDuck network throughput or Pi memory.
If measured throughput misses the gate, inspect timeouts, batch timings, payload
sizes, available memory, SQLite contention and service limits before increasing
batch size. Row count is bounded but one unusually large frame can still consume
substantial RAM. An invalid payload fails the whole batch and intentionally blocks
that batch until investigated; it is never silently discarded.

## Tests

```bash
python -m pip install .
python -m unittest discover -s tests -v
bash -n scripts/*.sh
```

Tests exercise real SQLite WAL and DuckDB for bulk SQL, exact payload preservation,
retry/atomicity, bounds, concurrent collector writes, index use and configurable
health limits. No production credentials are required. CI covers Python 3.10/3.11
with DuckDB 1.4.0 and the current dependency resolution.
