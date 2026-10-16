# Local Silver: Pi computes, MotherDuck serves prepared results

This opt-in pipeline replaces the cloud SQL refresh for this installation. The
collector remains in service; the old Bronze uploader can stay disabled in
local-only mode. No existing cloud
Silver tables are dropped or repurposed. The ingest account publishes into its
own `my_db.loxone_silver_local`, distinct from the personal account's `my_db`.

## Data path and limits

- Collector -> existing SQLite spool -> independent immutable, date-partitioned
  Parquet Bronze archive. The archiver uses an in-memory DuckDB connection to
  write bounded files; it never opens the persistent Silver working database.
- Local Python maps structure JSON without duplicating whole control trees in SQL.
  State/details/action/control-key priorities, inherited metadata, temporal
  resolution, ambiguity and late-structure replay follow the existing Silver rules.
- DuckDB computes events, current state and hourly numeric summaries locally.
- Immutable, checksummed Parquet batches wait in an outbox. A separate publisher
  combines up to 100 batches (16 MiB input soft bound) locally, then sends one
  atomic cloud transaction. Upload acknowledgement is retry-safe. It does not
  remotely parse Bronze JSON or remap history.
- MotherDuck still consumes compute for ingestion and queries. An hourly timer
  limits connection frequency; no promise of a fixed compute saving is made.
- Offline Silver worker has `PrivateNetwork=true`, one DuckDB thread, 96 MB DuckDB memory,
  240 MiB process limit, no swap, 75% of one CPU and low priority. Publisher and
  worker share a local resource lock. The archiver has its own lock, 180 MiB
  process limit, no swap, 50% of one CPU and idle I/O priority. It can run while
  Silver is stopped. Bronze collection is not stopped for local refreshes.
- Work is message-bounded, not event-bounded: one reconnect can contain many states.
  SQLite/DuckDB commits and hard service timeouts make retries safe. Free space below
  2 GiB stops new archival. Monitor failures and growth; disk is not an infinite archive.
- Local data lives at `/srv/raspi-data/loxone-silver/silver.duckdb`,
  `motherduck-bronze/` and `outbox/`. `loxone_bronze.parquet_ws_messages` and
  `loxone_bronze.parquet_structures` expose the raw archive through DuckDB.
  Processed raw messages remain in the DuckDB working copy for 30 days by default,
  then leave it in bounded batches only after Parquet has caught up. A structure
  arriving later than that working window requires an explicit historical replay
  from Parquet if old Silver mappings must change.
  Both jobs use `loxonebronze` to avoid widening access to the collector spool.
  Only the publisher loads `/etc/loxone-bronze/motherduck.env` through systemd.
  No credential is copied to Git, arguments or a new environment file.

`state_events` preserves individual events. `current_state` is materialized,
not a full-history window query in MotherDuck. `hourly_state` contains event count,
numeric count/min/max/mean/first/last by source/state/hour. These are sample-based
summaries, not time-weighted averages or calculated energy consumption. Cloud
`published_batches` records the source batch IDs and counts.

The legacy `bronze_ingested_at` event field is the local archive ingestion time
for this pipeline, not the exact MotherDuck Bronze ingestion time.

## Retention and crash recovery

The spool's `local_silver_checkpoints` and `parquet_archive_checkpoints` tables
form the local retention gate. The independent archiver updates only the Parquet
checkpoint and never prunes SQLite rows. Local-only pruning requires both the Silver working
copy and the checksummed Parquet file to have acknowledged a row. Parquet files
are written below `motherduck-bronze/ws_messages/date=YYYY-MM-DD/` and exposed only
after an atomic rename; an adjacent manifest records row count, time bounds and
SHA-256. Structures use the same file/manifest protocol without daily directories.
The local cursor commits in DuckDB BEFORE its SQLite acknowledgement. Imports use
stable message IDs, making a crash between those commits safe to retry. The last
row in each SQLite source table is retained so future rowids remain monotonic.
Do not VACUUM/rebuild/replace the spool while the rowid-based consumer is active.

The archive is not backed up merely because a local write succeeded. Preserve the
Parquet tree, manifests, DuckDB file and its WAL. Restoring any component older than
its source acknowledgement requires reconciliation before resuming. Do not remove
the retention gate just to unblock pruning. Already-pruned historical data is not
recoverable locally.

When raw Bronze upload to MotherDuck is intentionally retired, set
`LOCAL_SILVER_PRUNE_LOCAL_ONLY=1`. The offline worker then prunes only rows older
than `LOCAL_SILVER_SPOOL_RETENTION_DAYS` whose local archive acknowledgement is
durable. It does not mark them as cloud-uploaded. Enable this mode before stopping
`loxone-bronze-uploader.timer`; otherwise the SQLite spool cannot be reclaimed.

An outbox manifest is exposed only after the data/queue transaction commits.
Crashes before commit may leave an unexposed directory; the next attempt replaces
that directory. A committed batch missing its manifest is finalized on restart.
Cloud commits include every component batch's ledger entry. Lost local ACKs are
recovered from the cloud ledger before combining fresh batches, preventing an old
retry from overwriting newer current-state values. Acknowledged Parquet transport
copies are removed; local archived Bronze and Silver remain.

## Deployment and activation

Before starting Silver on a small Pi, check that `memory` appears in
`/sys/fs/cgroup/cgroup.controllers`. A configured systemd `MemoryMax` does not
protect the host when the kernel memory controller is disabled. On current
Raspberry Pi kernels, append `cgroup_enable=memory` to the single line in
`/boot/firmware/cmdline.txt`, after backing up that file, and perform a controlled
reboot. Verify the controller and a running service's numeric `MemoryCurrent`
afterwards. A reboot interrupts the collector and other Pi services.

Normal Silver startup reuses existing Parquet archive views instead of opening
every archived file's footer. Their stored globs still discover newly archived
files when queried. Explicit archival/view refreshes can still be expensive.
Event mapping considers only the structure versions needed by the current batch;
hourly aggregation restricts scans to the complete hours touched by that batch.
The local queue selects oldest receipts first and acknowledges exactly that same
slice. Its ledger lookup is materialized for the selected keys, avoiding a large
correlated hash table over all processed history. Committed spool imports close
and reopen their DuckDB connection before event transformation to release index
buffers; a failed reopen leaves the committed rows queued for the next run.
The chosen raw slice is also materialized, with receipt bounds, before JSON
predicates run. This prevents predicates pushed through a view from parsing all
historical raw payloads. Newly transformed rows carry their explicit transaction
load timestamp and feed the latest-state update and event transport directly;
those steps do not reread the entire persistent event history. Hourly refreshes
still include previously processed events from the touched hours.

New local raw working archives have no primary-key ART index. The worker holds
both pipeline locks and excludes already archived keys inside the import
transaction; the SQLite spool supplies unique message IDs. This avoids keeping a
large, non-spillable raw-key index in RAM. Existing indexed archives require an
explicit one-time migration with `scripts/retire-local-archive-index.py`, stopped
Silver timers, and effective CPU/RAM/I/O limits. Pass the verified row count with
`--expected-rows` and the existing file with `--database`. The script streams a
constraint-free copy, checks counts, column names/types and unchanged import
cursors, then atomically swaps names and restores the working view. It retains
the entire original table as `archive_messages_before_index_retirement` and
refuses to overwrite an earlier migration. It does not delete spool or event
data. Keep the existing independent backup as well. A failed COMMIT may already
have ended the transaction; error handling preserves its original cause rather
than masking it with a second rollback error.

These changes do not guarantee that a large historical backlog can be processed
fast enough on a 1 GB Pi. Validate individual runs under effective CPU/memory
limits before enabling timers, and compare throughput with incoming traffic.

For a Pi 3 with effective memory cgroups, the optional
`config/local-silver-pi3.env.example` profile uses 250 imported/selected messages,
96 MB DuckDB memory and exactly one batch per process. Merge its values into the
existing configuration, preserving paths and checkpoints. Multiple batches in
one process can succeed initially and then exhaust the internal memory budget;
reopening the connection alone does not release all native allocations. Install
`systemd/loxone-local-silver-pi3.service.conf` as a service drop-in and
`systemd/loxone-local-silver-pi3.timer.conf` as a timer drop-in, then reload
systemd. They cap the service at 200 MiB, disable swap, allow half of one CPU
core, check the memory controller, enforce a 600-second hard timeout and pause
ten seconds after each process. The 30-second application budget is checked
between batches and does not interrupt an individual batch. A WAL-triggered
checkpoint can make COMMIT or connection close exceed two minutes under these
resource caps; repeatedly killing it prevents progress. The hard bound allows
that occasional checkpoint to finish without increasing CPU or RAM limits.
Phase start/end logs identify database open, import/transform commit and close,
including elapsed time without SQL, payloads or exception text. Do not activate
the timer until individual runs pass; observe repeated runs and collector
freshness before enabling it across reboots. The profile does not enable pruning
or publication.

For the separate publisher, the optional
`systemd/loxone-silver-publish-pi3.service.conf` drop-in sets a 240 MiB hard
limit, no swap, a quarter of one CPU core and a four-minute hard timeout.
It bounds each invocation to 20 prepared batches using `/usr/bin/env` in
`ExecStart`: systemd `EnvironmentFile` values override `Environment` values,
so a batch limit set only with `Environment=` may not take effect. The companion
`systemd/loxone-silver-publish-pi3.timer.conf` waits five minutes after completion
and retains a 15-minute boot trigger after resetting the monotonic timer list.
Install it with a name that sorts after existing timer drop-ins, such as
`zz-bounded-cadence.conf`, so an older override cannot replace its cadence.
The pipeline lock serializes publication with local computation. Validate
publication, cloud ledger acknowledgements and duplicate-free retries before
enabling this timer. For an exact file-to-cloud comparison, pause the local
Silver timer and drain its current job: the next local job normally deletes
Parquet files for acknowledged batches. Resume the local timer after checking.
Keep credentials in the existing protected environment file.
The Pi publisher profile enables Python's fault handler, so a native crash can
record thread stacks in the journal without dumping local variables or creating
a full process core. This aids diagnosis; it does not repair a native crash.

The existing GitHub workflow and bridge v3 remain the deployment path. A root-owned
`/etc/loxone-silver/local.enabled` marker opts into the local installer after the
normal Silver installation. The root installer links the immutable release at
`/opt/loxone-local-silver/current`, preserves configuration and installs four units.
It drains existing local jobs, preserves timer state and restores units/symlink
if activation fails. First installation does not enable timers. Bronze is upgraded
before the local retention gate is created.

1. Review/merge this change and create `local.enabled` as root. The old
   `loxone-silver-refresh.timer` and service must be stopped.
2. Run **Deploy Loxone Bronze + Silver** on main. No bridge/sudoers expansion is needed.
3. Run `sudo systemctl start loxone-bronze-archive.service` and inspect its
   summary, Parquet manifests, archive checkpoint and collector health. It does
   not open the persistent Silver DuckDB or call MotherDuck. Enable
   `loxone-bronze-archive.timer` after a bounded run succeeds, even when the
   Silver timer must remain stopped.
4. Run `sudo systemctl start loxone-local-silver.service` only after its
   database and resource limits have been validated. Inspect the summary,
   local event counts and collector health. This performs no cloud calls.
5. After confirming the ingest token and isolated destination, run
   `sudo systemctl start loxone-silver-publish.service`. Check cloud event/current/
   hourly counts and the batch ledger; retry once to verify no duplicates.
6. Enable the Silver and publication timers only after those checks succeed.

On restart, imports reconcile their committed DuckDB cursor with the SQLite
acknowledgement. A failed transformation transaction leaves its selected queue
keys intact; the next invocation reuses its sequence and replaces unpublished
transport files. A committed queue entry can restore a missing manifest.
Publication records the same batch IDs in the cloud transaction before local
ACKs, so retries after a partial acknowledgement do not insert another copy.
Hourly outputs replace the affected keys rather than incrementing stored counts.

Config: `/etc/loxone-silver/local.env`. Local error logs include a reason category,
commit phase when recognized and the originating function/line, without logging
the exception text. Service logs intentionally omit error text
that might contain credentials. Diagnostic scripts must redact secrets and must
not print environment files or token-bearing connection strings.

## Historical coverage and rollback

Local queues store receipt timestamps directly so selecting a small batch does
not join the entire queue to the archive. New local queues and processing ledgers
have no primary-key ART indexes: the exclusive pipeline/database locks and
transactional import guards and message replacement preserve uniqueness.
Cloud processing keeps its primary keys. A newly mapped structure registers a
resumable archive scan; each iteration examines at most `LOCAL_SILVER_IMPORT_SIZE`
raw rows and queues only messages whose ledger mapping differs. Existing queued
keys are retained without duplicates. Working-archive pruning waits for this scan.

An existing two-column local queue requires the explicit migration
`scripts/migrate-local-work-indexes.py --database PATH --expected-queue COUNT
--expected-ledger COUNT`. Stop the local worker and publisher timers, drain both
jobs and obtain the current counts first. Run the migration with effective CPU,
RAM and I/O caps. It verifies queue keys and all ledger columns in both directions,
preserves import cursors and publication metadata, and swaps table names in one
transaction. Original tables remain as `process_queue_before_work_migration` and
`loxone_silver.processed_messages_before_work_migration`. Never drop them as part
of a trial. A failed pre-swap migration leaves the active tables unchanged and
refuses to overwrite its shadows on retry; inspect those artifacts first.
The worker refuses the old queue format until migration succeeds. If an archive
is rebuilt while a remapping scan is pending, restart the scan with
`enqueue_remapping` under the same locks, because its scan cursor uses local rowids.

Initial archival starts at the oldest retained local message. Older cloud-only
Bronze history needs a separate bounded backfill; this is not silently downloaded
or transformed when cloud compute is exhausted. Old events preceding all retained
structures use the explicitly marked earliest-structure fallback until historical
structures are imported. Do not present that fallback as proven historical metadata.

Stop all three local timers and drain their jobs before changing their release or
restoring files. Keep the retention gate: it prevents data loss while local work is
paused. The separate cloud SQL timer must remain disabled during this transition.
Switching code never deletes cloud tables or the local archive.

An uncommitted transformation that exceeds the DuckDB memory budget rolls back
and retries with a fresh connection and half the message count, down to one
message. The CPU/RAM caps remain unchanged. A single-message failure still stops
the job with an error; retries never skip or acknowledge failed messages.

Large local state snapshots are decoded one selected message at a time into
128-row insert chunks before the SQL window and mapping join. This avoids
json_each lateral expansion retaining the full JSON snapshot per state.
Duplicate object members keep last-key-wins event semantics, while nested JSON
objects retain their member order and duplicate members. Cloud SQL is unchanged.

Publication files and their directories are synced before the database commit.
Ready manifests use a synced temporary file and atomic replacement, followed
by a directory sync. Missing, empty, malformed or mismatching manifests are
recovered from committed publication metadata only after all transport-file
checksums match. Damaged transport data fails closed without acknowledgement.
