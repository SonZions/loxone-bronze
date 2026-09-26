# Local Silver: Pi computes, MotherDuck serves prepared results

This opt-in pipeline replaces the cloud SQL refresh for this installation. The
collector and existing Bronze uploader remain in service. No existing cloud
Silver tables are dropped or repurposed. The ingest account publishes into its
own `my_db.loxone_silver_local`, distinct from the personal account's `my_db`.

## Data path and limits

- Collector -> existing SQLite spool -> durable local DuckDB Bronze archive.
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
- Offline worker has `PrivateNetwork=true`, one DuckDB thread, 96 MB DuckDB memory,
  240 MiB process limit, no swap, 75% of one CPU and low priority. Publisher and
  worker share a local resource lock. Bronze is not stopped for local refreshes.
- Work is message-bounded, not event-bounded: one reconnect can contain many states.
  SQLite/DuckDB commits and a hard service timeout make retry safe. Free space below
  2 GiB stops new archival. Monitor failures and growth; disk is not an infinite archive.
- Local data lives at `/srv/raspi-data/loxone-silver/silver.duckdb` and `outbox/`.
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

The spool's `local_silver_checkpoints` table enables a second-consumer retention
gate. Bronze pruning then requires both cloud upload and durable local archival.
The local cursor commits in DuckDB BEFORE its SQLite acknowledgement. Imports use
stable message IDs, making a crash between those commits safe to retry. The last
row in each SQLite source table is retained so future rowids remain monotonic.
Do not VACUUM/rebuild/replace the spool while the rowid-based consumer is active.

The archive is not backed up merely because an upload succeeded. Preserve the
local DuckDB file and its WAL together, using a stopped worker or DuckDB's supported
backup/export process. Restoring an archive older than the source acknowledgement
fails closed; restore/reconcile it before resuming. Do not remove the retention gate
just to unblock pruning. Already-pruned historical data is not recoverable locally.

An outbox manifest is exposed only after the data/queue transaction commits.
Crashes before commit may leave an unexposed directory; the next attempt replaces
that directory. A committed batch missing its manifest is finalized on restart.
Cloud commits include every component batch's ledger entry. Lost local ACKs are
recovered from the cloud ledger before combining fresh batches, preventing an old
retry from overwriting newer current-state values. Acknowledged Parquet transport
copies are removed; local archived Bronze and Silver remain.

## Deployment and activation

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
3. Run `sudo systemctl start loxone-local-silver.service` and inspect its summary,
   result, local event counts and collector health. This performs no cloud calls.
4. After confirming the ingest token and isolated destination, run
   `sudo systemctl start loxone-silver-publish.service`. Check cloud event/current/
   hourly counts and the batch ledger; retry once to verify no duplicates.
5. Enable the local and publication timers only after those checks succeed.

Config: `/etc/loxone-silver/local.env`. Service logs intentionally omit error text
that might contain credentials. Diagnostic scripts must redact secrets and must
not print environment files or token-bearing connection strings.

## Historical coverage and rollback

Initial archival starts at the oldest retained local message. Older cloud-only
Bronze history needs a separate bounded backfill; this is not silently downloaded
or transformed when cloud compute is exhausted. Old events preceding all retained
structures use the explicitly marked earliest-structure fallback until historical
structures are imported. Do not present that fallback as proven historical metadata.

Stop both local timers and drain their jobs before changing their release or
restoring files. Keep the retention gate: it prevents data loss while local work is
paused. The separate cloud SQL timer must remain disabled during this transition.
Switching code never deletes cloud tables or the local archive.
