# Flight-free Silver with an immutable local raw archive

The collector and independent Parquet archiver remain on the Pi and its SSD.
An ordinary DuckDB client uploads a bounded raw chunk into MotherDuck, computes
Silver and its current/hourly tables remotely, then advances a separate local
checkpoint. No Flight is created. Duckling SQL compute and storage are still
limited/billed; this does not bypass the account's compute allowance.

Use this pipeline as the sole writer of the existing `loxone_silver_local`
tables. The former local worker and publisher must be drained and their timers
disabled before enabling this timer. Keep collection and archival running.
Normal deployment must not re-enable the old writers. This installer is explicit
and leaves all timers untouched; running the legacy local installer afterwards
requires stopping the cloud writer first.

## Guarantees and bounds

The source is read-only SQLite. Its durable Parquet checkpoint bounds each read;
cloud staging never runs ahead of the local archive. This code never prunes any
source, archive, local working database or outbox. Existing local-only pruning
must remain disabled during cutover. Retain original messages of unsupported types
as well as all structure snapshots. Existing archive coverage must be checked
before any subsequent retention change.

A chunk is bounded by rows and payload bytes (defaults: 1,000 and 4 MiB). An
oversized single record fails without skipping it. The current cloud raw chunk
is retained until all eligible messages have committed events and derived tables.
The cloud chunk ledger commits before the local SQLite FULL checkpoint advances.
A lost local acknowledgement is recovered from that ledger. Source rowids must
remain stable: never VACUUM/rebuild/replace the spool during consumption.

The raw staging owner and local destination metadata reject accidental reuse of
another pipeline's state/schema. Preserve the local state database with its
pipeline ID. Do not silently recreate it or point it at another destination.
Cloud staging holds one chunk, durable structure snapshots and acknowledgement
metadata. It does not hold another complete raw archive.

Derived current state is recomputed from retained events for touched state keys,
so older backfill cannot overwrite newer values. Hourly numeric summaries are
replaced for touched keys using all retained events in the affected hours.
These remain sample-based statistics, not time-weighted energy calculations.
Late structure mappings schedule bounded replay from lossless event values;
the original raw payload remains on SSD for full parser-version replay. Metadata
can be partially corrected while a remapping backlog drains. Arbitrary parser
changes still require explicit raw-archive replay; never clear only the ledger.

The service uses 25% of one CPU, 220/240 MiB soft/hard memory, no swap, one client
thread, a 96 MB DuckDB budget and a five-minute hard deadline. It checks the memory
controller before starting. Existing venv dependencies are reused, not rebuilt on
production. A local lock prevents overlap. After catch-up, the client waits for
1,000 messages or an oldest-record age of five minutes before connecting; idle
runs make no MotherDuck calls. A structure change or pending remap also triggers
work. Every cloud statement still requires available account compute.

## Installation and cutover

Install from the reviewed commit using `scripts/install-cloud-edge.sh SHA PYTHON`,
where PYTHON is the existing immutable Silver venv Python. This installs an
immutable source release and units, preserving the existing credential file.
Review `cloud-edge.env` separately; it contains only safe pipeline settings.
The service loads the existing MotherDuck environment itself. Do not dump tokens
or environments into logs, shell arguments, Git or reports.

1. Validate schema/data equivalence in isolated cloud schemas, including snapshots,
   late arrivals, late mappings, replay, failed transactions and lost acknowledgements.
2. Record timer states and the checkpoint. Stop the local worker/publisher timers,
   drain the worker, then publish the remaining outbox. Confirm no unacknowledged
   manifests remain. Keep the local database and outbox as rollback artifacts.
3. Inspect the remaining local queue. Choose an explicit cutover cursor at or before
   its earliest source row, with a conservative overlap. Replaying already-published
   messages replaces their event sets; it does not duplicate them. Do not simply
   copy the local import highwater, which can include untransformed queue entries.
4. Initialize `cloud-edge.sqlite3` under the service account with `open_state(path,
   initial_cursor=VERIFIED_CURSOR)`. Never alter the old pipeline's checkpoints.
5. Start one bounded production run, validate tables and collector/archiver health,
   then enable the cloud timer and disable the old writer timers across reboots.
   Check live shares/readers still point at the existing target tables.
6. Measure several completed production runs against incoming receipts over the
   same wall-clock interval. Forecast `(backlog)/(processed_per_hour-arrivals_per_hour)`
   only when the denominator is positive; report the observation window and limits.

On failure, stop the new timer and preserve all state. Collection and archival can
continue without either Silver writer. A crash can leave a committed cloud batch
with a stale local cursor; retry this client to reconcile it. Do not resume the old
publisher blindly after the cloud worker has advanced: its older current-state
transport can regress a newer cloud value. Returning to the old writer requires
explicit reconciliation/replay or restoration of a verified target snapshot.
The retained old code/database is a recovery option, not an automatic safe rollback.

Logs `edge_start`, `edge_connected`, `edge_structures_ready`, `edge_transform_start`,
`edge_batch`, `edge_summary` and `edge_idle` contain counts/cursors/timings rather
than household values or exception text. Hard-limit errors remain visible in the
systemd journal. Local wall time does not equal billed compute-unit consumption;
use the account billing page or available usage metadata for that measurement.
