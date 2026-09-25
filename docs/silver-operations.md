# Raspberry Silver refresh

## What moves

The Raspberry runs the Python client and systemd timer. All Bronze/Silver data,
processed-message acknowledgements and staging tables remain in MotherDuck.
The heavy SQL still uses cloud compute. This removes the Flight runtime and its
scheduling dependency; it does **not** eliminate MotherDuck compute charges.
The Pi never downloads the event history, writes a local Silver database or reads
the Bronze SQLite spool. Only scalar counts/timestamps return to Python.

Source: `loxone_ingest.loxone_bronze`, the existing read-only share.
Target: `my_db.loxone_silver`, including the existing tables and five views.
Do not use the empty `my_db.loxone_bronze` placeholder tables.

On 2026-09-24 a read-only inspection found the former Flight
`e9dba048-9dba-4d04-9708-f309ca301d8d` absent and `state_events` empty. The last
ledger entry was still `running` from September 9. Its cause is unproven. The new
job was reconstructed from the live schema and the Silver guide; it is not a copy
of the deleted Flight source. It safely rebuilds the empty target from Bronze.
No production data was changed while preparing this implementation.

## Correctness and resource bounds

- New structures are loaded one at a time. At most `SILVER_MAX_BATCHES` structures
  are inspected per run; event processing waits until available mappings catch up.
- The validity checks exclude tiny/error payloads and empty/non-object `controls`.
- The mapper retains state/details/action/control-key priorities, subcontrols,
  details UUID leaves, all competing meanings and the as-of/earliest fallback.
  Non-UUID subcontrol keys such as `.../AI5` must be retained too.
- Existing mappings are kept unchanged. New subcontrol mappings inherit missing
  room/category from their parent, as specified by the Silver guide. A live
  comparison found that the oldest stored snapshot lacks this inheritance for
  379 candidates; UUID/control/state meanings agree. This metadata correction is
  applied to new snapshots only, not as a destructive rewrite of old mappings.
- Each event batch handles at most 5,000 Bronze messages. A reconnect message may
  contain many states; the event count can be higher. No value-based reduction.
- A cloud `processed_messages` ledger tracks `(source_id,message_id,structure_id)`.
  It is not an event-time watermark. Late uploads are found even if their receipt
  time precedes all current Silver events.
- Late structures change the desired as-of assignment. Affected messages are
  replayed in batches; unaffected messages do not need a full rebuild. During
  catch-up, views may contain a mix of old and corrected mapping assignments.
- Each batch deletes only its selected messages, reinserts their full event sets
  and updates acknowledgements/counters in one transaction. A crash before commit
  leaves the previous batch state intact. A crash after commit does not duplicate.
- Only the new job writes to Silver after cutover. `flock` excludes local manual
  overlap; a transactional `refresh_guard` row makes other instances of this job
  conflict rather than share staging tables. Old/custom writers do not honor it.
- Work tables are explicitly qualified in the remote target schema. They are
  created/dropped inside the transaction; no local `CREATE TEMP TABLE` is used.
- The 600s budget is checked between transactions. The 720s systemd timeout bounds
  a stuck process. A killed process may leave a `running` log entry; committed
  acknowledgements remain authoritative. It does not mean that a job is alive.
- Discovery anti-joins scan the cloud source/ledger; processing is row-bounded,
  but scan cost is not constant. Measure catch-up throughput and cloud usage before
  shortening the timer interval. A 15-minute pause starts after each run finishes.
- Existing populated targets with no ledger are deliberately replayed once. This
  repairs duplicates/partial historical batches instead of assuming complete data.
  Do not clear the ledger alone unless a full replay is intended.
- Unsupported/unparsed/empty messages stay in Bronze and create no Silver events.

## Recommended: existing GitHub deployment action

The **Deploy Loxone Bronze + Silver** action now deploys both components from
the exact approved `main` SHA. It is still manually dispatched and retains the
`loxberry-production` environment gate and the existing restricted sudo command.
Merge alone does not deploy. Do not run a separate manual Git update/installer
alongside the action.

### One-time bridge upgrade (trusted administrator)

After reviewing and merging the deployment integration, update the separately
installed root-owned bridge. The runner cannot upgrade its own privileged bridge.
Use these commands on the Pi; they fetch code but do not restart services:

```bash
sudo -u loxberry git -C /srv/raspi-data/repos/loxone-bronze fetch origin main
silver_bridge_review="$(mktemp -d)"
sudo -u loxberry git -C /srv/raspi-data/repos/loxone-bronze show origin/main:scripts/deploy-from-github.sh > "$silver_bridge_review/deploy.sh"
less "$silver_bridge_review/deploy.sh"
sudo install -o root -g root -m 0755 "$silver_bridge_review/deploy.sh" /usr/local/sbin/loxone-bronze-deploy
/usr/local/sbin/loxone-bronze-deploy --version
```

Expected: `loxone-bronze-deploy-v3`. No sudoers change is required. Old bridges
are rejected by the workflow before stopping services. Then select **Run workflow**
on `main` for **Deploy Loxone Bronze + Silver** and approve the environment gate.

The bridge completes Bronze's health check and records its successful revision,
then calls the root-owned deployed Silver installer with `--resume` and the same
SHA. Silver is prepared in a separate environment while existing services continue.
During activation it pauses only Silver's timer and drains its active refresh.
First installation leaves Silver stopped even if a token is already configured.
An upgrade resumes a previously active timer; stopped timers remain stopped and
the enabled/disabled boot state is not changed. Resuming a timer may naturally
schedule a run immediately; the installer does not explicitly request a refresh.

If Silver fails, the action is red, but healthy Bronze remains deployed. Activation
failures restore Silver's previous code symlink, actual unit files, and previously
active timer. First-activation failure removes only the newly installed units and
current symlink, retaining release files. Existing env files are never overwritten.
SIGKILL/power loss cannot execute shell rollback; verify the current symlink/units
and rerun the reviewed installer after such an interruption. The workflow allows
30 minutes for dependency installation, service draining and both deployment phases.

After the first successful action, continue with **Token and cutover** below.
After that, normal code updates require only the same action. Changes to the
privileged bridge itself still require explicit administrator review/installation.

## Alternative: manual Silver installation

A trusted Raspberry administrator performs the first installation. This uses a
separate account (`loxonesilver`) and environment, preserving Bronze and its
restricted GitHub deployment bridge. Use this alternative only when not deploying
via the action above. No additional runner sudo permissions are needed or added.

```bash
sudo -u loxberry git -C /srv/raspi-data/repos/loxone-bronze fetch origin main
sudo -u loxberry git -C /srv/raspi-data/repos/loxone-bronze switch main
sudo -u loxberry git -C /srv/raspi-data/repos/loxone-bronze merge --ff-only origin/main
silver_revision="$(sudo -u loxberry git -C /srv/raspi-data/repos/loxone-bronze rev-parse HEAD)"
sudo bash /srv/raspi-data/repos/loxone-bronze/scripts/install-silver.sh "$silver_revision"
```

The installer requires a clean checkout at the reviewed `origin/main` revision.
It creates a release, installs the pinned `duckdb`/`pytz` client in its own venv,
checks package resources and switches a `current` symlink. It preserves the old
release under `previous`, never overwrites an existing env file, and leaves the
timer stopped. Prerequisites are `python3-venv`, `git`, `tar`, and systemd, with
outbound access to Python packages and MotherDuck. Never run it from unreviewed code.

## Token and cutover

Create a token under the MotherDuck identity that can read `loxone_ingest` and
write `my_db`. The Bronze uploader's token belongs to a separate ingest identity;
do not assume it has those rights. Configure it locally, never in Git/chat/logs:

```bash
sudoedit /etc/loxone-silver/silver.env
```

Set `MOTHERDUCK_TOKEN`. Keep the supplied source/target defaults. The file is
root-owned 0600; systemd reads it before switching to `loxonesilver`.
A connection made with this identity must already expose the `loxone_ingest`
share, using the same alias as the existing Silver views. If the source is absent,
attach the existing share under that identity first; there is no fallback source.

Read-only connectivity check with systemd supplying the protected environment:

```bash
sudo systemd-run --wait --pipe --collect \
  --unit=loxone-silver-check \
  -p User=loxonesilver -p Group=loxonesilver \
  -p EnvironmentFile=/etc/loxone-silver/silver.env \
  /opt/loxone-silver/current/venv/bin/loxone-silver-refresh --check
```

The check verifies readable source and attached target. It cannot prove write
permissions without writing. Check that no other Silver writer/Flight is active,
then start one controlled run (this writes Silver in the cloud):

```bash
sudo systemctl start loxone-silver-refresh.service
sudo journalctl -u loxone-silver-refresh.service -n 50 --no-pager
systemctl show loxone-silver-refresh.service -p Result -p ExecMainStatus
```

Inspect the load ledger and counts below. `partial` is expected during backlog
recovery; each successful batch is durable. Only after a successful first run:

```bash
sudo systemctl enable --now loxone-silver-refresh.timer
systemctl list-timers --all loxone-silver-refresh.timer
```

A stopped oneshot is normal. Check `Result=success`, logs and advancing Silver
timestamps. Do not infer success from timer activation alone.

## Cloud verification

```sql
SELECT started_at, finished_at, status, messages_processed, events_inserted,
       structures_loaded, mappings_loaded, source_max_ingested_at, error_message
FROM my_db.loxone_silver.load_runs
WHERE load_run_id LIKE 'raspberry-silver-%'
ORDER BY started_at DESC LIMIT 10;

SELECT count(*) AS events, max(received_at) AS latest_receipt,
       max(bronze_ingested_at) AS latest_ingest
FROM my_db.loxone_silver.state_events;

SELECT count(*) AS duplicate_keys FROM (
  SELECT source_id,message_id,state_uuid
  FROM my_db.loxone_silver.state_events
  GROUP BY ALL HAVING count(*) > 1
);

SELECT count(*) AS acknowledged_messages
FROM my_db.loxone_silver.processed_messages;
```

`events_inserted` is the number inserted in this run including replacements; it is
not net growth. `source_max_ingested_at` is diagnostic, not a skip watermark.
Run the existing `data_quality_summary` after catch-up to check unresolved and
ambiguous states. These queries scan data; they are verification, not per-batch work.

## Rollback and upgrades

Stop the timer and let any running service finish before replacing the code:

```bash
sudo systemctl stop loxone-silver-refresh.timer
systemctl show loxone-silver-refresh.service -p ActiveState -p Result
```

For code rollback, after the service is inactive, point `current` to the previous
release, restore that release's service/timer files, then `daemon-reload` and run
one controlled refresh. Committed cloud data is retained. Do not drop Silver or
its ledger for a code rollback. Re-enable the timer after verifying success.
For upgrades, repeat the reviewed-SHA installation procedure; it waits for active
work and pauses scheduling before activation. Keep old releases until verified.

## Validation scope

Local DuckDB tests cover retries, delete/insert/ack rollback, late messages, late
structures, mapping conflicts, fallback, text/nested JSON and batch bounds.
The existing Bronze suite must remain green. Read-only live checks validate the
existing schema/mappings. Raspberry resource use, real token permissions and
MotherDuck write transactions are verified by the first controlled Pi run, not
claimed from local tests.
