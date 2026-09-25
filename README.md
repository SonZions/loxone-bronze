# Loxone → MotherDuck Bronze

Durable edge collector for Loxone live states.

## Architecture

```text
Loxone Miniserver
  │
  │ encrypted/authenticated WebSocket
  │ enablebinstatusupdate
  ▼
Raspberry / LoxBerry
  ├─ loxone-bronze-collector (continuous)
  │    └─ SQLite WAL spool
  │         ├─ raw WebSocket messages
  │         └─ versioned LoxAPP3.json
  │
  └─ loxone-bronze-uploader.timer (every 5 min)
       │ HTTPS / MotherDuck
       ▼
MotherDuck my_db.loxone_bronze
  ├─ ws_messages
  └─ structures
```

## Why event-driven instead of 1-minute polling?

`jdev/sps/enablebinstatusupdate` sends the current states when the WebSocket
connection starts and then sends state changes. This captures short-lived states
that polling could miss.

Bronze keeps the original WebSocket transport payload (base64 for binary frames).
For message types already parsed by `loxwebsocket` (notably value/text states), the
parsed form is stored next to the raw payload. Unknown/unparsed message types are
still retained losslessly.

## Durability / idempotency

1. Collector writes to local SQLite first.
2. SQLite runs in WAL mode.
3. Uploader sends batches every five minutes.
4. Every local row has a stable UUID.
5. MotherDuck primary keys + `INSERT OR IGNORE` make retries idempotent.
6. Only after a successful MotherDuck statement is the local row marked uploaded.
7. Uploaded rows remain locally for 7 days by default.

If internet/MotherDuck is unavailable, the backlog simply grows locally.

## Security

Use two dedicated credentials:

- **Loxone:** a dedicated low-privilege visualization user. It must be allowed to
  see the controls whose states you want to collect. Do not use the Miniserver
  admin account.
- **MotherDuck:** a dedicated service/access token used only by the Raspberry.

Secrets live in root-owned mode-0600 files under `/etc/loxone-bronze/`.
They are not in source code.

No inbound port or port-forwarding is required.

## MotherDuck setup

Run `sql/001_motherduck_bronze.sql` once against `my_db`.

It creates only:

- `my_db.loxone_bronze.ws_messages`
- `my_db.loxone_bronze.structures`

Existing `loxone` and `solar` schemas are not changed.

## Raspberry installation

Copy/extract this project on the Raspberry, then:

```bash
sudo bash scripts/install.sh
```

Edit:

```bash
sudo nano /etc/loxone-bronze/collector.env
sudo nano /etc/loxone-bronze/motherduck.env
```

Start:

```bash
sudo systemctl enable --now loxone-bronze-collector.service
sudo systemctl enable --now loxone-bronze-uploader.timer
```

Check:

```bash
sudo journalctl -u loxone-bronze-collector -f
sudo journalctl -u loxone-bronze-uploader -n 100
sudo bash /opt/loxone-bronze/app/scripts/healthcheck.sh
systemctl list-timers | grep loxone-bronze
```

## Suggested Loxone user

Create a user such as `loxone_data` with visualization/read access to all states
you want in the warehouse. Loxone only publishes states exposed to the user's UI
context, so overly narrow permissions will intentionally produce an incomplete
dataset.

## Local files

- spool: `/var/lib/loxone-bronze/spool.sqlite3`
- Loxone credentials: `/etc/loxone-bronze/collector.env`
- MotherDuck token: `/etc/loxone-bronze/motherduck.env`

## Bronze semantics

`ws_messages.received_at` is the Raspberry receive time. Loxone value-state event
tables do not carry an event timestamp per individual state update, so this is the
correct source timestamp available for the live stream.

`structures` contains full `LoxAPP3.json` snapshots. The collector checks the
LoxAPP version periodically and stores a new structure only when it changes.

Silver should later:
- explode value/text state payloads,
- map state UUID → control/state using the correct LoxAPP3 structure version,
- normalize rooms/categories/control types,
- derive state intervals and current state,
- handle reconnect initial-state batches explicitly.

## Python requirement

Python **3.10+** is intentionally required. The collector pins `loxwebsocket 0.6.0`
because that release safely routes request/response commands while the background
WebSocket listener is active. Older releases are not used as a fallback.

## Uploader throughput and backlog recovery

The uploader drains up to **12 batches of 5,000 messages per five-minute run**, with
an overall **240-second soft deadline**. It bulk-loads a local JSONL relation with
one `INSERT OR IGNORE ... SELECT` per batch, then acknowledges that batch in one
SQLite transaction. No additional dependency or remote per-row staging is needed.
A failed batch remains retryable; earlier acknowledgements survive.

**Existing Raspberry installations must explicitly update `motherduck.env`.**
Neither example files nor the deploy drop-in override an existing
`UPLOAD_BATCH_SIZE=1000` in that file. Startup `upload_config` logs show effective
safe values. Structured `upload_summary` logs show pending counts, age, throughput
and the stop reason. Health checks separately report stale collection and backlog;
count and age thresholds are configurable.

See [Uploader operations](docs/uploader-operations.md) for the exact secret-safe
one-time update, deployment bridge update, verification commands, read-only
MotherDuck SQL and measured-rate ETA calculation. For 430,000 pending messages and
323,000 arrivals/day, twelve-hour recovery requires **at least 13.7 uploads/s over
wall time**, including timer pauses. This must be verified on the Raspberry.

## Raspberry Silver refresh

The optional `loxone-silver-refresh` job replaces the former Silver Flight's
scheduling and Python client. Bronze, Silver, the processing ledger, and SQL work
tables remain in MotherDuck; the Pi stores only code, credentials and logs.
It reads `loxone_ingest.loxone_bronze` and updates `my_db.loxone_silver` in bounded,
atomic batches, including late uploads and temporal remapping after new structures.
It uses its own service account, pinned Python environment and 15-minute timer.
The existing **Deploy Loxone Bronze + Silver** action installs both components
after a one-time administrator upgrade of the restricted bridge to v3.
First Silver installation leaves its timer stopped for token setup and validation;
later deployments resume a previously active Silver timer. Silver activation failure
restores Silver without undoing a successful Bronze deployment.
See [Silver operations](docs/silver-operations.md) for installation and cutover.
