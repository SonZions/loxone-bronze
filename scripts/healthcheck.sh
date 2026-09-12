#!/usr/bin/env bash
set -euo pipefail
sudo -u loxonebronze \
  env SPOOL_DB=/var/lib/loxone-bronze/spool.sqlite3 \
  /opt/loxone-bronze/venv/bin/loxone-bronze-health
