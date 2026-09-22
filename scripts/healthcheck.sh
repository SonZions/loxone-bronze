#!/usr/bin/env bash
set -euo pipefail
# systemd parses the root-only EnvironmentFile without shell evaluation or
# printing its contents. No token is placed in argv or the process journal.
sudo systemd-run --quiet --wait --pipe --collect \
  --property=User=loxonebronze \
  --property=EnvironmentFile=/etc/loxone-bronze/motherduck.env \
  /opt/loxone-bronze/venv/bin/loxone-bronze-health
