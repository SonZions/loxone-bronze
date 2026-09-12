#!/usr/bin/env bash
set -Eeuo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run as root." >&2
  exit 1
fi

expected_sha="${1:-}"
if [[ ! "${expected_sha}" =~ ^[0-9a-f]{40}$ ]]; then
  echo "Usage: $(basename "$0") <40-character main commit SHA>" >&2
  exit 2
fi

repo="/srv/raspi-data/repos/loxone-bronze"
repo_user="loxberry"
app_dir="/opt/loxone-bronze/app"
venv="/opt/loxone-bronze/venv"
collector="loxone-bronze-collector.service"
uploader="loxone-bronze-uploader.service"
timer="loxone-bronze-uploader.timer"

exec 9>/run/lock/loxone-bronze-deploy.lock
if ! flock -n 9; then
  echo "Another Loxone Bronze deployment is already running." >&2
  exit 1
fi

for required in "$repo/.git" "$venv/bin/pip"; do
  [[ -e "$required" ]] || {
    echo "Missing required deployment path: $required" >&2
    exit 1
  }
done

previous_sha="$(runuser -u "$repo_user" -- git -C "$repo" rev-parse HEAD)"
timer_was_active=false
if systemctl is-active --quiet "$timer"; then
  timer_was_active=true
fi

sync_app() {
  rsync -a --delete --chown=root:root \
    --exclude='.git/' \
    --exclude='__pycache__/' \
    --exclude='*.pyc' \
    --exclude='build/' \
    "$repo/" "$app_dir/"
}

install_units() {
  install -m 0644 \
    "$repo/deploy/systemd/loxone-bronze-collector.service" \
    "$repo/deploy/systemd/loxone-bronze-uploader.service" \
    "$repo/deploy/systemd/loxone-bronze-uploader.timer" \
    /etc/systemd/system/

  if [[ -d "$repo/deploy/systemd/loxone-bronze-uploader.service.d" ]]; then
    install -d -m 0755 /etc/systemd/system/loxone-bronze-uploader.service.d
    install -m 0644 \
      "$repo/deploy/systemd/loxone-bronze-uploader.service.d/"*.conf \
      /etc/systemd/system/loxone-bronze-uploader.service.d/
  fi
}

rollback() {
  local exit_code="$?"
  trap - ERR
  echo "Deployment failed; restoring $previous_sha." >&2

  systemctl stop "$timer" || true
  systemctl stop "$uploader" || true
  systemctl stop "$collector" || true

  runuser -u "$repo_user" -- git -C "$repo" reset --hard "$previous_sha" || true
  sync_app || true
  "$venv/bin/pip" install --no-deps --force-reinstall "$app_dir" || true
  install_units || true
  systemctl daemon-reload || true
  systemctl restart "$collector" || true
  if "$timer_was_active"; then
    systemctl start "$timer" || true
  fi

  exit "$exit_code"
}
trap rollback ERR

runuser -u "$repo_user" -- git -C "$repo" fetch --prune origin main
remote_sha="$(runuser -u "$repo_user" -- git -C "$repo" rev-parse origin/main)"

if [[ "$remote_sha" != "$expected_sha" ]]; then
  echo "Refusing deploy: approved SHA is $expected_sha, origin/main is $remote_sha." >&2
  exit 1
fi

echo "Deploying Loxone Bronze commit $expected_sha"
systemctl stop "$timer"
systemctl stop "$uploader" || true
systemctl stop "$collector"

runuser -u "$repo_user" -- git -C "$repo" reset --hard "$expected_sha"
sync_app
"$venv/bin/pip" install --no-deps --force-reinstall "$app_dir"
install_units
systemctl daemon-reload
systemctl restart "$collector"

if "$timer_was_active"; then
  systemctl start "$timer"
fi

sleep 5
systemctl is-active --quiet "$collector"
if "$timer_was_active"; then
  systemctl is-active --quiet "$timer"
fi

# A running process is not enough: after reconnect the collector must write fresh
# data to the spool. Failure here is treated as a failed deployment and rolls back.
sleep 15
runuser -u loxonebronze -- env \
  SPOOL_DB=/var/lib/loxone-bronze/spool.sqlite3 \
  HEALTH_MAX_EVENT_AGE_MINUTES=2 \
  "$venv/bin/loxone-bronze-health"

trap - ERR
echo "Deployment completed and passed post-deploy health check: $expected_sha"
