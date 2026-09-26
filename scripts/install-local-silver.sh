#!/usr/bin/env bash
# Opt-in adjunct to the reviewed main deployment; first install never starts jobs.
set -Eeuo pipefail
[[ "$EUID" -eq 0 ]] || exit 1
release="$(readlink -f "${1:?Expected immutable installed Silver release}")"
[[ "$release" == /opt/loxone-silver/releases/* && -x "$release/venv/bin/python" ]] || exit 2
base=/opt/loxone-local-silver
units=(loxone-local-silver.service loxone-local-silver.timer loxone-silver-publish.service loxone-silver-publish.timer)
timers=(loxone-local-silver.timer loxone-silver-publish.timer)
install -d -o root -g root -m 0755 "$base"
install -d -o loxonebronze -g loxonebronze -m 0700 /srv/raspi-data/loxone-silver
if [[ ! -f /etc/loxone-silver/local.env ]]; then
  install -o root -g root -m 0600 "$release/app/config/local-silver.env.example" /etc/loxone-silver/local.env
fi
# Existing cloud-transform scheduling is incompatible with local mode.
if systemctl is-active --quiet loxone-silver-refresh.timer || systemctl is-active --quiet loxone-silver-refresh.service; then
  echo 'Stop the old cloud Silver writer before local cutover.' >&2
  exit 1
fi
backup="$(mktemp -d "$base/rollback.XXXXXX")"
previous="$(readlink -f "$base/current" 2>/dev/null || true)"
active=()
for timer in "${timers[@]}"; do
  if systemctl is-active --quiet "$timer"; then active+=("$timer"); fi
  if systemctl cat "$timer" >/dev/null 2>&1; then systemctl stop "$timer"; fi
done
for unit in loxone-local-silver.service loxone-silver-publish.service; do
  while [[ "$(systemctl show "$unit" -p ActiveState --value)" =~ ^(active|activating|deactivating|reloading)$ ]]; do sleep 2; done
done
for unit in "${units[@]}"; do
  if [[ -f "/etc/systemd/system/$unit" ]]; then cp -a "/etc/systemd/system/$unit" "$backup/"; fi
done
restore() {
  local code="$?"
  trap - EXIT
  [[ "$code" -eq 0 ]] && return
  set +e
  for timer in "${timers[@]}"; do systemctl stop "$timer"; done
  systemctl stop loxone-local-silver.service loxone-silver-publish.service
  if [[ -d "$previous" ]]; then ln -sfn "$previous" "$base/current"; else unlink "$base/current"; fi
  for unit in "${units[@]}"; do
    if [[ -f "$backup/$unit" ]]; then cp -a "$backup/$unit" /etc/systemd/system/;
    elif [[ -f "/etc/systemd/system/$unit" ]]; then unlink "/etc/systemd/system/$unit"; fi
  done
  systemctl daemon-reload
  for timer in "${active[@]}"; do systemctl start "$timer"; done
  exit "$code"
}
trap restore EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
ln -sfn "$release" "$base/.current-new"
mv -Tf "$base/.current-new" "$base/current"
for unit in "${units[@]}"; do install -o root -g root -m 0644 "$release/app/systemd/$unit" /etc/systemd/system/; done
systemctl daemon-reload
# Deployment has already upgraded Bronze's pruning code before reaching here.
runuser -u loxonebronze -- "$release/venv/bin/python" -m loxone_bronze.local_silver --enable-retention-guard
for timer in "${active[@]}"; do systemctl start "$timer"; done
trap - EXIT INT TERM
echo 'Local Silver installed. First-run validation is required before enabling timers.'
