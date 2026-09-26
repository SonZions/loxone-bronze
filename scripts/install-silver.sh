#!/usr/bin/env bash
# Called by the restricted deploy bridge, or directly by a trusted administrator.
set -Eeuo pipefail
[[ "$EUID" -eq 0 ]] || { echo 'Run as root.' >&2; exit 1; }
expected_sha="${1:-}"
[[ "$expected_sha" =~ ^[0-9a-f]{40}$ ]] || { echo 'Usage: install-silver.sh <reviewed main SHA>' >&2; exit 2; }
mode="${2:---pause}"
[[ "$mode" == --pause || "$mode" == --resume ]] || { echo 'Expected --pause or --resume.' >&2; exit 2; }
repo="${3:-$(cd -- "$(dirname -- "$0")/.." && pwd)}"
# Git is run as the owner, without changing global safe.directory configuration.
repo_owner="$(stat -c %U "$repo")"
repo_git() { runuser -u "$repo_owner" -- git -C "$repo" "$@"; }
[[ "$(repo_git rev-parse HEAD)" == "$expected_sha" ]] || { echo 'Checkout is not the reviewed SHA.' >&2; exit 1; }
[[ "$(repo_git rev-parse origin/main)" == "$expected_sha" ]] || { echo 'Reviewed SHA must be origin/main. Fetch it first.' >&2; exit 1; }
[[ -z "$(repo_git status --porcelain)" ]] || { echo 'Checkout must be clean.' >&2; exit 1; }
exec 9>/run/lock/loxone-silver-install.lock
flock -n 9 || { echo 'Silver installation already running.' >&2; exit 1; }

base=/opt/loxone-silver
install -d -o root -g root -m 0755 "$base/releases" /etc/loxone-silver
if ! id loxonesilver >/dev/null 2>&1; then
  useradd --system --home-dir /var/lib/loxone-silver --shell /usr/sbin/nologin loxonesilver
fi
install -d -o loxonesilver -g loxonesilver -m 0700 /var/lib/loxone-silver
release="$(mktemp -d "$base/releases/${expected_sha}.XXXXXX")"
chmod 0755 "$release"
install -d -m 0755 "$release/app"
repo_git archive "$expected_sha" | tar -x -C "$release/app"
python3 -m venv "$release/venv"
# A separate pinned client avoids upgrading the running Bronze environment.
"$release/venv/bin/pip" install -r "$release/app/requirements-silver.txt"
"$release/venv/bin/pip" install --no-deps "$release/app"
"$release/venv/bin/python" -c 'from loxone_bronze.silver import SilverConfig; SilverConfig().sql("batch.sql")'

if [[ ! -e /etc/loxone-silver/silver.env ]]; then
  install -o root -g root -m 0600 "$release/app/config/silver.env.example" /etc/loxone-silver/silver.env
fi
# First installation never starts cloud writes. Pause scheduling and let an active run
# finish before changing the symlink. Its systemd timeout provides a hard bound.
timer=loxone-silver-refresh.timer
timer_was_active=false
if systemctl is-active --quiet "$timer"; then
  timer_was_active=true
fi
previous=""
if [[ -L "$base/current" ]]; then
  previous="$(readlink -f "$base/current")"
fi
# Preserve the actual installed units, including any admin edits, for rollback.
backup="$release/rollback-units"
install -d -m 0700 "$backup"
for unit in loxone-silver-refresh.service loxone-silver-refresh.timer; do
  if [[ -e "/etc/systemd/system/$unit" ]]; then
    cp -a "/etc/systemd/system/$unit" "$backup/$unit"
  fi
done
activation_started=false
restore_silver() {
  local code="$1"
  trap - EXIT INT TERM
  [[ "$code" -eq 0 ]] && return
  set +e
  echo 'Silver installation failed; restoring its previous activation.' >&2
  systemctl stop "$timer"
  if "$activation_started"; then
    # A resumed timer may already have scheduled a refresh. Stop it before
    # restoring code; Silver's cloud transactions make interruption retry-safe.
    systemctl stop loxone-silver-refresh.service
    if [[ -n "$previous" ]]; then
      ln -sfn "$previous" "$base/.current-rollback"
      mv -Tf "$base/.current-rollback" "$base/current"
    else
      # Only the symlink created by this first activation; retain release files.
      [[ -L "$base/current" ]] && unlink "$base/current"
    fi
    for unit in loxone-silver-refresh.service loxone-silver-refresh.timer; do
      if [[ -e "$backup/$unit" ]]; then
        cp -a "$backup/$unit" "/etc/systemd/system/$unit"
      else
        [[ -f "/etc/systemd/system/$unit" ]] && unlink "/etc/systemd/system/$unit"
      fi
    done
    systemctl daemon-reload
  fi
  if "$timer_was_active"; then
    systemctl start "$timer" || echo 'ERROR: previous Silver timer could not be resumed.' >&2
  fi
  exit "$code"
}
trap 'exit 130' INT
trap 'exit 143' TERM
# EXIT also handles errexit and handled termination signals; SIGKILL cannot recover.
trap 'restore_silver "$?"' EXIT
if systemctl cat "$timer" >/dev/null 2>&1; then
  systemctl stop "$timer"
fi
while true; do
  silver_state="$(systemctl show loxone-silver-refresh.service -p ActiveState --value 2>/dev/null || true)"
  case "$silver_state" in
    active|activating|deactivating|reloading) sleep 2 ;;
    *) break ;;
  esac
done
if [[ -n "$previous" ]]; then
  ln -sfn "$previous" "$base/previous"
fi
activation_started=true
ln -sfn "$release" "$base/.current-new"
mv -Tf "$base/.current-new" "$base/current"
install -o root -g root -m 0644 "$release/app/systemd/loxone-silver-refresh.service" /etc/systemd/system/
install -o root -g root -m 0644 "$release/app/systemd/loxone-silver-refresh.timer" /etc/systemd/system/
systemctl daemon-reload
runuser -u loxonesilver -- "$release/venv/bin/python" -c 'from loxone_bronze.silver import SilverConfig; SilverConfig().sql("batch.sql")'
if [[ "$mode" == --resume ]] && "$timer_was_active"; then
  systemctl start "$timer"
  systemctl is-active --quiet "$timer"
  echo 'Previously active Silver timer resumed. No forced refresh was triggered.'
else
  echo 'Silver timer remains stopped. Configure the token and verify the first run before enabling.'
fi
trap - ERR INT TERM EXIT
printf 'Installed Silver revision %s.\n' "$expected_sha"

# Explicit administrator opt-in; existing cloud-only installations stay unchanged.
if [[ -f /etc/loxone-silver/local.enabled ]]; then
  bash "$release/app/scripts/install-local-silver.sh" "$release"
fi
