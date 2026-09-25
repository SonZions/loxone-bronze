#!/usr/bin/env bash
# Separate from Bronze's restricted deploy bridge; run as a trusted administrator.
set -Eeuo pipefail
[[ "$EUID" -eq 0 ]] || { echo 'Run as root.' >&2; exit 1; }
expected_sha="${1:-}"
[[ "$expected_sha" =~ ^[0-9a-f]{40}$ ]] || { echo 'Usage: install-silver.sh <reviewed main SHA>' >&2; exit 2; }
repo="$(cd -- "$(dirname -- "$0")/.." && pwd)"
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
# Installation never starts cloud writes. Pause scheduling and let an active run
# finish before changing the symlink. Its systemd timeout provides a hard bound.
systemctl stop loxone-silver-refresh.timer 2>/dev/null || true
while true; do
  silver_state="$(systemctl show loxone-silver-refresh.service -p ActiveState --value 2>/dev/null || true)"
  case "$silver_state" in
    active|activating|deactivating|reloading) sleep 2 ;;
    *) break ;;
  esac
done
if [[ -L "$base/current" ]]; then
  previous="$(readlink -f "$base/current")"
  ln -sfn "$previous" "$base/previous"
fi
ln -sfn "$release" "$base/.current-new"
mv -Tf "$base/.current-new" "$base/current"
install -o root -g root -m 0644 "$release/app/systemd/loxone-silver-refresh.service" /etc/systemd/system/
install -o root -g root -m 0644 "$release/app/systemd/loxone-silver-refresh.timer" /etc/systemd/system/
systemctl daemon-reload
printf 'Installed Silver revision %s. Timer is stopped.\n' "$expected_sha"
echo 'Set the token with sudoedit /etc/loxone-silver/silver.env.'
echo 'Then follow docs/silver-operations.md for the read-only check, first run and timer.'
