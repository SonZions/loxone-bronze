#!/usr/bin/env bash
# Install reviewed code/units without changing production timers or data cursors.
set -Eeuo pipefail
[[ "$EUID" -eq 0 ]] || exit 1
revision="${1:?Expected reviewed full Git commit SHA}"
python_path="${2:?Expected existing pinned Silver venv Python path}"
[[ "$revision" =~ ^[0-9a-f]{40}$ && -x "$python_path" ]] || exit 2
source_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
runtime_dir="$(cd "$(dirname "$python_path")/.." && pwd -P)"
[[ "$runtime_dir" == /opt/loxone-silver/releases/*/venv ]] || exit 2
base=/opt/loxone-cloud-edge
release="$base/releases/$revision"
[[ ! -e "$release" ]] || { echo 'Release already exists; inspect before reinstalling.' >&2; exit 2; }
# No dependency installation or build work on the small production Pi.
"$python_path" -c 'import duckdb; assert duckdb.__version__ == "1.5.5"'
install -d -o root -g root -m 0755 "$release/app" "$base/releases"
cp -R "$source_root/src" "$source_root/systemd" "$source_root/config" "$release/app/"
chown -R root:root "$release"
chmod -R a+rX "$release"
ln -s "$release" "$base/.current-new"
mv -Tf "$base/.current-new" "$base/current"
if [[ ! -e /etc/loxone-silver/cloud-edge.env ]]; then
 install -o root -g root -m 0600 "$release/app/config/cloud-edge.env.example" /etc/loxone-silver/cloud-edge.env
fi
for unit in loxone-cloud-edge.service loxone-cloud-edge.timer; do
 install -o root -g root -m 0644 "$release/app/systemd/$unit" "/etc/systemd/system/$unit"
done
install -d -m 0755 /etc/systemd/system/loxone-cloud-edge.service.d
printf '[Service]\nExecStart=\nExecStart=%s/bin/python -m loxone_bronze.cloud_edge\n' "$runtime_dir" > /etc/systemd/system/loxone-cloud-edge.service.d/10-python.conf
systemctl daemon-reload
echo 'Installed without enabling timers. Drain old writers, initialize a verified cursor, then validate before activation.'
