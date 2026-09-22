#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run as root: sudo bash scripts/install.sh" >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
APP_DIR="/opt/loxone-bronze"
ETC_DIR="/etc/loxone-bronze"
DATA_DIR="/var/lib/loxone-bronze"
SERVICE_USER="loxonebronze"

python3 - <<'PY'
import sys
if sys.version_info < (3, 10):
    raise SystemExit("Python >= 3.10 is required (loxwebsocket 0.6.0)")
print("Python:", sys.version.split()[0])
PY

if ! id "${SERVICE_USER}" >/dev/null 2>&1; then
  useradd --system --home "${DATA_DIR}" --shell /usr/sbin/nologin "${SERVICE_USER}"
fi

mkdir -p "${APP_DIR}" "${ETC_DIR}" "${DATA_DIR}"
rm -rf "${APP_DIR}/app"
mkdir -p "${APP_DIR}/app"
cp -a "${ROOT_DIR}/." "${APP_DIR}/app/"

if ! python3 -m venv "${APP_DIR}/venv"; then
  echo "Could not create a virtualenv. On Debian/Raspberry Pi OS install python3-venv first." >&2
  exit 1
fi
"${APP_DIR}/venv/bin/pip" install --upgrade pip wheel
"${APP_DIR}/venv/bin/pip" install "${APP_DIR}/app"

if [[ ! -f "${ETC_DIR}/collector.env" ]]; then
  cp "${APP_DIR}/app/config/collector.env.example" "${ETC_DIR}/collector.env"
fi
if [[ ! -f "${ETC_DIR}/motherduck.env" ]]; then
  cp "${APP_DIR}/app/config/motherduck.env.example" "${ETC_DIR}/motherduck.env"
fi

chmod 600 "${ETC_DIR}/collector.env" "${ETC_DIR}/motherduck.env"
chown root:root "${ETC_DIR}/collector.env" "${ETC_DIR}/motherduck.env"
chown -R "${SERVICE_USER}:${SERVICE_USER}" "${DATA_DIR}"
chown -R root:root "${APP_DIR}"

cp "${APP_DIR}/app/systemd/"*.service /etc/systemd/system/
cp "${APP_DIR}/app/systemd/"*.timer /etc/systemd/system/
systemctl daemon-reload

echo
echo "Installed. Existing environment files and secrets were preserved."
echo "Existing installations: explicitly set UPLOAD_BATCH_SIZE=5000,"
echo "UPLOAD_MAX_BATCHES_PER_RUN=12 and UPLOAD_MAX_RUNTIME_SECONDS=240."
echo "See docs/uploader-operations.md; example files are NOT applied to existing env files."
echo "Now edit:"
echo "  ${ETC_DIR}/collector.env"
echo "  ${ETC_DIR}/motherduck.env"
echo
echo "Then enable:"
echo "  systemctl enable --now loxone-bronze-collector.service"
echo "  systemctl enable --now loxone-bronze-uploader.timer"
echo
echo "Logs:"
echo "  journalctl -u loxone-bronze-collector -f"
echo "  journalctl -u loxone-bronze-uploader -n 100"
