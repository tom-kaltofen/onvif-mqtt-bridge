#!/usr/bin/env bash
# Installs onvif-mqtt-bridge as a systemd service.
#
# What this does:
#   1. Creates a dedicated, unprivileged system user "onvif-mqtt-bridge"
#      (no login shell, no home directory content needed).
#   2. Copies the bridge's .py files + requirements.txt to /opt/onvif-mqtt-bridge
#      and creates a Python venv there with the dependencies installed.
#   3. Copies config.example.yaml to /etc/onvif-mqtt-bridge/config.yaml if
#      that file doesn't already exist (never overwrites an existing config).
#   4. Installs and enables the systemd unit.
#
# Usage (run from the project root, i.e. the directory containing this
# script's parent "systemd/" folder):
#   sudo systemd/install.sh
#
# Safe to re-run: re-running updates the code/venv in /opt but leaves an
# existing /etc/onvif-mqtt-bridge/config.yaml untouched.

set -euo pipefail

if [[ $EUID -ne 0 ]]; then
  echo "Please run as root (sudo systemd/install.sh)" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
INSTALL_DIR=/opt/onvif-mqtt-bridge
CONFIG_DIR=/etc/onvif-mqtt-bridge
SERVICE_USER=onvif-mqtt-bridge
UNIT_NAME=onvif-mqtt-bridge.service

echo "==> Creating system user '${SERVICE_USER}' (if missing)"
if ! id "$SERVICE_USER" &>/dev/null; then
  useradd --system --no-create-home --shell /usr/sbin/nologin "$SERVICE_USER"
fi

echo "==> Installing code to ${INSTALL_DIR}"
mkdir -p "$INSTALL_DIR"
cp "$PROJECT_ROOT"/*.py "$INSTALL_DIR"/
cp "$PROJECT_ROOT"/requirements.txt "$INSTALL_DIR"/
chmod +x "$INSTALL_DIR/onvif_mqtt_bridge.py"

echo "==> Creating/updating venv in ${INSTALL_DIR}/venv"
python3 -m venv "$INSTALL_DIR/venv"
"$INSTALL_DIR/venv/bin/pip" install --upgrade pip --quiet
"$INSTALL_DIR/venv/bin/pip" install -r "$INSTALL_DIR/requirements.txt" --quiet

echo "==> Setting ownership"
chown -R "${SERVICE_USER}:${SERVICE_USER}" "$INSTALL_DIR"

echo "==> Installing config to ${CONFIG_DIR}"
mkdir -p "$CONFIG_DIR"
if [[ -f "$CONFIG_DIR/config.yaml" ]]; then
  echo "    ${CONFIG_DIR}/config.yaml already exists, leaving it untouched."
else
  cp "$PROJECT_ROOT/config.example.yaml" "$CONFIG_DIR/config.yaml"
  echo "    Installed example config - EDIT IT before starting the service:"
  echo "    sudo \$EDITOR ${CONFIG_DIR}/config.yaml"
fi
chown -R "${SERVICE_USER}:${SERVICE_USER}" "$CONFIG_DIR"
chmod 750 "$CONFIG_DIR"
chmod 640 "$CONFIG_DIR/config.yaml"

echo "==> Installing systemd unit"
cp "$SCRIPT_DIR/$UNIT_NAME" "/etc/systemd/system/$UNIT_NAME"
systemctl daemon-reload
systemctl enable "$UNIT_NAME"

cat <<MSG

==> Done.

Next steps:
  1. Edit ${CONFIG_DIR}/config.yaml (MQTT broker, cameras) if you haven't yet.
  2. Start it:   sudo systemctl start ${UNIT_NAME}
  3. Check it:   sudo systemctl status ${UNIT_NAME}
                 journalctl -u ${UNIT_NAME} -f

To re-deploy after editing the code, just re-run this script.
MSG
