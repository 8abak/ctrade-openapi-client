#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/home/ec2-user/cTrade"
VENV_ACTIVATE="/home/ec2-user/venvs/datavis/bin/activate"
UNIT_FILES=("datavis" "tickcollector")
RESTART_SERVICES=("datavis")
LEGACY_SERVICES=("ottprocessor" "envelopeprocessor" "zigzag" "envelopezigprocessor" "marketprofile")
HEALTH_URL="http://127.0.0.1:8000/api/health"

log() {
  printf '[deploy-datavis] %s\n' "$1"
}

show_service_status() {
  local service_name="$1"
  sudo systemctl status "$service_name" --no-pager -l || true
}

install_systemd_units() {
  log "Installing systemd units"
  for service_name in "${UNIT_FILES[@]}"; do
    sudo install -m 0644 "deploy/systemd/${service_name}.service" "/etc/systemd/system/${service_name}.service"
  done
  sudo systemctl daemon-reload
}

on_error() {
  log "Deployment failed"
  for service_name in "${RESTART_SERVICES[@]}"; do
    show_service_status "$service_name"
  done
}

disable_legacy_services() {
  for service_name in "${LEGACY_SERVICES[@]}"; do
    log "Disabling legacy service ${service_name}"
    sudo systemctl disable --now "${service_name}.service" >/dev/null 2>&1 || true
    sudo rm -f "/etc/systemd/system/${service_name}.service"
  done
  sudo systemctl daemon-reload
}

trap on_error ERR

cd "$APP_DIR"

if [[ ! -f "$VENV_ACTIVATE" ]]; then
  log "Missing virtualenv activate script: $VENV_ACTIVATE"
  exit 1
fi

log "Activating virtual environment"
source "$VENV_ACTIVATE"

log "Installing Python dependencies"
pip install -r requirements.txt

install_systemd_units
disable_legacy_services

for service_name in "${UNIT_FILES[@]}"; do
  log "Enabling ${service_name}"
  sudo systemctl enable "${service_name}.service" >/dev/null 2>&1 || true
done

for service_name in "${RESTART_SERVICES[@]}"; do
  log "Restarting ${service_name}"
  sudo systemctl restart "${service_name}.service"

  log "Verifying ${service_name} is active"
  if ! sudo systemctl is-active --quiet "${service_name}.service"; then
    show_service_status "${service_name}.service"
    exit 1
  fi
done

log "tickcollector is intentionally not restarted by deploy"

if command -v curl >/dev/null 2>&1; then
  log "Running local health check"
  for _ in {1..10}; do
    if curl --fail --silent --show-error "$HEALTH_URL" >/dev/null; then
      log "Deployment succeeded"
      exit 0
    fi
    sleep 2
  done
  curl --fail --silent --show-error "$HEALTH_URL" >/dev/null
fi

log "Deployment succeeded"
