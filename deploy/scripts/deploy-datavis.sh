#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/home/ec2-user/cTrade"
VENV_ACTIVATE="/home/ec2-user/venvs/datavis/bin/activate"
SERVICE_NAME="datavis"
HEALTH_URL="http://127.0.0.1:8000/api/health"

log() {
  printf '[deploy-datavis] %s\n' "$1"
}

show_service_status() {
  sudo systemctl status "$SERVICE_NAME" --no-pager -l || true
}

on_error() {
  log "Deployment failed"
  show_service_status
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

log "Restarting ${SERVICE_NAME}"
sudo systemctl restart "$SERVICE_NAME"

log "Verifying ${SERVICE_NAME} is active"
if ! sudo systemctl is-active --quiet "$SERVICE_NAME"; then
  show_service_status
  exit 1
fi

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
