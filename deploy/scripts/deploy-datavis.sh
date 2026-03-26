#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/home/ec2-user/cTrade"
VENV_DIR="/home/ec2-user/venvs/datavis"
SERVICE_NAME="datavis.service"
HEALTH_URL="http://127.0.0.1:8000/api/health"

log() {
  printf '[deploy-datavis] %s\n' "$1"
}

show_service_status() {
  sudo -n systemctl status "$SERVICE_NAME" --no-pager -l || true
}

on_exit() {
  local exit_code="$1"

  if [[ "$exit_code" -ne 0 ]]; then
    log "Deployment failed with exit code ${exit_code}"
    show_service_status
  fi
}

trap 'on_exit "$?"' EXIT

ensure_virtualenv() {
  if [[ -x "$VENV_DIR/bin/python3" ]]; then
    return
  fi

  log "Creating Python virtualenv at $VENV_DIR"
  mkdir -p "$(dirname "$VENV_DIR")"
  rm -rf "$VENV_DIR"
  python3 -m venv "$VENV_DIR"
}

if [[ ! -d "$APP_DIR/.git" ]]; then
  log "Missing git checkout at $APP_DIR"
  exit 1
fi

cd "$APP_DIR"

log "Fetching latest main from origin"
git fetch origin
git reset --hard origin/main

if [[ -e "$APP_DIR/server_snapshot" ]]; then
  log "Removing stale server snapshot"
  sudo -n rm -rf "$APP_DIR/server_snapshot"
fi

git clean -fd

ensure_virtualenv

log "Installing Python dependencies"
source "$VENV_DIR/bin/activate"
"$VENV_DIR/bin/python3" -m pip install --disable-pip-version-check --requirement requirements.txt

log "Restarting ${SERVICE_NAME}"
sudo -n systemctl restart "$SERVICE_NAME"

for _ in {1..15}; do
  if sudo -n systemctl is-active --quiet "$SERVICE_NAME"; then
    break
  fi
  sleep 2
done

sudo -n systemctl is-active --quiet "$SERVICE_NAME"

log "Checking ${HEALTH_URL}"
for _ in {1..15}; do
  if curl --fail --silent "$HEALTH_URL" >/dev/null 2>&1; then
    log "Deployment succeeded"
    exit 0
  fi
  sleep 2
done

log "Health check did not pass"
curl --fail --silent --show-error "$HEALTH_URL" >/dev/null
exit 1
