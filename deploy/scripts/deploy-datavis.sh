#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/home/ec2-user/cTrade"
VENV_ACTIVATE="/home/ec2-user/venvs/datavis/bin/activate"
ENV_FILE="/etc/datavis.env"
DEFAULT_DATABASE_URL="postgresql://babak:babak33044@localhost:5432/trading"
UNIT_FILES=("datavis" "tickcollector" "fastzig")
RESTART_SERVICES=("datavis" "fastzig")
LEGACY_SERVICES=("ottprocessor" "envelopeprocessor" "zigzag" "envelopezigprocessor" "marketprofile")
MIGRATION_FILES=("deploy/sql/20260403_fast_zig.sql" "deploy/sql/20260404_fast_zig_levels.sql")
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

apply_sql_migrations() {
  local database_url="${DATABASE_URL:-$DEFAULT_DATABASE_URL}"
  for migration_path in "${MIGRATION_FILES[@]}"; do
    log "Applying ${migration_path}"
    psql "$database_url" -v ON_ERROR_STOP=1 -f "$migration_path"
  done
}

trap on_error ERR

cd "$APP_DIR"

if [[ -f "$ENV_FILE" ]]; then
  log "Loading environment from ${ENV_FILE}"
  set -a
  if [[ -r "$ENV_FILE" ]]; then
    source "$ENV_FILE"
  else
    eval "$(sudo cat "$ENV_FILE")"
  fi
  set +a
fi

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
apply_sql_migrations

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
