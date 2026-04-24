#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/home/ec2-user/cTrade"
VENV_ACTIVATE="/home/ec2-user/venvs/datavis/bin/activate"
ENV_FILE="/etc/datavis.env"
DEFAULT_DATABASE_URL="postgresql://babak:babak33044@localhost:5432/trading"
HEALTH_URL="http://127.0.0.1:8000/api/health"
DEPLOY_STATE_DIR="/home/ec2-user/.datavis"
LAST_DEPLOYED_SHA_FILE="${DEPLOY_STATE_DIR}/last_deployed_commit"
UPDATE_STEPS_SCRIPT="deploy/scripts/run-update-steps.sh"
UPDATE_STEPS_MANIFEST="deploy/update_steps.json"
UNIT_FILES=("datavis" "tickcollector" "backbone")
AUTO_MIGRATION_FILES=(
  "deploy/sql/20260411_layer_zero_rects.sql"
  "deploy/sql/20260418_remove_legacy_structure_layer.sql"
  "deploy/sql/20260419_speed_cleanup.sql"
  "deploy/sql/20260420_backbone.sql"
  "deploy/sql/20260422_backbone_bigbones.sql"
  "deploy/sql/20260422_retire_structure_family.sql"
)
REMOVED_SERVICES=(
  "separation"
  "ottprocessor"
  "envelopeprocessor"
  "zigzag"
  "envelopezigprocessor"
  "marketprofile"
  "fastzig"
  "zonebuilder"
)

declare -A RESTART_SERVICES=()
declare -A SELECTED_MIGRATIONS=()
declare -a CHANGED_FILES=()

DAEMON_RELOAD_REQUIRED=0
NGINX_RELOAD_REQUIRED=0
FULL_DEPLOY=0

log() {
  printf '[deploy-datavis] %s\n' "$1"
}

show_service_status() {
  local service_name="$1"
  sudo systemctl status "${service_name}.service" --no-pager -l || true
}

mark_service() {
  local service_name="$1"
  RESTART_SERVICES["$service_name"]=1
}

valid_commit() {
  local value="${1:-}"
  [[ -n "$value" ]] && git cat-file -e "${value}^{commit}" >/dev/null 2>&1
}

current_commit() {
  git rev-parse HEAD
}

resolve_previous_deployed_commit() {
  if [[ -f "$LAST_DEPLOYED_SHA_FILE" ]]; then
    local stored_sha
    stored_sha="$(tr -d '\r\n' < "$LAST_DEPLOYED_SHA_FILE")"
    if valid_commit "$stored_sha"; then
      printf '%s\n' "$stored_sha"
      return 0
    fi
  fi
  if valid_commit "${DEPLOY_PREVIOUS_SHA:-}"; then
    printf '%s\n' "$DEPLOY_PREVIOUS_SHA"
    return 0
  fi
  return 1
}

resolve_new_commit() {
  if valid_commit "${DEPLOY_NEW_SHA:-}"; then
    printf '%s\n' "$DEPLOY_NEW_SHA"
    return 0
  fi
  current_commit
}

collect_changed_files() {
  local previous_sha="$1"
  local new_sha="$2"
  if valid_commit "$previous_sha" && [[ "$previous_sha" != "$new_sha" ]]; then
    mapfile -t CHANGED_FILES < <(git diff --name-only "$previous_sha" "$new_sha")
  else
    FULL_DEPLOY=1
    mapfile -t CHANGED_FILES < <(git ls-files)
  fi
}

migration_service_mapping() {
  local migration_path="$1"
  case "$migration_path" in
    deploy/sql/20260411_layer_zero_rects.sql|deploy/sql/20260419_speed_cleanup.sql)
      mark_service "datavis"
      mark_service "tickcollector"
      ;;
    deploy/sql/20260420_backbone.sql|deploy/sql/20260422_backbone_bigbones.sql|deploy/sql/20260422_retire_structure_family.sql)
      mark_service "datavis"
      mark_service "backbone"
      ;;
    deploy/sql/20260418_remove_legacy_structure_layer.sql)
      mark_service "datavis"
      ;;
    *)
      log "Unknown migration mapping for ${migration_path}; defaulting to datavis + backbone"
      mark_service "datavis"
      mark_service "backbone"
      ;;
  esac
}

is_automated_migration() {
  local migration_path="$1"
  local candidate
  for candidate in "${AUTO_MIGRATION_FILES[@]}"; do
    if [[ "$candidate" == "$migration_path" ]]; then
      return 0
    fi
  done
  return 1
}

classify_changed_file() {
  local path="$1"
  case "$path" in
    requirements.txt|datavis/db.py)
      mark_service "datavis"
      mark_service "tickcollector"
      mark_service "backbone"
      ;;
    frontend/*|datavis/app.py|datavis/trading.py|datavis/smart_scalp.py|datavis/structure.py|datavis/rects.py)
      mark_service "datavis"
      ;;
    datavis/backbone.py|datavis/backbone_runtime.py|datavis/backbone_jobs.py|datavis/brokerday.py)
      mark_service "datavis"
      mark_service "backbone"
      ;;
    tickCollectorRawToDB.py|datavis/tickcollector_runtime.py|ctrader_open_api/*|datavis/broker_creds.py|datavis/ctrader_auth.py)
      mark_service "tickcollector"
      mark_service "datavis"
      ;;
    deploy/systemd/*.service)
      DAEMON_RELOAD_REQUIRED=1
      local service_name
      service_name="$(basename "$path" .service)"
      case "$service_name" in
        datavis|tickcollector|backbone)
          mark_service "$service_name"
          ;;
      esac
      ;;
    deploy/nginx/*|deploy/scripts/recover-datavis-nginx.sh)
      NGINX_RELOAD_REQUIRED=1
      ;;
    deploy/sql/*.sql)
      if is_automated_migration "$path"; then
        SELECTED_MIGRATIONS["$path"]=1
        migration_service_mapping "$path"
      else
        log "Skipping non-automated SQL file ${path}"
      fi
      ;;
  esac
}

install_operational_bins() {
  log "Installing operational CLI wrappers"
  sudo install -d /usr/local/bin
  sudo install -m 0755 "deploy/bin/getCsv" "/usr/local/bin/getCsv"
}

install_systemd_units() {
  log "Installing current systemd units"
  for service_name in "${UNIT_FILES[@]}"; do
    sudo install -m 0644 "deploy/systemd/${service_name}.service" "/etc/systemd/system/${service_name}.service"
  done
}

disable_removed_services() {
  for service_name in "${REMOVED_SERVICES[@]}"; do
    log "Disabling removed service ${service_name}"
    sudo systemctl disable --now "${service_name}.service" >/dev/null 2>&1 || true
    sudo rm -f "/etc/systemd/system/${service_name}.service"
  done
}

apply_selected_migrations() {
  local database_url="${DATABASE_URL:-$DEFAULT_DATABASE_URL}"
  local migration_path
  if [[ "${#SELECTED_MIGRATIONS[@]}" -eq 0 ]]; then
    log "No SQL migrations selected for this deploy"
    return
  fi
  mapfile -t migration_files < <(printf '%s\n' "${!SELECTED_MIGRATIONS[@]}" | sort)
  for migration_path in "${migration_files[@]}"; do
    log "Applying ${migration_path}"
    psql "$database_url" -v ON_ERROR_STOP=1 -f "$migration_path"
  done
}

recover_nginx_site() {
  if ! command -v nginx >/dev/null 2>&1; then
    log "nginx not installed; skipping site recovery"
    return
  fi
  log "Recovering nginx datavis.au site config"
  bash deploy/scripts/recover-datavis-nginx.sh
}

restart_selected_services() {
  local service_name
  if [[ "${#RESTART_SERVICES[@]}" -eq 0 ]]; then
    log "No runtime services selected for restart"
    return
  fi
  for service_name in datavis tickcollector backbone; do
    if [[ -n "${RESTART_SERVICES[$service_name]:-}" ]]; then
      log "Restarting ${service_name}.service"
      sudo systemctl restart "${service_name}.service"
      if ! sudo systemctl is-active --quiet "${service_name}.service"; then
        show_service_status "$service_name"
        exit 1
      fi
    fi
  done
}

enable_current_services() {
  local service_name
  for service_name in "${UNIT_FILES[@]}"; do
    sudo systemctl enable "${service_name}.service" >/dev/null 2>&1 || true
  done
}

log_change_summary() {
  local previous_sha="$1"
  local new_sha="$2"
  log "Previously deployed commit: ${previous_sha:-none}"
  log "Deploying commit: ${new_sha}"
  if [[ "$FULL_DEPLOY" -eq 1 ]]; then
    log "Changed files: full deploy"
  elif [[ "${#CHANGED_FILES[@]}" -eq 0 ]]; then
    log "Changed files: none"
  else
    log "Changed files:"
    printf '  %s\n' "${CHANGED_FILES[@]}"
  fi
  if [[ "${#SELECTED_MIGRATIONS[@]}" -eq 0 ]]; then
    log "Selected migrations: none"
  else
    log "Selected migrations:"
    printf '  %s\n' "${!SELECTED_MIGRATIONS[@]}" | sort
  fi
  if [[ "${#RESTART_SERVICES[@]}" -eq 0 ]]; then
    log "Selected service restarts: none"
  else
    log "Selected service restarts:"
    for service_name in datavis tickcollector backbone; do
      if [[ -n "${RESTART_SERVICES[$service_name]:-}" ]]; then
        printf '  %s.service\n' "$service_name"
      fi
    done
  fi
  log "daemon-reload required: ${DAEMON_RELOAD_REQUIRED}"
  log "nginx reload required: ${NGINX_RELOAD_REQUIRED}"
}

write_success_commit() {
  local new_sha="$1"
  install -d "$DEPLOY_STATE_DIR"
  printf '%s\n' "$new_sha" > "$LAST_DEPLOYED_SHA_FILE"
}

on_error() {
  log "Deployment failed"
  show_service_status "datavis"
  show_service_status "tickcollector"
  show_service_status "backbone"
  show_service_status "mavg"
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
  log "Missing virtualenv activate script: ${VENV_ACTIVATE}"
  exit 1
fi

previous_sha=""
if previous_sha="$(resolve_previous_deployed_commit)"; then
  :
fi
new_sha="$(resolve_new_commit)"
collect_changed_files "$previous_sha" "$new_sha"

if [[ "$FULL_DEPLOY" -eq 1 ]]; then
  DAEMON_RELOAD_REQUIRED=1
  NGINX_RELOAD_REQUIRED=1
  mark_service "datavis"
  mark_service "tickcollector"
  mark_service "backbone"
  while IFS= read -r migration_path; do
    SELECTED_MIGRATIONS["$migration_path"]=1
  done < <(printf '%s\n' "${AUTO_MIGRATION_FILES[@]}")
else
  for changed_path in "${CHANGED_FILES[@]}"; do
    classify_changed_file "$changed_path"
  done
fi

log_change_summary "$previous_sha" "$new_sha"

log "Activating virtual environment"
source "$VENV_ACTIVATE"
export DATAVIS_DEPLOY_STATE_DIR="$DEPLOY_STATE_DIR"
export DATAVIS_DEFAULT_DATABASE_URL="$DEFAULT_DATABASE_URL"

log "Installing Python dependencies"
pip install -r requirements.txt

install_operational_bins
install_systemd_units
disable_removed_services

if [[ "$DAEMON_RELOAD_REQUIRED" -eq 1 || "$FULL_DEPLOY" -eq 1 ]]; then
  log "Reloading systemd daemon"
  sudo systemctl daemon-reload
fi

enable_current_services
apply_selected_migrations

if [[ "$NGINX_RELOAD_REQUIRED" -eq 1 || "$FULL_DEPLOY" -eq 1 ]]; then
  recover_nginx_site
fi

restart_selected_services

if [[ ! -f "$UPDATE_STEPS_MANIFEST" ]]; then
  log "Missing update manifest: ${UPDATE_STEPS_MANIFEST}"
  exit 1
fi

log "Running typed update steps from ${UPDATE_STEPS_MANIFEST}"
bash "$UPDATE_STEPS_SCRIPT" --manifest "$UPDATE_STEPS_MANIFEST"

if command -v curl >/dev/null 2>&1; then
  log "Running local health check"
  for _ in {1..10}; do
    if curl --fail --silent --show-error "$HEALTH_URL" >/dev/null; then
      write_success_commit "$new_sha"
      log "Deployment succeeded"
      exit 0
    fi
    sleep 2
  done
  curl --fail --silent --show-error "$HEALTH_URL" >/dev/null
fi

write_success_commit "$new_sha"
log "Deployment succeeded"
