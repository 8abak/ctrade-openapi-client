#!/usr/bin/env bash
set -euo pipefail

APP_DIR="/home/ec2-user/cTrade"
ENV_FILE="/etc/datavis.env"
BACKUP_DIR="/home/ec2-user/backups/datavis"
CLEANUP_SQL="${APP_DIR}/deploy/sql/20260402_layer0_cleanup.sql"
LEGACY_SERVICES=("ottprocessor" "envelopeprocessor" "zigzag" "envelopezigprocessor" "marketprofile")
DEFAULT_DATABASE_URL="postgresql://babak:babak33044@localhost:5432/trading"

log() {
  printf '[cleanup-layer0] %s\n' "$1"
}

if [[ -f "$ENV_FILE" ]]; then
  set -a
  source "$ENV_FILE"
  set +a
fi

DATABASE_URL="${DATABASE_URL:-$DEFAULT_DATABASE_URL}"
TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
BACKUP_PATH="${BACKUP_DIR}/public-pre-layer0-${TIMESTAMP}.sql.gz"

mkdir -p "$BACKUP_DIR"

log "Creating backup ${BACKUP_PATH}"
pg_dump --dbname "$DATABASE_URL" --schema=public --no-owner --no-privileges --format=plain | gzip -c > "$BACKUP_PATH"

log "Applying ${CLEANUP_SQL}"
psql "$DATABASE_URL" -v ON_ERROR_STOP=1 -f "$CLEANUP_SQL"

for service_name in "${LEGACY_SERVICES[@]}"; do
  log "Disabling legacy service ${service_name}"
  sudo systemctl disable --now "${service_name}.service" >/dev/null 2>&1 || true
  sudo rm -f "/etc/systemd/system/${service_name}.service"
done

sudo systemctl daemon-reload

log "Layer 0 cleanup complete"
log "Backup written to ${BACKUP_PATH}"
