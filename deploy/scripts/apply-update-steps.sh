#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
LOG_DIR="${REPO_ROOT}/logs/update_journal"
TIMESTAMP="$(date '+%Y%m%d_%H%M%S')"
RELATIVE_JOURNAL_PATH="logs/update_journal/update_${TIMESTAMP}.log"
JOURNAL_PATH="${REPO_ROOT}/${RELATIVE_JOURNAL_PATH}"
ENV_FILE="/etc/datavis.env"
SQL_FILES=(
  "deploy/sql/20260424_motion_trade_spots.sql"
  "deploy/sql/20260425_motion_fingerprints.sql"
  "deploy/sql/20260425_motion_model_scenarios.sql"
)
VALIDATION_SQL_FILE="deploy/sql/20260425_motion_model_scenarios_validation.sql"
API_URL="http://127.0.0.1:8000/api/motion/signals/recent?limit=5"

mkdir -p "${LOG_DIR}"

exec > >(tee -a "${JOURNAL_PATH}") 2>&1

on_exit() {
  local exit_code=$?
  if [[ "${exit_code}" -eq 0 ]]; then
    printf 'SUCCESS: update steps completed\n'
  else
    printf 'FAILED: see %s\n' "${RELATIVE_JOURNAL_PATH}"
  fi
}

trap on_exit EXIT
trap 'printf "[apply-update-steps] ERROR: command failed at line %s\n" "${LINENO}"' ERR

resolve_python_bin() {
  if [[ -n "${VIRTUAL_ENV:-}" && -x "${VIRTUAL_ENV}/bin/python" ]]; then
    printf '%s\n' "${VIRTUAL_ENV}/bin/python"
    return 0
  fi
  if [[ -x "/home/ec2-user/venvs/datavis/bin/python" ]]; then
    printf '%s\n' "/home/ec2-user/venvs/datavis/bin/python"
    return 0
  fi
  if command -v python >/dev/null 2>&1; then
    command -v python
    return 0
  fi
  if command -v python3 >/dev/null 2>&1; then
    command -v python3
    return 0
  fi
  printf '[apply-update-steps] ERROR: no python interpreter was found in PATH\n'
  exit 1
}

cd "${REPO_ROOT}"

printf '[apply-update-steps] repo_root=%s\n' "${REPO_ROOT}"
printf '[apply-update-steps] journal=%s\n' "${RELATIVE_JOURNAL_PATH}"
printf '[apply-update-steps] git_commit=%s\n' "$(git rev-parse HEAD)"

if [[ -r "${ENV_FILE}" ]]; then
  printf '[apply-update-steps] Loading environment from %s\n' "${ENV_FILE}"
  set -a
  # shellcheck disable=SC1090
  source "${ENV_FILE}"
  set +a
else
  printf '[apply-update-steps] WARNING: %s is not readable; continuing with the current shell environment only\n' "${ENV_FILE}"
fi

PYTHON_BIN="$(resolve_python_bin)"
printf '[apply-update-steps] python=%s\n' "${PYTHON_BIN}"

DB_URL="$("${PYTHON_BIN}" deploy/scripts/resolve_db_url.py)"
if [[ -z "${DB_URL}" ]]; then
  printf '%s\n' "No DATABASE_URL or DATAVIS_DB_URL was available. Run with environment loaded or fix /etc/datavis.env permissions/service env."
  exit 1
fi

printf '[apply-update-steps] Testing database connection\n'
psql "${DB_URL}" -v ON_ERROR_STOP=1 -c "select now(), current_database(), current_user;"

for sql_file in "${SQL_FILES[@]}"; do
  if [[ -f "${sql_file}" ]]; then
    printf '[apply-update-steps] Applying SQL migration %s\n' "${sql_file}"
    psql "${DB_URL}" -v ON_ERROR_STOP=1 -f "${sql_file}"
  else
    printf '[apply-update-steps] WARNING: SQL migration file not found, skipping: %s\n' "${sql_file}"
  fi
done

printf '[apply-update-steps] Running motion trade spots backfill\n'
"${PYTHON_BIN}" -m datavis.motion_trade_spots backfill --last-broker-days 2

printf '[apply-update-steps] Running validation queries\n'
psql "${DB_URL}" -v ON_ERROR_STOP=1 <<'SQL'
select windowsec, count(*), min(timestamp), max(timestamp)
from public.motionpoint
group by windowsec
order by windowsec;

select side, outcome, count(*), avg(score)
from public.motionsignal
group by side, outcome
order by side, outcome;

select *
from public.motionsignal
order by score desc
limit 20;
SQL

if [[ -f "${VALIDATION_SQL_FILE}" ]]; then
  printf '[apply-update-steps] Running scenario validation SQL %s\n' "${VALIDATION_SQL_FILE}"
  psql "${DB_URL}" -v ON_ERROR_STOP=1 -f "${VALIDATION_SQL_FILE}"
else
  printf '[apply-update-steps] WARNING: validation SQL file not found, skipping: %s\n' "${VALIDATION_SQL_FILE}"
fi

printf '[apply-update-steps] Restarting datavis.service\n'
sudo systemctl restart datavis.service
if ! sudo systemctl is-active --quiet datavis.service; then
  sudo systemctl status datavis.service --no-pager -l || true
  exit 1
fi

if command -v curl >/dev/null 2>&1 && sudo systemctl is-active --quiet datavis.service; then
  printf '[apply-update-steps] Querying local motion signals API\n'
  curl --fail -sS "${API_URL}"
  printf '\n'
else
  printf '[apply-update-steps] WARNING: curl is unavailable or datavis.service is not active; skipping API check\n'
fi
