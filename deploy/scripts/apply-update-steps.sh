#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
LOG_DIR="${REPO_ROOT}/logs/update_journal"
SUMMARY_PATH="${REPO_ROOT}/deploy/updateJournal.md"
HEALTH_URL="http://127.0.0.1:8000/api/health"
TIMESTAMP="$(date '+%Y%m%d_%H%M%S')"
RUN_LOG_PATH="${LOG_DIR}/update_${TIMESTAMP}.log"

RUN_RESULT="failed"
COMMIT_HASH=""
COMMIT_MESSAGE=""
LOG_READY=0

timestamp_now() {
  date '+%Y-%m-%d %H:%M:%S'
}

print_permission_repair() {
  cat <<'EOF' >&2
Deployment logging path is not writable by the current deploy user.
One-time repair on EC2:
  sudo chown -R ec2-user:ec2-user logs deploy/updateJournal.md
  chmod -R u+rwX logs
Then rerun the deployment.
EOF
}

fail_permission_check() {
  local target="$1"
  printf 'Deployment logging preflight failed for %s\n' "${target}" >&2
  print_permission_repair
  exit 1
}

ensure_writable_dir() {
  local path="$1"
  local probe_path=""

  if [[ -e "${path}" && ! -d "${path}" ]]; then
    fail_permission_check "${path}"
  fi
  if ! mkdir -p "${path}" 2>/dev/null; then
    fail_permission_check "${path}"
  fi
  if [[ ! -w "${path}" || ! -x "${path}" ]]; then
    fail_permission_check "${path}"
  fi
  probe_path="${path}/.permission_probe_${TIMESTAMP}_$$"
  if ! : > "${probe_path}" 2>/dev/null; then
    fail_permission_check "${path}"
  fi
  rm -f "${probe_path}" 2>/dev/null || true
}

log() {
  local message="$*"
  local rendered="[$(timestamp_now)] ${message}"
  if [[ "${LOG_READY}" -eq 1 ]]; then
    printf '%s\n' "${rendered}" | tee -a "${RUN_LOG_PATH}"
  else
    printf '%s\n' "${rendered}"
  fi
}

write_summary() {
  if [[ -e "${SUMMARY_PATH}" && ! -f "${SUMMARY_PATH}" ]]; then
    log "Skipping summary write because ${SUMMARY_PATH} is not a regular file."
    return 0
  fi

  if [[ ! -e "${SUMMARY_PATH}" ]]; then
    if ! touch "${SUMMARY_PATH}" 2>/dev/null; then
      log "Skipping summary write because ${SUMMARY_PATH} is not writable."
      return 0
    fi
  elif [[ ! -w "${SUMMARY_PATH}" ]]; then
    log "Skipping summary write because ${SUMMARY_PATH} is not writable."
    return 0
  fi

  cat > "${SUMMARY_PATH}" <<EOF
date: $(date '+%Y-%m-%d')
time: $(date '+%H:%M:%S %Z')
commit: ${COMMIT_HASH}
commit message: ${COMMIT_MESSAGE}
result: ${RUN_RESULT}
log: logs/update_journal/$(basename "${RUN_LOG_PATH}")
steps:
1. restart datavis.service
2. sleep 5
3. health check ${HEALTH_URL}
EOF
}

on_exit() {
  local exit_code=$?
  if [[ "${exit_code}" -eq 0 ]]; then
    RUN_RESULT="success"
    log "Deployment succeeded."
  else
    RUN_RESULT="failed"
    log "Deployment failed."
  fi
  write_summary || true
  exit "${exit_code}"
}

trap on_exit EXIT

main() {
  ensure_writable_dir "${REPO_ROOT}/logs"
  ensure_writable_dir "${LOG_DIR}"
  : > "${RUN_LOG_PATH}"
  LOG_READY=1

  cd "${REPO_ROOT}"

  COMMIT_HASH="$(git rev-parse HEAD)"
  COMMIT_MESSAGE="$(git log -1 --pretty=%s)"

  log "Repo root: ${REPO_ROOT}"
  log "Commit: ${COMMIT_HASH}"
  log "Commit message: ${COMMIT_MESSAGE}"
  log "Journal: logs/update_journal/$(basename "${RUN_LOG_PATH}")"

  log "Restarting datavis.service"
  sudo systemctl restart datavis.service

  log "Sleeping 5 seconds before health check"
  sleep 5

  log "Running health check: ${HEALTH_URL}"
  curl -fsS "${HEALTH_URL}" | while IFS= read -r line; do
    log "health check: ${line}"
  done
}

main "$@"
