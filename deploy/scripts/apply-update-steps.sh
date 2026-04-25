#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
MANIFEST_PATH="${REPO_ROOT}/deploy/update_steps.json"
SUMMARY_PATH="${REPO_ROOT}/deploy/updateJournal.md"
LOG_DIR="${REPO_ROOT}/logs/update_journal"
ENV_FILE="/etc/datavis.env"
DEFAULT_HEALTH_URL="http://127.0.0.1:8000/api/health"
TIMESTAMP="$(date '+%Y%m%d_%H%M%S')"
RELATIVE_LOG_PATH="logs/update_journal/update_${TIMESTAMP}.log"
RUN_LOG_PATH="${REPO_ROOT}/${RELATIVE_LOG_PATH}"

RUN_DATE="$(date '+%Y-%m-%d')"
RUN_TIME="$(date '+%H:%M:%S %Z')"
COMMIT_HASH=""
COMMIT_MESSAGE=""
MANIFEST_VERSION=""
MANIFEST_DESCRIPTION=""
OVERALL_RESULT="failed"
PENDING_DAEMON_RELOAD=0
TMP_DIR=""
PYTHON_BIN=""
LAST_COMMAND_FAILURE=""
STEP_NOTE_RESULT=""
CURRENT_STEP_INDEX=-1
CURRENT_STEP_LOG=""

declare -a STEP_NAMES=()
declare -a STEP_RESULTS=()
declare -a STEP_NOTES=()
declare -a STEP_LOG_FILES=()
declare -a MANIFEST_LINES=()

timestamp_now() {
  date '+%Y-%m-%d %H:%M:%S'
}

log() {
  local message="$*"
  local rendered
  rendered="[$(timestamp_now)] ${message}"
  printf '%s\n' "${rendered}" >> "${RUN_LOG_PATH}"
  if [[ -n "${CURRENT_STEP_LOG}" ]]; then
    printf '%s\n' "${rendered}" >> "${CURRENT_STEP_LOG}"
  fi
  printf '%s\n' "${rendered}"
}

trim_whitespace() {
  printf '%s' "$1" | tr '\r\n' '  ' | sed -E 's/[[:space:]]+/ /g; s/^ //; s/ $//'
}

decode_b64() {
  if [[ -z "${1:-}" ]]; then
    printf '\n'
    return 0
  fi
  printf '%s' "$1" | base64 --decode
  printf '\n'
}

resolve_python_bin() {
  if command -v python3 >/dev/null 2>&1; then
    command -v python3
    return 0
  fi
  if command -v python >/dev/null 2>&1; then
    command -v python
    return 0
  fi
  return 1
}

normalize_service_name() {
  local value
  value="$(trim_whitespace "${1:-}")"
  if [[ -z "${value}" ]]; then
    printf 'service name is required\n' >&2
    return 1
  fi
  if [[ "${value}" == *"/"* || "${value}" == *"\\"* ]]; then
    printf 'service name must not contain path separators: %s\n' "${value}" >&2
    return 1
  fi
  if [[ "${value}" != *.service ]]; then
    value="${value}.service"
  fi
  printf '%s\n' "${value}"
}

resolve_db_url() {
  local candidate=""
  if [[ -n "${DATABASE_URL:-}" ]]; then
    candidate="${DATABASE_URL}"
  elif [[ -n "${DATAVIS_DB_URL:-}" ]]; then
    candidate="${DATAVIS_DB_URL}"
  fi
  candidate="$(trim_whitespace "${candidate}")"
  if [[ "${candidate}" == postgresql+psycopg2://* ]]; then
    candidate="postgresql://${candidate#postgresql+psycopg2://}"
  fi
  printf '%s\n' "${candidate}"
}

load_ec2_env_if_available() {
  if [[ -r "${ENV_FILE}" ]]; then
    log "Loading environment from ${ENV_FILE}"
    set -a
    # shellcheck disable=SC1090
    source "${ENV_FILE}"
    set +a
    return 0
  fi
  log "Environment file ${ENV_FILE} is not readable; continuing with current shell environment"
}

start_step() {
  local name="$1"
  local note="$2"
  local index="${#STEP_NAMES[@]}"
  local step_number=$((index + 1))
  CURRENT_STEP_INDEX="${index}"
  CURRENT_STEP_LOG="${TMP_DIR}/step_${step_number}.log"
  : > "${CURRENT_STEP_LOG}"
  STEP_NAMES+=("${name}")
  STEP_RESULTS+=("pending")
  STEP_NOTES+=("${note}")
  STEP_LOG_FILES+=("${CURRENT_STEP_LOG}")
  log "Starting step ${step_number}: ${name}"
}

finish_step_success() {
  local note="$1"
  if [[ "${CURRENT_STEP_INDEX}" -lt 0 ]]; then
    return 0
  fi
  STEP_RESULTS[${CURRENT_STEP_INDEX}]="success"
  STEP_NOTES[${CURRENT_STEP_INDEX}]="${note}"
  log "Completed step $((CURRENT_STEP_INDEX + 1)): ${STEP_NAMES[${CURRENT_STEP_INDEX}]}"
  CURRENT_STEP_INDEX=-1
  CURRENT_STEP_LOG=""
}

finish_step_failure() {
  local note="$1"
  if [[ "${CURRENT_STEP_INDEX}" -ge 0 ]]; then
    STEP_RESULTS[${CURRENT_STEP_INDEX}]="failed"
    STEP_NOTES[${CURRENT_STEP_INDEX}]="${note}"
    log "Failed step $((CURRENT_STEP_INDEX + 1)): ${STEP_NAMES[${CURRENT_STEP_INDEX}]}"
  fi
  OVERALL_RESULT="failed"
  CURRENT_STEP_INDEX=-1
  CURRENT_STEP_LOG=""
}

render_summary() {
  local step_count="${#STEP_NAMES[@]}"
  {
    printf 'date: %s\n' "${RUN_DATE}"
    printf 'time: %s\n' "${RUN_TIME}"
    printf 'commit: %s\n' "${COMMIT_HASH}"
    printf 'commit message: %s\n' "${COMMIT_MESSAGE}"
    printf 'overall result: %s\n\n' "${OVERALL_RESULT}"
    printf 'steps:\n'
    if [[ "${step_count}" -eq 0 ]]; then
      printf '1. no deployment steps executed\n'
      printf '   result: %s\n' "${OVERALL_RESULT}"
      printf '   notes: Manifest load or preflight failed before any steps started.\n'
      printf '   last journal lines:\n'
      printf '   (none)\n'
    else
      local index
      for ((index = 0; index < step_count; index += 1)); do
        printf '%s. %s\n' "$((index + 1))" "${STEP_NAMES[${index}]}"
        printf '   result: %s\n' "${STEP_RESULTS[${index}]}"
        printf '   notes: %s\n' "$(trim_whitespace "${STEP_NOTES[${index}]}")"
        printf '   last journal lines:\n'
        if [[ -s "${STEP_LOG_FILES[${index}]}" ]]; then
          while IFS= read -r line; do
            printf '   %s\n' "${line}"
          done < <(tail -n 5 "${STEP_LOG_FILES[${index}]}")
        else
          printf '   (none)\n'
        fi
        if [[ "${index}" -lt $((step_count - 1)) ]]; then
          printf '\n'
        fi
      done
    fi
  } > "${SUMMARY_PATH}"
}

cleanup() {
  local exit_code=$?
  if [[ "${exit_code}" -eq 0 && "${OVERALL_RESULT}" != "failed" ]]; then
    OVERALL_RESULT="success"
  fi
  render_summary
  CURRENT_STEP_INDEX=-1
  CURRENT_STEP_LOG=""
  if [[ -n "${TMP_DIR}" && -d "${TMP_DIR}" ]]; then
    rm -rf "${TMP_DIR}"
  fi
  if [[ "${exit_code}" -eq 0 ]]; then
    log "Deployment completed successfully"
  else
    log "Deployment failed"
  fi
}

trap cleanup EXIT

run_logged_command() {
  local label="$1"
  local timeout_seconds="${2:-}"
  shift 2
  local -a cmd=("$@")
  local exit_code=0

  LAST_COMMAND_FAILURE=""
  if [[ "${#cmd[@]}" -eq 0 ]]; then
    LAST_COMMAND_FAILURE="${label} has no command to execute."
    return 1
  fi

  set +e
  if [[ -n "${timeout_seconds}" ]] && command -v timeout >/dev/null 2>&1; then
    timeout "${timeout_seconds}" "${cmd[@]}" \
      > >(while IFS= read -r line; do log "${label}: ${line}"; done) \
      2> >(while IFS= read -r line; do log "${label} [stderr]: ${line}"; done)
    exit_code=$?
  else
    "${cmd[@]}" \
      > >(while IFS= read -r line; do log "${label}: ${line}"; done) \
      2> >(while IFS= read -r line; do log "${label} [stderr]: ${line}"; done)
    exit_code=$?
  fi
  set -e

  if [[ "${exit_code}" -eq 124 ]]; then
    LAST_COMMAND_FAILURE="${label} timed out after ${timeout_seconds}s."
    return 1
  fi
  if [[ "${exit_code}" -ne 0 ]]; then
    LAST_COMMAND_FAILURE="${label} failed with exit code ${exit_code}."
    return 1
  fi
  return 0
}

ensure_daemon_reload_if_pending() {
  if [[ "${PENDING_DAEMON_RELOAD}" -eq 0 ]]; then
    return 0
  fi
  log "Running pending systemd daemon-reload before service operation"
  if ! run_logged_command "systemctl daemon-reload" "60" sudo systemctl daemon-reload; then
    return 1
  fi
  PENDING_DAEMON_RELOAD=0
}

run_health_check() {
  local url="$1"
  local retries="$2"
  local interval_seconds="$3"
  local request_timeout="$4"
  local attempt
  local curl_output=""

  if ! command -v curl >/dev/null 2>&1; then
    LAST_COMMAND_FAILURE="curl is required for health checks but is not installed."
    return 1
  fi

  for ((attempt = 1; attempt <= retries; attempt += 1)); do
    log "Health check attempt ${attempt}/${retries}: ${url}"
    set +e
    curl_output="$(curl --silent --show-error --fail --max-time "${request_timeout}" "${url}" 2>&1)"
    local exit_code=$?
    set -e
    if [[ "${exit_code}" -eq 0 ]]; then
      if [[ -n "${curl_output}" ]]; then
        while IFS= read -r line; do
          log "health check: ${line}"
        done <<< "${curl_output}"
      fi
      STEP_NOTE_RESULT="Health check passed after ${attempt} attempt(s)."
      return 0
    fi
    if [[ -n "${curl_output}" ]]; then
      while IFS= read -r line; do
        log "health check failure: ${line}"
      done <<< "${curl_output}"
    fi
    if [[ "${attempt}" -lt "${retries}" ]]; then
      sleep "${interval_seconds}"
    fi
  done

  LAST_COMMAND_FAILURE="Health check failed after ${retries} attempt(s) for ${url}."
  return 1
}

load_manifest_lines() {
  "${PYTHON_BIN}" - "${MANIFEST_PATH}" <<'PY'
import base64
import json
import sys

SUPPORTED_TYPES = {
    "run_sql_file",
    "run_command",
    "backfill_command",
    "scenario_command",
    "verify_command",
    "sleep",
    "install_systemd_unit",
    "daemon_reload",
    "restart_service",
    "reload_service",
    "start_service",
    "enable_service",
    "health_check",
}

def b64(value: str) -> str:
    return base64.b64encode(value.encode("utf-8")).decode("ascii")

path = sys.argv[1]
with open(path, "r", encoding="utf-8") as handle:
    payload = json.load(handle)

if not isinstance(payload, dict):
    raise SystemExit("deploy/update_steps.json must contain a JSON object.")

version = str(payload.get("version") or "").strip()
description = str(payload.get("description") or "").strip()
actions = payload.get("actions")
if not version:
    raise SystemExit("deploy/update_steps.json requires a non-empty version.")
if not description:
    raise SystemExit("deploy/update_steps.json requires a non-empty description.")
if not isinstance(actions, list):
    raise SystemExit("deploy/update_steps.json requires an actions array.")

print("\t".join(["MANIFEST", b64(version), b64(description)]))
for index, action in enumerate(actions, start=1):
    if not isinstance(action, dict):
        raise SystemExit(f"action {index} must be a JSON object.")
    action_id = str(action.get("id") or "").strip()
    action_name = str(action.get("name") or action.get("description") or action_id or f"step_{index}").strip()
    action_description = str(action.get("description") or action_name).strip()
    action_type = str(action.get("type") or "").strip()
    if not action_id:
        raise SystemExit(f"action {index} requires a non-empty id.")
    if action_type not in SUPPORTED_TYPES:
        raise SystemExit(f"action {action_id} has unsupported type {action_type!r}.")
    timeout_seconds = "" if action.get("timeout_seconds") in (None, "") else str(int(action["timeout_seconds"]))
    sleep_seconds = "" if action.get("seconds") in (None, "") else str(action["seconds"])
    retries = "" if action.get("retries") in (None, "") else str(int(action["retries"]))
    interval_seconds = "" if action.get("interval_seconds") in (None, "") else str(action["interval_seconds"])
    fields = [
        "ACTION",
        str(index),
        b64(action_id),
        b64(action_name),
        b64(action_description),
        b64(action_type),
        b64(str(action.get("command") or "")),
        b64(str(action.get("file") or "")),
        b64(str(action.get("service") or "")),
        b64(str(action.get("url") or "")),
        timeout_seconds,
        sleep_seconds,
        retries,
        interval_seconds,
    ]
    print("\t".join(fields))
PY
}

run_manifest_action() {
  local action_name="$1"
  local action_description="$2"
  local action_type="$3"
  local action_command="$4"
  local action_file="$5"
  local action_service="$6"
  local action_url="$7"
  local action_timeout="$8"
  local action_seconds="$9"
  local action_retries="${10}"
  local action_interval="${11}"
  local service_name=""
  local sql_path=""
  local db_url=""

  STEP_NOTE_RESULT="${action_description}"
  LAST_COMMAND_FAILURE=""

  case "${action_type}" in
    run_sql_file)
      sql_path="$(trim_whitespace "${action_file}")"
      if [[ -z "${sql_path}" || ! -f "${REPO_ROOT}/${sql_path}" ]]; then
        LAST_COMMAND_FAILURE="SQL file not found: ${sql_path}"
        return 1
      fi
      db_url="$(resolve_db_url)"
      if [[ -z "${db_url}" ]]; then
        LAST_COMMAND_FAILURE="DATABASE_URL or DATAVIS_DB_URL is required for SQL steps."
        return 1
      fi
      if ! run_logged_command "psql ${sql_path}" "${action_timeout:-60}" psql "${db_url}" -v ON_ERROR_STOP=1 -f "${REPO_ROOT}/${sql_path}"; then
        return 1
      fi
      STEP_NOTE_RESULT="Applied SQL migration ${sql_path}."
      ;;
    run_command|backfill_command|scenario_command|verify_command)
      if [[ -z "$(trim_whitespace "${action_command}")" ]]; then
        LAST_COMMAND_FAILURE="Shell command is required for ${action_name}."
        return 1
      fi
      if ! run_logged_command "${action_name}" "${action_timeout}" bash -lc "${action_command}"; then
        return 1
      fi
      STEP_NOTE_RESULT="${action_description}"
      ;;
    sleep)
      if [[ -z "$(trim_whitespace "${action_seconds}")" ]]; then
        LAST_COMMAND_FAILURE="seconds is required for sleep steps."
        return 1
      fi
      log "Sleeping for ${action_seconds} second(s)"
      sleep "${action_seconds}"
      STEP_NOTE_RESULT="Slept for ${action_seconds} second(s)."
      ;;
    install_systemd_unit)
      sql_path="$(trim_whitespace "${action_file}")"
      if [[ -z "${sql_path}" || ! -f "${REPO_ROOT}/${sql_path}" ]]; then
        LAST_COMMAND_FAILURE="Systemd unit file not found: ${sql_path}"
        return 1
      fi
      if ! service_name="$(normalize_service_name "${action_service}")"; then
        LAST_COMMAND_FAILURE="Invalid service name for ${action_name}."
        return 1
      fi
      if ! run_logged_command "install ${service_name}" "${action_timeout:-60}" sudo install -m 0644 "${REPO_ROOT}/${sql_path}" "/etc/systemd/system/${service_name}"; then
        return 1
      fi
      PENDING_DAEMON_RELOAD=1
      STEP_NOTE_RESULT="Installed ${service_name} from ${sql_path}."
      ;;
    daemon_reload)
      if ! run_logged_command "systemctl daemon-reload" "${action_timeout:-60}" sudo systemctl daemon-reload; then
        return 1
      fi
      PENDING_DAEMON_RELOAD=0
      STEP_NOTE_RESULT="Reloaded systemd units."
      ;;
    restart_service|reload_service|start_service|enable_service)
      if ! ensure_daemon_reload_if_pending; then
        return 1
      fi
      if ! service_name="$(normalize_service_name "${action_service}")"; then
        LAST_COMMAND_FAILURE="Invalid service name for ${action_name}."
        return 1
      fi
      if ! run_logged_command "systemctl ${action_type%%_*} ${service_name}" "${action_timeout:-60}" sudo systemctl "${action_type%%_*}" "${service_name}"; then
        return 1
      fi
      if [[ "${action_type}" == "restart_service" || "${action_type}" == "start_service" ]]; then
        if ! run_logged_command "systemctl is-active ${service_name}" "15" sudo systemctl is-active --quiet "${service_name}"; then
          run_logged_command "systemctl status ${service_name}" "20" sudo systemctl status "${service_name}" --no-pager -l || true
          LAST_COMMAND_FAILURE="${service_name} is not active after ${action_type%%_*}."
          return 1
        fi
      fi
      STEP_NOTE_RESULT="${action_type%%_*} completed for ${service_name}."
      ;;
    health_check)
      if [[ -z "$(trim_whitespace "${action_url}")" ]]; then
        action_url="${DEFAULT_HEALTH_URL}"
      fi
      if [[ -z "$(trim_whitespace "${action_retries}")" ]]; then
        action_retries="20"
      fi
      if [[ -z "$(trim_whitespace "${action_interval}")" ]]; then
        action_interval="1"
      fi
      if [[ -z "$(trim_whitespace "${action_timeout}")" ]]; then
        action_timeout="5"
      fi
      if ! run_health_check "${action_url}" "${action_retries}" "${action_interval}" "${action_timeout}"; then
        return 1
      fi
      ;;
    *)
      LAST_COMMAND_FAILURE="Unsupported action type ${action_type}."
      return 1
      ;;
  esac

  return 0
}

execute_manifest_steps() {
  local line=""
  local record_type=""
  local index=""
  local action_name=""
  local action_description=""
  local action_type=""
  local action_command=""
  local action_file=""
  local action_service=""
  local action_url=""
  local action_timeout=""
  local action_seconds=""
  local action_retries=""
  local action_interval=""

  for line in "${MANIFEST_LINES[@]}"; do
    [[ -z "${line}" ]] && continue
    IFS=$'\t' read -r record_type index _ action_name action_description action_type action_command action_file action_service action_url action_timeout action_seconds action_retries action_interval <<< "${line}"
    if [[ "${record_type}" != "ACTION" ]]; then
      continue
    fi
    action_name="$(trim_whitespace "$(decode_b64 "${action_name}")")"
    action_description="$(trim_whitespace "$(decode_b64 "${action_description}")")"
    action_type="$(trim_whitespace "$(decode_b64 "${action_type}")")"
    action_command="$(decode_b64 "${action_command}")"
    action_command="${action_command%$'\n'}"
    action_file="$(trim_whitespace "$(decode_b64 "${action_file}")")"
    action_service="$(trim_whitespace "$(decode_b64 "${action_service}")")"
    action_url="$(trim_whitespace "$(decode_b64 "${action_url}")")"

    start_step "${action_name}" "${action_description}"
    if run_manifest_action \
      "${action_name}" \
      "${action_description}" \
      "${action_type}" \
      "${action_command}" \
      "${action_file}" \
      "${action_service}" \
      "${action_url}" \
      "${action_timeout}" \
      "${action_seconds}" \
      "${action_retries}" \
      "${action_interval}"; then
      finish_step_success "${STEP_NOTE_RESULT}"
    else
      finish_step_failure "${LAST_COMMAND_FAILURE}"
      return 1
    fi
  done
}

main() {
  mkdir -p "${LOG_DIR}"
  : > "${RUN_LOG_PATH}"
  : > "${SUMMARY_PATH}"
  TMP_DIR="$(mktemp -d "${LOG_DIR}/.apply-update-steps.${TIMESTAMP}.XXXXXX")"

  cd "${REPO_ROOT}"

  if ! PYTHON_BIN="$(resolve_python_bin)"; then
    log "No python interpreter was found in PATH"
    exit 1
  fi

  if [[ ! -f "${MANIFEST_PATH}" ]]; then
    log "Missing update manifest: ${MANIFEST_PATH}"
    exit 1
  fi

  COMMIT_HASH="$(trim_whitespace "$(git rev-parse HEAD)")"
  COMMIT_MESSAGE="$(trim_whitespace "$(git log -1 --pretty=%s)")"

  log "Repo root: ${REPO_ROOT}"
  log "Run log: ${RELATIVE_LOG_PATH}"
  log "Commit: ${COMMIT_HASH}"
  log "Commit message: ${COMMIT_MESSAGE}"

  load_ec2_env_if_available

  mapfile -t MANIFEST_LINES <<< "$(load_manifest_lines)"
  if [[ "${#MANIFEST_LINES[@]}" -eq 0 ]]; then
    log "No manifest data was loaded from ${MANIFEST_PATH}"
    exit 1
  fi

  IFS=$'\t' read -r _ manifest_version_b64 manifest_description_b64 <<< "${MANIFEST_LINES[0]}"
  MANIFEST_VERSION="$(trim_whitespace "$(decode_b64 "${manifest_version_b64}")")"
  MANIFEST_DESCRIPTION="$(trim_whitespace "$(decode_b64 "${manifest_description_b64}")")"
  log "Update manifest version: ${MANIFEST_VERSION}"
  log "Update manifest description: ${MANIFEST_DESCRIPTION}"

  execute_manifest_steps

  OVERALL_RESULT="success"
}

main "$@"
