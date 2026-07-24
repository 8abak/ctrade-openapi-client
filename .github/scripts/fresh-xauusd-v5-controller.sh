#!/usr/bin/env bash
set -Eeuo pipefail
umask 077

branch="$1"
commit_sha="$2"
run_id="$3"
attempt="$4"
restart_archive="$5"
terminal_archive="$6"
expected_controller_sha="$7"
ready_receipt="$8"
failure_receipt="$9"
terminal_receipt="${10}"

readonly v5_lineage_sha="6377d53891675e02b645bf83b52b24b5ffb5a7b8cc76b701cd3450b2cecd7473"
readonly run19_artifact_id="8585919266"
readonly run19_archive_sha="f947348d892d1c996df15188c3221595066c019957f4dccf24697502d2d4fbf9"
readonly production_repo="/home/ec2-user/cTrade"
readonly durable_state_root="/home/ec2-user/.local/state/datavis/fresh-xauusd-research-v2"
readonly scratch_root="/home/ec2-user/.local/state/datavis/fresh-xauusd-scratch-v1"
readonly launch_root="/home/ec2-user/.local/state/datavis/fresh-xauusd-launch-v1"
readonly artifact_root="/home/ec2-user/.local/state/datavis/fresh-xauusd-artifacts-v1"

worktree=""
output=""
restart_directory=""
scratch=""
log_path=""
pipeline_pid=""
pipeline_start_ticks=""
controller_start_ticks=""
pipeline_reaped=0
ready_written=0
durable_state_root_resolved=""

write_receipt() {
  target="$1"
  kind="$2"
  status="$3"
  archive_size="${4:-}"
  archive_sha="${5:-}"
  archive_device="${6:-}"
  archive_inode="${7:-}"
  python3 -B - \
    "${target}" "${kind}" "${status}" \
    "${run_id}" "${attempt}" "${branch}" "${commit_sha}" \
    "${v5_lineage_sha}" "${run19_artifact_id}" "${run19_archive_sha}" \
    "${expected_controller_sha}" "$$" "${controller_start_ticks}" \
    "${pipeline_pid}" "${pipeline_start_ticks}" \
    "${worktree}" "${output}" "${scratch}" "${restart_directory}" \
    "$(dirname -- "${restart_archive}")" "${terminal_archive}" "${log_path}" \
    "${archive_size}" "${archive_sha}" "${archive_device}" "${archive_inode}" <<'PY'
import json
import os
from pathlib import Path
import sys

(
    target,
    kind,
    status,
    run_id,
    attempt,
    branch,
    commit_sha,
    lineage_sha,
    artifact_id,
    restart_sha,
    controller_sha,
    controller_pid,
    controller_start_ticks,
    pipeline_pid,
    pipeline_start_ticks,
    worktree,
    output,
    scratch,
    restart_directory,
    transfer_directory,
    terminal_archive,
    log_path,
    archive_size,
    archive_sha,
    archive_device,
    archive_inode,
) = sys.argv[1:]

payload = {
    "schema": "fresh-xauusd-detached-research-receipt/v1",
    "kind": kind,
    "status": (
        status
        if kind == "launch_ready"
        else ("succeeded" if int(status) == 0 else "failed")
    ),
    "processExitStatus": (
        None if kind == "launch_ready" else int(status)
    ),
    "githubRunId": int(run_id),
    "githubRunAttempt": int(attempt),
    "branch": branch,
    "commitSha": commit_sha,
    "studyLineageSha256": lineage_sha,
    "run19ArtifactId": int(artifact_id),
    "run19TerminalArchiveSha256": restart_sha,
    "controllerSha256": controller_sha,
    "controllerPid": int(controller_pid),
    "controllerStartTicks": int(controller_start_ticks),
    "pipelinePid": int(pipeline_pid) if pipeline_pid else None,
    "pipelineStartTicks": (
        int(pipeline_start_ticks) if pipeline_start_ticks else None
    ),
    "paths": {
        "worktree": worktree or None,
        "output": output or None,
        "scratch": scratch or None,
        "restart": restart_directory or None,
        "transfer": transfer_directory,
        "terminalArchive": terminal_archive,
        "serverLog": log_path or None,
    },
    "terminalArchive": (
        {
            "size": int(archive_size),
            "sha256": archive_sha,
            "device": int(archive_device),
            "inode": int(archive_inode),
        }
        if archive_size
        else None
    ),
}
target_path = Path(target)
temporary_name = f".{target_path.name}.{os.getpid()}.partial"
flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
if hasattr(os, "O_NOFOLLOW"):
    flags |= os.O_NOFOLLOW
directory_descriptor = os.open(
    target_path.parent,
    os.O_RDONLY
    | getattr(os, "O_DIRECTORY", 0)
    | getattr(os, "O_NOFOLLOW", 0),
)
try:
    descriptor = os.open(
        temporary_name,
        flags,
        0o600,
        dir_fd=directory_descriptor,
    )
    with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.link(
        temporary_name,
        target_path.name,
        src_dir_fd=directory_descriptor,
        dst_dir_fd=directory_descriptor,
        follow_symlinks=False,
    )
    os.fsync(directory_descriptor)
finally:
    try:
        os.unlink(temporary_name, dir_fd=directory_descriptor)
    except FileNotFoundError:
        pass
    os.close(directory_descriptor)
PY
}

binding_value() {
  binding_path="$1"
  binding_key="$2"
  python3 -B - "${binding_path}" "${binding_key}" <<'PY'
import json
import os
import sys

binding_path, binding_key = sys.argv[1:]
with open(binding_path, "r", encoding="utf-8") as handle:
    payload = json.load(handle)
value = payload.get(binding_key)
if (
    not isinstance(value, str)
    or not value
    or not os.path.isabs(value)
    or any(character in value for character in "\x00\r\n")
):
    raise SystemExit(f"invalid {binding_key} in research state binding")
sys.stdout.write(value)
PY
}

snapshot_state_file() {
  source_path="$1"
  destination_name="$2"
  destination_path="${output}/${destination_name}"
  if [[ ! -e "${source_path}" && ! -L "${source_path}" ]]; then
    return 0
  fi
  resolved_source="$(readlink -f -- "${source_path}")" || return 1
  case "${resolved_source}" in
    "${durable_state_root_resolved}"/*)
      ;;
    *)
      printf 'refusing state source outside durable root: %s\n' \
        "${resolved_source}" >&2
      return 1
      ;;
  esac
  if [[ ! -f "${resolved_source}" || -L "${destination_path}" ]]; then
    printf 'refusing non-regular state snapshot\n' >&2
    return 1
  fi
  if [[ -e "${destination_path}" ]]; then
    [[ -f "${destination_path}" && ! -L "${destination_path}" ]]
    return
  fi
  cp --no-clobber -- "${resolved_source}" "${destination_path}"
}

snapshot_durable_state() {
  binding_path="${output}/fresh_research_state_binding_v4.json"
  if [[ ! -e "${binding_path}" && ! -L "${binding_path}" ]]; then
    return 0
  fi
  if [[ ! -f "${binding_path}" || -L "${binding_path}" ]]; then
    printf 'v5 state binding is not a real regular file\n' >&2
    return 1
  fi
  ledger_source="$(
    binding_value "${binding_path}" experimentLedgerPath
  )" || return 1
  holdout_source="$(
    binding_value "${binding_path}" holdoutAuthorizationRegistryPath
  )" || return 1
  snapshot_state_file \
    "${ledger_source}" fresh_experiment_ledger_v1.jsonl || return 1
  snapshot_state_file \
    "${holdout_source}" fresh_holdout_authorization_v1.json || return 1
}

fsync_regular_and_parent() {
  target="$1"
  python3 -B - "${target}" <<'PY'
import os
from pathlib import Path
import stat
import sys

target = Path(sys.argv[1])
flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
descriptor = os.open(target, flags)
try:
    metadata = os.fstat(descriptor)
    if not stat.S_ISREG(metadata.st_mode):
        raise SystemExit("terminal archive is not a regular file")
    os.fsync(descriptor)
finally:
    os.close(descriptor)
directory_descriptor = os.open(
    target.parent,
    os.O_RDONLY
    | getattr(os, "O_DIRECTORY", 0)
    | getattr(os, "O_NOFOLLOW", 0),
)
try:
    os.fsync(directory_descriptor)
finally:
    os.close(directory_descriptor)
PY
}

finalize() {
  status="$?"
  trap - EXIT
  set +e
  if [[ -n "${pipeline_pid}" && "${pipeline_reaped}" -ne 1 ]]; then
    wait "${pipeline_pid}"
    child_status="$?"
    pipeline_reaped=1
    if [[ "${status}" -eq 0 && "${child_status}" -ne 0 ]]; then
      status="${child_status}"
    fi
  fi
  archive_size=""
  archive_sha=""
  archive_device=""
  archive_inode=""

  if [[ -n "${output}" && -d "${output}" && ! -L "${output}" ]]; then
    if [[ -n "${log_path}" && -f "${log_path}" && ! -L "${log_path}" ]]; then
      if [[ ! -e "${output}/server-run.log" ]]; then
        cp --no-clobber -- "${log_path}" "${output}/server-run.log" || status=1
      fi
    fi
    snapshot_durable_state || status=1
    if [[ ! -e "${output}/remote-exit-status.txt" ]]; then
      (
        set -o noclobber
        printf '%s\n' "${status}" > "${output}/remote-exit-status.txt"
      ) || status=1
    fi

    archive_partial="${terminal_archive}.partial"
    if [[ -e "${archive_partial}" || -L "${archive_partial}" ||
      -e "${terminal_archive}" || -L "${terminal_archive}" ]]; then
      printf 'refusing to replace an existing terminal archive\n' >&2
      status=1
    elif (
      set -o noclobber
      tar -C "${output}" -czf - . > "${archive_partial}"
    ) &&
      [[ -f "${archive_partial}" && ! -L "${archive_partial}" ]] &&
      tar -tzf "${archive_partial}" >/dev/null &&
      sha256sum -- "${archive_partial}" >/dev/null &&
      ln -- "${archive_partial}" "${terminal_archive}" &&
      rm -f -- "${archive_partial}" &&
      fsync_regular_and_parent "${terminal_archive}"; then
      archive_size="$(stat -c '%s' -- "${terminal_archive}")"
      archive_sha="$(sha256sum -- "${terminal_archive}" | awk '{print $1}')"
      archive_device="$(stat -c '%d' -- "${terminal_archive}")"
      archive_inode="$(stat -c '%i' -- "${terminal_archive}")"
    else
      printf 'failed to create immutable terminal archive\n' >&2
      status=1
    fi
  else
    status=1
  fi

  if [[ "${ready_written}" -ne 1 && ! -e "${failure_receipt}" ]]; then
    write_receipt "${failure_receipt}" failure "${status}" \
      "${archive_size}" "${archive_sha}" "${archive_device}" "${archive_inode}" ||
      true
  fi
  if [[ ! -e "${terminal_receipt}" ]]; then
    write_receipt "${terminal_receipt}" terminal "${status}" \
      "${archive_size}" "${archive_sha}" "${archive_device}" "${archive_inode}" ||
      true
  fi
  exit "${status}"
}

if [[ "${branch}" != "codex/xauusd-fresh-walkforward" ||
  ! "${commit_sha}" =~ ^[0-9a-f]{40}$ ||
  ! "${run_id}" =~ ^[0-9]+$ ||
  ! "${attempt}" =~ ^[0-9]+$ ||
  ! "${expected_controller_sha}" =~ ^[0-9a-f]{64}$ ]]; then
  printf 'invalid detached-controller identity arguments\n' >&2
  exit 1
fi
controller_start_ticks="$(
  python3 -B - "$$" <<'PY'
from pathlib import Path
import sys

raw = Path(f"/proc/{sys.argv[1]}/stat").read_text(encoding="ascii")
print(raw.rsplit(") ", 1)[1].split()[19])
PY
)"

controller_path="$(readlink -f -- "$0")"
transfer_directory="$(dirname -- "${restart_archive}")"
transfer_resolved="$(readlink -f -- "${transfer_directory}")"
case "${transfer_resolved}" in
  /tmp/fresh-xauusd-transfer.*)
    ;;
  *)
    printf 'refusing unexpected transfer directory: %s\n' \
      "${transfer_resolved}" >&2
    exit 1
    ;;
esac
if [[ -L "${transfer_directory}" || ! -d "${transfer_directory}" ||
  "$(dirname -- "${terminal_archive}")" != "${artifact_root}" ||
  "$(dirname -- "${controller_path}")" != "${transfer_resolved}" ]]; then
  printf 'detached-controller paths failed containment checks\n' >&2
  exit 1
fi
if [[ "$(sha256sum -- "${controller_path}" | awk '{print $1}')" != \
  "${expected_controller_sha}" ]]; then
  printf 'detached-controller digest changed\n' >&2
  exit 1
fi
if [[ ! -f "${restart_archive}" || -L "${restart_archive}" ||
  "$(stat -c '%s' -- "${restart_archive}")" != "125255" ||
  "$(sha256sum -- "${restart_archive}" | awk '{print $1}')" != \
  "${run19_archive_sha}" ]]; then
  printf 'Run 19 restart archive identity changed\n' >&2
  exit 1
fi

if [[ -L "${launch_root}" || -L "${artifact_root}" ]]; then
  printf 'refusing symlink detached-launch or artifact root\n' >&2
  exit 1
fi
mkdir -p -- "${launch_root}/claims"
mkdir -p -- "${artifact_root}"
chmod 700 -- "${launch_root}" "${launch_root}/claims" "${artifact_root}"
launch_root_resolved="$(readlink -f -- "${launch_root}")"
artifact_root_resolved="$(readlink -f -- "${artifact_root}")"
if [[ "${launch_root_resolved}" != "${launch_root}" ||
  "${artifact_root_resolved}" != "${artifact_root}" ||
  ! -d "${artifact_root_resolved}" ||
  -L "${launch_root}/claims" ||
  ! -d "${launch_root}/claims" ||
  "$(readlink -f -- "${launch_root}/claims")" != \
  "${launch_root_resolved}/claims" ]]; then
  printf 'detached durable directories failed containment checks\n' >&2
  exit 1
fi
for receipt in "${ready_receipt}" "${failure_receipt}" "${terminal_receipt}"; do
  if [[ "$(dirname -- "${receipt}")" != "${launch_root_resolved}" ||
    -e "${receipt}" || -L "${receipt}" ]]; then
    printf 'receipt path is not new and scoped: %s\n' "${receipt}" >&2
    exit 1
  fi
done

trap finalize EXIT

exec 9<"${launch_root_resolved}"
flock -n 9 || {
  printf 'another sealed research controller holds the global lock\n' >&2
  exit 1
}

if pgrep -af 'datavis[.]research[.]fresh_pipeline_cli' >/dev/null; then
  printf 'another sealed research pipeline is already active\n' >&2
  exit 1
fi

worktree="$(mktemp -d /tmp/fresh-xauusd-worktree.XXXXXX)"
output="$(mktemp -d /tmp/fresh-xauusd-output.XXXXXX)"
restart_directory="$(mktemp -d /tmp/fresh-xauusd-restart.XXXXXX)"
log_path="$(mktemp /tmp/fresh-xauusd-run.XXXXXX.log)"

python3 -B - "${restart_archive}" "${restart_directory}" <<'PY'
import hashlib
import os
from pathlib import Path, PurePosixPath
import sys
import tarfile

archive = Path(sys.argv[1]).resolve()
destination = Path(sys.argv[2]).resolve()
expected_archive = "f947348d892d1c996df15188c3221595066c019957f4dccf24697502d2d4fbf9"
allowed = {
    "fresh_corpus_manifest_v1.json",
    "fresh_entry_bank_v1.json",
    "fresh_experiment_ledger_v1.jsonl",
    "fresh_implementation_manifest_v1.json",
    "fresh_preregistration_v4.json",
    "fresh_quantile_bank_v1.json",
    "fresh_research_state_binding_v3.json",
    "fresh_source_inventory_v1.json",
    "fresh_split_manifest_v2.json",
    "fresh_threshold_domain_preflight_v1.json",
    "predecessor_fresh_experiment_ledger_v1.jsonl",
    "predecessor_fresh_implementation_manifest_v1.json",
    "predecessor_fresh_preregistration_v3.json",
    "predecessor_fresh_research_state_binding_v2.json",
    "remote-exit-status.txt",
    "server-run.log",
}
digest = hashlib.sha256()
with archive.open("rb") as handle:
    for chunk in iter(lambda: handle.read(1024 * 1024), b""):
        digest.update(chunk)
if digest.hexdigest() != expected_archive:
    raise SystemExit("remote Run 19 archive digest changed")
with tarfile.open(archive, "r:gz") as bundle:
    normalized = []
    root_seen = False
    for member in bundle.getmembers():
        if member.name == ".":
            if root_seen or not member.isdir():
                raise SystemExit("unsafe Run 19 archive root")
            root_seen = True
            continue
        name = member.name[2:] if member.name.startswith("./") else member.name
        if (
            not name
            or name in {".", ".."}
            or not member.isfile()
            or name != PurePosixPath(name).name
        ):
            raise SystemExit("unsafe Run 19 archive member")
        normalized.append((name, member))
    names = [name for name, _ in normalized]
    if not root_seen or len(names) != len(allowed) or set(names) != allowed:
        raise SystemExit("Run 19 archive member set changed")
    for name, member in normalized:
        source = bundle.extractfile(member)
        if source is None:
            raise SystemExit("unreadable Run 19 archive member")
        target = destination / name
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
        if hasattr(os, "O_NOFOLLOW"):
            flags |= os.O_NOFOLLOW
        descriptor = os.open(target, flags, 0o600)
        with os.fdopen(descriptor, "wb") as writer:
            while True:
                chunk = source.read(1024 * 1024)
                if not chunk:
                    break
                writer.write(chunk)
            writer.flush()
            os.fsync(writer.fileno())
PY

if [[ -L "${durable_state_root}" ]]; then
  printf 'refusing symlink durable research state root\n' >&2
  exit 1
fi
mkdir -p -- "${durable_state_root}"
chmod 700 -- "${durable_state_root}"
durable_state_root_resolved="$(readlink -f -- "${durable_state_root}")"
if [[ ! -d "${durable_state_root_resolved}" ||
  "${durable_state_root_resolved}" == /tmp ||
  "${durable_state_root_resolved}" == /tmp/* ]]; then
  printf 'durable research state root is unsafe\n' >&2
  exit 1
fi

if [[ -L "${scratch_root}" ]]; then
  printf 'refusing symlink research scratch root\n' >&2
  exit 1
fi
mkdir -p -- "${scratch_root}"
chmod 700 -- "${scratch_root}"
scratch_root_resolved="$(readlink -f -- "${scratch_root}")"
if [[ ! -d "${scratch_root_resolved}" ||
  "${scratch_root_resolved}" == "${durable_state_root_resolved}" ||
  "${scratch_root_resolved}" == /tmp ||
  "${scratch_root_resolved}" == /tmp/* ||
  "${scratch_root_resolved}" == "${durable_state_root_resolved}"/* ]]; then
  printf 'research scratch root is unsafe\n' >&2
  exit 1
fi
scratch="$(
  mktemp -d "${scratch_root_resolved}/run.${run_id}.${attempt}.XXXXXX"
)"
scratch_resolved="$(readlink -f -- "${scratch}")"
if [[ "$(dirname -- "${scratch_resolved}")" != "${scratch_root_resolved}" ||
  -L "${scratch}" ]]; then
  printf 'allocated research scratch escaped its root\n' >&2
  exit 1
fi
scratch_free_bytes="$(
  df --output=avail -B1 -- "${scratch_resolved}" | awk 'NR == 2 {print $1}'
)"
scratch_free_inodes="$(
  df --output=iavail -- "${scratch_resolved}" | awk 'NR == 2 {print $1}'
)"
if [[ ! "${scratch_free_bytes}" =~ ^[0-9]+$ ||
  "${scratch_free_bytes}" -lt $((12 * 1024 * 1024 * 1024)) ||
  ! "${scratch_free_inodes}" =~ ^[0-9]+$ ||
  "${scratch_free_inodes}" -lt 4096 ]]; then
  printf 'research scratch capacity is insufficient\n' >&2
  exit 1
fi

git -C "${production_repo}" fetch origin "${branch}"
git -C "${production_repo}" cat-file -e "${commit_sha}^{commit}"
git -C "${production_repo}" worktree add --detach "${worktree}" "${commit_sha}"
cd "${worktree}"
if [[ "$(git rev-parse HEAD)" != "${commit_sha}" ]]; then
  printf 'detached worktree commit changed\n' >&2
  exit 1
fi
set -a
eval "$(sudo cat /etc/datavis.env)"
set +a

research_venv="${worktree}/.fresh-venv"
python3 -m venv "${research_venv}"
research_python="${research_venv}/bin/python"
"${research_python}" -m pip install \
  --disable-pip-version-check \
  --no-cache-dir \
  numpy==2.0.2 \
  pandas==2.2.3 \
  psycopg2-binary==2.9.10 \
  python-dotenv==1.1.1
"${research_python}" -c 'import numpy, pandas, psycopg2, dotenv'
fresh_test_modules=()
for test_path in test_fresh_*.py; do
  fresh_test_modules+=("${test_path%.py}")
done
"${research_python}" -m unittest "${fresh_test_modules[@]}"

if pgrep -af 'datavis[.]research[.]fresh_pipeline_cli' >/dev/null; then
  printf 'a sealed research pipeline appeared during preflight\n' >&2
  exit 1
fi

claim_path="${launch_root_resolved}/claims/v5-${v5_lineage_sha}.claim"
python3 -B - \
  "${claim_path}" "${run_id}" "${attempt}" "${commit_sha}" \
  "${v5_lineage_sha}" "${run19_archive_sha}" <<'PY'
import json
import os
from pathlib import Path
import sys

target = Path(sys.argv[1])
payload = {
    "schema": "fresh-xauusd-study-execution-claim/v1",
    "githubRunId": int(sys.argv[2]),
    "githubRunAttempt": int(sys.argv[3]),
    "commitSha": sys.argv[4],
    "studyLineageSha256": sys.argv[5],
    "restartArchiveSha256": sys.argv[6],
}
flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
if hasattr(os, "O_NOFOLLOW"):
    flags |= os.O_NOFOLLOW
directory_descriptor = os.open(
    target.parent,
    os.O_RDONLY
    | getattr(os, "O_DIRECTORY", 0)
    | getattr(os, "O_NOFOLLOW", 0),
)
try:
    descriptor = os.open(
        target.name,
        flags,
        0o600,
        dir_fd=directory_descriptor,
    )
    with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
        json.dump(payload, handle, sort_keys=True, separators=(",", ":"))
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.fsync(directory_descriptor)
finally:
    os.close(directory_descriptor)
PY

set +e
"${research_python}" -m datavis.research.fresh_pipeline_cli \
  --repository-root "${worktree}" \
  --output-dir "${output}" \
  --scratch-dir "${scratch_resolved}" \
  --research-state-dir "${durable_state_root_resolved}" \
  --restart-v5-artifact-dir "${restart_directory}" \
  --execute \
  >"${log_path}" 2>&1 &
pipeline_pid="$!"
set -e
pipeline_start_ticks="$(
  python3 -B - "${pipeline_pid}" <<'PY'
from pathlib import Path
import sys

raw = Path(f"/proc/{sys.argv[1]}/stat").read_text(encoding="ascii")
print(raw.rsplit(") ", 1)[1].split()[19])
PY
)"
sleep 5
python3 -B - "${pipeline_pid}" "${pipeline_start_ticks}" <<'PY'
from pathlib import Path
import sys

expected_pid, expected_start = sys.argv[1:]
raw = Path(f"/proc/{expected_pid}/stat").read_text(encoding="ascii")
fields = raw.rsplit(") ", 1)[1].split()
if fields[0] in {"X", "Z"} or fields[19] != expected_start:
    raise SystemExit("pipeline did not survive its launch grace period")
PY
if [[ -e "${terminal_receipt}" || -L "${terminal_receipt}" ]]; then
  printf 'pipeline became terminal before launch receipt sealing\n' >&2
  exit 1
fi
write_receipt "${ready_receipt}" launch_ready running
ready_written=1

set +e
wait "${pipeline_pid}"
research_status="$?"
pipeline_reaped=1
set -e
exit "${research_status}"
