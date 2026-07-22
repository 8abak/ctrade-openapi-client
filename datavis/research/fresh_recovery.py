"""One-time audited continuation of the run-14 discovery batch.

This module is intentionally specific to GitHub Actions run 29881509856.  It
does not implement general checkpoint reuse.  The only accepted input is the
immutable artifact produced after that run terminated with status 137, before
any candidate outcome had been written.  Every original artifact is bound by
an exact file digest and every research identity is checked again before the
single mechanical recovery attempt is authorized.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping, Sequence

from datavis.research.fresh_preregistration import (
    validate_fresh_implementation_manifest,
    validate_fresh_preregistration_v2,
)
from datavis.research.fresh_protocol import append_fresh_record, canonical_hash
from datavis.research.fresh_search import EntryCandidateSpec, FrozenEntryCandidate
from datavis.research.fresh_thresholds import fresh_quantile_bank_from_payload


RUN14_RUN_ID = 29_881_509_856
RUN14_RUN_ATTEMPT = 1
RUN14_JOB_ID = 88_803_218_405
RUN14_ARTIFACT_ID = 8_517_123_097
RUN14_COMMIT_SHA = "551ee42a176122402ef08d0935157dda7d0f75b4"
RUN14_TGZ_SHA256 = "f3755595fca5f7b9978531824913c241ff69815d6e2cb06ee9bcfd35c4938687"
RUN14_LEDGER_SHA256 = "8ce007182b3fa2b30d50023460506e8eed2e0c68cb671eeb3fb7d4168ac16dfa"
RUN14_PREREGISTRATION_SHA256 = (
    "209108a553eb186e9048e739981545975bd128528bb1891b28261f09bf1ca2cf"
)
RUN14_IMPLEMENTATION_MANIFEST_SHA256 = (
    "160f29e8136c967297b16ee438d12fe89ae31ee0901af9293b1949d3193cd094"
)
RUN14_SPLIT_MANIFEST_SHA256 = (
    "59a0df375a3b8934c14a355a4fc91bb9aade6ada88052d5096c4b9a29e2744bd"
)
RUN14_RESEARCH_WINDOW_SET_SHA256 = (
    "0215cfa1ca0954bae1d6eaafbab44a62ddf663a356bb9f7978071afed4595371"
)
RUN14_DISCOVERY_WINDOW_SHA256 = (
    "66555bc7a1d991dc9e7cf485d07cafd40c30bab30dd9945a00840500a7518708"
)
RUN14_HOLDOUT_WINDOW_SHA256 = (
    "8d599150987e32430a5d012b4973590bda56f7d548c42e7dad9714e2f0fe40b7"
)
RUN14_INVENTORY_SHA256 = (
    "f766d21bfa5a60d6f7b81f5393a24458626cead6069dd8b1719f89de68924d5b"
)
RUN14_CORPUS_SHA256 = "f24e090b4e149a12a981c4adfbfbc4f68fb57fc9413a0e386b69c53ac3da0c79"
RUN14_QUANTILE_BANK_SHA256 = (
    "3243793cdc3d1ee2f7a64b2184a8f91bdf729f2115b06adfb9060dd60a3f78ae"
)
RUN14_THRESHOLD_PAYLOAD_SHA256 = (
    "9545c7fa7e5c5eef9d64d867f5b9b1b81415e405389471b14e67d1761b1ddf84"
)
RUN14_CANDIDATE_GRID_SHA256 = (
    "de4f51a15a32fd64f46e2230c51d4ee80df0a3af3b309cecee1e6b1f712327e6"
)
RUN14_FILTER_VARIANT_BANK_SHA256 = (
    "50f0a3a39f008465a6a1d0e9506506e57d072bf608207c842dc72b7a84c5b671"
)
RUN14_EXIT_GRID_SHA256 = (
    "e9460aabbad5b1e85ec97790a8a62acf0b5bd091a57573d36c3663fae41d1280"
)
RUN14_EXECUTION_SCENARIOS_SHA256 = (
    "98355f3b5514d0a5baa8ea3fb441d6aa2b9f2484543884d00344343d3322d2f4"
)
RUN14_ORDERED_CANDIDATE_SHA256 = (
    "d4163395adb43ec49a5f0e10df1fcc82bb698703d2462d735eed5b7ed40ba19c"
)
RUN14_BATCH_ACCESS_IDENTITY_SHA256 = (
    "4e2a6708365650619453564c1e8d8f4a6e793af5e01d57e5399447ba48750c5e"
)
RUN14_ENTRY_BANK_FILE_SHA256 = (
    "7be58142337fc1b440fe61dae3ad0721c5058e4a1eae3dfde7c223bb8021b28c"
)
RUN14_RECORD_SHA256 = (
    "53b8ad1b941915e4b53e71bc48c753ea7dcc322068d6d92ac680554252d234fd",
    "d8925600e087ea1612016c9308532c3ced5bf6729695ad6243761fbf543481b2",
)

RUN14_RECOVERY_TEST_MODULES = (
    "test_fresh_spool",
    "test_fresh_search",
    "test_fresh_pipeline",
    "test_fresh_recovery",
    "test_fresh_preregistration",
)


_EXPECTED_FILE_SHA256 = {
    "fresh_corpus_manifest_v1.json": (
        "fe59805f49ed40ae7996bd8333bba6ea2531ce67c04e904f4f228ea01a54dec2"
    ),
    "fresh_entry_bank_v1.json": (RUN14_ENTRY_BANK_FILE_SHA256),
    "fresh_experiment_ledger_v1.jsonl": RUN14_LEDGER_SHA256,
    "fresh_implementation_manifest_v1.json": (
        "bc304741db8649da2bca478cafb6f94acc85a25b325a7cbd91f97feb2ef64503"
    ),
    "fresh_preregistration_v2.json": (
        "dd00fbc7670319906bfe4a474232dc8cdac08c918734d960367175745c6d07b3"
    ),
    "fresh_quantile_bank_v1.json": (
        "5076a373f6cfc25a6a37e8a63b90eb4633282b425021b55879cb193bb76bab46"
    ),
    "fresh_research_state_binding_v1.json": (
        "42fb4adfcb0da4a323c0d6dc04e6525a6c2ba7fb88c78946adce629107f4d84a"
    ),
    "fresh_source_inventory_v1.json": (
        "ab1125638e76cd35517859b4e292abb3908a49d79fb92534c5fc2fd7a32e9ab8"
    ),
    "fresh_split_manifest_v2.json": (
        "b179c8e359b0ab998258a1bbbdac41e33b970d63e60131f3057f2dc224c1a0dc"
    ),
    "fresh_threshold_domain_preflight_v1.json": (
        "55d94106f9860676f9d42be8c7023de2bd7d7234ee812155fedd578eae6d98dc"
    ),
    "remote-exit-status.txt": (
        "e3b9c2844b5a5c2677b3a2279db2ec8487491dd9a23d6b22fac153391b3bb63c"
    ),
    "server-run.log": (
        "a0246978c5ffd0d65480e78ca0a68bae51bee7cb95a8c97c53f9e69060e5257c"
    ),
}


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _load_json(path: Path, name: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise PermissionError(f"run-14 {name} is not valid JSON") from exc
    if not isinstance(value, dict):
        raise PermissionError(f"run-14 {name} must be a JSON object")
    return value


def _verify_embedded_hash(
    payload: Mapping[str, Any], field: str, expected: str, name: str
) -> None:
    body = dict(payload)
    claimed = body.pop(field, None)
    if claimed != expected or canonical_hash(body) != expected:
        raise PermissionError(f"run-14 {name} identity is invalid")


def _verified_ledger_records(path: Path) -> tuple[dict[str, Any], ...]:
    records: list[dict[str, Any]] = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line:
            raise PermissionError("run-14 ledger contains a blank record")
        try:
            raw = json.loads(line)
        except json.JSONDecodeError as exc:
            raise PermissionError("run-14 ledger is invalid JSONL") from exc
        if not isinstance(raw, dict):
            raise PermissionError("run-14 ledger records must be objects")
        body = dict(raw)
        claimed_number = body.pop("recordNumber", None)
        claimed_sha = body.pop("recordSha256", None)
        if (
            claimed_number != number
            or claimed_sha != RUN14_RECORD_SHA256[number - 1]
            or canonical_hash(body) != claimed_sha
        ):
            raise PermissionError("run-14 ledger chain is invalid")
        records.append(raw)
    if len(records) != 2:
        raise PermissionError("run-14 recovery requires exactly two ledger records")
    first, second = records
    if (
        first.get("recordKind") != "stage-window-access"
        or first.get("status") != "window_access_started"
        or second.get("recordKind") != "batch-window-access"
        or second.get("status") != "batch_access_started"
        or second.get("identitySha256") != RUN14_BATCH_ACCESS_IDENTITY_SHA256
        or any(record.get("role") != "discovery" for record in records)
        or any(record.get("outcomesRevealed") is not True for record in records)
    ):
        raise PermissionError("run-14 ledger is not the incomplete discovery prefix")
    parameters = second.get("parameters")
    if (
        not isinstance(parameters, Mapping)
        or len(parameters.get("candidateIds", ())) != 240
        or len(parameters.get("candidateSha256", ())) != 240
    ):
        raise PermissionError("run-14 batch does not contain 240 candidates")
    return tuple(records)


@dataclass(frozen=True, slots=True)
class Run14RecoveryBundle:
    """Fully verified immutable inputs for the one permitted continuation."""

    directory: Path
    paths: Mapping[str, Path]
    inventory: Mapping[str, Any]
    corpus: Mapping[str, Any]
    split: Mapping[str, Any]
    state_binding: Mapping[str, Any]
    implementation: Mapping[str, Any]
    preregistration: Mapping[str, Any]
    quantile_bank: Mapping[str, Any]
    threshold_preflight: Mapping[str, Any]
    entry_bank: Mapping[str, Any]
    ledger_records: tuple[Mapping[str, Any], ...]


def load_run14_recovery_bundle(directory: str | Path) -> Run14RecoveryBundle:
    """Load only the exact run-14 artifact and reject any substituted input."""

    selected_root = Path(directory).expanduser()
    if selected_root.is_symlink():
        raise PermissionError("the run-14 recovery directory is unavailable")
    root = selected_root.resolve()
    if not root.is_dir():
        raise PermissionError("the run-14 recovery directory is unavailable")
    children = tuple(root.iterdir())
    if {item.name for item in children} != set(_EXPECTED_FILE_SHA256):
        raise PermissionError("the run-14 recovery artifact has unexpected members")
    paths: dict[str, Path] = {}
    for item in children:
        if item.is_symlink() or not item.is_file():
            raise PermissionError("run-14 recovery members must be regular files")
        if _file_sha256(item) != _EXPECTED_FILE_SHA256[item.name]:
            raise PermissionError(f"run-14 artifact digest changed: {item.name}")
        paths[item.name] = item

    if paths["remote-exit-status.txt"].read_bytes() != b"137\n":
        raise PermissionError("run-14 did not terminate with the audited status")
    log_lines = paths["server-run.log"].read_text(encoding="utf-8").splitlines()
    progress_rows: list[dict[str, Any]] = []
    for line in log_lines:
        if not line.startswith("{"):
            continue
        try:
            parsed = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            progress_rows.append(parsed)
    threshold_rows = [
        row for row in progress_rows if row.get("stage") == "threshold_fit"
    ]
    discovery_rows = [row for row in progress_rows if row.get("stage") == "discovery"]
    if (
        [row.get("sessionOrdinal") for row in threshold_rows] != list(range(1, 41))
        or len(discovery_rows) != 1
        or discovery_rows[0].get("sessionOrdinal") != 1
        or discovery_rows[0].get("sessionCount") != 40
        or discovery_rows[0].get("sessionAnchor") != "2026-01-02"
        or any(row.get("stage") == "fresh_research_complete" for row in progress_rows)
    ):
        raise PermissionError("run-14 progress evidence is not the audited termination")

    inventory = _load_json(paths["fresh_source_inventory_v1.json"], "inventory")
    corpus = _load_json(paths["fresh_corpus_manifest_v1.json"], "corpus")
    split = _load_json(paths["fresh_split_manifest_v2.json"], "split")
    binding = _load_json(paths["fresh_research_state_binding_v1.json"], "state binding")
    implementation = _load_json(
        paths["fresh_implementation_manifest_v1.json"], "implementation manifest"
    )
    preregistration = _load_json(
        paths["fresh_preregistration_v2.json"], "preregistration"
    )
    quantile = _load_json(paths["fresh_quantile_bank_v1.json"], "quantile bank")
    preflight = _load_json(
        paths["fresh_threshold_domain_preflight_v1.json"], "threshold preflight"
    )
    entry_bank = _load_json(paths["fresh_entry_bank_v1.json"], "entry bank")

    _verify_embedded_hash(
        inventory, "inventorySha256", RUN14_INVENTORY_SHA256, "inventory"
    )
    _verify_embedded_hash(corpus, "corpusManifestSha256", RUN14_CORPUS_SHA256, "corpus")
    _verify_embedded_hash(split, "manifestSha256", RUN14_SPLIT_MANIFEST_SHA256, "split")
    if (
        validate_fresh_implementation_manifest(
            implementation, verify_current_files=False
        )
        != RUN14_IMPLEMENTATION_MANIFEST_SHA256
        or validate_fresh_preregistration_v2(
            preregistration, verify_current_implementation_files=False
        )
        != RUN14_PREREGISTRATION_SHA256
    ):
        raise PermissionError("run-14 protocol identities changed")
    bank = fresh_quantile_bank_from_payload(quantile)
    if (
        bank.bank_sha256 != RUN14_QUANTILE_BANK_SHA256
        or canonical_hash(quantile) != RUN14_THRESHOLD_PAYLOAD_SHA256
        or preflight.get("quantileBankSha256") != RUN14_QUANTILE_BANK_SHA256
        or preflight.get("candidateGridSha256") != RUN14_CANDIDATE_GRID_SHA256
        or preflight.get("eventFilterVariantBankSha256")
        != RUN14_FILTER_VARIANT_BANK_SHA256
        or preflight.get("exitGridSha256") != RUN14_EXIT_GRID_SHA256
        or preflight.get("executionScenariosSha256") != RUN14_EXECUTION_SCENARIOS_SHA256
        or preflight.get("totalRuntimeEntryCount") != 240
        or entry_bank.get("candidateCount") != 240
    ):
        raise PermissionError("run-14 frozen search banks changed")
    source = preregistration.get("sourceBindings")
    if (
        not isinstance(source, Mapping)
        or source.get("splitManifestSha256") != RUN14_SPLIT_MANIFEST_SHA256
        or source.get("inventorySha256") != RUN14_INVENTORY_SHA256
        or source.get("corpusManifestSha256") != RUN14_CORPUS_SHA256
        or source.get("implementationManifestSha256")
        != RUN14_IMPLEMENTATION_MANIFEST_SHA256
        or binding.get("splitManifestSha256") != RUN14_SPLIT_MANIFEST_SHA256
        or binding.get("researchWindowSetSha256") != RUN14_RESEARCH_WINDOW_SET_SHA256
        or binding.get("holdoutWindowSha256") != RUN14_HOLDOUT_WINDOW_SHA256
    ):
        raise PermissionError("run-14 source bindings changed")
    discovery_window = split.get("windows", {}).get("discovery")
    if (
        not isinstance(discovery_window, Mapping)
        or canonical_hash(discovery_window) != RUN14_DISCOVERY_WINDOW_SHA256
    ):
        raise PermissionError("run-14 discovery window changed")
    ledger_records = _verified_ledger_records(paths["fresh_experiment_ledger_v1.jsonl"])

    return Run14RecoveryBundle(
        directory=root,
        paths=paths,
        inventory=inventory,
        corpus=corpus,
        split=split,
        state_binding=binding,
        implementation=implementation,
        preregistration=preregistration,
        quantile_bank=quantile,
        threshold_preflight=preflight,
        entry_bank=entry_bank,
        ledger_records=ledger_records,
    )


def _validate_recovery_equivalence_evidence(
    evidence: Mapping[str, Any],
    *,
    recovery_implementation_manifest: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate the exact pre-outcome test evidence and current test bytes."""

    normalized = json.loads(
        json.dumps(
            evidence,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    expected_keys = {
        "schema",
        "allRequiredTestsPassed",
        "command",
        "testModules",
        "testSourceSha256",
        "processExitCode",
        "stdoutSha256",
        "stderrSha256",
        "completedBeforeRecoveryOutcomeAccess",
        "fixtureIdentity",
    }
    modules = list(RUN14_RECOVERY_TEST_MODULES)
    expected_command = ["python", "-m", "unittest", *modules]
    expected_fixture = {
        "runId": RUN14_RUN_ID,
        "archiveSha256": RUN14_TGZ_SHA256,
        "ledgerSha256": RUN14_LEDGER_SHA256,
        "entryBankFileSha256": RUN14_ENTRY_BANK_FILE_SHA256,
        "preregistrationSha256": RUN14_PREREGISTRATION_SHA256,
    }
    if (
        not isinstance(normalized, dict)
        or set(normalized) != expected_keys
        or normalized.get("schema") != "fresh-xauusd-recovery-equivalence-preflight/v1"
        or normalized.get("allRequiredTestsPassed") is not True
        or normalized.get("processExitCode") != 0
        or normalized.get("completedBeforeRecoveryOutcomeAccess") is not True
        or normalized.get("command") != expected_command
        or normalized.get("testModules") != modules
        or normalized.get("fixtureIdentity") != expected_fixture
    ):
        raise PermissionError("recovery equivalence evidence is incomplete")
    for field in ("stdoutSha256", "stderrSha256"):
        value = normalized.get(field)
        if not isinstance(value, str) or len(value) != 64:
            raise PermissionError("recovery equivalence output identity is invalid")
        try:
            int(value, 16)
        except ValueError as exc:
            raise PermissionError(
                "recovery equivalence output identity is invalid"
            ) from exc

    validate_fresh_implementation_manifest(
        recovery_implementation_manifest, verify_current_files=True
    )
    root = Path(str(recovery_implementation_manifest["repositoryRoot"])).resolve()
    manifest_files = {
        str(item["path"]): str(item["sha256"])
        for item in recovery_implementation_manifest["files"]
    }
    expected_source_names = [f"{module}.py" for module in modules]
    source_hashes = normalized.get("testSourceSha256")
    if not isinstance(source_hashes, dict) or list(source_hashes) != sorted(
        expected_source_names
    ):
        raise PermissionError("recovery equivalence test-source set changed")
    for name in expected_source_names:
        source = (root / name).resolve()
        try:
            source.relative_to(root)
        except ValueError as exc:
            raise PermissionError(
                "recovery test source escapes the repository"
            ) from exc
        if source.is_symlink() or not source.is_file():
            raise PermissionError("recovery equivalence test source is unavailable")
        actual_sha = _file_sha256(source)
        if (
            source_hashes.get(name) != actual_sha
            or manifest_files.get(name) != actual_sha
        ):
            raise PermissionError("recovery equivalence test-source bytes changed")
    return normalized


def run_run14_recovery_equivalence_preflight(
    repository_root: str | Path,
    recovery_artifact_directory: str | Path,
    *,
    recovery_implementation_manifest: Mapping[str, Any],
) -> dict[str, Any]:
    """Run the mandatory exact-fixture equivalence suite inside the runner."""

    root = Path(repository_root).expanduser().resolve()
    load_run14_recovery_bundle(recovery_artifact_directory)
    modules = RUN14_RECOVERY_TEST_MODULES
    sources = tuple(root / f"{module}.py" for module in modules)
    if any(path.is_symlink() or not path.is_file() for path in sources):
        raise PermissionError("recovery equivalence test sources are unavailable")
    command = (sys.executable, "-m", "unittest", *modules)
    environment = dict(os.environ)
    environment.update(
        {
            "FRESH_RUN14_RECOVERY_ARTIFACT_DIR": str(
                Path(recovery_artifact_directory).expanduser().resolve()
            ),
            "FRESH_REQUIRE_RUN14_RECOVERY_FIXTURE": "1",
        }
    )
    completed = subprocess.run(
        command,
        cwd=root,
        check=False,
        capture_output=True,
        timeout=600,
        env=environment,
    )
    if completed.returncode != 0:
        sys.stderr.buffer.write(completed.stdout)
        sys.stderr.buffer.write(completed.stderr)
        raise PermissionError("recovery equivalence preflight failed")
    evidence = {
        "schema": "fresh-xauusd-recovery-equivalence-preflight/v1",
        "allRequiredTestsPassed": True,
        "command": ["python", "-m", "unittest", *modules],
        "testModules": list(modules),
        "testSourceSha256": {path.name: _file_sha256(path) for path in sorted(sources)},
        "processExitCode": completed.returncode,
        "stdoutSha256": hashlib.sha256(completed.stdout).hexdigest(),
        "stderrSha256": hashlib.sha256(completed.stderr).hexdigest(),
        "completedBeforeRecoveryOutcomeAccess": True,
        "fixtureIdentity": {
            "runId": RUN14_RUN_ID,
            "archiveSha256": RUN14_TGZ_SHA256,
            "ledgerSha256": RUN14_LEDGER_SHA256,
            "entryBankFileSha256": RUN14_ENTRY_BANK_FILE_SHA256,
            "preregistrationSha256": RUN14_PREREGISTRATION_SHA256,
        },
    }
    return _validate_recovery_equivalence_evidence(
        evidence,
        recovery_implementation_manifest=recovery_implementation_manifest,
    )


def build_run14_recovery_contract(
    bundle: Run14RecoveryBundle,
    *,
    entry_specs: Sequence[EntryCandidateSpec],
    recovery_implementation_manifest: Mapping[str, Any],
    generated_entry_bank_path: str | Path,
    equivalence_evidence: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Bind the reconstructed batch and mechanical amendment before access."""

    recovery_implementation_sha = validate_fresh_implementation_manifest(
        recovery_implementation_manifest, verify_current_files=True
    )
    threshold_sha = canonical_hash(bundle.quantile_bank)
    candidates = tuple(
        FrozenEntryCandidate.freeze(spec, threshold_bank_sha256=threshold_sha)
        for spec in entry_specs
    )
    sequence = [
        {
            "candidateId": candidate.candidate_id,
            "entrySha256": candidate.entry_sha256,
        }
        for candidate in candidates
    ]
    entry_bank_file_sha = _file_sha256(Path(generated_entry_bank_path).resolve())
    original_batch = bundle.ledger_records[1]["parameters"]
    if (
        len(candidates) != 240
        or canonical_hash(sequence) != RUN14_ORDERED_CANDIDATE_SHA256
        or [item["candidateId"] for item in sequence]
        != original_batch.get("candidateIds")
        or [item["entrySha256"] for item in sequence]
        != original_batch.get("candidateSha256")
        or entry_bank_file_sha != _EXPECTED_FILE_SHA256["fresh_entry_bank_v1.json"]
    ):
        raise PermissionError("the reconstructed run-14 entry bank changed")
    evidence = _validate_recovery_equivalence_evidence(
        equivalence_evidence,
        recovery_implementation_manifest=recovery_implementation_manifest,
    )

    scoring_identity = canonical_hash(
        {
            "entryDiagnostics": bundle.preregistration["entryDiagnostics"],
            "robustnessAndGates": bundle.preregistration["robustnessAndGates"],
        }
    )
    execution_identity = canonical_hash(bundle.preregistration["execution"])
    audit = {
        "schema": "fresh-xauusd-infrastructure-recovery/v1",
        "recoveryAttemptId": "run14-discovery-recovery-attempt-1",
        "recoveryAttempt": 1,
        "maximumRecoveryAttempts": 1,
        "originalRunId": RUN14_RUN_ID,
        "originalCommitSha": RUN14_COMMIT_SHA,
        "ledgerPrefixSha256": RUN14_LEDGER_SHA256,
        "originalRecordSha256": list(RUN14_RECORD_SHA256),
        "candidateOutcomeRecordCount": 0,
        "laterRoleRecordCount": 0,
        "holdoutAuthorizationPresent": False,
        "oomEvidence": {
            "classification": "exit-137-consistent-with-memory-exhaustion",
            "kernelOomConfirmationAvailable": False,
            "remoteExitStatus": 137,
            "thresholdSessionsCompleted": 40,
            "discoverySessionsCompletedBeforeTermination": 1,
            "recomputeDiscoveryFromSessionOrdinal": 1,
            "partialCandidateMetricsRecovered": False,
            "serverLogSha256": _EXPECTED_FILE_SHA256["server-run.log"],
            "statusFileSha256": _EXPECTED_FILE_SHA256["remote-exit-status.txt"],
        },
        "identity": {
            "splitManifestSha256": RUN14_SPLIT_MANIFEST_SHA256,
            "researchWindowSetSha256": RUN14_RESEARCH_WINDOW_SET_SHA256,
            "discoveryWindowSha256": RUN14_DISCOVERY_WINDOW_SHA256,
            "holdoutWindowSha256": RUN14_HOLDOUT_WINDOW_SHA256,
            "inventorySha256": RUN14_INVENTORY_SHA256,
            "corpusManifestSha256": RUN14_CORPUS_SHA256,
            "preregistrationSha256": RUN14_PREREGISTRATION_SHA256,
            "originalImplementationManifestSha256": (
                RUN14_IMPLEMENTATION_MANIFEST_SHA256
            ),
            "recoveryImplementationManifestSha256": recovery_implementation_sha,
            "thresholdBankSha256": RUN14_THRESHOLD_PAYLOAD_SHA256,
            "quantileBankSha256": RUN14_QUANTILE_BANK_SHA256,
            "candidateGridSha256": RUN14_CANDIDATE_GRID_SHA256,
            "eventFilterVariantBankSha256": RUN14_FILTER_VARIANT_BANK_SHA256,
            "exitGridSha256": RUN14_EXIT_GRID_SHA256,
            "executionScenariosSha256": RUN14_EXECUTION_SCENARIOS_SHA256,
            "entryBankFileSha256": entry_bank_file_sha,
            "orderedCandidateSequenceSha256": RUN14_ORDERED_CANDIDATE_SHA256,
            "candidateCount": len(candidates),
            "scoringDefinitionSha256": scoring_identity,
            "executionDefinitionSha256": execution_identity,
            "equivalenceEvidenceSha256": canonical_hash(evidence),
        },
        "permittedProcedure": {
            "mode": "disk-spooled-full-discovery-recomputation",
            "recomputeFromFirstDiscoverySession": True,
            "reusePartialSessionOrCandidateMetrics": False,
            "retainAllOriginalCandidates": True,
            "candidateCount": 240,
            "candidateDefinitionsChanged": False,
            "thresholdsChanged": False,
            "scoringChanged": False,
            "gatesChanged": False,
            "duplicateTickSemanticsChanged": False,
            "maximumAttempts": 1,
        },
    }
    contract_body = {
        "schema": "fresh-xauusd-run14-recovery-contract/v1",
        "originalRun": {
            "runId": RUN14_RUN_ID,
            "runAttempt": RUN14_RUN_ATTEMPT,
            "jobId": RUN14_JOB_ID,
            "artifactId": RUN14_ARTIFACT_ID,
            "commitSha": RUN14_COMMIT_SHA,
            "archiveSha256": RUN14_TGZ_SHA256,
        },
        "audit": audit,
        "equivalenceEvidence": evidence,
    }
    contract = {
        **contract_body,
        "recoveryContractSha256": canonical_hash(contract_body),
    }
    return audit, contract


def validate_run14_recovery_for_holdout(
    *,
    records: Sequence[Mapping[str, Any]],
    preregistration: Mapping[str, Any],
    preregistration_sha256: str,
    split_manifest: Mapping[str, Any],
    split_manifest_sha256: str,
    recovery_contract: Mapping[str, Any],
    recovery_implementation_manifest: Mapping[str, Any],
    sealed_batch_result_path: str | Path,
) -> dict[str, Any]:
    """Prove the exact completed run-14 continuation before holdout opens."""

    prereg_sha = str(preregistration_sha256)
    split_sha = str(split_manifest_sha256)
    contract = json.loads(
        json.dumps(
            recovery_contract,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    claimed_contract_sha = contract.pop("recoveryContractSha256", None)
    if (
        set(contract) != {"schema", "originalRun", "audit", "equivalenceEvidence"}
        or contract.get("schema") != "fresh-xauusd-run14-recovery-contract/v1"
        or claimed_contract_sha != canonical_hash(contract)
        or contract.get("originalRun")
        != {
            "runId": RUN14_RUN_ID,
            "runAttempt": RUN14_RUN_ATTEMPT,
            "jobId": RUN14_JOB_ID,
            "artifactId": RUN14_ARTIFACT_ID,
            "commitSha": RUN14_COMMIT_SHA,
            "archiveSha256": RUN14_TGZ_SHA256,
        }
    ):
        raise PermissionError("the run-14 recovery contract identity changed")
    audit = contract.get("audit")
    if not isinstance(audit, Mapping):
        raise PermissionError("the run-14 recovery audit is unavailable")
    required_audit_keys = {
        "schema",
        "recoveryAttemptId",
        "recoveryAttempt",
        "maximumRecoveryAttempts",
        "originalRunId",
        "originalCommitSha",
        "ledgerPrefixSha256",
        "originalRecordSha256",
        "candidateOutcomeRecordCount",
        "laterRoleRecordCount",
        "holdoutAuthorizationPresent",
        "oomEvidence",
        "identity",
        "permittedProcedure",
    }
    attempt_id = "run14-discovery-recovery-attempt-1"
    expected_oom = {
        "classification": "exit-137-consistent-with-memory-exhaustion",
        "kernelOomConfirmationAvailable": False,
        "remoteExitStatus": 137,
        "thresholdSessionsCompleted": 40,
        "discoverySessionsCompletedBeforeTermination": 1,
        "recomputeDiscoveryFromSessionOrdinal": 1,
        "partialCandidateMetricsRecovered": False,
        "serverLogSha256": _EXPECTED_FILE_SHA256["server-run.log"],
        "statusFileSha256": _EXPECTED_FILE_SHA256["remote-exit-status.txt"],
    }
    expected_procedure = {
        "mode": "disk-spooled-full-discovery-recomputation",
        "recomputeFromFirstDiscoverySession": True,
        "reusePartialSessionOrCandidateMetrics": False,
        "retainAllOriginalCandidates": True,
        "candidateCount": 240,
        "candidateDefinitionsChanged": False,
        "thresholdsChanged": False,
        "scoringChanged": False,
        "gatesChanged": False,
        "duplicateTickSemanticsChanged": False,
        "maximumAttempts": 1,
    }
    if (
        set(audit) != required_audit_keys
        or audit.get("schema") != "fresh-xauusd-infrastructure-recovery/v1"
        or audit.get("recoveryAttemptId") != attempt_id
        or audit.get("recoveryAttempt") != 1
        or audit.get("maximumRecoveryAttempts") != 1
        or audit.get("originalRunId") != RUN14_RUN_ID
        or audit.get("originalCommitSha") != RUN14_COMMIT_SHA
        or audit.get("ledgerPrefixSha256") != RUN14_LEDGER_SHA256
        or audit.get("originalRecordSha256") != list(RUN14_RECORD_SHA256)
        or audit.get("candidateOutcomeRecordCount") != 0
        or audit.get("laterRoleRecordCount") != 0
        or audit.get("holdoutAuthorizationPresent") is not False
        or audit.get("oomEvidence") != expected_oom
        or audit.get("permittedProcedure") != expected_procedure
    ):
        raise PermissionError("the run-14 recovery audit changed")

    recovery_implementation_sha = validate_fresh_implementation_manifest(
        recovery_implementation_manifest, verify_current_files=True
    )
    evidence = _validate_recovery_equivalence_evidence(
        contract["equivalenceEvidence"],
        recovery_implementation_manifest=recovery_implementation_manifest,
    )
    identity = audit.get("identity")
    windows = split_manifest.get("windows")
    if not isinstance(identity, Mapping) or not isinstance(windows, Mapping):
        raise PermissionError("the recovery window identities are unavailable")
    role_order = (
        "discovery",
        "walk_forward_1",
        "walk_forward_2",
        "walk_forward_3",
        "validation",
        "holdout",
    )
    if any(not isinstance(windows.get(role), Mapping) for role in role_order):
        raise PermissionError("the recovery split windows are unavailable")
    source_bindings = preregistration.get("sourceBindings")
    if not isinstance(source_bindings, Mapping):
        raise PermissionError("the recovery source bindings are unavailable")
    expected_identity_links = {
        "splitManifestSha256": split_sha,
        "researchWindowSetSha256": canonical_hash(
            [canonical_hash(windows[role]) for role in role_order]
        ),
        "discoveryWindowSha256": canonical_hash(windows["discovery"]),
        "holdoutWindowSha256": canonical_hash(windows["holdout"]),
        "inventorySha256": split_manifest.get("inventorySha256"),
        "corpusManifestSha256": source_bindings.get("corpusManifestSha256"),
        "preregistrationSha256": prereg_sha,
        "originalImplementationManifestSha256": source_bindings.get(
            "implementationManifestSha256"
        ),
        "recoveryImplementationManifestSha256": recovery_implementation_sha,
        "thresholdBankSha256": RUN14_THRESHOLD_PAYLOAD_SHA256,
        "quantileBankSha256": RUN14_QUANTILE_BANK_SHA256,
        "candidateGridSha256": RUN14_CANDIDATE_GRID_SHA256,
        "eventFilterVariantBankSha256": RUN14_FILTER_VARIANT_BANK_SHA256,
        "exitGridSha256": RUN14_EXIT_GRID_SHA256,
        "executionScenariosSha256": RUN14_EXECUTION_SCENARIOS_SHA256,
        "entryBankFileSha256": RUN14_ENTRY_BANK_FILE_SHA256,
        "orderedCandidateSequenceSha256": RUN14_ORDERED_CANDIDATE_SHA256,
        "candidateCount": 240,
        "scoringDefinitionSha256": canonical_hash(
            {
                "entryDiagnostics": preregistration["entryDiagnostics"],
                "robustnessAndGates": preregistration["robustnessAndGates"],
            }
        ),
        "executionDefinitionSha256": canonical_hash(preregistration["execution"]),
        "equivalenceEvidenceSha256": canonical_hash(evidence),
    }
    if dict(identity) != expected_identity_links:
        raise PermissionError("the run-14 recovery identities changed")

    selected_records = tuple(records)
    if len(selected_records) < 248 or [
        record.get("recordSha256") for record in selected_records[:2]
    ] != list(RUN14_RECORD_SHA256):
        raise PermissionError("the original run-14 ledger prefix changed")
    original_stage, original_batch_record = selected_records[:2]
    discovery_window_sha = canonical_hash([canonical_hash(windows["discovery"])])
    if (
        original_stage.get("recordNumber") != 1
        or original_stage.get("recordKind") != "stage-window-access"
        or original_stage.get("status") != "window_access_started"
        or original_batch_record.get("recordNumber") != 2
        or original_batch_record.get("recordKind") != "batch-window-access"
        or original_batch_record.get("status") != "batch_access_started"
        or original_batch_record.get("identitySha256")
        != RUN14_BATCH_ACCESS_IDENTITY_SHA256
        or any(
            record.get("stage") != "discovery"
            or record.get("role") != "discovery"
            or record.get("outcomesRevealed") is not True
            or record.get("windowSha256") != discovery_window_sha
            or record.get("preregistrationSha256") != prereg_sha
            for record in selected_records[:2]
        )
    ):
        raise PermissionError("the original run-14 ledger prefix changed")
    original_batch = original_batch_record.get("parameters")
    if not isinstance(original_batch, Mapping):
        raise PermissionError("the original run-14 batch is unavailable")
    candidate_ids = original_batch.get("candidateIds")
    candidate_sha = original_batch.get("candidateSha256")
    if (
        not isinstance(candidate_ids, list)
        or not isinstance(candidate_sha, list)
        or len(candidate_ids) != 240
        or len(candidate_sha) != 240
        or len(candidate_ids) != len(set(candidate_ids))
        or canonical_hash(
            [
                {"candidateId": candidate_id, "entrySha256": entry_sha}
                for candidate_id, entry_sha in zip(candidate_ids, candidate_sha)
            ]
        )
        != RUN14_ORDERED_CANDIDATE_SHA256
    ):
        raise PermissionError("the original candidate order changed")

    recovery_records = [
        record
        for record in selected_records
        if record.get("recordKind") == "infrastructure-resume"
    ]
    expected_statuses = (
        "resume_eligibility_audit",
        "resume_authorized",
        "resume_identity_verified",
        "batch_resume_started",
        "batch_resume_completed",
        "resume_stage_completed",
    )
    if tuple(
        record.get("status") for record in recovery_records
    ) != expected_statuses or [
        record.get("recordNumber") for record in recovery_records
    ] != [3, 4, 5, 6, 7, 248]:
        raise PermissionError("the run-14 recovery chain is incomplete")
    for status, record in zip(expected_statuses, recovery_records):
        payload = record.get("parameters")
        if (
            record.get("candidateId") != f"protocol-infrastructure-resume::{status}"
            or record.get("family") != "protocol-infrastructure-recovery"
            or record.get("stage") != "discovery"
            or record.get("role") != "discovery"
            or record.get("outcomesRevealed") is not True
            or record.get("gatePassed") is not False
            or record.get("windowSha256") != discovery_window_sha
            or record.get("preregistrationSha256") != prereg_sha
            or not isinstance(payload, Mapping)
            or payload.get("kind") != "fresh-infrastructure-recovery"
            or payload.get("status") != status
            or payload.get("stage") != "discovery"
            or payload.get("recoveryAttemptId") != attempt_id
            or canonical_hash(payload) != record.get("identitySha256")
        ):
            raise PermissionError("a run-14 recovery protocol record changed")

    protocol_parameters = [
        record["parameters"]["parameters"] for record in recovery_records
    ]
    if any(not isinstance(item, Mapping) for item in protocol_parameters):
        raise PermissionError("the run-14 recovery parameters are unavailable")
    if (
        protocol_parameters[0].get("ledgerPrefixSha256") != RUN14_LEDGER_SHA256
        or protocol_parameters[0].get("originalRecordSha256")
        != list(RUN14_RECORD_SHA256)
        or protocol_parameters[0].get("oomEvidence") != expected_oom
        or protocol_parameters[0].get("candidateOutcomeRecordCount") != 0
        or protocol_parameters[0].get("laterRoleRecordCount") != 0
        or protocol_parameters[0].get("holdoutAuthorizationPresent") is not False
        or protocol_parameters[1]
        != {
            key: audit[key]
            for key in (
                "recoveryAttemptId",
                "recoveryAttempt",
                "maximumRecoveryAttempts",
                "originalRunId",
                "originalCommitSha",
                "permittedProcedure",
            )
        }
        or dict(protocol_parameters[2]) != dict(identity)
        or protocol_parameters[3].get("originalBatchAccessRecordNumber") != 2
        or protocol_parameters[3].get("originalBatchAccessRecordSha256")
        != RUN14_RECORD_SHA256[1]
        or protocol_parameters[3].get("candidateCount") != 240
        or protocol_parameters[3].get("orderedCandidateSequenceSha256")
        != RUN14_ORDERED_CANDIDATE_SHA256
    ):
        raise PermissionError("the run-14 recovery authorization linkage changed")

    completed_parameters = protocol_parameters[4]
    batch_sha = completed_parameters.get("batchResultSha256")
    batch_file_sha = completed_parameters.get("batchResultFileSha256")
    if (
        not isinstance(batch_sha, str)
        or not isinstance(batch_file_sha, str)
        or completed_parameters.get("candidateCount") != 240
        or completed_parameters.get("orderedCandidateSequenceSha256")
        != RUN14_ORDERED_CANDIDATE_SHA256
    ):
        raise PermissionError("the sealed discovery batch linkage changed")

    discovery_records = [
        record
        for record in selected_records
        if record.get("role") == "discovery"
        and record.get("recordKind")
        not in ("batch-window-access", "stage-window-access", "infrastructure-resume")
    ]
    if (
        len(discovery_records) != 240
        or [record.get("recordNumber") for record in discovery_records]
        != list(range(8, 248))
        or [record.get("candidateId") for record in discovery_records] != candidate_ids
        or [record.get("frozenEntrySha256") for record in discovery_records]
        != candidate_sha
    ):
        raise PermissionError("the recovered discovery candidate order changed")

    selected_batch_path = Path(sealed_batch_result_path).expanduser()
    if selected_batch_path.is_symlink():
        raise PermissionError("the sealed recovery batch is unavailable")
    batch_path = selected_batch_path.resolve()
    if not batch_path.is_file() or _file_sha256(batch_path) != batch_file_sha:
        raise PermissionError("the sealed recovery batch file changed")
    batch_document = _load_json(batch_path, "sealed recovery batch")
    batch_body = dict(batch_document)
    embedded_batch_sha = batch_body.pop("batchResultSha256", None)
    ordered_results = batch_body.get("orderedResults")
    if (
        embedded_batch_sha != batch_sha
        or canonical_hash(batch_body) != batch_sha
        or set(batch_body)
        != {
            "schema",
            "recoveryAttemptId",
            "preregistrationSha256",
            "candidateCount",
            "orderedResults",
        }
        or batch_body.get("schema") != "fresh-xauusd-recovery-discovery-batch/v1"
        or batch_body.get("recoveryAttemptId") != attempt_id
        or batch_body.get("preregistrationSha256") != prereg_sha
        or batch_body.get("candidateCount") != 240
        or not isinstance(ordered_results, list)
        or len(ordered_results) != 240
    ):
        raise PermissionError("the sealed recovery batch content changed")

    for index, (sealed, ledger_record) in enumerate(
        zip(ordered_results, discovery_records)
    ):
        if not isinstance(sealed, Mapping):
            raise PermissionError("a sealed candidate result is invalid")
        evaluation = sealed.get("evaluation")
        parameters = ledger_record.get("parameters")
        score = evaluation.get("score") if isinstance(evaluation, Mapping) else None
        if (
            sealed.get("candidateId") != candidate_ids[index]
            or sealed.get("entrySha256") != candidate_sha[index]
            or not isinstance(evaluation, Mapping)
            or set(evaluation)
            != {"identitySha256", "passed", "metrics", "leakageChecks", "score"}
            or not isinstance(evaluation.get("passed"), bool)
            or not isinstance(evaluation.get("metrics"), Mapping)
            or not isinstance(evaluation.get("leakageChecks"), Mapping)
            or (
                score is not None
                and (
                    isinstance(score, bool)
                    or not isinstance(score, (int, float))
                    or not math.isfinite(float(score))
                )
            )
            or evaluation.get("identitySha256") != candidate_sha[index]
            or evaluation.get("passed") != ledger_record.get("gatePassed")
            or evaluation.get("metrics") != ledger_record.get("metrics")
            or evaluation.get("leakageChecks") != ledger_record.get("leakageChecks")
            or evaluation.get("score") != ledger_record.get("balancedScore")
            or ledger_record.get("status")
            != ("passed" if evaluation.get("passed") else "rejected")
            or ledger_record.get("identitySha256") != candidate_sha[index]
            or ledger_record.get("outcomesRevealed") is not True
            or ledger_record.get("windowSha256") != discovery_window_sha
            or ledger_record.get("preregistrationSha256") != prereg_sha
            or not isinstance(parameters, Mapping)
            or parameters.get("recoveryAttemptId") != attempt_id
            or parameters.get("sealedBatchResultSha256") != batch_sha
        ):
            raise PermissionError("a sealed candidate result differs from the ledger")

    stage_parameters = protocol_parameters[5]
    promoted_ids = stage_parameters.get("promotedCandidateIds")
    budgets = preregistration.get("candidateSearch", {}).get("budgets", {})
    promotion_limit = (
        budgets.get("walkForward1FrozenCandidates")
        if isinstance(budgets, Mapping)
        else None
    )
    if not isinstance(promotion_limit, int) or isinstance(promotion_limit, bool):
        raise PermissionError("the discovery promotion budget is unavailable")
    passed_results = [
        sealed
        for sealed in ordered_results
        if sealed["evaluation"].get("passed") is True
    ]

    def promotion_key(sealed: Mapping[str, Any]) -> tuple[Any, ...]:
        score = sealed["evaluation"].get("score")
        score_missing = score is None
        numeric_score = float(score) if score is not None else float("-inf")
        return (score_missing, -numeric_score, str(sealed["candidateId"]))

    expected_promoted = [
        str(sealed["candidateId"])
        for sealed in sorted(passed_results, key=promotion_key)[:promotion_limit]
    ]
    stage_metrics = recovery_records[5].get("metrics")
    if (
        stage_parameters.get("batchResultSha256") != batch_sha
        or stage_parameters.get("candidateCount") != 240
        or stage_parameters.get("candidateOutcomeRecordCount") != 240
        or promoted_ids != expected_promoted
        or not isinstance(stage_metrics, Mapping)
        or stage_metrics.get("candidateCount") != 240
        or stage_metrics.get("promotedCandidateCount") != len(expected_promoted)
        or stage_metrics.get("studyFailed") != (not expected_promoted)
    ):
        raise PermissionError("the completed recovery promotion record changed")

    return {
        "schema": "fresh-xauusd-run14-holdout-recovery-proof/v1",
        "recoveryContractSha256": claimed_contract_sha,
        "recoveryImplementationManifestSha256": recovery_implementation_sha,
        "equivalenceEvidenceSha256": canonical_hash(evidence),
        "sealedBatchResultSha256": batch_sha,
        "sealedBatchResultFileSha256": batch_file_sha,
        "orderedCandidateSequenceSha256": RUN14_ORDERED_CANDIDATE_SHA256,
        "candidateOutcomeRecordCount": 240,
    }


def finalize_interrupted_run14_recovery(
    ledger_path: str | Path, *, exit_status: int
) -> bool:
    """Seal any incomplete run-14 recovery prefix as one terminal failure.

    The process can be killed between any two durable appends, including after
    the complete batch artifact is sealed but before all 240 candidate records
    or the final discovery-stage record are present.  A recovery is complete
    only when that entire ordered sequence exists.  Every shorter valid prefix
    consumes the sole permitted attempt and is terminated here.
    """

    if not isinstance(exit_status, int) or isinstance(exit_status, bool):
        raise TypeError("exit_status must be an integer")
    selected_ledger = Path(ledger_path).expanduser()
    if selected_ledger.is_symlink():
        raise PermissionError("the durable recovery ledger is unavailable")
    ledger = selected_ledger.resolve()
    if not ledger.is_file():
        raise PermissionError("the durable recovery ledger is unavailable")
    records: list[dict[str, Any]] = []
    for number, line in enumerate(ledger.read_text(encoding="utf-8").splitlines(), 1):
        raw = json.loads(line)
        if not isinstance(raw, dict):
            raise PermissionError("recovery ledger records must be objects")
        body = dict(raw)
        claimed_number = body.pop("recordNumber", None)
        claimed_sha = body.pop("recordSha256", None)
        if claimed_number != number or canonical_hash(body) != claimed_sha:
            raise PermissionError("the durable recovery ledger chain is invalid")
        records.append(raw)

    if len(records) < 2 or [
        record.get("recordSha256") for record in records[:2]
    ] != list(RUN14_RECORD_SHA256):
        raise PermissionError("the original interrupted ledger prefix changed")
    if (
        records[0].get("status") != "window_access_started"
        or records[1].get("status") != "batch_access_started"
        or records[0].get("stage") != "discovery"
        or records[1].get("stage") != "discovery"
        or records[0].get("windowSha256") != records[1].get("windowSha256")
    ):
        raise PermissionError("the original interrupted ledger prefix changed")
    original_batch = records[1].get("parameters")
    if not isinstance(original_batch, Mapping):
        raise PermissionError("the original discovery batch identity is missing")
    candidate_ids = original_batch.get("candidateIds")
    candidate_sha256 = original_batch.get("candidateSha256")
    if (
        not isinstance(candidate_ids, list)
        or not isinstance(candidate_sha256, list)
        or len(candidate_ids) != 240
        or len(candidate_sha256) != 240
        or len(candidate_ids) != len(set(candidate_ids))
    ):
        raise PermissionError("the original discovery candidate sequence changed")

    attempt_id = "run14-discovery-recovery-attempt-1"
    window_sha = records[0].get("windowSha256")
    if not isinstance(window_sha, str):
        raise PermissionError("the recovery window identity is missing")

    def validate_resume_record(record: Mapping[str, Any], status: str) -> None:
        payload = record.get("parameters")
        if (
            record.get("recordKind") != "infrastructure-resume"
            or record.get("candidateId") != f"protocol-infrastructure-resume::{status}"
            or record.get("family") != "protocol-infrastructure-recovery"
            or record.get("stage") != "discovery"
            or record.get("role") != "discovery"
            or record.get("status") != status
            or record.get("outcomesRevealed") is not True
            or record.get("windowSha256") != window_sha
            or record.get("preregistrationSha256") != RUN14_PREREGISTRATION_SHA256
            or not isinstance(payload, Mapping)
            or payload.get("kind") != "fresh-infrastructure-recovery"
            or payload.get("status") != status
            or payload.get("stage") != "discovery"
            or payload.get("recoveryAttemptId") != attempt_id
            or canonical_hash(payload) != record.get("identitySha256")
        ):
            raise PermissionError(f"the {status} recovery record changed")

    tail = records[2:]
    terminal_error: Mapping[str, Any] | None = None
    error_positions = [
        index
        for index, record in enumerate(tail)
        if record.get("status") == "batch_resume_error"
    ]
    if error_positions:
        if error_positions != [len(tail) - 1]:
            raise PermissionError("a recovery error must be the terminal ledger record")
        terminal_error = tail[-1]
        validate_resume_record(terminal_error, "batch_resume_error")
        sequence = tail[:-1]
    else:
        sequence = tail

    expected_authorization_statuses = (
        "resume_eligibility_audit",
        "resume_authorized",
        "resume_identity_verified",
    )
    position = 0
    incomplete = False
    batch_completed = False
    batch_result_sha: str | None = None
    candidate_outcomes = 0

    for expected in expected_authorization_statuses:
        if position == len(sequence):
            incomplete = True
            break
        validate_resume_record(sequence[position], expected)
        position += 1

    if not incomplete:
        if position == len(sequence):
            incomplete = True
        else:
            validate_resume_record(sequence[position], "batch_resume_started")
            position += 1

    if not incomplete:
        if position == len(sequence):
            incomplete = True
        else:
            validate_resume_record(sequence[position], "batch_resume_completed")
            completed_parameters = sequence[position]["parameters"].get("parameters")
            if not isinstance(completed_parameters, Mapping):
                raise PermissionError("the sealed recovery batch identity is missing")
            batch_result_sha = completed_parameters.get("batchResultSha256")
            if (
                not isinstance(batch_result_sha, str)
                or len(batch_result_sha) != 64
                or completed_parameters.get("candidateCount") != len(candidate_ids)
                or completed_parameters.get("orderedCandidateSequenceSha256")
                != RUN14_ORDERED_CANDIDATE_SHA256
            ):
                raise PermissionError("the sealed recovery batch identity changed")
            batch_completed = True
            position += 1

    if batch_completed:
        while position < len(sequence) and candidate_outcomes < len(candidate_ids):
            record = sequence[position]
            if record.get("recordKind") == "infrastructure-resume":
                break
            expected_id = candidate_ids[candidate_outcomes]
            expected_sha = candidate_sha256[candidate_outcomes]
            parameters = record.get("parameters")
            if (
                record.get("candidateId") != expected_id
                or record.get("stage") != "discovery"
                or record.get("role") != "discovery"
                or record.get("status") not in ("passed", "rejected")
                or record.get("outcomesRevealed") is not True
                or record.get("identitySha256") != expected_sha
                or record.get("frozenEntrySha256") != expected_sha
                or record.get("windowSha256") != window_sha
                or record.get("preregistrationSha256") != RUN14_PREREGISTRATION_SHA256
                or not isinstance(parameters, Mapping)
                or parameters.get("recoveryAttemptId") != attempt_id
                or parameters.get("sealedBatchResultSha256") != batch_result_sha
            ):
                raise PermissionError("the recovered candidate sequence changed")
            candidate_outcomes += 1
            position += 1

        if candidate_outcomes < len(candidate_ids):
            if position != len(sequence):
                raise PermissionError(
                    "the recovery stage completed before every candidate outcome"
                )
            incomplete = True
        elif position == len(sequence):
            incomplete = True
        else:
            validate_resume_record(sequence[position], "resume_stage_completed")
            stage_parameters = sequence[position]["parameters"].get("parameters")
            if (
                not isinstance(stage_parameters, Mapping)
                or stage_parameters.get("batchResultSha256") != batch_result_sha
                or stage_parameters.get("candidateCount") != len(candidate_ids)
                or stage_parameters.get("candidateOutcomeRecordCount")
                != len(candidate_ids)
            ):
                raise PermissionError("the completed recovery stage identity changed")
            position += 1
            if any(
                record.get("recordKind") == "infrastructure-resume"
                for record in sequence[position:]
            ):
                raise PermissionError("the recovery protocol was appended twice")
            incomplete = False

    if terminal_error is not None:
        if not incomplete:
            raise PermissionError("a completed recovery cannot end in a recovery error")
        error_parameters = terminal_error["parameters"].get("parameters")
        if (
            not isinstance(error_parameters, Mapping)
            or error_parameters.get("recoveryAttemptId") != attempt_id
            or error_parameters.get("candidateOutcomesAppended") != candidate_outcomes
        ):
            raise PermissionError("the terminal recovery error identity changed")
        return False

    if not incomplete:
        return False

    inner = {
        "recoveryAttemptId": attempt_id,
        "errorType": "ExternalProcessTermination",
        "externalExitStatus": exit_status,
        "candidateOutcomesAppended": candidate_outcomes,
        "batchResultSealed": batch_completed,
        "lastDurableStatus": records[-1].get("status"),
    }
    identity = {
        "kind": "fresh-infrastructure-recovery",
        "status": "batch_resume_error",
        "stage": "discovery",
        "recoveryAttemptId": attempt_id,
        "parameters": inner,
    }
    append_fresh_record(
        ledger,
        {
            "recordKind": "infrastructure-resume",
            "candidateId": "protocol-infrastructure-resume::batch_resume_error",
            "family": "protocol-infrastructure-recovery",
            "stage": "discovery",
            "trainingWindow": "discovery",
            "evaluationWindow": "discovery",
            "parameters": identity,
            "entryVariant": "infrastructure-resume",
            "exitVariant": "infrastructure-resume",
            "metrics": {
                "errorType": "ExternalProcessTermination",
                "externalExitStatus": exit_status,
                "candidateOutcomesAppended": candidate_outcomes,
                "batchResultSealed": batch_completed,
            },
            "status": "batch_resume_error",
            "leakageChecks": {
                "recoveryAttemptConsumed": True,
                "candidatePromotionForbidden": True,
                "externalFinalizerUsed": True,
                "partialCandidateOutcomesCounted": True,
            },
            "role": "discovery",
            "outcomesRevealed": True,
            "gatePassed": False,
            "identitySha256": canonical_hash(identity),
            "windowSha256": window_sha,
            "preregistrationSha256": RUN14_PREREGISTRATION_SHA256,
        },
    )
    return True


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--finalize-interrupted-ledger")
    parser.add_argument("--exit-status", type=int)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _build_parser().parse_args(argv)
    if not arguments.finalize_interrupted_ledger or arguments.exit_status is None:
        raise SystemExit("a ledger path and exit status are required")
    appended = finalize_interrupted_run14_recovery(
        arguments.finalize_interrupted_ledger,
        exit_status=arguments.exit_status,
    )
    sys.stdout.write(
        json.dumps(
            {
                "recoveryInterruptionFinalized": appended,
                "exitStatus": arguments.exit_status,
            },
            sort_keys=True,
        )
        + "\n"
    )
    return 0


__all__ = [
    "RUN14_ARTIFACT_ID",
    "RUN14_COMMIT_SHA",
    "RUN14_ENTRY_BANK_FILE_SHA256",
    "RUN14_RUN_ATTEMPT",
    "RUN14_RUN_ID",
    "RUN14_TGZ_SHA256",
    "Run14RecoveryBundle",
    "build_run14_recovery_contract",
    "finalize_interrupted_run14_recovery",
    "load_run14_recovery_bundle",
    "main",
]


if __name__ == "__main__":
    raise SystemExit(main())
