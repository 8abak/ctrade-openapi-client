"""Exact eligibility evidence for the sole V5 infrastructure continuation.

The detached V5 process terminated with status 137 after all forty discovery
sessions had been reduced to temporary diagnostics, but before the discovery
batch callback returned.  Its durable ledger therefore contains only the
stage-access and batch-access start records.  This module accepts only that
audited terminal artifact.  It exposes immutable, outcome-blind inputs for one
full discovery recomputation; it never treats progress rows as checkpoints.
"""

from __future__ import annotations

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
    required_fresh_v5_implementation_files,
    validate_fresh_implementation_manifest,
    validate_fresh_preregistration_v5,
)
from datavis.research.fresh_protocol import append_fresh_record, canonical_hash
from datavis.research.fresh_restart_v5 import (
    FRESH_V5_STUDY_ID,
    RUN19_V5_STUDY_LINEAGE_SHA256,
)
from datavis.research.fresh_search import EntryCandidateSpec, FrozenEntryCandidate
from datavis.research.fresh_thresholds import fresh_quantile_bank_from_payload


V5_ORIGINAL_GITHUB_RUN_ID = 300_678_321_87
V5_ORIGINAL_GITHUB_RUN_ATTEMPT = 1
V5_ORIGINAL_GITHUB_COMMIT_SHA = "bc7c814876cc75a0fbe85ba824177ad8baccd5cf"
V5_LAUNCH_ARTIFACT_ID = 8_586_881_858
V5_ADOPTION_GITHUB_RUN_ID = 30_101_048_443
V5_ADOPTION_GITHUB_RUN_ATTEMPT = 1
V5_ADOPTION_GITHUB_JOB_ID = 89_506_876_763
V5_ADOPTION_GITHUB_COMMIT_SHA = "c730fd0a2c66426f995ac43f1d50035cf94265ff"
V5_ADOPTION_ARTIFACT_ID = 8_608_015_979
V5_ADOPTION_ARTIFACT_NAME = (
    "fresh-xauusd-v5-terminal-adopted-30101048443-1"
)
V5_ADOPTION_ARTIFACT_SIZE = 127_602
V5_ADOPTION_ARTIFACT_DIGEST = (
    "sha256:6ded0fc6a44e312a9d786991b093913783ce7a2c1d5afa56b58fcf0fbdb824f3"
)
V5_TERMINAL_ARCHIVE_NAME = "fresh-xauusd-30067832187-1.tgz"
V5_TERMINAL_ARCHIVE_SIZE = 125_470
V5_TERMINAL_ARCHIVE_SHA256 = (
    "397f687e897e45b4c6c41ed04000ecff8e048524ac9d117658b459b219d9ce3d"
)
V5_LEDGER_SHA256 = (
    "e95e1739987cdb56315adcbb98b2e85198cb14a1d536a07282214d2ef359744d"
)
V5_LEDGER_RECORD_SHA256 = (
    "83b8e201bab95195526f3580c98e2f4494331df3ecc44c1586cba72ef4f95cb3",
    "f300211bd30a73842539bc8c2365c3eb3fcbd8e7216a968bd97f76dff4f151f1",
)
V5_STAGE_ACCESS_IDENTITY_SHA256 = (
    "e5083ffa5e0952346009981ba1b2b214fef713608811d4b417464b4b706531a4"
)
V5_BATCH_ACCESS_IDENTITY_SHA256 = (
    "4e2a6708365650619453564c1e8d8f4a6e793af5e01d57e5399447ba48750c5e"
)
V5_PREREGISTRATION_SHA256 = (
    "ef72f00de02a144ab67dd75012a711473bcd47824cd5ee787b07268a92b11c8c"
)
V5_IMPLEMENTATION_MANIFEST_SHA256 = (
    "aadafdecc92cd4b1e3e1757a74c805bd6b119c8767d2a741ba4ef946bf645748"
)
V5_SPLIT_MANIFEST_SHA256 = (
    "59a0df375a3b8934c14a355a4fc91bb9aade6ada88052d5096c4b9a29e2744bd"
)
V5_RESEARCH_WINDOW_SET_SHA256 = (
    "0215cfa1ca0954bae1d6eaafbab44a62ddf663a356bb9f7978071afed4595371"
)
V5_HOLDOUT_WINDOW_SHA256 = (
    "8d599150987e32430a5d012b4973590bda56f7d548c42e7dad9714e2f0fe40b7"
)
V5_DISCOVERY_WINDOW_SHA256 = (
    "66555bc7a1d991dc9e7cf485d07cafd40c30bab30dd9945a00840500a7518708"
)
V5_DISCOVERY_ACCESS_WINDOW_SHA256 = (
    "2900eea1235cf0b25874a7a9b194b50a54810d479e3a49c33ad6f8caa4a3f25e"
)
V5_INVENTORY_SHA256 = (
    "f766d21bfa5a60d6f7b81f5393a24458626cead6069dd8b1719f89de68924d5b"
)
V5_CORPUS_MANIFEST_SHA256 = (
    "f24e090b4e149a12a981c4adfbfbc4f68fb57fc9413a0e386b69c53ac3da0c79"
)
V5_QUANTILE_BANK_SHA256 = (
    "3243793cdc3d1ee2f7a64b2184a8f91bdf729f2115b06adfb9060dd60a3f78ae"
)
V5_ENTRY_BANK_FILE_SHA256 = (
    "7be58142337fc1b440fe61dae3ad0721c5058e4a1eae3dfde7c223bb8021b28c"
)
V5_ORDERED_CANDIDATE_SEQUENCE_SHA256 = (
    "d4163395adb43ec49a5f0e10df1fcc82bb698703d2462d735eed5b7ed40ba19c"
)
V5_THRESHOLD_BANK_PAYLOAD_SHA256 = (
    "9545c7fa7e5c5eef9d64d867f5b9b1b81415e405389471b14e67d1761b1ddf84"
)
V5_CANDIDATE_GRID_SHA256 = (
    "de4f51a15a32fd64f46e2230c51d4ee80df0a3af3b309cecee1e6b1f712327e6"
)
V5_EVENT_FILTER_BANK_SHA256 = (
    "50f0a3a39f008465a6a1d0e9506506e57d072bf608207c842dc72b7a84c5b671"
)
V5_EXIT_GRID_SHA256 = (
    "e9460aabbad5b1e85ec97790a8a62acf0b5bd091a57573d36c3663fae41d1280"
)
V5_EXECUTION_SCENARIOS_SHA256 = (
    "98355f3b5514d0a5baa8ea3fb441d6aa2b9f2484543884d00344343d3322d2f4"
)
V5_SCIENTIFIC_SPECIFICATION_SHA256 = (
    "fef6b1a4898aaeb4ce33ad96ea270f0211448357399d94f76051b01c9dabcbd8"
)

V5_RECOVERY_ATTEMPT_ID = "v5-discovery-recovery-attempt-1"
V5_RECOVERY_CONTRACT_SCHEMA = "fresh-xauusd-v5-recovery-contract/v1"
V5_RECOVERY_EQUIVALENCE_SCHEMA = (
    "fresh-xauusd-v5-recovery-equivalence-preflight/v1"
)
V5_RECOVERY_HOLDOUT_PROOF_SCHEMA = (
    "fresh-xauusd-v5-holdout-recovery-proof/v1"
)
V5_RECOVERY_TEST_MODULES = (
    "test_fresh_numeric_spool",
    "test_fresh_pipeline",
    "test_fresh_pipeline_v5",
    "test_fresh_preregistration",
    "test_fresh_preregistration_v5",
    "test_fresh_recovery_v5",
    "test_fresh_scoring",
    "test_fresh_search",
    "test_fresh_spool",
    "test_fresh_v5_recovery_orchestration",
)


def required_fresh_v5_recovery_implementation_files() -> tuple[str, ...]:
    """Return the exact current-code closure permitted for the sole recovery."""

    return tuple(
        sorted(
            {
                *required_fresh_v5_implementation_files(),
                ".github/research-v5-recovery-launch.txt",
                ".github/scripts/fresh-xauusd-v5-recovery-controller.py",
                ".github/scripts/fresh-xauusd-v5-terminal-audit-input.py",
                ".github/ssh/fresh-xauusd-ec2-known-hosts",
                (
                    ".github/workflows/"
                    "fresh-xauusd-v5-recovery-detached-launch.yml"
                ),
                "datavis/research/fresh_numeric_spool.py",
                "datavis/research/fresh_recovery_v5.py",
                *(f"{module}.py" for module in V5_RECOVERY_TEST_MODULES),
            }
        )
    )


V5_RECOVERY_MEMBER_SHA256 = {
    "fresh_corpus_manifest_v1.json": (
        "fe59805f49ed40ae7996bd8333bba6ea2531ce67c04e904f4f228ea01a54dec2"
    ),
    "fresh_entry_bank_v1.json": V5_ENTRY_BANK_FILE_SHA256,
    "fresh_experiment_ledger_v1.jsonl": V5_LEDGER_SHA256,
    "fresh_implementation_manifest_v1.json": (
        "45c240012263986409add7d9f478a4e8990d7403bd3ed38b4fbd403b8f15ea23"
    ),
    "fresh_preregistration_v5.json": (
        "06c25b8733de70b75f7ae07b136a3bfecba5bd264f0ecdaa1b153db6d0f190a6"
    ),
    "fresh_quantile_bank_v1.json": (
        "5076a373f6cfc25a6a37e8a63b90eb4633282b425021b55879cb193bb76bab46"
    ),
    "fresh_research_state_binding_v4.json": (
        "696408161ef88e94436ec1713960bc5f5ecb0c4394ea06e15643e10ed0f60567"
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
    "predecessor_fresh_experiment_ledger_v1.jsonl": (
        "ac627bd986c044b12049f717eb3fc664321c08c169fd6a829a5fc8d51144c7b4"
    ),
    "predecessor_fresh_implementation_manifest_v1.json": (
        "d04bd2279c31922fc753b313f61b140a124c2fc7625227a5a0b9de29377ca1ee"
    ),
    "predecessor_fresh_preregistration_v4.json": (
        "fd203eed1ff5b1f407b6179b2fd18546106420a1d3ba50b7acddc65e090e0e87"
    ),
    "predecessor_fresh_research_state_binding_v3.json": (
        "62eacb704989a640478ab8a3d05a20cc91a0d69a3797d12dac330c9b3c606cee"
    ),
    "remote-exit-status.txt": (
        "e3b9c2844b5a5c2677b3a2279db2ec8487491dd9a23d6b22fac153391b3bb63c"
    ),
    "server-run.log": (
        "e99d19a11fea31762b6e49e85d4b24ca16a57dbd2be16862bce365ab6a9227d2"
    ),
}

_FORBIDDEN_MEMBERS = frozenset(
    {
        "fresh_exit_bank_v1.json",
        "fresh_final_strategy_frozen_v1.json",
        "fresh_holdout_authorization_v1.json",
        "fresh_holdout_results_v1.json",
        "fresh_recovery_contract_v1.json",
        "fresh_recovery_discovery_batch_v1.json",
        "fresh_run_summary_v1.json",
    }
)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _pairs_without_duplicates(pairs):
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _read_json(path: Path, label: str) -> Mapping[str, Any]:
    try:
        with path.open("r", encoding="utf-8") as source:
            value = json.load(
                source,
                object_pairs_hook=_pairs_without_duplicates,
                parse_constant=lambda value: (_ for _ in ()).throw(
                    ValueError(f"non-finite JSON value: {value}")
                ),
            )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        raise PermissionError(f"{label} is not strict JSON") from error
    if not isinstance(value, Mapping):
        raise PermissionError(f"{label} must be a JSON object")
    return value


def _load_verified_ledger(path: Path) -> tuple[Mapping[str, Any], ...]:
    raw = path.read_bytes()
    if not raw or not raw.endswith(b"\n"):
        raise PermissionError("the V5 recovery ledger is empty or unterminated")
    records: list[Mapping[str, Any]] = []
    for number, line in enumerate(raw.splitlines(keepends=True), start=1):
        if line == b"\n":
            raise PermissionError("the V5 recovery ledger contains a blank record")
        try:
            record = json.loads(
                line.decode("utf-8"),
                object_pairs_hook=_pairs_without_duplicates,
                parse_constant=lambda value: (_ for _ in ()).throw(
                    ValueError(f"non-finite JSON value: {value}")
                ),
            )
        except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
            raise PermissionError("the V5 recovery ledger is not strict JSON") from error
        if not isinstance(record, Mapping):
            raise PermissionError("V5 recovery ledger records must be objects")
        canonical = (
            json.dumps(
                record,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")
        if canonical != line:
            raise PermissionError("the V5 recovery ledger is not canonical")
        body = dict(record)
        claimed_number = body.pop("recordNumber", None)
        claimed_sha = body.pop("recordSha256", None)
        if (
            claimed_number != number
            or not isinstance(claimed_sha, str)
            or canonical_hash(body) != claimed_sha
        ):
            raise PermissionError("the V5 recovery ledger chain is invalid")
        records.append(record)
    return tuple(records)


def _verified_two_record_ledger(path: Path) -> tuple[Mapping[str, Any], ...]:
    records = _load_verified_ledger(path)
    if len(records) != 2:
        raise PermissionError("V5 recovery requires exactly two ledger records")
    stage, batch = records
    expected_stage = {
        "recordNumber": 1,
        "recordSha256": V5_LEDGER_RECORD_SHA256[0],
        "recordKind": "stage-window-access",
        "candidateId": "protocol-stage-access::discovery",
        "stage": "discovery",
        "role": "discovery",
        "status": "window_access_started",
        "outcomesRevealed": True,
        "family": "protocol-window-access",
        "trainingWindow": "discovery",
        "evaluationWindow": "discovery",
        "gatePassed": False,
        "identitySha256": V5_STAGE_ACCESS_IDENTITY_SHA256,
        "windowSha256": V5_DISCOVERY_ACCESS_WINDOW_SHA256,
        "preregistrationSha256": V5_PREREGISTRATION_SHA256,
    }
    expected_batch = {
        "recordNumber": 2,
        "recordSha256": V5_LEDGER_RECORD_SHA256[1],
        "recordKind": "batch-window-access",
        "candidateId": "protocol-batch-access::entry::discovery",
        "stage": "discovery",
        "role": "discovery",
        "status": "batch_access_started",
        "outcomesRevealed": True,
        "family": "protocol-window-access",
        "trainingWindow": "discovery",
        "evaluationWindow": "discovery",
        "gatePassed": False,
        "identitySha256": V5_BATCH_ACCESS_IDENTITY_SHA256,
        "windowSha256": V5_DISCOVERY_ACCESS_WINDOW_SHA256,
        "preregistrationSha256": V5_PREREGISTRATION_SHA256,
    }
    if any(stage.get(key) != value for key, value in expected_stage.items()):
        raise PermissionError("V5 stage-access ledger identity changed")
    if any(batch.get(key) != value for key, value in expected_batch.items()):
        raise PermissionError("V5 batch-access ledger identity changed")
    if (
        stage.get("metrics")
        != {
            "purpose": (
                "evaluate the immutable outcome-blind predecessor discovery "
                "threshold and entry bank in the new study"
            )
        }
        or stage.get("leakageChecks")
        != {
            "durableBeforeCallback": True,
            "windowConsumedBeforeCallback": True,
        }
        or batch.get("metrics") != {"candidateCount": 240, "errorType": None}
        or batch.get("leakageChecks")
        != {
            "callbackCompleted": False,
            "callbackErrored": False,
            "durableBeforeCallback": True,
        }
    ):
        raise PermissionError("V5 recovery protocol evidence changed")
    parameters = batch.get("parameters")
    if (
        not isinstance(parameters, Mapping)
        or canonical_hash(parameters) != V5_BATCH_ACCESS_IDENTITY_SHA256
        or parameters.get("batchKind") != "entry"
        or parameters.get("kind") != "fresh-batch-window-access"
        or parameters.get("stage") != "discovery"
        or parameters.get("status") != "batch_access_started"
        or parameters.get("trainingRoles") != ["discovery"]
        or parameters.get("evaluationRoles") != ["discovery"]
        or parameters.get("errorType") is not None
        or len(parameters.get("candidateIds", ())) != 240
        or len(parameters.get("candidateSha256", ())) != 240
        or len(set(parameters["candidateIds"])) != 240
        or canonical_hash(
            [
                {"candidateId": candidate_id, "entrySha256": entry_sha}
                for candidate_id, entry_sha in zip(
                    parameters["candidateIds"],
                    parameters["candidateSha256"],
                )
            ]
        )
        != V5_ORDERED_CANDIDATE_SEQUENCE_SHA256
    ):
        raise PermissionError("V5 started batch candidate identity changed")
    if canonical_hash(stage.get("parameters")) != V5_STAGE_ACCESS_IDENTITY_SHA256:
        raise PermissionError("V5 stage-access parameter identity changed")
    if any(
        record.get("recordKind")
        not in ("stage-window-access", "batch-window-access")
        or record.get("role") != "discovery"
        for record in records
    ):
        raise PermissionError("V5 recovery artifact contains outcome records")
    return records


@dataclass(frozen=True, slots=True)
class FreshV5RecoveryBundle:
    """Verified immutable evidence and inputs for one V5 continuation."""

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
    discovery_progress: tuple[Mapping[str, Any], ...]


def load_fresh_v5_recovery_bundle(
    directory: str | Path,
) -> FreshV5RecoveryBundle:
    """Accept only the exact adopted V5 exit-137 terminal member set."""

    selected = Path(directory).expanduser()
    if selected.is_symlink():
        raise PermissionError("the V5 recovery directory cannot be a symlink")
    root = selected.resolve()
    if not root.is_dir():
        raise PermissionError("the V5 recovery directory is unavailable")
    children = tuple(root.iterdir())
    names = {child.name for child in children}
    if names & _FORBIDDEN_MEMBERS or names != set(V5_RECOVERY_MEMBER_SHA256):
        raise PermissionError("the V5 recovery artifact member set changed")
    paths: dict[str, Path] = {}
    for child in children:
        if child.is_symlink() or not child.is_file():
            raise PermissionError("V5 recovery members must be regular files")
        if _file_sha256(child) != V5_RECOVERY_MEMBER_SHA256[child.name]:
            raise PermissionError(f"V5 recovery member digest changed: {child.name}")
        paths[child.name] = child
    if paths["remote-exit-status.txt"].read_bytes() != b"137\n":
        raise PermissionError("the V5 terminal process status changed")

    inventory = _read_json(paths["fresh_source_inventory_v1.json"], "inventory")
    corpus = _read_json(paths["fresh_corpus_manifest_v1.json"], "corpus")
    split = _read_json(paths["fresh_split_manifest_v2.json"], "split")
    state = _read_json(
        paths["fresh_research_state_binding_v4.json"],
        "state binding",
    )
    implementation = _read_json(
        paths["fresh_implementation_manifest_v1.json"],
        "implementation manifest",
    )
    preregistration = _read_json(
        paths["fresh_preregistration_v5.json"],
        "preregistration",
    )
    quantile = _read_json(paths["fresh_quantile_bank_v1.json"], "quantile bank")
    preflight = _read_json(
        paths["fresh_threshold_domain_preflight_v1.json"],
        "threshold preflight",
    )
    entry_bank = _read_json(paths["fresh_entry_bank_v1.json"], "entry bank")
    if (
        inventory.get("inventorySha256") != V5_INVENTORY_SHA256
        or corpus.get("corpusManifestSha256") != V5_CORPUS_MANIFEST_SHA256
        or corpus.get("inventorySha256") != V5_INVENTORY_SHA256
        or split.get("manifestSha256") != V5_SPLIT_MANIFEST_SHA256
        or split.get("inventorySha256") != V5_INVENTORY_SHA256
        or validate_fresh_implementation_manifest(
            implementation,
            verify_current_files=False,
        )
        != V5_IMPLEMENTATION_MANIFEST_SHA256
        or validate_fresh_preregistration_v5(
            preregistration,
            verify_current_implementation_files=False,
        )
        != V5_PREREGISTRATION_SHA256
        or fresh_quantile_bank_from_payload(quantile).bank_sha256
        != V5_QUANTILE_BANK_SHA256
        or canonical_hash(quantile) != V5_THRESHOLD_BANK_PAYLOAD_SHA256
        or entry_bank.get("candidateCount") != 240
        or entry_bank.get("quantileBankSha256") != V5_QUANTILE_BANK_SHA256
        or entry_bank.get("candidateGridSha256") != V5_CANDIDATE_GRID_SHA256
        or entry_bank.get("filterVariantBankSha256")
        != V5_EVENT_FILTER_BANK_SHA256
        or preflight
        != {
            "allRegisteredThresholdDomainsResolved": True,
            "baseCandidateCount": 93,
            "candidateGridSha256": V5_CANDIDATE_GRID_SHA256,
            "eventFilterVariantBankSha256": V5_EVENT_FILTER_BANK_SHA256,
            "eventFilterVariantCount": 147,
            "executionScenariosSha256": V5_EXECUTION_SCENARIOS_SHA256,
            "exitGridSha256": V5_EXIT_GRID_SHA256,
            "exitVariantCount": 72,
            "quantileBankSha256": V5_QUANTILE_BANK_SHA256,
            "schema": "fresh-xauusd-threshold-domain-preflight/v1",
            "totalRuntimeEntryCount": 240,
        }
    ):
        raise PermissionError("the V5 outcome-blind scientific inputs changed")
    windows = split.get("windows")
    discovery = windows.get("discovery") if isinstance(windows, Mapping) else None
    if (
        not isinstance(discovery, Mapping)
        or canonical_hash(discovery) != V5_DISCOVERY_WINDOW_SHA256
        or state.get("schema") != "fresh-xauusd-durable-research-state/v4"
        or state.get("studyId") != FRESH_V5_STUDY_ID
        or state.get("studyLineageSha256") != RUN19_V5_STUDY_LINEAGE_SHA256
        or state.get("splitManifestSha256") != V5_SPLIT_MANIFEST_SHA256
        or state.get("researchWindowSetSha256")
        != V5_RESEARCH_WINDOW_SET_SHA256
        or state.get("holdoutWindowSha256") != V5_HOLDOUT_WINDOW_SHA256
        or state.get("scientificSpecificationSha256")
        != V5_SCIENTIFIC_SPECIFICATION_SHA256
    ):
        raise PermissionError("the V5 split or durable state identity changed")

    records = _verified_two_record_ledger(
        paths["fresh_experiment_ledger_v1.jsonl"]
    )
    batch_parameters = records[1]["parameters"]
    rows = entry_bank.get("candidates")
    source = preregistration.get("sourceBindings")
    if (
        not isinstance(rows, list)
        or [row.get("candidateId") for row in rows]
        != batch_parameters["candidateIds"]
        or not isinstance(source, Mapping)
        or source.get("inventorySha256") != V5_INVENTORY_SHA256
        or source.get("corpusManifestSha256") != V5_CORPUS_MANIFEST_SHA256
        or source.get("splitManifestSha256") != V5_SPLIT_MANIFEST_SHA256
        or source.get("implementationManifestSha256")
        != V5_IMPLEMENTATION_MANIFEST_SHA256
        or source.get("experimentLedgerPath") != state.get("experimentLedgerPath")
        or source.get("holdoutAuthorizationRegistryPath")
        != state.get("holdoutAuthorizationRegistryPath")
    ):
        raise PermissionError("the V5 frozen input cross-bindings changed")
    progress: list[Mapping[str, Any]] = []
    for line in paths["server-run.log"].read_text(encoding="utf-8").splitlines():
        try:
            row = json.loads(line, object_pairs_hook=_pairs_without_duplicates)
        except (json.JSONDecodeError, ValueError) as error:
            raise PermissionError("the V5 discovery progress log changed") from error
        if not isinstance(row, Mapping):
            raise PermissionError("the V5 discovery progress log changed")
        progress.append(row)
    anchors = tuple(discovery.get("sessionAnchors", ()))
    if (
        len(progress) != 40
        or len(anchors) != 40
        or [
            (
                row.get("stage"),
                row.get("sessionOrdinal"),
                row.get("sessionCount"),
                row.get("sessionAnchor"),
                set(row),
            )
            for row in progress
        ]
        != [
            ("discovery", ordinal, 40, anchor, {
                "stage",
                "sessionOrdinal",
                "sessionCount",
                "sessionAnchor",
            })
            for ordinal, anchor in enumerate(anchors, start=1)
        ]
    ):
        raise PermissionError("the V5 discovery progress evidence changed")

    return FreshV5RecoveryBundle(
        directory=root,
        paths=paths,
        inventory=inventory,
        corpus=corpus,
        split=split,
        state_binding=state,
        implementation=implementation,
        preregistration=preregistration,
        quantile_bank=quantile,
        threshold_preflight=preflight,
        entry_bank=entry_bank,
        ledger_records=records,
        discovery_progress=tuple(progress),
    )


def _strict_json_clone(value: Mapping[str, Any], label: str) -> dict[str, Any]:
    try:
        encoded = json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        cloned = json.loads(
            encoded,
            object_pairs_hook=_pairs_without_duplicates,
            parse_constant=lambda item: (_ for _ in ()).throw(
                ValueError(f"non-finite JSON value: {item}")
            ),
        )
    except (TypeError, ValueError, json.JSONDecodeError) as error:
        raise PermissionError(f"{label} is not strict JSON") from error
    if not isinstance(cloned, dict):
        raise PermissionError(f"{label} must be a JSON object")
    return cloned


def _validate_recovery_implementation_manifest(
    manifest: Mapping[str, Any],
) -> tuple[str, Path, Mapping[str, str]]:
    try:
        manifest_sha = validate_fresh_implementation_manifest(
            manifest,
            verify_current_files=True,
        )
    except (TypeError, ValueError) as error:
        raise PermissionError(
            "the V5 recovery implementation manifest is invalid"
        ) from error
    root = Path(str(manifest.get("repositoryRoot", ""))).expanduser().resolve()
    raw_files = manifest.get("files")
    if not isinstance(raw_files, list):
        raise PermissionError(
            "the V5 recovery implementation manifest has no file closure"
        )
    files = {
        str(item.get("path")): str(item.get("sha256"))
        for item in raw_files
        if isinstance(item, Mapping)
    }
    required = required_fresh_v5_recovery_implementation_files()
    if set(files) != set(required) or len(files) != len(raw_files):
        raise PermissionError(
            "the V5 recovery implementation file closure changed"
        )
    return manifest_sha, root, files


def _v5_original_run_identity() -> dict[str, Any]:
    return {
        "runId": V5_ORIGINAL_GITHUB_RUN_ID,
        "runAttempt": V5_ORIGINAL_GITHUB_RUN_ATTEMPT,
        "launchArtifactId": V5_LAUNCH_ARTIFACT_ID,
        "commitSha": V5_ORIGINAL_GITHUB_COMMIT_SHA,
        "terminalArchive": {
            "name": V5_TERMINAL_ARCHIVE_NAME,
            "size": V5_TERMINAL_ARCHIVE_SIZE,
            "sha256": V5_TERMINAL_ARCHIVE_SHA256,
        },
        "adoption": {
            "runId": V5_ADOPTION_GITHUB_RUN_ID,
            "runAttempt": V5_ADOPTION_GITHUB_RUN_ATTEMPT,
            "jobId": V5_ADOPTION_GITHUB_JOB_ID,
            "commitSha": V5_ADOPTION_GITHUB_COMMIT_SHA,
            "artifactId": V5_ADOPTION_ARTIFACT_ID,
            "artifactName": V5_ADOPTION_ARTIFACT_NAME,
            "artifactSize": V5_ADOPTION_ARTIFACT_SIZE,
            "artifactDigest": V5_ADOPTION_ARTIFACT_DIGEST,
        },
    }


def _v5_equivalence_fixture_identity() -> dict[str, Any]:
    return {
        "originalRunId": V5_ORIGINAL_GITHUB_RUN_ID,
        "adoptionRunId": V5_ADOPTION_GITHUB_RUN_ID,
        "adoptionArtifactId": V5_ADOPTION_ARTIFACT_ID,
        "adoptionArtifactDigest": V5_ADOPTION_ARTIFACT_DIGEST,
        "terminalArchiveSha256": V5_TERMINAL_ARCHIVE_SHA256,
        "ledgerSha256": V5_LEDGER_SHA256,
        "entryBankFileSha256": V5_ENTRY_BANK_FILE_SHA256,
        "preregistrationSha256": V5_PREREGISTRATION_SHA256,
        "studyLineageSha256": RUN19_V5_STUDY_LINEAGE_SHA256,
    }


def validate_fresh_v5_recovery_equivalence_evidence(
    evidence: Mapping[str, Any],
    *,
    recovery_implementation_manifest: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate pre-outcome tests, their source bytes, and the exact fixture."""

    normalized = _strict_json_clone(evidence, "V5 recovery equivalence evidence")
    manifest_sha, root, manifest_files = (
        _validate_recovery_implementation_manifest(
            recovery_implementation_manifest
        )
    )
    modules = list(V5_RECOVERY_TEST_MODULES)
    required_files = list(required_fresh_v5_recovery_implementation_files())
    expected_command = ["python", "-m", "unittest", *modules]
    expected_keys = {
        "schema",
        "allRequiredTestsPassed",
        "command",
        "testModules",
        "testSourceSha256",
        "requiredImplementationFiles",
        "recoveryImplementationManifestSha256",
        "processExitCode",
        "stdoutSha256",
        "stderrSha256",
        "completedBeforeRecoveryOutcomeAccess",
        "fixtureIdentity",
    }
    if (
        set(normalized) != expected_keys
        or normalized.get("schema") != V5_RECOVERY_EQUIVALENCE_SCHEMA
        or normalized.get("allRequiredTestsPassed") is not True
        or normalized.get("command") != expected_command
        or normalized.get("testModules") != modules
        or normalized.get("requiredImplementationFiles") != required_files
        or normalized.get("recoveryImplementationManifestSha256")
        != manifest_sha
        or normalized.get("processExitCode") != 0
        or normalized.get("completedBeforeRecoveryOutcomeAccess") is not True
        or normalized.get("fixtureIdentity")
        != _v5_equivalence_fixture_identity()
    ):
        raise PermissionError("V5 recovery equivalence evidence is incomplete")
    for field in ("stdoutSha256", "stderrSha256"):
        digest = normalized.get(field)
        if not isinstance(digest, str) or len(digest) != 64:
            raise PermissionError(
                "V5 recovery equivalence output identity is invalid"
            )
        try:
            int(digest, 16)
        except ValueError as error:
            raise PermissionError(
                "V5 recovery equivalence output identity is invalid"
            ) from error

    expected_source_names = sorted(f"{module}.py" for module in modules)
    source_hashes = normalized.get("testSourceSha256")
    if (
        not isinstance(source_hashes, Mapping)
        or sorted(source_hashes) != expected_source_names
    ):
        raise PermissionError(
            "V5 recovery equivalence test-source set changed"
        )
    for name in expected_source_names:
        source = (root / name).resolve()
        try:
            source.relative_to(root)
        except ValueError as error:
            raise PermissionError(
                "V5 recovery test source escapes the repository"
            ) from error
        if source.is_symlink() or not source.is_file():
            raise PermissionError(
                "V5 recovery equivalence test source is unavailable"
            )
        actual_sha = _file_sha256(source)
        if (
            source_hashes.get(name) != actual_sha
            or manifest_files.get(name) != actual_sha
        ):
            raise PermissionError(
                "V5 recovery equivalence test-source bytes changed"
            )
    return normalized


def run_fresh_v5_recovery_equivalence_preflight(
    repository_root: str | Path,
    recovery_artifact_directory: str | Path,
    *,
    recovery_implementation_manifest: Mapping[str, Any],
) -> dict[str, Any]:
    """Run the exact-fixture suite before appending any recovery record."""

    root = Path(repository_root).expanduser().resolve()
    bundle = load_fresh_v5_recovery_bundle(recovery_artifact_directory)
    _, manifest_root, _ = _validate_recovery_implementation_manifest(
        recovery_implementation_manifest
    )
    if root != manifest_root:
        raise PermissionError(
            "the V5 recovery preflight repository root changed"
        )
    modules = V5_RECOVERY_TEST_MODULES
    sources = tuple(root / f"{module}.py" for module in modules)
    if any(path.is_symlink() or not path.is_file() for path in sources):
        raise PermissionError(
            "V5 recovery equivalence test sources are unavailable"
        )
    command = (sys.executable, "-m", "unittest", *modules)
    environment = dict(os.environ)
    environment.update(
        {
            "FRESH_V5_RECOVERY_ARTIFACT_DIR": str(bundle.directory),
            "FRESH_REQUIRE_V5_RECOVERY_FIXTURE": "1",
        }
    )
    completed = subprocess.run(
        command,
        cwd=root,
        check=False,
        capture_output=True,
        timeout=1800,
        env=environment,
    )
    if completed.returncode != 0:
        sys.stderr.buffer.write(completed.stdout)
        sys.stderr.buffer.write(completed.stderr)
        raise PermissionError("V5 recovery equivalence preflight failed")
    manifest_sha = validate_fresh_implementation_manifest(
        recovery_implementation_manifest,
        verify_current_files=True,
    )
    evidence = {
        "schema": V5_RECOVERY_EQUIVALENCE_SCHEMA,
        "allRequiredTestsPassed": True,
        "command": ["python", "-m", "unittest", *modules],
        "testModules": list(modules),
        "testSourceSha256": {
            path.name: _file_sha256(path) for path in sorted(sources)
        },
        "requiredImplementationFiles": list(
            required_fresh_v5_recovery_implementation_files()
        ),
        "recoveryImplementationManifestSha256": manifest_sha,
        "processExitCode": completed.returncode,
        "stdoutSha256": hashlib.sha256(completed.stdout).hexdigest(),
        "stderrSha256": hashlib.sha256(completed.stderr).hexdigest(),
        "completedBeforeRecoveryOutcomeAccess": True,
        "fixtureIdentity": _v5_equivalence_fixture_identity(),
    }
    return validate_fresh_v5_recovery_equivalence_evidence(
        evidence,
        recovery_implementation_manifest=recovery_implementation_manifest,
    )


def _v5_process_failure_evidence() -> dict[str, Any]:
    return {
        "classification": "exit-137-infrastructure-process-failure",
        "kernelOomConfirmationAvailable": False,
        "remoteExitStatus": 137,
        "discoverySessionsCompletedBeforeTermination": 40,
        "candidateBatchCallbackCompleted": False,
        "candidateBatchResultSealed": False,
        "recomputeDiscoveryFromSessionOrdinal": 1,
        "partialCandidateMetricsRecovered": False,
        "serverLogSha256": V5_RECOVERY_MEMBER_SHA256["server-run.log"],
        "statusFileSha256": V5_RECOVERY_MEMBER_SHA256[
            "remote-exit-status.txt"
        ],
    }


def _v5_permitted_recovery_procedure() -> dict[str, Any]:
    return {
        "mode": "bounded-streaming-full-discovery-recomputation",
        "recomputeFromFirstDiscoverySession": True,
        "reusePartialSessionOrCandidateMetrics": False,
        "retainAllOriginalCandidates": True,
        "candidateCount": 240,
        "candidateDefinitionsChanged": False,
        "thresholdsChanged": False,
        "scoringChanged": False,
        "scoringReductionImplementationChanged": True,
        "scoringEquivalenceVerifiedBeforeOutcomeAccess": True,
        "gatesChanged": False,
        "duplicateTickSemanticsChanged": False,
        "maximumAttempts": 1,
    }


def _v5_recovery_identity(
    *,
    preregistration: Mapping[str, Any],
    split_manifest: Mapping[str, Any],
    recovery_implementation_sha256: str,
    equivalence_evidence: Mapping[str, Any],
) -> dict[str, Any]:
    windows = split_manifest.get("windows")
    source = preregistration.get("sourceBindings")
    if not isinstance(windows, Mapping) or not isinstance(source, Mapping):
        raise PermissionError("V5 recovery source identities are unavailable")
    role_order = (
        "discovery",
        "walk_forward_1",
        "walk_forward_2",
        "walk_forward_3",
        "validation",
        "holdout",
    )
    if any(not isinstance(windows.get(role), Mapping) for role in role_order):
        raise PermissionError("V5 recovery windows are unavailable")
    repeated_quote_policy = preregistration.get("sessionAndData", {}).get(
        "repeatedQuotePolicy"
    )
    if not isinstance(repeated_quote_policy, Mapping):
        raise PermissionError("V5 duplicate-tick policy is unavailable")
    return {
        "studyId": FRESH_V5_STUDY_ID,
        "studyLineageSha256": RUN19_V5_STUDY_LINEAGE_SHA256,
        "scientificSpecificationSha256": V5_SCIENTIFIC_SPECIFICATION_SHA256,
        "splitManifestSha256": V5_SPLIT_MANIFEST_SHA256,
        "researchWindowSetSha256": V5_RESEARCH_WINDOW_SET_SHA256,
        "discoveryWindowSha256": V5_DISCOVERY_WINDOW_SHA256,
        "holdoutWindowSha256": V5_HOLDOUT_WINDOW_SHA256,
        "inventorySha256": V5_INVENTORY_SHA256,
        "corpusManifestSha256": V5_CORPUS_MANIFEST_SHA256,
        "preregistrationSha256": V5_PREREGISTRATION_SHA256,
        "originalImplementationManifestSha256": (
            V5_IMPLEMENTATION_MANIFEST_SHA256
        ),
        "recoveryImplementationManifestSha256": (
            recovery_implementation_sha256
        ),
        "thresholdBankSha256": V5_THRESHOLD_BANK_PAYLOAD_SHA256,
        "quantileBankSha256": V5_QUANTILE_BANK_SHA256,
        "candidateGridSha256": V5_CANDIDATE_GRID_SHA256,
        "eventFilterVariantBankSha256": V5_EVENT_FILTER_BANK_SHA256,
        "exitGridSha256": V5_EXIT_GRID_SHA256,
        "executionScenariosSha256": V5_EXECUTION_SCENARIOS_SHA256,
        "entryBankFileSha256": V5_ENTRY_BANK_FILE_SHA256,
        "orderedCandidateSequenceSha256": (
            V5_ORDERED_CANDIDATE_SEQUENCE_SHA256
        ),
        "candidateCount": 240,
        "scoringDefinitionSha256": canonical_hash(
            {
                "entryDiagnostics": preregistration["entryDiagnostics"],
                "robustnessAndGates": preregistration[
                    "robustnessAndGates"
                ],
            }
        ),
        "executionDefinitionSha256": canonical_hash(
            preregistration["execution"]
        ),
        "duplicateTickPolicySha256": canonical_hash(repeated_quote_policy),
        "equivalenceEvidenceSha256": canonical_hash(equivalence_evidence),
    }


def build_fresh_v5_recovery_contract(
    bundle: FreshV5RecoveryBundle,
    *,
    entry_specs: Sequence[EntryCandidateSpec],
    recovery_implementation_manifest: Mapping[str, Any],
    generated_entry_bank_path: str | Path,
    equivalence_evidence: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Bind the exact V5 batch and the sole mechanical recovery amendment."""

    if not isinstance(bundle, FreshV5RecoveryBundle):
        raise TypeError("bundle must be a FreshV5RecoveryBundle")
    recovery_implementation_sha, _, _ = (
        _validate_recovery_implementation_manifest(
            recovery_implementation_manifest
        )
    )
    evidence = validate_fresh_v5_recovery_equivalence_evidence(
        equivalence_evidence,
        recovery_implementation_manifest=recovery_implementation_manifest,
    )
    threshold_sha = canonical_hash(bundle.quantile_bank)
    if threshold_sha != V5_THRESHOLD_BANK_PAYLOAD_SHA256:
        raise PermissionError("the V5 threshold payload changed")
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
    selected_entry_bank = Path(generated_entry_bank_path).expanduser()
    if selected_entry_bank.is_symlink():
        raise PermissionError(
            "the reconstructed V5 entry bank cannot be a symlink"
        )
    entry_bank_path = selected_entry_bank.resolve()
    if not entry_bank_path.is_file():
        raise PermissionError("the reconstructed V5 entry bank is unavailable")
    entry_bank_file_sha = _file_sha256(entry_bank_path)
    original_batch = bundle.ledger_records[1].get("parameters")
    if (
        not isinstance(original_batch, Mapping)
        or len(candidates) != 240
        or canonical_hash(sequence)
        != V5_ORDERED_CANDIDATE_SEQUENCE_SHA256
        or [item["candidateId"] for item in sequence]
        != original_batch.get("candidateIds")
        or [item["entrySha256"] for item in sequence]
        != original_batch.get("candidateSha256")
        or entry_bank_file_sha != V5_ENTRY_BANK_FILE_SHA256
    ):
        raise PermissionError(
            "the reconstructed V5 candidate or entry-bank identity changed"
        )
    identity = _v5_recovery_identity(
        preregistration=bundle.preregistration,
        split_manifest=bundle.split,
        recovery_implementation_sha256=recovery_implementation_sha,
        equivalence_evidence=evidence,
    )
    audit = {
        "schema": "fresh-xauusd-infrastructure-recovery/v1",
        "recoveryAttemptId": V5_RECOVERY_ATTEMPT_ID,
        "recoveryAttempt": 1,
        "maximumRecoveryAttempts": 1,
        "originalRunId": V5_ORIGINAL_GITHUB_RUN_ID,
        "originalCommitSha": V5_ORIGINAL_GITHUB_COMMIT_SHA,
        "ledgerPrefixSha256": V5_LEDGER_SHA256,
        "originalRecordSha256": list(V5_LEDGER_RECORD_SHA256),
        "candidateOutcomeRecordCount": 0,
        "laterRoleRecordCount": 0,
        "holdoutAuthorizationPresent": False,
        "oomEvidence": _v5_process_failure_evidence(),
        "identity": identity,
        "permittedProcedure": _v5_permitted_recovery_procedure(),
    }
    contract_body = {
        "schema": V5_RECOVERY_CONTRACT_SCHEMA,
        "originalRun": _v5_original_run_identity(),
        "audit": audit,
        "equivalenceEvidence": evidence,
    }
    contract = {
        **contract_body,
        "recoveryContractSha256": canonical_hash(contract_body),
    }
    return audit, contract


def validate_fresh_v5_recovery_for_holdout(
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
    """Prove the completed, exact V5 continuation before holdout access."""

    try:
        validated_preregistration_sha = validate_fresh_preregistration_v5(
            preregistration,
            verify_current_implementation_files=False,
        )
    except (TypeError, ValueError) as error:
        raise PermissionError(
            "the V5 recovery preregistration is invalid"
        ) from error
    if (
        preregistration_sha256 != V5_PREREGISTRATION_SHA256
        or validated_preregistration_sha != V5_PREREGISTRATION_SHA256
        or split_manifest_sha256 != V5_SPLIT_MANIFEST_SHA256
        or split_manifest.get("manifestSha256") != V5_SPLIT_MANIFEST_SHA256
    ):
        raise PermissionError("the V5 recovery lineage identity changed")

    contract = _strict_json_clone(
        recovery_contract,
        "V5 recovery contract",
    )
    claimed_contract_sha = contract.pop("recoveryContractSha256", None)
    if (
        set(contract)
        != {"schema", "originalRun", "audit", "equivalenceEvidence"}
        or contract.get("schema") != V5_RECOVERY_CONTRACT_SCHEMA
        or claimed_contract_sha != canonical_hash(contract)
        or contract.get("originalRun") != _v5_original_run_identity()
    ):
        raise PermissionError("the V5 recovery contract identity changed")
    audit = contract.get("audit")
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
    if (
        not isinstance(audit, Mapping)
        or set(audit) != required_audit_keys
        or audit.get("schema") != "fresh-xauusd-infrastructure-recovery/v1"
        or audit.get("recoveryAttemptId") != V5_RECOVERY_ATTEMPT_ID
        or audit.get("recoveryAttempt") != 1
        or audit.get("maximumRecoveryAttempts") != 1
        or audit.get("originalRunId") != V5_ORIGINAL_GITHUB_RUN_ID
        or audit.get("originalCommitSha") != V5_ORIGINAL_GITHUB_COMMIT_SHA
        or audit.get("ledgerPrefixSha256") != V5_LEDGER_SHA256
        or audit.get("originalRecordSha256")
        != list(V5_LEDGER_RECORD_SHA256)
        or audit.get("candidateOutcomeRecordCount") != 0
        or audit.get("laterRoleRecordCount") != 0
        or audit.get("holdoutAuthorizationPresent") is not False
        or audit.get("oomEvidence") != _v5_process_failure_evidence()
        or audit.get("permittedProcedure")
        != _v5_permitted_recovery_procedure()
    ):
        raise PermissionError("the V5 recovery audit changed")

    recovery_implementation_sha, _, _ = (
        _validate_recovery_implementation_manifest(
            recovery_implementation_manifest
        )
    )
    evidence = validate_fresh_v5_recovery_equivalence_evidence(
        contract["equivalenceEvidence"],
        recovery_implementation_manifest=recovery_implementation_manifest,
    )
    windows = split_manifest.get("windows")
    if not isinstance(windows, Mapping):
        raise PermissionError("the V5 recovery windows are unavailable")
    role_order = (
        "discovery",
        "walk_forward_1",
        "walk_forward_2",
        "walk_forward_3",
        "validation",
        "holdout",
    )
    if any(not isinstance(windows.get(role), Mapping) for role in role_order):
        raise PermissionError("the V5 recovery windows are unavailable")
    if (
        canonical_hash(windows["discovery"]) != V5_DISCOVERY_WINDOW_SHA256
        or canonical_hash(windows["holdout"]) != V5_HOLDOUT_WINDOW_SHA256
        or canonical_hash(
            [canonical_hash(windows[role]) for role in role_order]
        )
        != V5_RESEARCH_WINDOW_SET_SHA256
    ):
        raise PermissionError("the V5 recovery window identity changed")
    expected_identity = _v5_recovery_identity(
        preregistration=preregistration,
        split_manifest=split_manifest,
        recovery_implementation_sha256=recovery_implementation_sha,
        equivalence_evidence=evidence,
    )
    if audit.get("identity") != expected_identity:
        raise PermissionError("the V5 recovery identities changed")

    selected_records = tuple(records)
    for number, record in enumerate(selected_records, start=1):
        if not isinstance(record, Mapping):
            raise PermissionError("V5 recovery ledger records must be objects")
        body = dict(record)
        claimed_number = body.pop("recordNumber", None)
        claimed_sha = body.pop("recordSha256", None)
        if (
            claimed_number != number
            or not isinstance(claimed_sha, str)
            or canonical_hash(body) != claimed_sha
        ):
            raise PermissionError("the V5 recovery ledger chain changed")
    if (
        len(selected_records) < 248
        or [
            record.get("recordSha256")
            for record in selected_records[:2]
        ]
        != list(V5_LEDGER_RECORD_SHA256)
    ):
        raise PermissionError("the original V5 ledger prefix changed")

    original_stage, original_batch_record = selected_records[:2]
    discovery_window_sha = canonical_hash(
        [canonical_hash(windows["discovery"])]
    )
    if discovery_window_sha != V5_DISCOVERY_ACCESS_WINDOW_SHA256:
        raise PermissionError("the V5 discovery access identity changed")
    if (
        original_stage.get("recordNumber") != 1
        or original_stage.get("recordKind") != "stage-window-access"
        or original_stage.get("status") != "window_access_started"
        or original_stage.get("identitySha256")
        != V5_STAGE_ACCESS_IDENTITY_SHA256
        or original_batch_record.get("recordNumber") != 2
        or original_batch_record.get("recordKind") != "batch-window-access"
        or original_batch_record.get("status") != "batch_access_started"
        or original_batch_record.get("identitySha256")
        != V5_BATCH_ACCESS_IDENTITY_SHA256
        or any(
            record.get("stage") != "discovery"
            or record.get("role") != "discovery"
            or record.get("outcomesRevealed") is not True
            or record.get("windowSha256") != discovery_window_sha
            or record.get("preregistrationSha256")
            != V5_PREREGISTRATION_SHA256
            for record in selected_records[:2]
        )
    ):
        raise PermissionError("the original V5 ledger prefix changed")
    original_batch = original_batch_record.get("parameters")
    if not isinstance(original_batch, Mapping):
        raise PermissionError("the original V5 candidate batch is unavailable")
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
                {
                    "candidateId": candidate_id,
                    "entrySha256": entry_sha,
                }
                for candidate_id, entry_sha in zip(
                    candidate_ids,
                    candidate_sha,
                )
            ]
        )
        != V5_ORDERED_CANDIDATE_SEQUENCE_SHA256
    ):
        raise PermissionError("the original V5 candidate order changed")

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
    if (
        tuple(record.get("status") for record in recovery_records)
        != expected_statuses
        or [
            record.get("recordNumber") for record in recovery_records
        ]
        != [3, 4, 5, 6, 7, 248]
    ):
        raise PermissionError("the V5 recovery chain is incomplete")
    for status, record in zip(expected_statuses, recovery_records):
        payload = record.get("parameters")
        if (
            record.get("candidateId")
            != f"protocol-infrastructure-resume::{status}"
            or record.get("family") != "protocol-infrastructure-recovery"
            or record.get("stage") != "discovery"
            or record.get("role") != "discovery"
            or record.get("outcomesRevealed") is not True
            or record.get("gatePassed") is not False
            or record.get("windowSha256") != discovery_window_sha
            or record.get("preregistrationSha256")
            != V5_PREREGISTRATION_SHA256
            or not isinstance(payload, Mapping)
            or payload.get("kind") != "fresh-infrastructure-recovery"
            or payload.get("status") != status
            or payload.get("stage") != "discovery"
            or payload.get("recoveryAttemptId")
            != V5_RECOVERY_ATTEMPT_ID
            or canonical_hash(payload) != record.get("identitySha256")
        ):
            raise PermissionError(
                "a V5 recovery protocol record changed"
            )

    protocol_parameters = [
        record["parameters"].get("parameters")
        for record in recovery_records
    ]
    if any(not isinstance(item, Mapping) for item in protocol_parameters):
        raise PermissionError("the V5 recovery parameters are unavailable")
    if (
        protocol_parameters[0].get("ledgerPrefixSha256")
        != V5_LEDGER_SHA256
        or protocol_parameters[0].get("originalRecordSha256")
        != list(V5_LEDGER_RECORD_SHA256)
        or protocol_parameters[0].get("oomEvidence")
        != _v5_process_failure_evidence()
        or protocol_parameters[0].get("candidateOutcomeRecordCount") != 0
        or protocol_parameters[0].get("laterRoleRecordCount") != 0
        or protocol_parameters[0].get("holdoutAuthorizationPresent")
        is not False
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
        or dict(protocol_parameters[2]) != expected_identity
        or protocol_parameters[3].get(
            "originalBatchAccessRecordNumber"
        )
        != 2
        or protocol_parameters[3].get(
            "originalBatchAccessRecordSha256"
        )
        != V5_LEDGER_RECORD_SHA256[1]
        or protocol_parameters[3].get("candidateCount") != 240
        or protocol_parameters[3].get(
            "orderedCandidateSequenceSha256"
        )
        != V5_ORDERED_CANDIDATE_SEQUENCE_SHA256
    ):
        raise PermissionError(
            "the V5 recovery authorization linkage changed"
        )

    completed_parameters = protocol_parameters[4]
    batch_sha = completed_parameters.get("batchResultSha256")
    batch_file_sha = completed_parameters.get("batchResultFileSha256")
    if (
        not isinstance(batch_sha, str)
        or len(batch_sha) != 64
        or not isinstance(batch_file_sha, str)
        or len(batch_file_sha) != 64
        or completed_parameters.get("candidateCount") != 240
        or completed_parameters.get(
            "orderedCandidateSequenceSha256"
        )
        != V5_ORDERED_CANDIDATE_SEQUENCE_SHA256
    ):
        raise PermissionError(
            "the V5 sealed discovery batch linkage changed"
        )

    discovery_records = [
        record
        for record in selected_records
        if record.get("role") == "discovery"
        and record.get("recordKind")
        not in (
            "batch-window-access",
            "stage-window-access",
            "infrastructure-resume",
        )
    ]
    if (
        len(discovery_records) != 240
        or [
            record.get("recordNumber") for record in discovery_records
        ]
        != list(range(8, 248))
        or [
            record.get("candidateId") for record in discovery_records
        ]
        != candidate_ids
        or [
            record.get("frozenEntrySha256")
            for record in discovery_records
        ]
        != candidate_sha
    ):
        raise PermissionError(
            "the recovered V5 discovery candidate order changed"
        )

    selected_batch_path = Path(sealed_batch_result_path).expanduser()
    if (
        selected_batch_path.is_symlink()
        or selected_batch_path.parent.is_symlink()
    ):
        raise PermissionError("the sealed V5 recovery batch is unavailable")
    batch_path = selected_batch_path.resolve()
    if (
        not batch_path.is_file()
        or _file_sha256(batch_path) != batch_file_sha
    ):
        raise PermissionError("the sealed V5 recovery batch file changed")
    batch_document = _read_json(batch_path, "sealed V5 recovery batch")
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
        or batch_body.get("schema")
        != "fresh-xauusd-recovery-discovery-batch/v1"
        or batch_body.get("recoveryAttemptId")
        != V5_RECOVERY_ATTEMPT_ID
        or batch_body.get("preregistrationSha256")
        != V5_PREREGISTRATION_SHA256
        or batch_body.get("candidateCount") != 240
        or not isinstance(ordered_results, list)
        or len(ordered_results) != 240
    ):
        raise PermissionError(
            "the sealed V5 recovery batch content changed"
        )

    for index, (sealed, ledger_record) in enumerate(
        zip(ordered_results, discovery_records)
    ):
        if not isinstance(sealed, Mapping):
            raise PermissionError("a sealed V5 candidate result is invalid")
        evaluation = sealed.get("evaluation")
        parameters = ledger_record.get("parameters")
        score = (
            evaluation.get("score")
            if isinstance(evaluation, Mapping)
            else None
        )
        if (
            sealed.get("candidateId") != candidate_ids[index]
            or sealed.get("entrySha256") != candidate_sha[index]
            or not isinstance(evaluation, Mapping)
            or set(evaluation)
            != {
                "identitySha256",
                "passed",
                "metrics",
                "leakageChecks",
                "score",
            }
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
            or evaluation.get("identitySha256")
            != candidate_sha[index]
            or evaluation.get("passed")
            != ledger_record.get("gatePassed")
            or evaluation.get("metrics") != ledger_record.get("metrics")
            or evaluation.get("leakageChecks")
            != ledger_record.get("leakageChecks")
            or evaluation.get("score")
            != ledger_record.get("balancedScore")
            or ledger_record.get("status")
            != (
                "passed"
                if evaluation.get("passed")
                else "rejected"
            )
            or ledger_record.get("identitySha256")
            != candidate_sha[index]
            or ledger_record.get("outcomesRevealed") is not True
            or ledger_record.get("windowSha256")
            != discovery_window_sha
            or ledger_record.get("preregistrationSha256")
            != V5_PREREGISTRATION_SHA256
            or not isinstance(parameters, Mapping)
            or parameters.get("recoveryAttemptId")
            != V5_RECOVERY_ATTEMPT_ID
            or parameters.get("sealedBatchResultSha256") != batch_sha
        ):
            raise PermissionError(
                "a sealed V5 candidate result differs from the ledger"
            )

    stage_parameters = protocol_parameters[5]
    promoted_ids = stage_parameters.get("promotedCandidateIds")
    budgets = preregistration.get("candidateSearch", {}).get("budgets", {})
    promotion_limit = (
        budgets.get("walkForward1FrozenCandidates")
        if isinstance(budgets, Mapping)
        else None
    )
    if (
        not isinstance(promotion_limit, int)
        or isinstance(promotion_limit, bool)
    ):
        raise PermissionError(
            "the V5 discovery promotion budget is unavailable"
        )
    passed_results = [
        sealed
        for sealed in ordered_results
        if sealed["evaluation"].get("passed") is True
    ]

    def promotion_key(sealed: Mapping[str, Any]) -> tuple[Any, ...]:
        score = sealed["evaluation"].get("score")
        score_missing = score is None
        numeric_score = (
            float(score) if score is not None else float("-inf")
        )
        return (
            score_missing,
            -numeric_score,
            str(sealed["candidateId"]),
        )

    expected_promoted = [
        str(sealed["candidateId"])
        for sealed in sorted(passed_results, key=promotion_key)[
            :promotion_limit
        ]
    ]
    stage_metrics = recovery_records[5].get("metrics")
    if (
        stage_parameters.get("batchResultSha256") != batch_sha
        or stage_parameters.get("candidateCount") != 240
        or stage_parameters.get("candidateOutcomeRecordCount") != 240
        or promoted_ids != expected_promoted
        or not isinstance(stage_metrics, Mapping)
        or stage_metrics.get("candidateCount") != 240
        or stage_metrics.get("promotedCandidateCount")
        != len(expected_promoted)
        or stage_metrics.get("studyFailed") != (not expected_promoted)
    ):
        raise PermissionError(
            "the completed V5 recovery promotion record changed"
        )

    return {
        "schema": V5_RECOVERY_HOLDOUT_PROOF_SCHEMA,
        "recoveryContractSha256": claimed_contract_sha,
        "recoveryImplementationManifestSha256": (
            recovery_implementation_sha
        ),
        "equivalenceEvidenceSha256": canonical_hash(evidence),
        "sealedBatchResultSha256": batch_sha,
        "sealedBatchResultFileSha256": batch_file_sha,
        "orderedCandidateSequenceSha256": (
            V5_ORDERED_CANDIDATE_SEQUENCE_SHA256
        ),
        "candidateOutcomeRecordCount": 240,
    }


def finalize_interrupted_fresh_v5_recovery(
    ledger_path: str | Path,
    *,
    exit_status: int,
) -> bool:
    """Seal one incomplete V5 recovery attempt after abrupt termination.

    A complete recovery consists of all authorization records, a sealed
    240-candidate batch, all 240 candidate records in original order, and the
    final discovery-stage record. Every valid shorter prefix consumes the sole
    recovery attempt and receives one terminal ``batch_resume_error`` record.
    """

    if not isinstance(exit_status, int) or isinstance(exit_status, bool):
        raise TypeError("exit_status must be an integer")
    selected_ledger = Path(ledger_path).expanduser()
    if selected_ledger.is_symlink():
        raise PermissionError("the durable V5 recovery ledger is unavailable")
    ledger = selected_ledger.resolve()
    if not ledger.is_file():
        raise PermissionError("the durable V5 recovery ledger is unavailable")
    records = list(_load_verified_ledger(ledger))
    if (
        len(records) < 2
        or [
            record.get("recordSha256") for record in records[:2]
        ]
        != list(V5_LEDGER_RECORD_SHA256)
    ):
        raise PermissionError("the original V5 interrupted prefix changed")
    if (
        records[0].get("recordKind") != "stage-window-access"
        or records[0].get("status") != "window_access_started"
        or records[1].get("recordKind") != "batch-window-access"
        or records[1].get("status") != "batch_access_started"
        or records[0].get("stage") != "discovery"
        or records[1].get("stage") != "discovery"
        or records[0].get("windowSha256")
        != V5_DISCOVERY_ACCESS_WINDOW_SHA256
        or records[1].get("windowSha256")
        != V5_DISCOVERY_ACCESS_WINDOW_SHA256
        or records[0].get("preregistrationSha256")
        != V5_PREREGISTRATION_SHA256
        or records[1].get("preregistrationSha256")
        != V5_PREREGISTRATION_SHA256
    ):
        raise PermissionError("the original V5 interrupted prefix changed")
    original_batch = records[1].get("parameters")
    if not isinstance(original_batch, Mapping):
        raise PermissionError(
            "the original V5 discovery batch identity is missing"
        )
    candidate_ids = original_batch.get("candidateIds")
    candidate_sha256 = original_batch.get("candidateSha256")
    if (
        not isinstance(candidate_ids, list)
        or not isinstance(candidate_sha256, list)
        or len(candidate_ids) != 240
        or len(candidate_sha256) != 240
        or len(candidate_ids) != len(set(candidate_ids))
        or canonical_hash(
            [
                {
                    "candidateId": candidate_id,
                    "entrySha256": entry_sha,
                }
                for candidate_id, entry_sha in zip(
                    candidate_ids,
                    candidate_sha256,
                )
            ]
        )
        != V5_ORDERED_CANDIDATE_SEQUENCE_SHA256
    ):
        raise PermissionError(
            "the original V5 discovery candidate sequence changed"
        )

    window_sha = V5_DISCOVERY_ACCESS_WINDOW_SHA256

    def validate_resume_record(
        record: Mapping[str, Any],
        status: str,
    ) -> None:
        payload = record.get("parameters")
        if (
            record.get("recordKind") != "infrastructure-resume"
            or record.get("candidateId")
            != f"protocol-infrastructure-resume::{status}"
            or record.get("family") != "protocol-infrastructure-recovery"
            or record.get("stage") != "discovery"
            or record.get("role") != "discovery"
            or record.get("status") != status
            or record.get("outcomesRevealed") is not True
            or record.get("gatePassed") is not False
            or record.get("windowSha256") != window_sha
            or record.get("preregistrationSha256")
            != V5_PREREGISTRATION_SHA256
            or not isinstance(payload, Mapping)
            or payload.get("kind") != "fresh-infrastructure-recovery"
            or payload.get("status") != status
            or payload.get("stage") != "discovery"
            or payload.get("recoveryAttemptId")
            != V5_RECOVERY_ATTEMPT_ID
            or canonical_hash(payload) != record.get("identitySha256")
        ):
            raise PermissionError(
                f"the {status} V5 recovery record changed"
            )

    tail = records[2:]
    terminal_error: Mapping[str, Any] | None = None
    error_positions = [
        index
        for index, record in enumerate(tail)
        if record.get("status") == "batch_resume_error"
    ]
    if error_positions:
        if error_positions != [len(tail) - 1]:
            raise PermissionError(
                "a V5 recovery error must be the terminal ledger record"
            )
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
            validate_resume_record(
                sequence[position],
                "batch_resume_started",
            )
            position += 1

    if not incomplete:
        if position == len(sequence):
            incomplete = True
        else:
            validate_resume_record(
                sequence[position],
                "batch_resume_completed",
            )
            completed_parameters = sequence[position]["parameters"].get(
                "parameters"
            )
            if not isinstance(completed_parameters, Mapping):
                raise PermissionError(
                    "the sealed V5 recovery batch identity is missing"
                )
            batch_result_sha = completed_parameters.get(
                "batchResultSha256"
            )
            if (
                not isinstance(batch_result_sha, str)
                or len(batch_result_sha) != 64
                or completed_parameters.get("candidateCount")
                != len(candidate_ids)
                or completed_parameters.get(
                    "orderedCandidateSequenceSha256"
                )
                != V5_ORDERED_CANDIDATE_SEQUENCE_SHA256
            ):
                raise PermissionError(
                    "the sealed V5 recovery batch identity changed"
                )
            batch_completed = True
            position += 1

    if batch_completed:
        while (
            position < len(sequence)
            and candidate_outcomes < len(candidate_ids)
        ):
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
                or record.get("preregistrationSha256")
                != V5_PREREGISTRATION_SHA256
                or not isinstance(parameters, Mapping)
                or parameters.get("recoveryAttemptId")
                != V5_RECOVERY_ATTEMPT_ID
                or parameters.get("sealedBatchResultSha256")
                != batch_result_sha
            ):
                raise PermissionError(
                    "the recovered V5 candidate sequence changed"
                )
            candidate_outcomes += 1
            position += 1

        if candidate_outcomes < len(candidate_ids):
            if position != len(sequence):
                raise PermissionError(
                    "the V5 recovery stage completed before every "
                    "candidate outcome"
                )
            incomplete = True
        elif position == len(sequence):
            incomplete = True
        else:
            validate_resume_record(
                sequence[position],
                "resume_stage_completed",
            )
            stage_parameters = sequence[position]["parameters"].get(
                "parameters"
            )
            if (
                not isinstance(stage_parameters, Mapping)
                or stage_parameters.get("batchResultSha256")
                != batch_result_sha
                or stage_parameters.get("candidateCount")
                != len(candidate_ids)
                or stage_parameters.get("candidateOutcomeRecordCount")
                != len(candidate_ids)
            ):
                raise PermissionError(
                    "the completed V5 recovery stage identity changed"
                )
            position += 1
            if any(
                record.get("recordKind") == "infrastructure-resume"
                for record in sequence[position:]
            ):
                raise PermissionError(
                    "the V5 recovery protocol was appended twice"
                )
            incomplete = False

    if terminal_error is not None:
        if not incomplete:
            raise PermissionError(
                "a completed V5 recovery cannot end in a recovery error"
            )
        error_parameters = terminal_error["parameters"].get("parameters")
        if (
            not isinstance(error_parameters, Mapping)
            or error_parameters.get("recoveryAttemptId")
            != V5_RECOVERY_ATTEMPT_ID
            or error_parameters.get("candidateOutcomesAppended")
            != candidate_outcomes
        ):
            raise PermissionError(
                "the terminal V5 recovery error identity changed"
            )
        return False

    if not incomplete:
        return False

    inner = {
        "recoveryAttemptId": V5_RECOVERY_ATTEMPT_ID,
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
        "recoveryAttemptId": V5_RECOVERY_ATTEMPT_ID,
        "parameters": inner,
    }
    append_fresh_record(
        ledger,
        {
            "recordKind": "infrastructure-resume",
            "candidateId": (
                "protocol-infrastructure-resume::batch_resume_error"
            ),
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
            "preregistrationSha256": V5_PREREGISTRATION_SHA256,
        },
    )
    return True


__all__ = [
    "FreshV5RecoveryBundle",
    "V5_ADOPTION_ARTIFACT_DIGEST",
    "V5_ADOPTION_ARTIFACT_ID",
    "V5_ADOPTION_ARTIFACT_NAME",
    "V5_ADOPTION_ARTIFACT_SIZE",
    "V5_ADOPTION_GITHUB_COMMIT_SHA",
    "V5_ADOPTION_GITHUB_JOB_ID",
    "V5_ADOPTION_GITHUB_RUN_ATTEMPT",
    "V5_ADOPTION_GITHUB_RUN_ID",
    "V5_DISCOVERY_WINDOW_SHA256",
    "V5_ENTRY_BANK_FILE_SHA256",
    "V5_IMPLEMENTATION_MANIFEST_SHA256",
    "V5_LEDGER_RECORD_SHA256",
    "V5_LEDGER_SHA256",
    "V5_ORIGINAL_GITHUB_COMMIT_SHA",
    "V5_ORIGINAL_GITHUB_RUN_ATTEMPT",
    "V5_ORIGINAL_GITHUB_RUN_ID",
    "V5_ORDERED_CANDIDATE_SEQUENCE_SHA256",
    "V5_PREREGISTRATION_SHA256",
    "V5_RECOVERY_ATTEMPT_ID",
    "V5_RECOVERY_CONTRACT_SCHEMA",
    "V5_RECOVERY_EQUIVALENCE_SCHEMA",
    "V5_RECOVERY_HOLDOUT_PROOF_SCHEMA",
    "V5_RECOVERY_MEMBER_SHA256",
    "V5_RECOVERY_TEST_MODULES",
    "V5_TERMINAL_ARCHIVE_NAME",
    "V5_TERMINAL_ARCHIVE_SHA256",
    "V5_TERMINAL_ARCHIVE_SIZE",
    "build_fresh_v5_recovery_contract",
    "finalize_interrupted_fresh_v5_recovery",
    "load_fresh_v5_recovery_bundle",
    "required_fresh_v5_recovery_implementation_files",
    "run_fresh_v5_recovery_equivalence_preflight",
    "validate_fresh_v5_recovery_equivalence_evidence",
    "validate_fresh_v5_recovery_for_holdout",
]
