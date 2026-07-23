"""Immutable run-17 evidence for a separate, full-recompute v4 study.

The v3 lineage consumed discovery outcome access before its spool failed with
ENOSPC.  It therefore cannot be resumed.  This module verifies the exact
GitHub artifact and exposes only the six outcome-blind scientific inputs that
may be copied into a separately preregistered v4 study.

No transient spool, partial candidate computation, candidate result, or later
window state is recoverable through this module.  A v4 study must recompute
discovery from session ordinal one.
"""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from datavis.research.fresh_protocol import canonical_hash
from datavis.research.fresh_search import _load_verified_records
from datavis.research.fresh_thresholds import fresh_quantile_bank_from_payload


FRESH_V4_STUDY_ID = "xauusd-fresh-causal-acceleration-v4"
FRESH_V3_STUDY_ID = "xauusd-fresh-causal-acceleration-v3"

RUN17_GITHUB_RUN_ID = 30000411128
RUN17_GITHUB_RUN_ATTEMPT = 1
RUN17_GITHUB_JOB_ID = 89184009635
RUN17_GITHUB_ARTIFACT_ID = 8562091360
RUN17_GITHUB_COMMIT_SHA = "50d3b60da902e86e416669b82922ab4d7436ef32"
RUN17_ARTIFACT_NAME = "fresh-xauusd-30000411128-1"
RUN17_ZIP_SHA256 = (
    "5a54b2bd7670d06234e4f1efab9566dcbf8b4b2a9392fd8238860f5eb0852490"
)
RUN17_ARCHIVE_SHA256 = (
    "13f3c091ecb54d58f1d467d9ce0022617658f80a1a7fa38f4c78c33a9c865ada"
)
RUN17_LEDGER_SHA256 = (
    "222bd02635243ce554ef666db3faf2e5008fd60aac08d92023db69b2fd52ac9f"
)
RUN17_ORDERED_LEDGER_RECORD_SHA256 = (
    "927772f08943c5832bf5e43cec89bfabda0ac6bc5e4fd1200974d65695225e7e",
    "dfe91c3749ac4baddbacac2362ec1177c1ed904f9870082e96365b85f8a73dc6",
    "1f4732107d914124f4cfb481fe05709df5dd20889a9527cfd362faed732f4f1d",
)
RUN17_PREREGISTRATION_SHA256 = (
    "0a40a427f8cbec99f874c7ad8d71e2ac397ab51bf825a6b262a1b5e2c52be3ac"
)
RUN17_IMPLEMENTATION_MANIFEST_SHA256 = (
    "b1284643338625cf026fd314960b7e9c528c0519a97eb4db5865194a5080d0c5"
)
RUN17_PREDECESSOR_STUDY_LINEAGE_SHA256 = (
    "d4f356999d55e2c66f502897c6b696a7f376a1bc5c4fd997f627bafb82805f52"
)
RUN17_STUDY_LINEAGE_SHA256 = (
    "aa894a42147c5b5436490470ea81b630e1d899bd3b079fa800715820c89eb928"
)
RUN17_SCIENTIFIC_SPECIFICATION_SHA256 = (
    "fef6b1a4898aaeb4ce33ad96ea270f0211448357399d94f76051b01c9dabcbd8"
)

# Short aliases are intentionally exported alongside the established GitHub
# names so later protocol code can use either predictable naming convention.
RUN17_RUN_ID = RUN17_GITHUB_RUN_ID
RUN17_ATTEMPT = RUN17_GITHUB_RUN_ATTEMPT
RUN17_JOB_ID = RUN17_GITHUB_JOB_ID
RUN17_ARTIFACT_ID = RUN17_GITHUB_ARTIFACT_ID
RUN17_COMMIT_SHA = RUN17_GITHUB_COMMIT_SHA

_V2_PREREGISTRATION_SHA256 = (
    "209108a553eb186e9048e739981545975bd128528bb1891b28261f09bf1ca2cf"
)
_V2_TERMINAL_LEDGER_SHA256 = (
    "209d80249abd3082df7b50b55c845b71c18401f0c9d2c61a25f5c66e4de28c40"
)
_SPLIT_MANIFEST_SHA256 = (
    "59a0df375a3b8934c14a355a4fc91bb9aade6ada88052d5096c4b9a29e2744bd"
)
_RESEARCH_WINDOW_SET_SHA256 = (
    "0215cfa1ca0954bae1d6eaafbab44a62ddf663a356bb9f7978071afed4595371"
)
_HOLDOUT_WINDOW_SHA256 = (
    "8d599150987e32430a5d012b4973590bda56f7d548c42e7dad9714e2f0fe40b7"
)
_STATE_DIRECTORY = (
    "/home/ec2-user/.local/state/datavis/fresh-xauusd-research-v2"
)

RUN17_INHERITED_SCIENTIFIC_IDENTITIES = {
    "candidateCount": 240,
    "candidateGridSha256": (
        "de4f51a15a32fd64f46e2230c51d4ee80df0a3af3b309cecee1e6b1f712327e6"
    ),
    "corpusManifestSha256": (
        "f24e090b4e149a12a981c4adfbfbc4f68fb57fc9413a0e386b69c53ac3da0c79"
    ),
    "discoveryWindowSha256": (
        "66555bc7a1d991dc9e7cf485d07cafd40c30bab30dd9945a00840500a7518708"
    ),
    "entryBankFileSha256": (
        "7be58142337fc1b440fe61dae3ad0721c5058e4a1eae3dfde7c223bb8021b28c"
    ),
    "equivalenceEvidenceSha256": (
        "90e2aba32bcc7c3de1d72ffb89da00897fd6e8bb35bcb2352ea69852ff0a06dd"
    ),
    "eventFilterVariantBankSha256": (
        "50f0a3a39f008465a6a1d0e9506506e57d072bf608207c842dc72b7a84c5b671"
    ),
    "executionDefinitionSha256": (
        "0d8222fb8f4a3a017fc3ea356984c9c863024a4cfe12ed76a32b93ab33a3f4bd"
    ),
    "executionScenariosSha256": (
        "98355f3b5514d0a5baa8ea3fb441d6aa2b9f2484543884d00344343d3322d2f4"
    ),
    "exitGridSha256": (
        "e9460aabbad5b1e85ec97790a8a62acf0b5bd091a57573d36c3663fae41d1280"
    ),
    "holdoutWindowSha256": _HOLDOUT_WINDOW_SHA256,
    "inventorySha256": (
        "f766d21bfa5a60d6f7b81f5393a24458626cead6069dd8b1719f89de68924d5b"
    ),
    "orderedCandidateSequenceSha256": (
        "d4163395adb43ec49a5f0e10df1fcc82bb698703d2462d735eed5b7ed40ba19c"
    ),
    "originalImplementationManifestSha256": (
        "160f29e8136c967297b16ee438d12fe89ae31ee0901af9293b1949d3193cd094"
    ),
    "preregistrationSha256": _V2_PREREGISTRATION_SHA256,
    "quantileBankSha256": (
        "3243793cdc3d1ee2f7a64b2184a8f91bdf729f2115b06adfb9060dd60a3f78ae"
    ),
    "recoveryImplementationManifestSha256": (
        "6999026509dd88a89b8de03203c8aa732e827c7b9539c74c79531ca5fa1b12f5"
    ),
    "researchWindowSetSha256": _RESEARCH_WINDOW_SET_SHA256,
    "scoringDefinitionSha256": (
        "48794b47baeab7a53420d2567294f891a010ef93bb3e77fa4544796a4afccc25"
    ),
    "splitManifestSha256": _SPLIT_MANIFEST_SHA256,
    "thresholdBankSha256": (
        "9545c7fa7e5c5eef9d64d867f5b9b1b81415e405389471b14e67d1761b1ddf84"
    ),
}

RUN17_MEMBER_FILE_SHA256 = {
    "fresh_corpus_manifest_v1.json": (
        "fe59805f49ed40ae7996bd8333bba6ea2531ce67c04e904f4f228ea01a54dec2"
    ),
    "fresh_entry_bank_v1.json": (
        "7be58142337fc1b440fe61dae3ad0721c5058e4a1eae3dfde7c223bb8021b28c"
    ),
    "fresh_experiment_ledger_v1.jsonl": RUN17_LEDGER_SHA256,
    "fresh_implementation_manifest_v1.json": (
        "7a7ba50d4cb7edee6b4e5f8fc99b8de8035847cb8e5264a95e757785f1e5bfbd"
    ),
    "fresh_preregistration_v3.json": (
        "f4a8d9b6f231ff717d14a227c273c084b27d8c68ec9eaab7cdac1048d6688d30"
    ),
    "fresh_quantile_bank_v1.json": (
        "5076a373f6cfc25a6a37e8a63b90eb4633282b425021b55879cb193bb76bab46"
    ),
    "fresh_research_state_binding_v2.json": (
        "2b6d637cf3761ba14e8f5b724cc640b4840e3c2b861d5b13051ea6b67121632e"
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
        "209d80249abd3082df7b50b55c845b71c18401f0c9d2c61a25f5c66e4de28c40"
    ),
    "predecessor_fresh_implementation_manifest_v1.json": (
        "bc304741db8649da2bca478cafb6f94acc85a25b325a7cbd91f97feb2ef64503"
    ),
    "predecessor_fresh_preregistration_v2.json": (
        "dd00fbc7670319906bfe4a474232dc8cdac08c918734d960367175745c6d07b3"
    ),
    "predecessor_fresh_research_state_binding_v1.json": (
        "42fb4adfcb0da4a323c0d6dc04e6525a6c2ba7fb88c78946adce629107f4d84a"
    ),
    "remote-exit-status.txt": (
        "4355a46b19d348dc2f57c046f8ef63d4538ebb936000f3c9ee954a27460dd865"
    ),
    "server-run.log": (
        "01c32540fee6e77d2f73d79b64bb1912cf7c8007e096cbbe6f41485af513a43f"
    ),
}

RUN17_REUSED_OUTCOME_BLIND_FILE_SHA256 = {
    name: RUN17_MEMBER_FILE_SHA256[name]
    for name in (
        "fresh_source_inventory_v1.json",
        "fresh_corpus_manifest_v1.json",
        "fresh_split_manifest_v2.json",
        "fresh_quantile_bank_v1.json",
        "fresh_threshold_domain_preflight_v1.json",
        "fresh_entry_bank_v1.json",
    )
}

_EXPECTED_DISCOVERY_ANCHORS = (
    "2026-01-02",
    "2026-01-05",
    "2026-01-06",
    "2026-01-07",
    "2026-01-08",
    "2026-01-09",
    "2026-01-12",
    "2026-01-14",
    "2026-01-15",
    "2026-01-16",
)


@dataclass(frozen=True, slots=True)
class FreshV4InfrastructureRestartBundle:
    """Verified outcome-blind inputs and terminal v3 evidence."""

    root: Path
    paths: Mapping[str, Path]
    inventory: Mapping[str, Any]
    corpus: Mapping[str, Any]
    split: Mapping[str, Any]
    predecessor_preregistration: Mapping[str, Any]
    predecessor_implementation_manifest: Mapping[str, Any]
    predecessor_state_binding: Mapping[str, Any]
    quantile_bank: Mapping[str, Any]
    threshold_preflight: Mapping[str, Any]
    entry_bank: Mapping[str, Any]
    ledger_records: tuple[Mapping[str, Any], ...]
    provenance: Mapping[str, Any]


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _object_pairs(pairs):
    output = {}
    for key, value in pairs:
        if key in output:
            raise ValueError(f"duplicate JSON key: {key}")
        output[key] = value
    return output


def _read_json(path: Path) -> Mapping[str, Any]:
    with path.open("r", encoding="utf-8") as source:
        value = json.load(source, object_pairs_hook=_object_pairs)
    if not isinstance(value, Mapping):
        raise ValueError(f"{path.name} must contain a JSON object")
    return value


def _require_exact_mapping(
    actual: Any,
    expected: Mapping[str, Any],
    label: str,
) -> None:
    if not isinstance(actual, Mapping) or dict(actual) != dict(expected):
        raise PermissionError(f"run-17 {label} changed")


def canonical_fresh_v4_study_lineage() -> dict[str, Any]:
    """Return the fresh copy of the exact v4 lineage identity."""

    lineage = {
        "schema": "fresh-xauusd-study-lineage/v1",
        "studyId": FRESH_V4_STUDY_ID,
        "predecessorStudyId": FRESH_V3_STUDY_ID,
        "predecessorPreregistrationSha256": RUN17_PREREGISTRATION_SHA256,
        "predecessorTerminalLedgerSha256": RUN17_LEDGER_SHA256,
        "splitManifestSha256": _SPLIT_MANIFEST_SHA256,
        "researchWindowSetSha256": _RESEARCH_WINDOW_SET_SHA256,
        "scientificSpecificationSha256": (
            RUN17_SCIENTIFIC_SPECIFICATION_SHA256
        ),
    }
    if canonical_hash(lineage) != RUN17_STUDY_LINEAGE_SHA256:
        raise RuntimeError("the registered v4 study lineage identity changed")
    return lineage


def _fresh_v4_restart_provenance_body() -> dict[str, Any]:
    lineage = canonical_fresh_v4_study_lineage()
    return {
        "schema": "fresh-xauusd-infrastructure-restart/v2",
        "classification": "new-study-after-terminal-infrastructure-failure",
        "studyId": FRESH_V4_STUDY_ID,
        "studyLineage": lineage,
        "studyLineageSha256": RUN17_STUDY_LINEAGE_SHA256,
        "predecessorStudyId": FRESH_V3_STUDY_ID,
        "predecessorRunId": RUN17_GITHUB_RUN_ID,
        "predecessorRunAttempt": RUN17_GITHUB_RUN_ATTEMPT,
        "predecessorJobId": RUN17_GITHUB_JOB_ID,
        "predecessorArtifactId": RUN17_GITHUB_ARTIFACT_ID,
        "predecessorCommitSha": RUN17_GITHUB_COMMIT_SHA,
        "predecessorArtifactName": RUN17_ARTIFACT_NAME,
        "predecessorZipSha256": RUN17_ZIP_SHA256,
        "predecessorArchiveSha256": RUN17_ARCHIVE_SHA256,
        "predecessorArtifactMemberFileSha256": dict(
            RUN17_MEMBER_FILE_SHA256
        ),
        "predecessorLedgerSha256": RUN17_LEDGER_SHA256,
        "predecessorOrderedLedgerRecordSha256": list(
            RUN17_ORDERED_LEDGER_RECORD_SHA256
        ),
        "predecessorPreregistrationSha256": (
            RUN17_PREREGISTRATION_SHA256
        ),
        "predecessorImplementationManifestSha256": (
            RUN17_IMPLEMENTATION_MANIFEST_SHA256
        ),
        "predecessorStateBindingSha256": (
            RUN17_MEMBER_FILE_SHA256[
                "fresh_research_state_binding_v2.json"
            ]
        ),
        "predecessorStudyLineageSha256": (
            RUN17_PREDECESSOR_STUDY_LINEAGE_SHA256
        ),
        "predecessorExitStatus": 1,
        "predecessorFailure": {
            "errorType": "OSError",
            "errno": 28,
            "message": "No space left on device",
            "operation": "fresh-spool-write",
        },
        "candidateOutcomeRecordCount": 0,
        "laterWindowOutcomeRecordCount": 0,
        "batchResultSealed": False,
        "completedDiscoverySessionCount": 10,
        "holdoutAuthorizationPresent": False,
        "predecessorLineageTerminal": True,
        "transientSpoolsRecovered": False,
        "transientCandidateComputationsRecovered": False,
        "partialCandidateResultsImported": False,
        "restartPolicy": {
            "mode": "separate-study-full-discovery-recomputation",
            "discardTransientSpools": True,
            "discardPartialCandidateComputations": True,
            "importCandidateResults": False,
            "recomputeFromDiscoverySessionOrdinal": 1,
            "reuseOutcomeBlindInputsOnly": True,
            "automaticResumeOfPredecessor": False,
        },
        "inheritedScientificIdentities": dict(
            RUN17_INHERITED_SCIENTIFIC_IDENTITIES
        ),
        "reusedOutcomeBlindInputs": dict(
            RUN17_REUSED_OUTCOME_BLIND_FILE_SHA256
        ),
        "scientificSpecificationSha256": (
            RUN17_SCIENTIFIC_SPECIFICATION_SHA256
        ),
        "scientificDefinitionsChanged": False,
        "permittedChange": (
            "infrastructure-only-with-full-discovery-recomputation"
        ),
    }


RUN17_V4_PROVENANCE_SHA256 = (
    "6c8d7eade6553209351e7117249c78d256c6dcd93a67aeac5b5ad19c01557237"
)


def canonical_fresh_v4_restart_provenance() -> dict[str, Any]:
    """Return an independent, canonically hashed expected provenance value."""

    body = _fresh_v4_restart_provenance_body()
    digest = canonical_hash(body)
    if digest != RUN17_V4_PROVENANCE_SHA256:
        raise RuntimeError("the registered v4 restart provenance changed")
    return {**copy.deepcopy(body), "provenanceSha256": digest}


def _validate_run17_ledger(
    records: tuple[Mapping[str, Any], ...],
) -> None:
    expected_kinds = (
        "stage-window-access",
        "batch-window-access",
        "batch-window-access",
    )
    expected_statuses = (
        "window_access_started",
        "batch_access_started",
        "batch_access_error",
    )
    expected_candidates = (
        "protocol-stage-access::discovery",
        "protocol-batch-access::entry::discovery",
        "protocol-batch-access::entry::discovery",
    )
    if (
        tuple(record.get("recordNumber") for record in records) != (1, 2, 3)
        or tuple(record.get("recordSha256") for record in records)
        != RUN17_ORDERED_LEDGER_RECORD_SHA256
        or tuple(record.get("recordKind") for record in records)
        != expected_kinds
        or tuple(record.get("status") for record in records)
        != expected_statuses
        or tuple(record.get("candidateId") for record in records)
        != expected_candidates
        or any(record.get("role") != "discovery" for record in records)
        or any(record.get("stage") != "discovery" for record in records)
        or any(record.get("outcomesRevealed") is not True for record in records)
        or any(record.get("gatePassed") is not False for record in records)
        or any(
            record.get("preregistrationSha256")
            != RUN17_PREREGISTRATION_SHA256
            for record in records
        )
    ):
        raise PermissionError("run-17 terminal ledger sequence changed")

    if (
        records[1].get("metrics")
        != {"candidateCount": 240, "errorType": None}
        or records[2].get("metrics")
        != {"candidateCount": 240, "errorType": "OSError"}
        or records[1].get("leakageChecks")
        != {
            "callbackCompleted": False,
            "callbackErrored": False,
            "durableBeforeCallback": True,
        }
        or records[2].get("leakageChecks")
        != {
            "callbackCompleted": False,
            "callbackErrored": True,
            "durableBeforeCallback": False,
        }
    ):
        raise PermissionError("run-17 batch failure evidence changed")

    protocol_kinds = {"stage-window-access", "batch-window-access"}
    candidate_outcomes = [
        record
        for record in records
        if record.get("recordKind") not in protocol_kinds
    ]
    later_roles = [
        record
        for record in records
        if record.get("role") != "discovery"
        or record.get("stage") != "discovery"
    ]
    sealed_batches = [
        record
        for record in records
        if record.get("status") == "batch_access_completed"
    ]
    holdout = [
        record
        for record in records
        if "holdout"
        in {
            record.get("role"),
            record.get("stage"),
            record.get("evaluationWindow"),
        }
    ]
    if candidate_outcomes or later_roles or sealed_batches or holdout:
        raise PermissionError("run-17 artifact contains forbidden outcomes")


def _validate_run17_progress_log(path: Path) -> None:
    lines = path.read_text(encoding="utf-8").splitlines()
    if len(lines) < 12:
        raise PermissionError("run-17 progress/terminal evidence changed")
    rows = []
    for line in lines[:10]:
        try:
            row = json.loads(line, object_pairs_hook=_object_pairs)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise PermissionError(
                "run-17 discovery progress evidence changed"
            ) from exc
        if not isinstance(row, Mapping):
            raise PermissionError("run-17 discovery progress evidence changed")
        rows.append(row)
    expected_rows = tuple(
        {
            "sessionAnchor": anchor,
            "sessionCount": 40,
            "sessionOrdinal": ordinal,
            "stage": "discovery",
        }
        for ordinal, anchor in enumerate(
            _EXPECTED_DISCOVERY_ANCHORS,
            start=1,
        )
    )
    if tuple(dict(row) for row in rows) != expected_rows:
        raise PermissionError("run-17 discovery progress evidence changed")

    traceback_lines = lines[10:]
    required_frames = (
        "Traceback (most recent call last):",
        "fresh_pipeline_cli.py",
        "run_v3_discovery",
        "score_entries_batch",
        "_entry_session_spool",
        "_append_entry_session_to_spool",
        "fresh_spool.py",
    )
    if (
        traceback_lines[0] != required_frames[0]
        or traceback_lines[-1]
        != "OSError: [Errno 28] No space left on device"
        or any(
            not any(fragment in line for line in traceback_lines)
            for fragment in required_frames[1:]
        )
    ):
        raise PermissionError("run-17 ENOSPC terminal evidence changed")


def _expected_v3_state_binding() -> dict[str, Any]:
    predecessor_ledger = (
        f"{_STATE_DIRECTORY}/studies/{_RESEARCH_WINDOW_SET_SHA256}/"
        "fresh_experiment_ledger_v1.jsonl"
    )
    ledger = (
        f"{_STATE_DIRECTORY}/studies/{_RESEARCH_WINDOW_SET_SHA256}/"
        f"lineages/{RUN17_PREDECESSOR_STUDY_LINEAGE_SHA256}/"
        "fresh_experiment_ledger_v1.jsonl"
    )
    holdout = (
        f"{_STATE_DIRECTORY}/holdouts/{_HOLDOUT_WINDOW_SHA256}/"
        "fresh_holdout_authorization_v1.json"
    )
    lineage = {
        "schema": "fresh-xauusd-study-lineage/v1",
        "studyId": FRESH_V3_STUDY_ID,
        "predecessorStudyId": "xauusd-fresh-causal-acceleration-v2",
        "predecessorPreregistrationSha256": _V2_PREREGISTRATION_SHA256,
        "predecessorTerminalLedgerSha256": _V2_TERMINAL_LEDGER_SHA256,
        "splitManifestSha256": _SPLIT_MANIFEST_SHA256,
        "researchWindowSetSha256": _RESEARCH_WINDOW_SET_SHA256,
        "scientificSpecificationSha256": (
            RUN17_SCIENTIFIC_SPECIFICATION_SHA256
        ),
    }
    return {
        "schema": "fresh-xauusd-durable-research-state/v2",
        "studyId": FRESH_V3_STUDY_ID,
        "stateDirectory": _STATE_DIRECTORY,
        "splitManifestSha256": _SPLIT_MANIFEST_SHA256,
        "researchWindowSetSha256": _RESEARCH_WINDOW_SET_SHA256,
        "holdoutWindowSha256": _HOLDOUT_WINDOW_SHA256,
        "scientificSpecificationSha256": (
            RUN17_SCIENTIFIC_SPECIFICATION_SHA256
        ),
        "predecessorStudyId": "xauusd-fresh-causal-acceleration-v2",
        "predecessorPreregistrationSha256": _V2_PREREGISTRATION_SHA256,
        "predecessorTerminalLedgerSha256": _V2_TERMINAL_LEDGER_SHA256,
        "studyLineage": lineage,
        "studyLineageSha256": RUN17_PREDECESSOR_STUDY_LINEAGE_SHA256,
        "experimentLedgerPath": ledger,
        "predecessorExperimentLedgerPath": predecessor_ledger,
        "holdoutAuthorizationRegistryPath": holdout,
    }


def _validate_run17_scientific_evidence(
    *,
    inventory: Mapping[str, Any],
    corpus: Mapping[str, Any],
    split: Mapping[str, Any],
    predecessor_preregistration: Mapping[str, Any],
    predecessor_implementation_manifest: Mapping[str, Any],
    predecessor_state_binding: Mapping[str, Any],
    quantile_bank: Mapping[str, Any],
    threshold_preflight: Mapping[str, Any],
    entry_bank: Mapping[str, Any],
) -> None:
    identity = RUN17_INHERITED_SCIENTIFIC_IDENTITIES
    source = predecessor_preregistration.get("sourceBindings")
    restart = predecessor_preregistration.get("infrastructureRestart")
    if not isinstance(source, Mapping) or not isinstance(restart, Mapping):
        raise PermissionError("run-17 v3 preregistration evidence changed")
    if (
        predecessor_preregistration.get("schema")
        != "fresh-xauusd-acceleration-preregistration/v3"
        or predecessor_preregistration.get("studyId") != FRESH_V3_STUDY_ID
        or predecessor_preregistration.get("preregistrationSha256")
        != RUN17_PREREGISTRATION_SHA256
        or predecessor_implementation_manifest.get("manifestSha256")
        != RUN17_IMPLEMENTATION_MANIFEST_SHA256
        or source.get("implementationManifestSha256")
        != RUN17_IMPLEMENTATION_MANIFEST_SHA256
        or restart.get("scientificSpecificationSha256")
        != RUN17_SCIENTIFIC_SPECIFICATION_SHA256
        or restart.get("inheritedScientificIdentities") != identity
        or restart.get("reusedOutcomeBlindInputs")
        != RUN17_REUSED_OUTCOME_BLIND_FILE_SHA256
        or restart.get("candidateOutcomeRecordCount") != 0
        or restart.get("laterWindowOutcomeRecordCount") != 0
        or restart.get("batchResultSealed") is not False
        or restart.get("holdoutAuthorizationPresent") is not False
    ):
        raise PermissionError("run-17 v3 protocol identity changed")

    _require_exact_mapping(
        predecessor_state_binding,
        _expected_v3_state_binding(),
        "v3 durable state binding",
    )
    state = _expected_v3_state_binding()
    if (
        source.get("experimentLedgerPath")
        != state["experimentLedgerPath"]
        or source.get("holdoutAuthorizationRegistryPath")
        != state["holdoutAuthorizationRegistryPath"]
    ):
        raise PermissionError("run-17 v3 durable path binding changed")

    if (
        inventory.get("inventorySha256") != identity["inventorySha256"]
        or corpus.get("inventorySha256") != identity["inventorySha256"]
        or corpus.get("corpusManifestSha256")
        != identity["corpusManifestSha256"]
        or split.get("inventorySha256") != identity["inventorySha256"]
        or split.get("manifestSha256") != identity["splitManifestSha256"]
        or source.get("inventorySha256") != identity["inventorySha256"]
        or source.get("corpusManifestSha256")
        != identity["corpusManifestSha256"]
        or source.get("splitManifestSha256")
        != identity["splitManifestSha256"]
        or quantile_bank.get("bankSha256")
        != identity["quantileBankSha256"]
        or entry_bank.get("candidateCount") != identity["candidateCount"]
        or entry_bank.get("candidateGridSha256")
        != identity["candidateGridSha256"]
        or entry_bank.get("filterVariantBankSha256")
        != identity["eventFilterVariantBankSha256"]
        or entry_bank.get("quantileBankSha256")
        != identity["quantileBankSha256"]
    ):
        raise PermissionError("run-17 frozen scientific inputs changed")

    preflight_expected = {
        "schema": "fresh-xauusd-threshold-domain-preflight/v1",
        "allRegisteredThresholdDomainsResolved": True,
        "baseCandidateCount": 93,
        "candidateGridSha256": identity["candidateGridSha256"],
        "eventFilterVariantBankSha256": identity[
            "eventFilterVariantBankSha256"
        ],
        "eventFilterVariantCount": 147,
        "executionScenariosSha256": identity["executionScenariosSha256"],
        "exitGridSha256": identity["exitGridSha256"],
        "exitVariantCount": 72,
        "quantileBankSha256": identity["quantileBankSha256"],
        "totalRuntimeEntryCount": identity["candidateCount"],
    }
    _require_exact_mapping(
        threshold_preflight,
        preflight_expected,
        "threshold-domain preflight",
    )


def load_fresh_v4_restart_bundle(
    artifact_directory: str | Path,
    *,
    github_zip_sha256: str = RUN17_ZIP_SHA256,
    nested_tgz_sha256: str = RUN17_ARCHIVE_SHA256,
) -> FreshV4InfrastructureRestartBundle:
    """Verify the terminal run-17 payload for a separate v4 study.

    The two envelope digests are required to remain the audited GitHub ZIP and
    its single nested TGZ identity.  The flat payload then has to contain the
    exact 16 regular files recorded above.
    """

    if github_zip_sha256 != RUN17_ZIP_SHA256:
        raise PermissionError("run-17 GitHub ZIP identity changed")
    if nested_tgz_sha256 != RUN17_ARCHIVE_SHA256:
        raise PermissionError("run-17 nested TGZ identity changed")

    selected = Path(artifact_directory).expanduser()
    if selected.is_symlink():
        raise PermissionError("restart artifact directory cannot be a symlink")
    root = selected.resolve()
    if not root.is_dir():
        raise NotADirectoryError(root)
    children = tuple(root.iterdir())
    if any(path.is_symlink() or not path.is_file() for path in children):
        raise PermissionError("restart artifact may contain only regular files")
    if {path.name for path in children} != set(RUN17_MEMBER_FILE_SHA256):
        raise PermissionError("run-17 artifact member set changed")
    paths = {path.name: path for path in children}
    for name, expected in RUN17_MEMBER_FILE_SHA256.items():
        if _file_sha256(paths[name]) != expected:
            raise PermissionError(f"run-17 artifact file changed: {name}")

    if paths["remote-exit-status.txt"].read_bytes() != b"1\n":
        raise PermissionError("run-17 terminal exit status changed")
    if "fresh_holdout_authorization_v1.json" in paths:
        raise PermissionError("run-17 holdout authorization was present")
    _validate_run17_progress_log(paths["server-run.log"])
    records = tuple(
        _load_verified_records(paths["fresh_experiment_ledger_v1.jsonl"])
    )
    _validate_run17_ledger(records)

    inventory = _read_json(paths["fresh_source_inventory_v1.json"])
    corpus = _read_json(paths["fresh_corpus_manifest_v1.json"])
    split = _read_json(paths["fresh_split_manifest_v2.json"])
    predecessor_preregistration = _read_json(
        paths["fresh_preregistration_v3.json"]
    )
    predecessor_implementation_manifest = _read_json(
        paths["fresh_implementation_manifest_v1.json"]
    )
    predecessor_state_binding = _read_json(
        paths["fresh_research_state_binding_v2.json"]
    )

    # Imported lazily because a future v4 preregistration module may import
    # this provenance builder and must not create an import cycle.
    from datavis.research.fresh_preregistration import (  # noqa: PLC0415
        validate_fresh_implementation_manifest,
        validate_fresh_preregistration_v3,
    )

    registered_preregistration = validate_fresh_preregistration_v3(
        predecessor_preregistration,
        verify_current_implementation_files=False,
    )
    if registered_preregistration != RUN17_PREREGISTRATION_SHA256:
        raise PermissionError("run-17 v3 preregistration identity changed")
    registered_implementation = validate_fresh_implementation_manifest(
        predecessor_implementation_manifest,
        verify_current_files=False,
    )
    if registered_implementation != RUN17_IMPLEMENTATION_MANIFEST_SHA256:
        raise PermissionError("run-17 implementation identity changed")

    quantile_bank = _read_json(paths["fresh_quantile_bank_v1.json"])
    fresh_quantile_bank_from_payload(quantile_bank)
    threshold_preflight = _read_json(
        paths["fresh_threshold_domain_preflight_v1.json"]
    )
    entry_bank = _read_json(paths["fresh_entry_bank_v1.json"])
    _validate_run17_scientific_evidence(
        inventory=inventory,
        corpus=corpus,
        split=split,
        predecessor_preregistration=predecessor_preregistration,
        predecessor_implementation_manifest=(
            predecessor_implementation_manifest
        ),
        predecessor_state_binding=predecessor_state_binding,
        quantile_bank=quantile_bank,
        threshold_preflight=threshold_preflight,
        entry_bank=entry_bank,
    )

    provenance = canonical_fresh_v4_restart_provenance()
    return FreshV4InfrastructureRestartBundle(
        root=root,
        paths=paths,
        inventory=inventory,
        corpus=corpus,
        split=split,
        predecessor_preregistration=predecessor_preregistration,
        predecessor_implementation_manifest=(
            predecessor_implementation_manifest
        ),
        predecessor_state_binding=predecessor_state_binding,
        quantile_bank=quantile_bank,
        threshold_preflight=threshold_preflight,
        entry_bank=entry_bank,
        ledger_records=records,
        provenance=provenance,
    )


__all__ = [
    "FRESH_V4_STUDY_ID",
    "FreshV4InfrastructureRestartBundle",
    "RUN17_ARCHIVE_SHA256",
    "RUN17_ARTIFACT_ID",
    "RUN17_ARTIFACT_NAME",
    "RUN17_ATTEMPT",
    "RUN17_COMMIT_SHA",
    "RUN17_GITHUB_ARTIFACT_ID",
    "RUN17_GITHUB_COMMIT_SHA",
    "RUN17_GITHUB_JOB_ID",
    "RUN17_GITHUB_RUN_ATTEMPT",
    "RUN17_GITHUB_RUN_ID",
    "RUN17_IMPLEMENTATION_MANIFEST_SHA256",
    "RUN17_INHERITED_SCIENTIFIC_IDENTITIES",
    "RUN17_JOB_ID",
    "RUN17_LEDGER_SHA256",
    "RUN17_MEMBER_FILE_SHA256",
    "RUN17_ORDERED_LEDGER_RECORD_SHA256",
    "RUN17_PREREGISTRATION_SHA256",
    "RUN17_REUSED_OUTCOME_BLIND_FILE_SHA256",
    "RUN17_RUN_ID",
    "RUN17_SCIENTIFIC_SPECIFICATION_SHA256",
    "RUN17_STUDY_LINEAGE_SHA256",
    "RUN17_V4_PROVENANCE_SHA256",
    "RUN17_ZIP_SHA256",
    "canonical_fresh_v4_restart_provenance",
    "canonical_fresh_v4_study_lineage",
    "load_fresh_v4_restart_bundle",
]
