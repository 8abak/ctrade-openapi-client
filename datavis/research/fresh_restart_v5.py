"""Immutable Run-19 evidence for a separate, full-recompute v5 study.

The v4 lineage reached discovery session 37 before its detached stdout pipe
failed.  Candidate outcomes were never durably recorded and the discovery
batch was never sealed, so the v4 lineage cannot be resumed.  This module
verifies the adopted terminal artifact and exposes only the same six
outcome-blind scientific inputs to a separately preregistered v5 study.

No transient spool, partial candidate computation, candidate result, later
window state, or holdout state is recoverable through this module.  V5 must
recompute discovery from session ordinal one.
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


FRESH_V5_STUDY_ID = "xauusd-fresh-causal-acceleration-v5"
FRESH_V4_STUDY_ID = "xauusd-fresh-causal-acceleration-v4"

RUN19_GITHUB_RUN_ID = 30042880650
RUN19_GITHUB_RUN_ATTEMPT = 1
RUN19_GITHUB_JOB_ID = 89326866829
RUN19_GITHUB_COMMIT_SHA = "48ef503cbb01d53629bd1156b5d95e1396b412fb"
RUN19_ADOPTION_GITHUB_RUN_ID = 30065029441
RUN19_ADOPTION_GITHUB_RUN_ATTEMPT = 1
RUN19_ADOPTION_GITHUB_ARTIFACT_ID = 8585919266
RUN19_ADOPTION_ARTIFACT_NAME = (
    "fresh-xauusd-run19-adopted-30065029441-1"
)
RUN19_TERMINAL_ARCHIVE_NAME = "fresh-xauusd-30042880650-1.tgz"
RUN19_EXTRACTED_TERMINAL_RELATIVE_PATH = (
    "artifacts/run19-adopted-30065029441-1/terminal"
)
RUN19_ZIP_SHA256 = (
    "7bd36760bda3fd4250be3221d144be4f3a4f0f7b94b7d445f5c1278796b33a1c"
)
RUN19_ARCHIVE_SHA256 = (
    "f947348d892d1c996df15188c3221595066c019957f4dccf24697502d2d4fbf9"
)
RUN19_LEDGER_SHA256 = (
    "ac627bd986c044b12049f717eb3fc664321c08c169fd6a829a5fc8d51144c7b4"
)
RUN19_ORDERED_LEDGER_RECORD_SHA256 = (
    "cb0aee3ff8c882ed78e46e4b9dfc6d09dba2f61847531683d8f59e494d38175f",
    "ab58a3a9cff1eb6415626a1e72175a98e553a30a1c919a3a239f0605cbbb5282",
    "7b1087954533d9d6220f20338f03b19d35720eb06da5ac1a3508fb6341fa4c01",
)
RUN19_PREREGISTRATION_SHA256 = (
    "27fa61d18a42249959c2f9a0b3a2392a8e9140d24e6afc7d712d2afd7e190bd2"
)
RUN19_IMPLEMENTATION_MANIFEST_SHA256 = (
    "7c55e172d80fd8582f1ff691ce84e8f8ce171d04f0d7796f8671ba742180e126"
)
RUN19_PREDECESSOR_STUDY_LINEAGE_SHA256 = (
    "aa894a42147c5b5436490470ea81b630e1d899bd3b079fa800715820c89eb928"
)
RUN19_V4_STUDY_LINEAGE_SHA256 = (
    RUN19_PREDECESSOR_STUDY_LINEAGE_SHA256
)
RUN19_SCIENTIFIC_SPECIFICATION_SHA256 = (
    "fef6b1a4898aaeb4ce33ad96ea270f0211448357399d94f76051b01c9dabcbd8"
)
RUN19_V5_STUDY_LINEAGE_SHA256 = (
    "6377d53891675e02b645bf83b52b24b5ffb5a7b8cc76b701cd3450b2cecd7473"
)

# Predictable aliases for downstream preregistration and launch code.
RUN19_RUN_ID = RUN19_GITHUB_RUN_ID
RUN19_ATTEMPT = RUN19_GITHUB_RUN_ATTEMPT
RUN19_JOB_ID = RUN19_GITHUB_JOB_ID
RUN19_COMMIT_SHA = RUN19_GITHUB_COMMIT_SHA
RUN19_ARTIFACT_ID = RUN19_ADOPTION_GITHUB_ARTIFACT_ID
RUN19_ARTIFACT_NAME = RUN19_ADOPTION_ARTIFACT_NAME
# This v4-style name identifies the terminal predecessor lineage.  The new
# lineage has the explicit RUN19_V5_STUDY_LINEAGE_SHA256 name above.
RUN19_STUDY_LINEAGE_SHA256 = RUN19_PREDECESSOR_STUDY_LINEAGE_SHA256

_V3_STUDY_ID = "xauusd-fresh-causal-acceleration-v3"
_V3_PREREGISTRATION_SHA256 = (
    "0a40a427f8cbec99f874c7ad8d71e2ac397ab51bf825a6b262a1b5e2c52be3ac"
)
_V3_TERMINAL_LEDGER_SHA256 = (
    "222bd02635243ce554ef666db3faf2e5008fd60aac08d92023db69b2fd52ac9f"
)
_V3_STUDY_LINEAGE_SHA256 = (
    "d4f356999d55e2c66f502897c6b696a7f376a1bc5c4fd997f627bafb82805f52"
)
_V4_RESTART_PROVENANCE_SHA256 = (
    "6c8d7eade6553209351e7117249c78d256c6dcd93a67aeac5b5ad19c01557237"
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

RUN19_INHERITED_SCIENTIFIC_IDENTITIES = {
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
    "preregistrationSha256": (
        "209108a553eb186e9048e739981545975bd128528bb1891b28261f09bf1ca2cf"
    ),
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

RUN19_MEMBER_FILE_SHA256 = {
    "fresh_corpus_manifest_v1.json": (
        "fe59805f49ed40ae7996bd8333bba6ea2531ce67c04e904f4f228ea01a54dec2"
    ),
    "fresh_entry_bank_v1.json": (
        "7be58142337fc1b440fe61dae3ad0721c5058e4a1eae3dfde7c223bb8021b28c"
    ),
    "fresh_experiment_ledger_v1.jsonl": RUN19_LEDGER_SHA256,
    "fresh_implementation_manifest_v1.json": (
        "d04bd2279c31922fc753b313f61b140a124c2fc7625227a5a0b9de29377ca1ee"
    ),
    "fresh_preregistration_v4.json": (
        "fd203eed1ff5b1f407b6179b2fd18546106420a1d3ba50b7acddc65e090e0e87"
    ),
    "fresh_quantile_bank_v1.json": (
        "5076a373f6cfc25a6a37e8a63b90eb4633282b425021b55879cb193bb76bab46"
    ),
    "fresh_research_state_binding_v3.json": (
        "62eacb704989a640478ab8a3d05a20cc91a0d69a3797d12dac330c9b3c606cee"
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
        "222bd02635243ce554ef666db3faf2e5008fd60aac08d92023db69b2fd52ac9f"
    ),
    "predecessor_fresh_implementation_manifest_v1.json": (
        "7a7ba50d4cb7edee6b4e5f8fc99b8de8035847cb8e5264a95e757785f1e5bfbd"
    ),
    "predecessor_fresh_preregistration_v3.json": (
        "f4a8d9b6f231ff717d14a227c273c084b27d8c68ec9eaab7cdac1048d6688d30"
    ),
    "predecessor_fresh_research_state_binding_v2.json": (
        "2b6d637cf3761ba14e8f5b724cc640b4840e3c2b861d5b13051ea6b67121632e"
    ),
    "remote-exit-status.txt": (
        "97b912eb4a61df5f806ca6239dde3e1a4f51ad20aced1642cbb83dc510a5fa6b"
    ),
    "server-run.log": (
        "064b0b3f708ed8c8d8acc0ba1f765a5355bce3746b4fab862b0af768f6758e7d"
    ),
}

RUN19_REUSED_OUTCOME_BLIND_FILE_SHA256 = {
    name: RUN19_MEMBER_FILE_SHA256[name]
    for name in (
        "fresh_source_inventory_v1.json",
        "fresh_corpus_manifest_v1.json",
        "fresh_split_manifest_v2.json",
        "fresh_quantile_bank_v1.json",
        "fresh_threshold_domain_preflight_v1.json",
        "fresh_entry_bank_v1.json",
    )
}

_FORBIDDEN_TERMINAL_MEMBERS = frozenset(
    {
        "fresh_exit_bank_v1.json",
        "fresh_final_strategy_v1.json",
        "fresh_holdout_authorization_v1.json",
        "fresh_holdout_results_v1.json",
        "fresh_research_summary_v1.json",
        "fresh_selected_candidate_v1.json",
    }
)

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
    "2026-01-21",
    "2026-01-22",
    "2026-01-23",
    "2026-01-26",
    "2026-01-27",
    "2026-01-28",
    "2026-01-29",
    "2026-01-30",
    "2026-02-02",
    "2026-02-03",
    "2026-02-04",
    "2026-02-05",
    "2026-02-06",
    "2026-02-09",
    "2026-02-10",
    "2026-02-11",
    "2026-02-12",
    "2026-02-13",
    "2026-02-17",
    "2026-02-18",
    "2026-02-19",
    "2026-02-20",
    "2026-02-23",
    "2026-02-25",
    "2026-02-27",
    "2026-03-03",
    "2026-03-04",
)


@dataclass(frozen=True, slots=True)
class FreshV5InfrastructureRestartBundle:
    """Verified outcome-blind inputs and terminal v4 evidence."""

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


def default_run19_terminal_artifact_path() -> Path:
    """Return the repository-relative adopted terminal fixture path."""

    return (
        Path(__file__).resolve().parents[2]
        / RUN19_EXTRACTED_TERMINAL_RELATIVE_PATH
    )


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
        raise PermissionError(f"run-19 {label} changed")


def _canonical_v4_lineage() -> dict[str, Any]:
    lineage = {
        "schema": "fresh-xauusd-study-lineage/v1",
        "studyId": FRESH_V4_STUDY_ID,
        "predecessorStudyId": _V3_STUDY_ID,
        "predecessorPreregistrationSha256": _V3_PREREGISTRATION_SHA256,
        "predecessorTerminalLedgerSha256": _V3_TERMINAL_LEDGER_SHA256,
        "splitManifestSha256": _SPLIT_MANIFEST_SHA256,
        "researchWindowSetSha256": _RESEARCH_WINDOW_SET_SHA256,
        "scientificSpecificationSha256": (
            RUN19_SCIENTIFIC_SPECIFICATION_SHA256
        ),
    }
    if canonical_hash(lineage) != RUN19_PREDECESSOR_STUDY_LINEAGE_SHA256:
        raise RuntimeError("the registered v4 study lineage identity changed")
    return lineage


def canonical_fresh_v5_study_lineage() -> dict[str, Any]:
    """Return a fresh copy of the exact v5 lineage identity."""

    lineage = {
        "schema": "fresh-xauusd-study-lineage/v1",
        "studyId": FRESH_V5_STUDY_ID,
        "predecessorStudyId": FRESH_V4_STUDY_ID,
        "predecessorPreregistrationSha256": (
            RUN19_PREREGISTRATION_SHA256
        ),
        "predecessorTerminalLedgerSha256": RUN19_LEDGER_SHA256,
        "splitManifestSha256": _SPLIT_MANIFEST_SHA256,
        "researchWindowSetSha256": _RESEARCH_WINDOW_SET_SHA256,
        "scientificSpecificationSha256": (
            RUN19_SCIENTIFIC_SPECIFICATION_SHA256
        ),
    }
    if canonical_hash(lineage) != RUN19_V5_STUDY_LINEAGE_SHA256:
        raise RuntimeError("the registered v5 study lineage identity changed")
    return lineage


def _fresh_v5_restart_provenance_body() -> dict[str, Any]:
    lineage = canonical_fresh_v5_study_lineage()
    return {
        "schema": "fresh-xauusd-infrastructure-restart/v3",
        "classification": "new-study-after-terminal-infrastructure-failure",
        "studyId": FRESH_V5_STUDY_ID,
        "studyLineage": lineage,
        "studyLineageSha256": RUN19_V5_STUDY_LINEAGE_SHA256,
        "predecessorStudyId": FRESH_V4_STUDY_ID,
        "predecessorRunId": RUN19_GITHUB_RUN_ID,
        "predecessorRunAttempt": RUN19_GITHUB_RUN_ATTEMPT,
        "predecessorJobId": RUN19_GITHUB_JOB_ID,
        "predecessorCommitSha": RUN19_GITHUB_COMMIT_SHA,
        "predecessorArtifactProducerRunId": RUN19_ADOPTION_GITHUB_RUN_ID,
        "predecessorArtifactProducerRunAttempt": (
            RUN19_ADOPTION_GITHUB_RUN_ATTEMPT
        ),
        "predecessorArtifactId": RUN19_ADOPTION_GITHUB_ARTIFACT_ID,
        "predecessorArtifactName": RUN19_ADOPTION_ARTIFACT_NAME,
        "predecessorArtifactExtractedTerminalRelativePath": (
            RUN19_EXTRACTED_TERMINAL_RELATIVE_PATH
        ),
        "predecessorZipSha256": RUN19_ZIP_SHA256,
        "predecessorArchiveName": RUN19_TERMINAL_ARCHIVE_NAME,
        "predecessorArchiveSha256": RUN19_ARCHIVE_SHA256,
        "predecessorArtifactMemberFileSha256": dict(
            RUN19_MEMBER_FILE_SHA256
        ),
        "predecessorLedgerSha256": RUN19_LEDGER_SHA256,
        "predecessorOrderedLedgerRecordSha256": list(
            RUN19_ORDERED_LEDGER_RECORD_SHA256
        ),
        "predecessorPreregistrationSha256": (
            RUN19_PREREGISTRATION_SHA256
        ),
        "predecessorImplementationManifestSha256": (
            RUN19_IMPLEMENTATION_MANIFEST_SHA256
        ),
        "predecessorStateBindingSha256": (
            RUN19_MEMBER_FILE_SHA256[
                "fresh_research_state_binding_v3.json"
            ]
        ),
        "predecessorStudyLineageSha256": (
            RUN19_PREDECESSOR_STUDY_LINEAGE_SHA256
        ),
        "predecessorExitStatus": 120,
        "predecessorFailure": {
            "errorType": "BrokenPipeError",
            "evidence": "terminal-ledger-record-3",
        },
        "candidateOutcomeRecordCount": 0,
        "laterWindowOutcomeRecordCount": 0,
        "batchResultSealed": False,
        "completedDiscoverySessionCount": 37,
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
            RUN19_INHERITED_SCIENTIFIC_IDENTITIES
        ),
        "reusedOutcomeBlindInputs": dict(
            RUN19_REUSED_OUTCOME_BLIND_FILE_SHA256
        ),
        "scientificSpecificationSha256": (
            RUN19_SCIENTIFIC_SPECIFICATION_SHA256
        ),
        "scientificDefinitionsChanged": False,
        "permittedChange": (
            "infrastructure-only-with-full-discovery-recomputation"
        ),
    }


RUN19_V5_PROVENANCE_SHA256 = (
    "a04a46ac13c7ae4046b65e78966ee7f23734265f102e5db719d74938620d9274"
)


def canonical_fresh_v5_restart_provenance() -> dict[str, Any]:
    """Return an independent, canonically hashed expected provenance value."""

    body = _fresh_v5_restart_provenance_body()
    digest = canonical_hash(body)
    if digest != RUN19_V5_PROVENANCE_SHA256:
        raise RuntimeError("the registered v5 restart provenance changed")
    return {**copy.deepcopy(body), "provenanceSha256": digest}


def _validate_run19_ledger(
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
        != RUN19_ORDERED_LEDGER_RECORD_SHA256
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
            != RUN19_PREREGISTRATION_SHA256
            for record in records
        )
    ):
        raise PermissionError("run-19 terminal ledger sequence changed")

    if (
        records[1].get("metrics")
        != {"candidateCount": 240, "errorType": None}
        or records[2].get("metrics")
        != {"candidateCount": 240, "errorType": "BrokenPipeError"}
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
        raise PermissionError("run-19 batch failure evidence changed")

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
        or record.get("evaluationWindow") != "discovery"
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
            record.get("trainingWindow"),
        }
    ]
    if candidate_outcomes or later_roles or sealed_batches or holdout:
        raise PermissionError("run-19 artifact contains forbidden outcomes")


def _validate_run19_progress_log(path: Path) -> None:
    lines = path.read_text(encoding="utf-8").splitlines()
    if len(lines) != len(_EXPECTED_DISCOVERY_ANCHORS):
        raise PermissionError("run-19 progress evidence changed")
    rows = []
    for line in lines:
        try:
            row = json.loads(line, object_pairs_hook=_object_pairs)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise PermissionError(
                "run-19 discovery progress evidence changed"
            ) from exc
        if not isinstance(row, Mapping):
            raise PermissionError("run-19 discovery progress evidence changed")
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
        raise PermissionError("run-19 discovery progress evidence changed")


def _expected_v4_state_binding() -> dict[str, Any]:
    predecessor_ledger = (
        f"{_STATE_DIRECTORY}/studies/{_RESEARCH_WINDOW_SET_SHA256}/"
        f"lineages/{_V3_STUDY_LINEAGE_SHA256}/"
        "fresh_experiment_ledger_v1.jsonl"
    )
    ledger = (
        f"{_STATE_DIRECTORY}/studies/{_RESEARCH_WINDOW_SET_SHA256}/"
        f"lineages/{RUN19_PREDECESSOR_STUDY_LINEAGE_SHA256}/"
        "fresh_experiment_ledger_v1.jsonl"
    )
    holdout = (
        f"{_STATE_DIRECTORY}/holdouts/{_HOLDOUT_WINDOW_SHA256}/"
        "fresh_holdout_authorization_v1.json"
    )
    lineage = _canonical_v4_lineage()
    return {
        "schema": "fresh-xauusd-durable-research-state/v3",
        "studyId": FRESH_V4_STUDY_ID,
        "stateDirectory": _STATE_DIRECTORY,
        "splitManifestSha256": _SPLIT_MANIFEST_SHA256,
        "researchWindowSetSha256": _RESEARCH_WINDOW_SET_SHA256,
        "holdoutWindowSha256": _HOLDOUT_WINDOW_SHA256,
        "scientificSpecificationSha256": (
            RUN19_SCIENTIFIC_SPECIFICATION_SHA256
        ),
        "predecessorStudyId": _V3_STUDY_ID,
        "predecessorPreregistrationSha256": _V3_PREREGISTRATION_SHA256,
        "predecessorTerminalLedgerSha256": _V3_TERMINAL_LEDGER_SHA256,
        "studyLineage": lineage,
        "studyLineageSha256": RUN19_PREDECESSOR_STUDY_LINEAGE_SHA256,
        "experimentLedgerPath": ledger,
        "predecessorExperimentLedgerPath": predecessor_ledger,
        "holdoutAuthorizationRegistryPath": holdout,
    }


def _validate_run19_scientific_evidence(
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
    identity = RUN19_INHERITED_SCIENTIFIC_IDENTITIES
    source = predecessor_preregistration.get("sourceBindings")
    restart = predecessor_preregistration.get("infrastructureRestart")
    if not isinstance(source, Mapping) or not isinstance(restart, Mapping):
        raise PermissionError("run-19 v4 preregistration evidence changed")
    if (
        predecessor_preregistration.get("schema")
        != "fresh-xauusd-acceleration-preregistration/v4"
        or predecessor_preregistration.get("studyId") != FRESH_V4_STUDY_ID
        or predecessor_preregistration.get("preregistrationSha256")
        != RUN19_PREREGISTRATION_SHA256
        or predecessor_implementation_manifest.get("manifestSha256")
        != RUN19_IMPLEMENTATION_MANIFEST_SHA256
        or source.get("implementationManifestSha256")
        != RUN19_IMPLEMENTATION_MANIFEST_SHA256
        or restart.get("provenanceSha256")
        != _V4_RESTART_PROVENANCE_SHA256
        or restart.get("studyLineage") != _canonical_v4_lineage()
        or restart.get("studyLineageSha256")
        != RUN19_PREDECESSOR_STUDY_LINEAGE_SHA256
        or restart.get("scientificSpecificationSha256")
        != RUN19_SCIENTIFIC_SPECIFICATION_SHA256
        or restart.get("inheritedScientificIdentities") != identity
        or restart.get("reusedOutcomeBlindInputs")
        != RUN19_REUSED_OUTCOME_BLIND_FILE_SHA256
        or restart.get("candidateOutcomeRecordCount") != 0
        or restart.get("laterWindowOutcomeRecordCount") != 0
        or restart.get("batchResultSealed") is not False
        or restart.get("holdoutAuthorizationPresent") is not False
    ):
        raise PermissionError("run-19 v4 protocol identity changed")

    state = _expected_v4_state_binding()
    _require_exact_mapping(
        predecessor_state_binding,
        state,
        "v4 durable state binding",
    )
    if (
        source.get("experimentLedgerPath")
        != state["experimentLedgerPath"]
        or source.get("holdoutAuthorizationRegistryPath")
        != state["holdoutAuthorizationRegistryPath"]
    ):
        raise PermissionError("run-19 v4 durable path binding changed")

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
        raise PermissionError("run-19 frozen scientific inputs changed")

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


def load_fresh_v5_restart_bundle(
    artifact_directory: str | Path | None = None,
    *,
    github_zip_sha256: str = RUN19_ZIP_SHA256,
    nested_tgz_sha256: str = RUN19_ARCHIVE_SHA256,
) -> FreshV5InfrastructureRestartBundle:
    """Verify the adopted Run-19 payload for a separate v5 study."""

    if github_zip_sha256 != RUN19_ZIP_SHA256:
        raise PermissionError("run-19 adopter GitHub ZIP identity changed")
    if nested_tgz_sha256 != RUN19_ARCHIVE_SHA256:
        raise PermissionError("run-19 nested TGZ identity changed")

    selected = (
        default_run19_terminal_artifact_path()
        if artifact_directory is None
        else Path(artifact_directory).expanduser()
    )
    if selected.is_symlink():
        raise PermissionError("restart artifact directory cannot be a symlink")
    root = selected.resolve()
    if not root.is_dir():
        raise NotADirectoryError(root)
    children = tuple(root.iterdir())
    if any(path.is_symlink() or not path.is_file() for path in children):
        raise PermissionError("restart artifact may contain only regular files")
    names = {path.name for path in children}
    if names & _FORBIDDEN_TERMINAL_MEMBERS:
        raise PermissionError("run-19 artifact contains forbidden result state")
    if names != set(RUN19_MEMBER_FILE_SHA256):
        raise PermissionError("run-19 artifact member set changed")
    paths = {path.name: path for path in children}
    for name, expected in RUN19_MEMBER_FILE_SHA256.items():
        if _file_sha256(paths[name]) != expected:
            raise PermissionError(f"run-19 artifact file changed: {name}")

    if paths["remote-exit-status.txt"].read_bytes() != b"120\n":
        raise PermissionError("run-19 terminal exit status changed")
    _validate_run19_progress_log(paths["server-run.log"])
    records = tuple(
        _load_verified_records(paths["fresh_experiment_ledger_v1.jsonl"])
    )
    _validate_run19_ledger(records)

    inventory = _read_json(paths["fresh_source_inventory_v1.json"])
    corpus = _read_json(paths["fresh_corpus_manifest_v1.json"])
    split = _read_json(paths["fresh_split_manifest_v2.json"])
    predecessor_preregistration = _read_json(
        paths["fresh_preregistration_v4.json"]
    )
    predecessor_implementation_manifest = _read_json(
        paths["fresh_implementation_manifest_v1.json"]
    )
    predecessor_state_binding = _read_json(
        paths["fresh_research_state_binding_v3.json"]
    )

    # Imported lazily because future v5 preregistration code may import this
    # provenance builder and must not create an import cycle.
    from datavis.research.fresh_preregistration import (  # noqa: PLC0415
        validate_fresh_implementation_manifest,
        validate_fresh_preregistration_v4,
    )

    registered_preregistration = validate_fresh_preregistration_v4(
        predecessor_preregistration,
        verify_current_implementation_files=False,
    )
    if registered_preregistration != RUN19_PREREGISTRATION_SHA256:
        raise PermissionError("run-19 v4 preregistration identity changed")
    registered_implementation = validate_fresh_implementation_manifest(
        predecessor_implementation_manifest,
        verify_current_files=False,
    )
    if registered_implementation != RUN19_IMPLEMENTATION_MANIFEST_SHA256:
        raise PermissionError("run-19 implementation identity changed")

    quantile_bank = _read_json(paths["fresh_quantile_bank_v1.json"])
    fresh_quantile_bank_from_payload(quantile_bank)
    threshold_preflight = _read_json(
        paths["fresh_threshold_domain_preflight_v1.json"]
    )
    entry_bank = _read_json(paths["fresh_entry_bank_v1.json"])
    _validate_run19_scientific_evidence(
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

    provenance = canonical_fresh_v5_restart_provenance()
    return FreshV5InfrastructureRestartBundle(
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
    "FRESH_V5_STUDY_ID",
    "FreshV5InfrastructureRestartBundle",
    "RUN19_ADOPTION_ARTIFACT_NAME",
    "RUN19_ADOPTION_GITHUB_ARTIFACT_ID",
    "RUN19_ADOPTION_GITHUB_RUN_ATTEMPT",
    "RUN19_ADOPTION_GITHUB_RUN_ID",
    "RUN19_ARCHIVE_SHA256",
    "RUN19_ARTIFACT_ID",
    "RUN19_ARTIFACT_NAME",
    "RUN19_ATTEMPT",
    "RUN19_COMMIT_SHA",
    "RUN19_EXTRACTED_TERMINAL_RELATIVE_PATH",
    "RUN19_GITHUB_COMMIT_SHA",
    "RUN19_GITHUB_JOB_ID",
    "RUN19_GITHUB_RUN_ATTEMPT",
    "RUN19_GITHUB_RUN_ID",
    "RUN19_IMPLEMENTATION_MANIFEST_SHA256",
    "RUN19_INHERITED_SCIENTIFIC_IDENTITIES",
    "RUN19_JOB_ID",
    "RUN19_LEDGER_SHA256",
    "RUN19_MEMBER_FILE_SHA256",
    "RUN19_ORDERED_LEDGER_RECORD_SHA256",
    "RUN19_PREREGISTRATION_SHA256",
    "RUN19_PREDECESSOR_STUDY_LINEAGE_SHA256",
    "RUN19_REUSED_OUTCOME_BLIND_FILE_SHA256",
    "RUN19_RUN_ID",
    "RUN19_SCIENTIFIC_SPECIFICATION_SHA256",
    "RUN19_STUDY_LINEAGE_SHA256",
    "RUN19_TERMINAL_ARCHIVE_NAME",
    "RUN19_V4_STUDY_LINEAGE_SHA256",
    "RUN19_V5_PROVENANCE_SHA256",
    "RUN19_V5_STUDY_LINEAGE_SHA256",
    "RUN19_ZIP_SHA256",
    "canonical_fresh_v5_restart_provenance",
    "canonical_fresh_v5_study_lineage",
    "default_run19_terminal_artifact_path",
    "load_fresh_v5_restart_bundle",
]
