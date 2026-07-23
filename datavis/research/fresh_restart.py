"""Immutable provenance for the v3 study after the terminal run-16 failure.

This module does not resume or mutate the v2/run-14 lineage.  It verifies the
complete immutable run-16 artifact, proves that no candidate result was sealed
or appended, and exposes only the outcome-blind inputs that a separately
preregistered study may reuse.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

from datavis.research.fresh_search import _load_verified_records
from datavis.research.fresh_thresholds import fresh_quantile_bank_from_payload


FRESH_V3_STUDY_ID = "xauusd-fresh-causal-acceleration-v3"
RUN16_GITHUB_RUN_ID = 29918347818
RUN16_GITHUB_RUN_ATTEMPT = 1
RUN16_GITHUB_JOB_ID = 88917398289
RUN16_GITHUB_ARTIFACT_ID = 8533447491
RUN16_GITHUB_COMMIT_SHA = "740c149b145cbb26f26c6583d8cfd9861b6a8d0f"
RUN16_ARTIFACT_NAME = "fresh-xauusd-29918347818-1"
RUN16_ARCHIVE_SHA256 = (
    "59cca29ee5536b9c2dccb07c4c3c029f75e558bf8d468fff7a0f261338fdc830"
)
RUN16_LEDGER_SHA256 = (
    "209d80249abd3082df7b50b55c845b71c18401f0c9d2c61a25f5c66e4de28c40"
)
RUN16_TERMINAL_RECORD_SHA256 = (
    "a78e6d19cd487700a6168880f1a70e07e1f332d1195c048f47dda131c60f6def"
)
RUN16_PREDECESSOR_PREREGISTRATION_SHA256 = (
    "209108a553eb186e9048e739981545975bd128528bb1891b28261f09bf1ca2cf"
)
RUN16_PREDECESSOR_IMPLEMENTATION_MANIFEST_SHA256 = (
    "160f29e8136c967297b16ee438d12fe89ae31ee0901af9293b1949d3193cd094"
)
RUN16_ORDERED_LEDGER_RECORD_SHA256 = (
    "53b8ad1b941915e4b53e71bc48c753ea7dcc322068d6d92ac680554252d234fd",
    "d8925600e087ea1612016c9308532c3ced5bf6729695ad6243761fbf543481b2",
    "b8f0f66212cad2c114ff3e65863ecfd7618c7a1fae10939332ea302599f95671",
    "57a2a5bce85265f9a96fdd0c758ecd4e9c560e619a7411fcb343d5ec75f24d8e",
    "b8aef1bfc894aed02922cd93cd0e8dbbc5bb21aab7739456439b15f3affcc64b",
    "d9cf15ad32ab9bb828814b2ae2a099198f5a9290150a56d9de4cd5514d67a641",
    RUN16_TERMINAL_RECORD_SHA256,
)
RUN16_INHERITED_SCIENTIFIC_IDENTITIES = {
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
    "holdoutWindowSha256": (
        "8d599150987e32430a5d012b4973590bda56f7d548c42e7dad9714e2f0fe40b7"
    ),
    "inventorySha256": (
        "f766d21bfa5a60d6f7b81f5393a24458626cead6069dd8b1719f89de68924d5b"
    ),
    "orderedCandidateSequenceSha256": (
        "d4163395adb43ec49a5f0e10df1fcc82bb698703d2462d735eed5b7ed40ba19c"
    ),
    "originalImplementationManifestSha256": (
        RUN16_PREDECESSOR_IMPLEMENTATION_MANIFEST_SHA256
    ),
    "preregistrationSha256": RUN16_PREDECESSOR_PREREGISTRATION_SHA256,
    "quantileBankSha256": (
        "3243793cdc3d1ee2f7a64b2184a8f91bdf729f2115b06adfb9060dd60a3f78ae"
    ),
    "recoveryImplementationManifestSha256": (
        "6999026509dd88a89b8de03203c8aa732e827c7b9539c74c79531ca5fa1b12f5"
    ),
    "researchWindowSetSha256": (
        "0215cfa1ca0954bae1d6eaafbab44a62ddf663a356bb9f7978071afed4595371"
    ),
    "scoringDefinitionSha256": (
        "48794b47baeab7a53420d2567294f891a010ef93bb3e77fa4544796a4afccc25"
    ),
    "splitManifestSha256": (
        "59a0df375a3b8934c14a355a4fc91bb9aade6ada88052d5096c4b9a29e2744bd"
    ),
    "thresholdBankSha256": (
        "9545c7fa7e5c5eef9d64d867f5b9b1b81415e405389471b14e67d1761b1ddf84"
    ),
}

_EXPECTED_FILES = {
    "fresh_corpus_manifest_v1.json": (
        "fe59805f49ed40ae7996bd8333bba6ea2531ce67c04e904f4f228ea01a54dec2"
    ),
    "fresh_entry_bank_v1.json": (
        "7be58142337fc1b440fe61dae3ad0721c5058e4a1eae3dfde7c223bb8021b28c"
    ),
    "fresh_experiment_ledger_v1.jsonl": RUN16_LEDGER_SHA256,
    "fresh_implementation_manifest_v1.json": (
        "bc304741db8649da2bca478cafb6f94acc85a25b325a7cbd91f97feb2ef64503"
    ),
    "fresh_preregistration_v2.json": (
        "dd00fbc7670319906bfe4a474232dc8cdac08c918734d960367175745c6d07b3"
    ),
    "fresh_quantile_bank_v1.json": (
        "5076a373f6cfc25a6a37e8a63b90eb4633282b425021b55879cb193bb76bab46"
    ),
    "fresh_recovery_contract_v1.json": (
        "8067d798e6039c437e388878b414a73f1eab16fe30743e04ad764c7891a04add"
    ),
    "fresh_recovery_implementation_manifest_v1.json": (
        "0c45d2030eb91a139f5e693d6d3d711fdb2efb19bb16e59c83efa01969a5c8f8"
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
    "run14_remote-exit-status.txt": (
        "e3b9c2844b5a5c2677b3a2279db2ec8487491dd9a23d6b22fac153391b3bb63c"
    ),
    "run14_server-run.log": (
        "a0246978c5ffd0d65480e78ca0a68bae51bee7cb95a8c97c53f9e69060e5257c"
    ),
    "server-run.log": (
        "5a2b4b670aaa1bdd6ffb79387fbfc0e749797862fbc71e506d2ec1554764ee6d"
    ),
}
RUN16_REUSED_OUTCOME_BLIND_FILE_SHA256 = {
    name: _EXPECTED_FILES[name]
    for name in (
        "fresh_source_inventory_v1.json",
        "fresh_corpus_manifest_v1.json",
        "fresh_split_manifest_v2.json",
        "fresh_quantile_bank_v1.json",
        "fresh_threshold_domain_preflight_v1.json",
        "fresh_entry_bank_v1.json",
    )
}


@dataclass(frozen=True, slots=True)
class FreshInfrastructureRestartBundle:
    root: Path
    paths: Mapping[str, Path]
    inventory: Mapping[str, Any]
    corpus: Mapping[str, Any]
    split: Mapping[str, Any]
    predecessor_preregistration: Mapping[str, Any]
    predecessor_implementation_manifest: Mapping[str, Any]
    predecessor_recovery_contract: Mapping[str, Any]
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
        raise PermissionError(f"run-16 {label} changed")


def _validate_run16_ledger(
    records: tuple[Mapping[str, Any], ...],
) -> None:
    expected_kinds = (
        "stage-window-access",
        "batch-window-access",
        "infrastructure-resume",
        "infrastructure-resume",
        "infrastructure-resume",
        "infrastructure-resume",
        "infrastructure-resume",
    )
    expected_statuses = (
        "window_access_started",
        "batch_access_started",
        "resume_eligibility_audit",
        "resume_authorized",
        "resume_identity_verified",
        "batch_resume_started",
        "batch_resume_error",
    )
    if (
        tuple(record.get("recordNumber") for record in records)
        != tuple(range(1, 8))
        or tuple(record.get("recordSha256") for record in records)
        != RUN16_ORDERED_LEDGER_RECORD_SHA256
        or tuple(record.get("recordKind") for record in records)
        != expected_kinds
        or tuple(record.get("status") for record in records)
        != expected_statuses
        or any(record.get("role") != "discovery" for record in records)
    ):
        raise PermissionError("run-16 terminal ledger sequence changed")

    terminal = records[-1]
    if (
        terminal.get("recordSha256") != RUN16_TERMINAL_RECORD_SHA256
        or terminal.get("recordNumber") != 7
        or terminal.get("metrics")
        != {
            "batchResultSealed": False,
            "candidateOutcomesAppended": 0,
            "errorType": "ExternalProcessTermination",
            "externalExitStatus": 137,
        }
        or terminal.get("leakageChecks", {}).get("recoveryAttemptConsumed")
        is not True
    ):
        raise PermissionError("run-16 terminal failure evidence changed")

    protocol_kinds = {
        "stage-window-access",
        "batch-window-access",
        "infrastructure-resume",
    }
    if any(record.get("recordKind") not in protocol_kinds for record in records):
        raise PermissionError("run-16 artifact reveals candidate outcomes")


def _validate_run16_scientific_evidence(
    *,
    inventory: Mapping[str, Any],
    corpus: Mapping[str, Any],
    split: Mapping[str, Any],
    predecessor_preregistration: Mapping[str, Any],
    predecessor_implementation_manifest: Mapping[str, Any],
    predecessor_recovery_contract: Mapping[str, Any],
    predecessor_state_binding: Mapping[str, Any],
    quantile_bank: Mapping[str, Any],
    threshold_preflight: Mapping[str, Any],
    entry_bank: Mapping[str, Any],
) -> None:
    identity = RUN16_INHERITED_SCIENTIFIC_IDENTITIES
    source = predecessor_preregistration.get("sourceBindings")
    if not isinstance(source, Mapping):
        raise PermissionError("run-16 predecessor source bindings changed")
    if (
        predecessor_preregistration.get("studyId")
        != "xauusd-fresh-causal-acceleration-v2"
        or predecessor_preregistration.get("preregistrationSha256")
        != RUN16_PREDECESSOR_PREREGISTRATION_SHA256
        or source.get("implementationManifestSha256")
        != RUN16_PREDECESSOR_IMPLEMENTATION_MANIFEST_SHA256
        or predecessor_implementation_manifest.get("manifestSha256")
        != RUN16_PREDECESSOR_IMPLEMENTATION_MANIFEST_SHA256
    ):
        raise PermissionError("run-16 predecessor protocol identity changed")

    recovery_audit = predecessor_recovery_contract.get("audit")
    if not isinstance(recovery_audit, Mapping):
        raise PermissionError("run-16 predecessor recovery audit changed")
    _require_exact_mapping(
        recovery_audit.get("identity"),
        identity,
        "inherited scientific identities",
    )
    recovery_equivalence = predecessor_recovery_contract.get(
        "equivalenceEvidence"
    )
    if (
        predecessor_recovery_contract.get("schema")
        != "fresh-xauusd-run14-recovery-contract/v1"
        or predecessor_recovery_contract.get("recoveryContractSha256")
        != "1a477c36992505b93b86fd205e61d5ed95d7a025abbd924b04c70675d7bab2d7"
        or not isinstance(recovery_equivalence, Mapping)
        or recovery_equivalence.get("allRequiredTestsPassed") is not True
        or recovery_equivalence.get("completedBeforeRecoveryOutcomeAccess")
        is not True
        or recovery_equivalence.get("processExitCode") != 0
        or recovery_audit.get("candidateOutcomeRecordCount") != 0
        or recovery_audit.get("laterRoleRecordCount") != 0
        or recovery_audit.get("holdoutAuthorizationPresent") is not False
        or recovery_audit.get("maximumRecoveryAttempts") != 1
        or recovery_audit.get("recoveryAttempt") != 1
    ):
        raise PermissionError("run-16 predecessor recovery evidence changed")

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
    ):
        raise PermissionError("run-16 frozen source identities changed")

    state_expected = {
        "schema": "fresh-xauusd-durable-research-state/v1",
        "studyId": "xauusd-fresh-causal-acceleration-v2",
        "splitManifestSha256": identity["splitManifestSha256"],
        "researchWindowSetSha256": identity["researchWindowSetSha256"],
        "holdoutWindowSha256": identity["holdoutWindowSha256"],
    }
    if any(
        predecessor_state_binding.get(key) != value
        for key, value in state_expected.items()
    ):
        raise PermissionError("run-16 durable state binding changed")
    if (
        quantile_bank.get("bankSha256") != identity["quantileBankSha256"]
        or entry_bank.get("candidateCount") != identity["candidateCount"]
        or entry_bank.get("candidateGridSha256")
        != identity["candidateGridSha256"]
        or entry_bank.get("filterVariantBankSha256")
        != identity["eventFilterVariantBankSha256"]
        or entry_bank.get("quantileBankSha256")
        != identity["quantileBankSha256"]
    ):
        raise PermissionError("run-16 frozen candidate inputs changed")

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


def _validate_run16_progress_log(path: Path) -> None:
    expected_anchors = (
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
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            row = json.loads(line, object_pairs_hook=_object_pairs)
        except (TypeError, ValueError, json.JSONDecodeError) as exc:
            raise PermissionError("run-16 progress evidence changed") from exc
        if not isinstance(row, Mapping):
            raise PermissionError("run-16 progress evidence changed")
        rows.append(row)
    if len(rows) != 11:
        raise PermissionError("run-16 progress evidence changed")
    _require_exact_mapping(
        rows[0],
        {
            "evidenceSha256": RUN16_INHERITED_SCIENTIFIC_IDENTITIES[
                "equivalenceEvidenceSha256"
            ],
            "stage": "recovery_equivalence_preflight",
            "status": "passed",
            "testModuleCount": 5,
        },
        "equivalence progress evidence",
    )
    expected_rows = tuple(
        {
            "sessionAnchor": anchor,
            "sessionCount": 40,
            "sessionOrdinal": ordinal,
            "stage": "discovery",
        }
        for ordinal, anchor in enumerate(expected_anchors, start=1)
    )
    if tuple(dict(row) for row in rows[1:]) != expected_rows:
        raise PermissionError("run-16 discovery progress evidence changed")


def load_fresh_v3_restart_bundle(
    artifact_directory: str | Path,
) -> FreshInfrastructureRestartBundle:
    """Verify the exact terminal run-16 artifact for a new-study restart."""

    selected = Path(artifact_directory).expanduser()
    if selected.is_symlink():
        raise PermissionError("restart artifact directory cannot be a symlink")
    root = selected.resolve()
    if not root.is_dir():
        raise NotADirectoryError(root)
    children = tuple(root.iterdir())
    if any(path.is_symlink() or not path.is_file() for path in children):
        raise PermissionError("restart artifact may contain only regular files")
    if {path.name for path in children} != set(_EXPECTED_FILES):
        raise PermissionError("restart artifact member set changed")
    paths = {path.name: path for path in children}
    for name, expected in _EXPECTED_FILES.items():
        if _file_sha256(paths[name]) != expected:
            raise PermissionError(f"restart artifact file changed: {name}")

    if paths["remote-exit-status.txt"].read_bytes() != b"137\n":
        raise PermissionError("run-16 terminal exit status changed")
    if paths["run14_remote-exit-status.txt"].read_bytes() != b"137\n":
        raise PermissionError("run-14 predecessor exit status changed")
    _validate_run16_progress_log(paths["server-run.log"])
    records = tuple(
        _load_verified_records(paths["fresh_experiment_ledger_v1.jsonl"])
    )
    _validate_run16_ledger(records)

    inventory = _read_json(paths["fresh_source_inventory_v1.json"])
    corpus = _read_json(paths["fresh_corpus_manifest_v1.json"])
    split = _read_json(paths["fresh_split_manifest_v2.json"])
    predecessor_preregistration = _read_json(
        paths["fresh_preregistration_v2.json"]
    )
    predecessor_implementation_manifest = _read_json(
        paths["fresh_implementation_manifest_v1.json"]
    )
    predecessor_recovery_contract = _read_json(
        paths["fresh_recovery_contract_v1.json"]
    )
    from datavis.research.fresh_preregistration import (  # noqa: PLC0415
        validate_fresh_preregistration_v2,
    )

    validate_fresh_preregistration_v2(
        predecessor_preregistration,
        verify_current_implementation_files=False,
    )
    predecessor_state_binding = _read_json(
        paths["fresh_research_state_binding_v1.json"]
    )
    quantile_bank = _read_json(paths["fresh_quantile_bank_v1.json"])
    fresh_quantile_bank_from_payload(quantile_bank)
    threshold_preflight = _read_json(
        paths["fresh_threshold_domain_preflight_v1.json"]
    )
    entry_bank = _read_json(paths["fresh_entry_bank_v1.json"])
    _validate_run16_scientific_evidence(
        inventory=inventory,
        corpus=corpus,
        split=split,
        predecessor_preregistration=predecessor_preregistration,
        predecessor_implementation_manifest=predecessor_implementation_manifest,
        predecessor_recovery_contract=predecessor_recovery_contract,
        predecessor_state_binding=predecessor_state_binding,
        quantile_bank=quantile_bank,
        threshold_preflight=threshold_preflight,
        entry_bank=entry_bank,
    )

    frozen_inputs = dict(RUN16_REUSED_OUTCOME_BLIND_FILE_SHA256)
    provenance = {
        "schema": "fresh-xauusd-infrastructure-restart/v1",
        "classification": "new-study-after-terminal-infrastructure-failure",
        "predecessorStudyId": "xauusd-fresh-causal-acceleration-v2",
        "predecessorRunId": RUN16_GITHUB_RUN_ID,
        "predecessorRunAttempt": RUN16_GITHUB_RUN_ATTEMPT,
        "predecessorJobId": RUN16_GITHUB_JOB_ID,
        "predecessorArtifactId": RUN16_GITHUB_ARTIFACT_ID,
        "predecessorCommitSha": RUN16_GITHUB_COMMIT_SHA,
        "predecessorArtifactName": RUN16_ARTIFACT_NAME,
        "predecessorArchiveSha256": RUN16_ARCHIVE_SHA256,
        "predecessorLedgerSha256": RUN16_LEDGER_SHA256,
        "predecessorTerminalRecordSha256": RUN16_TERMINAL_RECORD_SHA256,
        "predecessorOrderedLedgerRecordSha256": list(
            RUN16_ORDERED_LEDGER_RECORD_SHA256
        ),
        "predecessorPreregistrationSha256": (
            RUN16_PREDECESSOR_PREREGISTRATION_SHA256
        ),
        "predecessorImplementationManifestSha256": (
            RUN16_PREDECESSOR_IMPLEMENTATION_MANIFEST_SHA256
        ),
        "predecessorExitStatus": 137,
        "candidateOutcomeRecordCount": 0,
        "laterWindowOutcomeRecordCount": 0,
        "batchResultSealed": False,
        "completedDiscoverySessionCount": 10,
        "nextDiscoverySessionOrdinal": 11,
        "transientCandidateComputationsRecovered": False,
        "holdoutAuthorizationPresent": False,
        "predecessorRecoveryAttemptConsumed": True,
        "inheritedScientificIdentities": dict(
            RUN16_INHERITED_SCIENTIFIC_IDENTITIES
        ),
        "reusedOutcomeBlindInputs": frozen_inputs,
        "permittedChange": "bounded-memory-computation-only",
        "scientificDefinitionsChanged": False,
        "predecessorLineageTerminal": True,
    }
    return FreshInfrastructureRestartBundle(
        root=root,
        paths=paths,
        inventory=inventory,
        corpus=corpus,
        split=split,
        predecessor_preregistration=predecessor_preregistration,
        predecessor_implementation_manifest=predecessor_implementation_manifest,
        predecessor_recovery_contract=predecessor_recovery_contract,
        predecessor_state_binding=predecessor_state_binding,
        quantile_bank=quantile_bank,
        threshold_preflight=threshold_preflight,
        entry_bank=entry_bank,
        ledger_records=records,
        provenance=provenance,
    )


__all__ = [
    "FRESH_V3_STUDY_ID",
    "FreshInfrastructureRestartBundle",
    "RUN16_ARCHIVE_SHA256",
    "RUN16_ARTIFACT_NAME",
    "RUN16_GITHUB_ARTIFACT_ID",
    "RUN16_GITHUB_COMMIT_SHA",
    "RUN16_GITHUB_JOB_ID",
    "RUN16_GITHUB_RUN_ATTEMPT",
    "RUN16_GITHUB_RUN_ID",
    "RUN16_INHERITED_SCIENTIFIC_IDENTITIES",
    "RUN16_LEDGER_SHA256",
    "RUN16_ORDERED_LEDGER_RECORD_SHA256",
    "RUN16_PREDECESSOR_IMPLEMENTATION_MANIFEST_SHA256",
    "RUN16_PREDECESSOR_PREREGISTRATION_SHA256",
    "RUN16_REUSED_OUTCOME_BLIND_FILE_SHA256",
    "RUN16_TERMINAL_RECORD_SHA256",
    "load_fresh_v3_restart_bundle",
]
