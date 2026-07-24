from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import unittest
from unittest.mock import patch
import uuid

from datavis.research.fresh_pipeline import RegisteredFreshResearchPipeline
from datavis.research.fresh_preregistration import (
    build_fresh_implementation_manifest,
)
from datavis.research.fresh_recovery_v5 import (
    V5_ADOPTION_ARTIFACT_ID,
    V5_ADOPTION_GITHUB_RUN_ID,
    V5_ENTRY_BANK_FILE_SHA256,
    V5_LEDGER_RECORD_SHA256,
    V5_LEDGER_SHA256,
    V5_ORIGINAL_GITHUB_RUN_ID,
    V5_ORDERED_CANDIDATE_SEQUENCE_SHA256,
    V5_PREREGISTRATION_SHA256,
    V5_RECOVERY_ATTEMPT_ID,
    V5_RECOVERY_CONTRACT_SCHEMA,
    V5_RECOVERY_EQUIVALENCE_SCHEMA,
    V5_RECOVERY_HOLDOUT_PROOF_SCHEMA,
    V5_RECOVERY_MEMBER_SHA256,
    V5_RECOVERY_TEST_MODULES,
    V5_SPLIT_MANIFEST_SHA256,
    V5_TERMINAL_ARCHIVE_SHA256,
    build_fresh_v5_recovery_contract,
    finalize_interrupted_fresh_v5_recovery,
    load_fresh_v5_recovery_bundle,
    required_fresh_v5_recovery_implementation_files,
    validate_fresh_v5_recovery_equivalence_evidence,
    validate_fresh_v5_recovery_for_holdout,
)
from datavis.research.fresh_protocol import append_fresh_record, canonical_hash
from datavis.research.fresh_search import (
    CandidateEvaluation,
    EvaluationContext,
    FreshChronologicalSearch,
    FreshSearchCallbacks,
    FrozenResearchWindow,
)
from datavis.research.fresh_thresholds import fresh_quantile_bank_from_payload


ROOT = Path(__file__).resolve().parent


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()


class FreshV5RecoveryIdentityTests(unittest.TestCase):
    def test_external_and_ledger_identities_are_exact(self) -> None:
        self.assertEqual(V5_ORIGINAL_GITHUB_RUN_ID, 30_067_832_187)
        self.assertEqual(V5_ADOPTION_GITHUB_RUN_ID, 30_101_048_443)
        self.assertEqual(V5_ADOPTION_ARTIFACT_ID, 8_608_015_979)
        self.assertEqual(
            V5_TERMINAL_ARCHIVE_SHA256,
            "397f687e897e45b4c6c41ed04000ecff8e048524ac9d117658b459b219d9ce3d",
        )
        self.assertEqual(
            V5_LEDGER_SHA256,
            "e95e1739987cdb56315adcbb98b2e85198cb14a1d536a07282214d2ef359744d",
        )
        self.assertEqual(
            V5_LEDGER_RECORD_SHA256,
            (
                "83b8e201bab95195526f3580c98e2f4494331df3ecc44c1586cba72ef4f95cb3",
                "f300211bd30a73842539bc8c2365c3eb3fcbd8e7216a968bd97f76dff4f151f1",
            ),
        )
        self.assertEqual(len(V5_RECOVERY_MEMBER_SHA256), 16)

    def test_recovery_implementation_closure_is_explicit(self) -> None:
        required = required_fresh_v5_recovery_implementation_files()
        self.assertEqual(required, tuple(sorted(set(required))))
        self.assertIn("datavis/research/fresh_recovery_v5.py", required)
        self.assertIn("datavis/research/fresh_numeric_spool.py", required)
        for path in (
            ".github/research-v5-recovery-launch.txt",
            ".github/scripts/fresh-xauusd-v5-recovery-controller.py",
            ".github/scripts/fresh-xauusd-v5-terminal-audit-input.py",
            ".github/ssh/fresh-xauusd-ec2-known-hosts",
            (
                ".github/workflows/"
                "fresh-xauusd-v5-recovery-detached-launch.yml"
            ),
        ):
            self.assertIn(path, required)
        for module in V5_RECOVERY_TEST_MODULES:
            self.assertIn(f"{module}.py", required)


class FreshV5RecoveryFixtureTests(unittest.TestCase):
    fixture: Path

    @classmethod
    def setUpClass(cls) -> None:
        raw = os.environ.get("FRESH_V5_RECOVERY_ARTIFACT_DIR")
        required = os.environ.get("FRESH_REQUIRE_V5_RECOVERY_FIXTURE") == "1"
        if not raw:
            if required:
                raise AssertionError("required V5 recovery fixture is unavailable")
            raise unittest.SkipTest("exact V5 recovery fixture was not supplied")
        cls.fixture = Path(raw).expanduser().resolve()
        load_fresh_v5_recovery_bundle(cls.fixture)

    def _copy(self) -> Path:
        destination = ROOT / f".fresh-v5-recovery-test-{uuid.uuid4().hex}"
        destination.mkdir(mode=0o777)
        self.addCleanup(shutil.rmtree, destination, True)
        for source in self.fixture.iterdir():
            shutil.copyfile(source, destination / source.name)
        return destination

    def test_exact_fixture_is_two_record_zero_outcome_terminal_prefix(self) -> None:
        bundle = load_fresh_v5_recovery_bundle(self.fixture)
        self.assertEqual(len(bundle.ledger_records), 2)
        self.assertEqual(len(bundle.discovery_progress), 40)
        self.assertEqual(
            [row["sessionOrdinal"] for row in bundle.discovery_progress],
            list(range(1, 41)),
        )
        self.assertEqual(
            {record["recordKind"] for record in bundle.ledger_records},
            {"stage-window-access", "batch-window-access"},
        )
        self.assertFalse(
            any(record.get("role") != "discovery" for record in bundle.ledger_records)
        )
        for name, expected in V5_RECOVERY_MEMBER_SHA256.items():
            self.assertEqual(_sha256(self.fixture / name), expected)

    def test_extra_or_holdout_member_is_rejected(self) -> None:
        for name in ("unexpected.bin", "fresh_holdout_authorization_v1.json"):
            with self.subTest(name=name):
                directory = self._copy()
                (directory / name).write_bytes(b"forbidden\n")
                with self.assertRaisesRegex(PermissionError, "member set"):
                    load_fresh_v5_recovery_bundle(directory)

    def test_mutated_ledger_is_rejected_before_semantic_use(self) -> None:
        directory = self._copy()
        ledger = directory / "fresh_experiment_ledger_v1.jsonl"
        ledger.write_bytes(ledger.read_bytes() + b"\n")
        with self.assertRaisesRegex(PermissionError, "digest"):
            load_fresh_v5_recovery_bundle(directory)

    def test_validly_chained_candidate_outcome_is_not_recoverable(self) -> None:
        directory = self._copy()
        ledger = directory / "fresh_experiment_ledger_v1.jsonl"
        append_fresh_record(
            ledger,
            {
                "candidateId": "forbidden-outcome",
                "entryVariant": "forbidden",
                "evaluationWindow": "discovery",
                "exitVariant": "entry-edge-only",
                "family": "forbidden",
                "gatePassed": False,
                "identitySha256": "a" * 64,
                "leakageChecks": {"causal": True},
                "metrics": {"outcome": 1},
                "outcomesRevealed": True,
                "parameters": {"forbidden": True},
                "preregistrationSha256": (
                    "ef72f00de02a144ab67dd75012a711473bcd47824cd5ee787b07268a92b11c8c"
                ),
                "role": "discovery",
                "stage": "discovery",
                "status": "rejected",
                "trainingWindow": "discovery",
                "windowSha256": "b" * 64,
            },
        )
        changed_hashes = dict(V5_RECOVERY_MEMBER_SHA256)
        changed_hashes[ledger.name] = _sha256(ledger)
        with (
            patch.dict(V5_RECOVERY_MEMBER_SHA256, changed_hashes, clear=True),
            self.assertRaisesRegex(PermissionError, "exactly two"),
        ):
            load_fresh_v5_recovery_bundle(directory)

    def test_wrong_exit_status_is_rejected(self) -> None:
        directory = self._copy()
        status = directory / "remote-exit-status.txt"
        status.write_bytes(b"0\n")
        changed_hashes = dict(V5_RECOVERY_MEMBER_SHA256)
        changed_hashes[status.name] = _sha256(status)
        with (
            patch.dict(V5_RECOVERY_MEMBER_SHA256, changed_hashes, clear=True),
            self.assertRaisesRegex(PermissionError, "process status"),
        ):
            load_fresh_v5_recovery_bundle(directory)


class FreshV5RecoveryProtocolTests(unittest.TestCase):
    fixture: Path
    bundle = None
    repository_root = ROOT
    shared_root: Path
    output: Path
    entry_specs = ()
    recovery_implementation = None
    equivalence_evidence = None
    recovery_audit = None
    recovery_contract = None
    completed_records = ()
    completed_ledger: Path
    sealed_batch_path: Path

    @staticmethod
    def _equivalence_evidence(
        manifest: dict,
    ) -> dict[str, object]:
        return {
            "schema": V5_RECOVERY_EQUIVALENCE_SCHEMA,
            "allRequiredTestsPassed": True,
            "command": [
                "python",
                "-m",
                "unittest",
                *V5_RECOVERY_TEST_MODULES,
            ],
            "testModules": list(V5_RECOVERY_TEST_MODULES),
            "testSourceSha256": {
                f"{module}.py": _sha256(ROOT / f"{module}.py")
                for module in V5_RECOVERY_TEST_MODULES
            },
            "requiredImplementationFiles": list(
                required_fresh_v5_recovery_implementation_files()
            ),
            "recoveryImplementationManifestSha256": manifest[
                "manifestSha256"
            ],
            "processExitCode": 0,
            "stdoutSha256": "a" * 64,
            "stderrSha256": "b" * 64,
            "completedBeforeRecoveryOutcomeAccess": True,
            "fixtureIdentity": {
                "originalRunId": V5_ORIGINAL_GITHUB_RUN_ID,
                "adoptionRunId": V5_ADOPTION_GITHUB_RUN_ID,
                "adoptionArtifactId": V5_ADOPTION_ARTIFACT_ID,
                "adoptionArtifactDigest": (
                    "sha256:"
                    "6ded0fc6a44e312a9d786991b093913783ce7a2c1d5afa56b58fcf0fbdb824f3"
                ),
                "terminalArchiveSha256": V5_TERMINAL_ARCHIVE_SHA256,
                "ledgerSha256": V5_LEDGER_SHA256,
                "entryBankFileSha256": V5_ENTRY_BANK_FILE_SHA256,
                "preregistrationSha256": V5_PREREGISTRATION_SHA256,
                "studyLineageSha256": (
                    "6377d53891675e02b645bf83b52b24b5ffb5a7b8cc76b701cd3450b2cecd7473"
                ),
            },
        }

    @classmethod
    def setUpClass(cls) -> None:
        raw = os.environ.get("FRESH_V5_RECOVERY_ARTIFACT_DIR")
        required = os.environ.get("FRESH_REQUIRE_V5_RECOVERY_FIXTURE") == "1"
        if not raw:
            if required:
                raise AssertionError(
                    "required V5 recovery fixture is unavailable"
                )
            raise unittest.SkipTest(
                "exact V5 recovery fixture was not supplied"
            )
        cls.fixture = Path(raw).expanduser().resolve()
        cls.bundle = load_fresh_v5_recovery_bundle(cls.fixture)
        cls.shared_root = ROOT / (
            f".fresh-v5-recovery-protocol-{uuid.uuid4().hex}"
        )
        cls.shared_root.mkdir(mode=0o777)
        cls.addClassCleanup(shutil.rmtree, cls.shared_root, True)
        cls.output = cls.shared_root / "output"
        spool = cls.shared_root / "spool"
        cls.output.mkdir()
        spool.mkdir()

        pipeline = RegisteredFreshResearchPipeline(
            repository_root=ROOT,
            output_directory=cls.output,
            spool_directory=spool,
            spool_maximum_bytes=32 * 1024 * 1024,
            connection_context_factory=lambda: None,
            corpus_manifest=cls.bundle.corpus,
            split_manifest=cls.bundle.split,
            preregistration=cls.bundle.preregistration,
            verify_preregistration_implementation_files=False,
        )
        pipeline.quantile_bank = fresh_quantile_bank_from_payload(
            cls.bundle.quantile_bank
        )
        pipeline.threshold_preflight = dict(
            cls.bundle.threshold_preflight
        )
        discovery = cls.bundle.split["windows"]["discovery"]
        context = EvaluationContext(
            stage="discovery",
            training_roles=("discovery",),
            evaluation_roles=("discovery",),
            windows=(
                FrozenResearchWindow(
                    role="discovery",
                    session_anchors=tuple(discovery["sessionAnchors"]),
                    window_sha256=canonical_hash(discovery),
                ),
            ),
        )
        cls.entry_specs = tuple(
            pipeline.build_entry_candidates(
                cls.bundle.quantile_bank,
                context,
            )
        )
        cls.recovery_implementation = build_fresh_implementation_manifest(
            repository_root=ROOT,
            relative_paths=(
                required_fresh_v5_recovery_implementation_files()
            ),
        )
        cls.equivalence_evidence = cls._equivalence_evidence(
            cls.recovery_implementation
        )
        (
            cls.recovery_audit,
            cls.recovery_contract,
        ) = build_fresh_v5_recovery_contract(
            cls.bundle,
            entry_specs=cls.entry_specs,
            recovery_implementation_manifest=cls.recovery_implementation,
            generated_entry_bank_path=(
                cls.output / "fresh_entry_bank_v1.json"
            ),
            equivalence_evidence=cls.equivalence_evidence,
        )

        cls.completed_ledger = cls.shared_root / "completed-ledger.jsonl"
        shutil.copyfile(
            cls.fixture / "fresh_experiment_ledger_v1.jsonl",
            cls.completed_ledger,
        )
        cls.sealed_batch_path = (
            cls.shared_root / "sealed-discovery-batch.json"
        )

        def unavailable(*_args, **_kwargs):
            raise AssertionError("an unused recovery callback was invoked")

        def score_entries_batch(candidates, _context):
            return {
                candidate.candidate_id: CandidateEvaluation(
                    identity_sha256=candidate.entry_sha256,
                    passed=index < 4,
                    metrics={
                        "candidateOrdinal": index,
                        "syntheticRecoveryResult": True,
                    },
                    leakage_checks={
                        "causal": True,
                        "syntheticOnly": True,
                    },
                    score=float(len(candidates) - index),
                )
                for index, candidate in enumerate(candidates)
            }

        callbacks = FreshSearchCallbacks(
            fit_thresholds=unavailable,
            build_entry_candidates=unavailable,
            generate_signals=unavailable,
            score_entry=unavailable,
            build_exit_variants=unavailable,
            run_execution_scenarios=unavailable,
            score_strategy=unavailable,
            score_entries_batch=score_entries_batch,
        )
        resumed = FreshChronologicalSearch.resume_incomplete_discovery(
            split_manifest=cls.bundle.split,
            ledger_path=cls.completed_ledger,
            budgets=pipeline._search_budgets(),
            callbacks=callbacks,
            preregistration_sha256=V5_PREREGISTRATION_SHA256,
            threshold_bank=cls.bundle.quantile_bank,
            entry_specs=cls.entry_specs,
            recovery_audit=cls.recovery_audit,
            recovery_batch_result_path=cls.sealed_batch_path,
        )
        result = resumed.resume_discovery()
        if len(result.evaluated_ids) != 240:
            raise AssertionError(
                "the completed V5 recovery chain is incomplete"
            )
        cls.completed_records = tuple(
            json.loads(line)
            for line in cls.completed_ledger.read_text(
                encoding="utf-8"
            ).splitlines()
        )

    @classmethod
    def _validate_completed(
        cls,
        *,
        records=None,
        contract=None,
        preregistration=None,
        preregistration_sha256=V5_PREREGISTRATION_SHA256,
        batch_path=None,
    ) -> dict[str, object]:
        return validate_fresh_v5_recovery_for_holdout(
            records=cls.completed_records if records is None else records,
            preregistration=(
                cls.bundle.preregistration
                if preregistration is None
                else preregistration
            ),
            preregistration_sha256=preregistration_sha256,
            split_manifest=cls.bundle.split,
            split_manifest_sha256=V5_SPLIT_MANIFEST_SHA256,
            recovery_contract=(
                cls.recovery_contract
                if contract is None
                else contract
            ),
            recovery_implementation_manifest=cls.recovery_implementation,
            sealed_batch_result_path=(
                cls.sealed_batch_path
                if batch_path is None
                else batch_path
            ),
        )

    @staticmethod
    def _reseal_contract(contract: dict) -> None:
        body = {
            key: value
            for key, value in contract.items()
            if key != "recoveryContractSha256"
        }
        contract["recoveryContractSha256"] = canonical_hash(body)

    def _scratch_directory(self) -> Path:
        selected = self.shared_root / uuid.uuid4().hex
        selected.mkdir()
        return selected

    def _copy_original_ledger(self) -> Path:
        ledger = self._scratch_directory() / "ledger.jsonl"
        shutil.copyfile(
            self.fixture / "fresh_experiment_ledger_v1.jsonl",
            ledger,
        )
        return ledger

    def _append_resume_record(
        self,
        ledger: Path,
        status: str,
        *,
        inner: dict[str, object] | None = None,
    ) -> None:
        original = [
            json.loads(line)
            for line in ledger.read_text(encoding="utf-8").splitlines()
        ]
        payload = {
            "kind": "fresh-infrastructure-recovery",
            "status": status,
            "stage": "discovery",
            "recoveryAttemptId": V5_RECOVERY_ATTEMPT_ID,
            "parameters": (
                inner
                if inner is not None
                else {"recoveryAttemptId": V5_RECOVERY_ATTEMPT_ID}
            ),
        }
        append_fresh_record(
            ledger,
            {
                "recordKind": "infrastructure-resume",
                "candidateId": (
                    f"protocol-infrastructure-resume::{status}"
                ),
                "family": "protocol-infrastructure-recovery",
                "stage": "discovery",
                "trainingWindow": "discovery",
                "evaluationWindow": "discovery",
                "parameters": payload,
                "entryVariant": "infrastructure-resume",
                "exitVariant": "infrastructure-resume",
                "metrics": {},
                "status": status,
                "leakageChecks": {},
                "role": "discovery",
                "outcomesRevealed": True,
                "gatePassed": False,
                "identitySha256": canonical_hash(payload),
                "windowSha256": original[0]["windowSha256"],
                "preregistrationSha256": V5_PREREGISTRATION_SHA256,
            },
        )

    def _append_authorization_and_start(self, ledger: Path) -> None:
        for status in (
            "resume_eligibility_audit",
            "resume_authorized",
            "resume_identity_verified",
            "batch_resume_started",
        ):
            self._append_resume_record(ledger, status)

    def _append_batch_completed(
        self,
        ledger: Path,
        batch_sha: str,
    ) -> None:
        self._append_resume_record(
            ledger,
            "batch_resume_completed",
            inner={
                "recoveryAttemptId": V5_RECOVERY_ATTEMPT_ID,
                "batchResultSha256": batch_sha,
                "batchResultFileSha256": "b" * 64,
                "candidateCount": 240,
                "orderedCandidateSequenceSha256": (
                    V5_ORDERED_CANDIDATE_SEQUENCE_SHA256
                ),
            },
        )

    def _append_candidate_outcome(
        self,
        ledger: Path,
        candidate_index: int,
        batch_sha: str,
    ) -> None:
        records = [
            json.loads(line)
            for line in ledger.read_text(encoding="utf-8").splitlines()
        ]
        batch = records[1]["parameters"]
        candidate_id = batch["candidateIds"][candidate_index]
        candidate_sha = batch["candidateSha256"][candidate_index]
        append_fresh_record(
            ledger,
            {
                "candidateId": candidate_id,
                "family": "synthetic",
                "stage": "discovery",
                "trainingWindow": "discovery",
                "evaluationWindow": "discovery",
                "parameters": {
                    "recoveryAttemptId": V5_RECOVERY_ATTEMPT_ID,
                    "sealedBatchResultSha256": batch_sha,
                },
                "entryVariant": "synthetic",
                "exitVariant": "entry-edge-only",
                "metrics": {},
                "status": "rejected",
                "leakageChecks": {},
                "role": "discovery",
                "outcomesRevealed": True,
                "gatePassed": False,
                "identitySha256": candidate_sha,
                "frozenEntrySha256": candidate_sha,
                "frozenStrategySha256": None,
                "windowSha256": records[0]["windowSha256"],
                "preregistrationSha256": V5_PREREGISTRATION_SHA256,
            },
        )

    def test_contract_binds_exact_candidate_sequence_and_one_attempt(
        self,
    ) -> None:
        self.assertEqual(len(self.entry_specs), 240)
        self.assertEqual(
            self.recovery_contract["schema"],
            V5_RECOVERY_CONTRACT_SCHEMA,
        )
        self.assertEqual(
            self.recovery_audit["recoveryAttemptId"],
            V5_RECOVERY_ATTEMPT_ID,
        )
        self.assertEqual(self.recovery_audit["maximumRecoveryAttempts"], 1)
        self.assertEqual(self.recovery_audit["candidateOutcomeRecordCount"], 0)
        self.assertEqual(self.recovery_audit["laterRoleRecordCount"], 0)
        self.assertFalse(
            self.recovery_audit["holdoutAuthorizationPresent"]
        )
        self.assertEqual(
            self.recovery_audit["identity"][
                "orderedCandidateSequenceSha256"
            ],
            V5_ORDERED_CANDIDATE_SEQUENCE_SHA256,
        )

    def test_completed_chain_and_batch_prove_holdout_eligibility(
        self,
    ) -> None:
        proof = self._validate_completed()
        self.assertEqual(
            proof["schema"],
            V5_RECOVERY_HOLDOUT_PROOF_SCHEMA,
        )
        self.assertEqual(proof["candidateOutcomeRecordCount"], 240)
        self.assertEqual(
            proof["recoveryContractSha256"],
            self.recovery_contract["recoveryContractSha256"],
        )
        self.assertEqual(
            proof["sealedBatchResultFileSha256"],
            _sha256(self.sealed_batch_path),
        )

    def test_equivalence_contract_tampering_fails_closed(self) -> None:
        changed = copy.deepcopy(self.equivalence_evidence)
        changed["command"] = list(reversed(changed["command"]))
        with self.assertRaisesRegex(PermissionError, "incomplete"):
            validate_fresh_v5_recovery_equivalence_evidence(
                changed,
                recovery_implementation_manifest=(
                    self.recovery_implementation
                ),
            )

        changed = copy.deepcopy(self.equivalence_evidence)
        source_name = sorted(changed["testSourceSha256"])[0]
        changed["testSourceSha256"][source_name] = "0" * 64
        with self.assertRaisesRegex(PermissionError, "bytes changed"):
            validate_fresh_v5_recovery_equivalence_evidence(
                changed,
                recovery_implementation_manifest=(
                    self.recovery_implementation
                ),
            )

        contract = copy.deepcopy(self.recovery_contract)
        contract["audit"]["identity"]["candidateCount"] = 239
        self._reseal_contract(contract)
        with self.assertRaisesRegex(PermissionError, "identities changed"):
            self._validate_completed(contract=contract)

    def test_unknown_and_cross_lineage_contracts_fail_closed(self) -> None:
        unknown = copy.deepcopy(self.recovery_contract)
        unknown["schema"] = "fresh-xauusd-unknown-recovery/v1"
        self._reseal_contract(unknown)
        with self.assertRaisesRegex(PermissionError, "contract identity"):
            self._validate_completed(contract=unknown)

        predecessor = json.loads(
            (
                self.fixture / "predecessor_fresh_preregistration_v4.json"
            ).read_text(encoding="utf-8")
        )
        with self.assertRaisesRegex(PermissionError, "preregistration"):
            self._validate_completed(
                preregistration=predecessor,
                preregistration_sha256=predecessor[
                    "preregistrationSha256"
                ],
            )

    def test_resealed_candidate_or_batch_tampering_is_rejected(self) -> None:
        records = copy.deepcopy(list(self.completed_records))
        records[7], records[8] = records[8], records[7]
        with self.assertRaises(PermissionError):
            self._validate_completed(records=records)

        batch_path = self._scratch_directory() / "batch.json"
        document = json.loads(
            self.sealed_batch_path.read_text(encoding="utf-8")
        )
        document["orderedResults"][0]["evaluation"]["metrics"][
            "candidateOrdinal"
        ] = 999
        body = {
            key: value
            for key, value in document.items()
            if key != "batchResultSha256"
        }
        document["batchResultSha256"] = canonical_hash(body)
        batch_path.write_text(
            json.dumps(
                document,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n",
            encoding="utf-8",
        )
        with self.assertRaisesRegex(PermissionError, "batch file changed"):
            self._validate_completed(batch_path=batch_path)

    def test_finalizer_consumes_incomplete_attempt_once(self) -> None:
        ledger = self._copy_original_ledger()
        self.assertTrue(
            finalize_interrupted_fresh_v5_recovery(
                ledger,
                exit_status=137,
            )
        )
        records = [
            json.loads(line)
            for line in ledger.read_text(encoding="utf-8").splitlines()
        ]
        self.assertEqual(records[-1]["status"], "batch_resume_error")
        self.assertEqual(records[-1]["metrics"]["externalExitStatus"], 137)
        before = ledger.read_bytes()
        self.assertFalse(
            finalize_interrupted_fresh_v5_recovery(
                ledger,
                exit_status=137,
            )
        )
        self.assertEqual(ledger.read_bytes(), before)

    def test_finalizer_counts_partial_sealed_batch_outcomes(self) -> None:
        ledger = self._copy_original_ledger()
        self._append_authorization_and_start(ledger)
        batch_sha = "a" * 64
        self._append_batch_completed(ledger, batch_sha)
        self._append_candidate_outcome(ledger, 0, batch_sha)
        self.assertTrue(
            finalize_interrupted_fresh_v5_recovery(
                ledger,
                exit_status=137,
            )
        )
        terminal = json.loads(
            ledger.read_text(encoding="utf-8").splitlines()[-1]
        )
        self.assertEqual(
            terminal["metrics"]["candidateOutcomesAppended"],
            1,
        )
        self.assertTrue(terminal["metrics"]["batchResultSealed"])

    def test_finalizer_does_not_mutate_completed_or_cross_lineage(
        self,
    ) -> None:
        before = self.completed_ledger.read_bytes()
        self.assertFalse(
            finalize_interrupted_fresh_v5_recovery(
                self.completed_ledger,
                exit_status=1,
            )
        )
        self.assertEqual(self.completed_ledger.read_bytes(), before)

        predecessor = (
            self.fixture / "predecessor_fresh_experiment_ledger_v1.jsonl"
        )
        with self.assertRaisesRegex(PermissionError, "prefix"):
            finalize_interrupted_fresh_v5_recovery(
                predecessor,
                exit_status=137,
            )


if __name__ == "__main__":
    unittest.main()
