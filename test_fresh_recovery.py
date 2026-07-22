from __future__ import annotations

import copy
import gzip
import hashlib
import io
import json
import os
import shutil
import subprocess
import sys
import tarfile
import textwrap
import unittest
import uuid
from pathlib import Path

from datavis.research.fresh_pipeline import RegisteredFreshResearchPipeline
from datavis.research.fresh_preregistration import (
    build_fresh_implementation_manifest,
    required_fresh_implementation_files,
)
from datavis.research.fresh_protocol import append_fresh_record, canonical_hash
from datavis.research.fresh_recovery import (
    RUN14_ENTRY_BANK_FILE_SHA256,
    RUN14_LEDGER_SHA256,
    RUN14_ORDERED_CANDIDATE_SHA256,
    RUN14_PREREGISTRATION_SHA256,
    RUN14_RECOVERY_TEST_MODULES,
    RUN14_RUN_ID,
    RUN14_SPLIT_MANIFEST_SHA256,
    RUN14_TGZ_SHA256,
    RUN14_THRESHOLD_PAYLOAD_SHA256,
    build_run14_recovery_contract,
    finalize_interrupted_run14_recovery,
    load_run14_recovery_bundle,
    validate_run14_recovery_for_holdout,
)
from datavis.research.fresh_search import (
    CandidateEvaluation,
    EvaluationContext,
    FreshChronologicalSearch,
    FreshSearchCallbacks,
    FrozenResearchWindow,
)
from datavis.research.fresh_thresholds import fresh_quantile_bank_from_payload


class FreshRun14RecoveryTests(unittest.TestCase):
    bundle = None
    fixture_path: Path | None = None
    recovery_attempt_id = "run14-discovery-recovery-attempt-1"
    repository_root: Path | None = None
    shared_output: Path | None = None
    entry_specs = ()
    recovery_implementation = None
    recovery_audit = None
    recovery_contract = None
    completed_records = ()
    sealed_batch_path: Path | None = None

    def scratch_directory(self) -> Path:
        root = Path(__file__).resolve().parent / "artifacts" / "test-fresh-recovery"
        root.mkdir(parents=True, exist_ok=True)
        selected = root / uuid.uuid4().hex
        selected.mkdir(mode=0o777)
        self.addCleanup(shutil.rmtree, selected, True)
        return selected

    @staticmethod
    def equivalence_evidence(repository_root: Path) -> dict[str, object]:
        sources = {
            f"{module}.py": hashlib.sha256(
                (repository_root / f"{module}.py").read_bytes()
            ).hexdigest()
            for module in RUN14_RECOVERY_TEST_MODULES
        }
        return {
            "schema": "fresh-xauusd-recovery-equivalence-preflight/v1",
            "allRequiredTestsPassed": True,
            "command": [
                "python",
                "-m",
                "unittest",
                *RUN14_RECOVERY_TEST_MODULES,
            ],
            "testModules": list(RUN14_RECOVERY_TEST_MODULES),
            "testSourceSha256": sources,
            "processExitCode": 0,
            "stdoutSha256": "a" * 64,
            "stderrSha256": "b" * 64,
            "completedBeforeRecoveryOutcomeAccess": True,
            "fixtureIdentity": {
                "runId": RUN14_RUN_ID,
                "archiveSha256": RUN14_TGZ_SHA256,
                "ledgerSha256": RUN14_LEDGER_SHA256,
                "entryBankFileSha256": RUN14_ENTRY_BANK_FILE_SHA256,
                "preregistrationSha256": RUN14_PREREGISTRATION_SHA256,
            },
        }

    @classmethod
    def setUpClass(cls) -> None:
        raw = os.environ.get("FRESH_RUN14_RECOVERY_ARTIFACT_DIR")
        required = os.environ.get("FRESH_REQUIRE_RUN14_RECOVERY_FIXTURE") == "1"
        if not raw:
            if required:
                raise AssertionError("required run-14 recovery fixture is missing")
            raise unittest.SkipTest("run-14 immutable recovery artifact not supplied")
        cls.fixture_path = Path(raw).expanduser().resolve()
        cls.bundle = load_run14_recovery_bundle(cls.fixture_path)
        cls.repository_root = Path(__file__).resolve().parent
        shared_root = (
            cls.repository_root / "artifacts" / "test-fresh-recovery" / uuid.uuid4().hex
        )
        shared_root.mkdir(parents=True, mode=0o777)
        cls.addClassCleanup(shutil.rmtree, shared_root, True)
        cls.shared_output = shared_root / "output"
        cls.shared_output.mkdir(mode=0o777)

        pipeline = RegisteredFreshResearchPipeline(
            repository_root=cls.repository_root,
            output_directory=cls.shared_output,
            connection_context_factory=lambda: None,
            corpus_manifest=cls.bundle.corpus,
            split_manifest=cls.bundle.split,
            preregistration=cls.bundle.preregistration,
            verify_preregistration_implementation_files=False,
        )
        pipeline.quantile_bank = fresh_quantile_bank_from_payload(
            cls.bundle.quantile_bank
        )
        pipeline.threshold_preflight = dict(cls.bundle.threshold_preflight)
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
            pipeline.build_entry_candidates(cls.bundle.quantile_bank, context)
        )
        cls.recovery_implementation = build_fresh_implementation_manifest(
            repository_root=cls.repository_root,
            relative_paths=(
                *required_fresh_implementation_files(),
                "datavis/research/fresh_recovery.py",
                "datavis/research/fresh_spool.py",
                *(f"{module}.py" for module in RUN14_RECOVERY_TEST_MODULES),
            ),
        )
        cls.recovery_audit, cls.recovery_contract = build_run14_recovery_contract(
            cls.bundle,
            entry_specs=cls.entry_specs,
            recovery_implementation_manifest=cls.recovery_implementation,
            generated_entry_bank_path=cls.shared_output / "fresh_entry_bank_v1.json",
            equivalence_evidence=cls.equivalence_evidence(cls.repository_root),
        )

        ledger = shared_root / "completed-ledger.jsonl"
        shutil.copyfile(cls.fixture_path / "fresh_experiment_ledger_v1.jsonl", ledger)
        cls.sealed_batch_path = shared_root / "sealed-discovery-batch.json"

        def unavailable(*_args, **_kwargs):
            raise AssertionError("an unused recovery callback was invoked")

        def score_entries_batch(candidates, _context):
            return {
                candidate.candidate_id: CandidateEvaluation(
                    identity_sha256=candidate.entry_sha256,
                    passed=index < 4,
                    metrics={"candidateOrdinal": index, "fixtureResult": True},
                    leakage_checks={"causal": True, "fixtureOnly": True},
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
            ledger_path=ledger,
            budgets=pipeline._search_budgets(),
            callbacks=callbacks,
            preregistration_sha256=RUN14_PREREGISTRATION_SHA256,
            threshold_bank=cls.bundle.quantile_bank,
            entry_specs=cls.entry_specs,
            recovery_audit=cls.recovery_audit,
            recovery_batch_result_path=cls.sealed_batch_path,
        )
        result = resumed.resume_discovery()
        if len(result.evaluated_ids) != 240:
            raise AssertionError("the exact completed recovery fixture is incomplete")
        cls.completed_records = tuple(
            json.loads(line) for line in ledger.read_text(encoding="utf-8").splitlines()
        )

    @classmethod
    def validate_completed_recovery(
        cls,
        *,
        records=None,
        contract=None,
        batch_path: Path | None = None,
    ) -> dict[str, object]:
        assert cls.bundle is not None
        assert cls.recovery_contract is not None
        assert cls.recovery_implementation is not None
        assert cls.sealed_batch_path is not None
        return validate_run14_recovery_for_holdout(
            records=cls.completed_records if records is None else records,
            preregistration=cls.bundle.preregistration,
            preregistration_sha256=RUN14_PREREGISTRATION_SHA256,
            split_manifest=cls.bundle.split,
            split_manifest_sha256=RUN14_SPLIT_MANIFEST_SHA256,
            recovery_contract=(cls.recovery_contract if contract is None else contract),
            recovery_implementation_manifest=cls.recovery_implementation,
            sealed_batch_result_path=(
                cls.sealed_batch_path if batch_path is None else batch_path
            ),
        )

    @staticmethod
    def reseal_contract(contract: dict[str, object]) -> None:
        body = {
            key: value
            for key, value in contract.items()
            if key != "recoveryContractSha256"
        }
        contract["recoveryContractSha256"] = canonical_hash(body)

    @staticmethod
    def reseal_record(record: dict[str, object]) -> None:
        body = {
            key: value
            for key, value in record.items()
            if key not in {"recordNumber", "recordSha256"}
        }
        record["recordSha256"] = canonical_hash(body)

    def test_exact_immutable_bundle_has_only_the_incomplete_discovery_prefix(self):
        assert self.bundle is not None
        self.assertEqual(RUN14_RUN_ID, 29_881_509_856)
        self.assertEqual(
            self.bundle.split["manifestSha256"], RUN14_SPLIT_MANIFEST_SHA256
        )
        self.assertEqual(
            self.bundle.preregistration["preregistrationSha256"],
            RUN14_PREREGISTRATION_SHA256,
        )
        self.assertEqual(
            canonical_hash(self.bundle.quantile_bank),
            RUN14_THRESHOLD_PAYLOAD_SHA256,
        )
        self.assertEqual(len(self.bundle.ledger_records), 2)
        self.assertEqual(
            [record["status"] for record in self.bundle.ledger_records],
            ["window_access_started", "batch_access_started"],
        )
        self.assertNotIn("fresh_holdout_authorization_v1.json", self.bundle.paths)

    def test_any_artifact_byte_change_is_rejected_before_recovery(self):
        assert self.fixture_path is not None
        destination = self.scratch_directory() / "bundle"
        shutil.copytree(self.fixture_path, destination)
        status = destination / "remote-exit-status.txt"
        status.write_bytes(b"0\n")
        with self.assertRaisesRegex(PermissionError, "digest changed"):
            load_run14_recovery_bundle(destination)

    def test_workflow_extractor_accepts_only_the_tar_root_directory_marker(self):
        assert self.repository_root is not None
        workflow = (
            self.repository_root / ".github/workflows/fresh-xauusd-research.yml"
        ).read_text(encoding="utf-8")
        invocation = 'python3 - "${recovery_archive}" "${recovery_directory}" <<\'PY\''
        invocation_offset = workflow.index(invocation)
        script_start = workflow.index("          import hashlib\n", invocation_offset)
        script_end = workflow.index("\n          PY", script_start)
        extractor = textwrap.dedent(workflow[script_start:script_end])

        allowed = {
            "fresh_corpus_manifest_v1.json",
            "fresh_entry_bank_v1.json",
            "fresh_experiment_ledger_v1.jsonl",
            "fresh_implementation_manifest_v1.json",
            "fresh_preregistration_v2.json",
            "fresh_quantile_bank_v1.json",
            "fresh_research_state_binding_v1.json",
            "fresh_source_inventory_v1.json",
            "fresh_split_manifest_v2.json",
            "fresh_threshold_domain_preflight_v1.json",
            "remote-exit-status.txt",
            "server-run.log",
        }
        scratch = self.scratch_directory()
        expected_payloads = {
            name: f"fixture:{name}\n".encode() for name in sorted(allowed)
        }

        def directory_header(name: str) -> bytes:
            member = tarfile.TarInfo(name)
            member.mode = 0o700
            header = bytearray(member.tobuf(format=tarfile.USTAR_FORMAT))
            header[156] = tarfile.DIRTYPE[0]
            header[148:156] = b"        "
            checksum = sum(header)
            header[148:156] = f"{checksum:06o}\0 ".encode("ascii")
            return bytes(header)

        def special_header(name: str, member_type: bytes) -> bytes:
            member = tarfile.TarInfo(name)
            member.mode = 0o600
            member.type = member_type
            if member_type == tarfile.SYMTYPE:
                member.linkname = "outside"
            return member.tobuf(format=tarfile.USTAR_FORMAT)

        def write_archive(
            path: Path,
            *,
            leading_headers: tuple[bytes, ...],
            file_names: tuple[str, ...],
        ) -> None:
            payload_stream = io.BytesIO()
            with tarfile.open(fileobj=payload_stream, mode="w") as bundle:
                for name in file_names:
                    payload = expected_payloads.get(name, f"fixture:{name}\n".encode())
                    member = tarfile.TarInfo(f"./{name}")
                    member.mode = 0o600
                    member.size = len(payload)
                    bundle.addfile(member, io.BytesIO(payload))
            uncompressed = b"".join(leading_headers) + payload_stream.getvalue()
            path.write_bytes(gzip.compress(uncompressed, mtime=0))

        ordered_names = tuple(sorted(allowed))
        archive = scratch / "run14-shape.tgz"
        write_archive(
            archive,
            leading_headers=(directory_header("."),),
            file_names=ordered_names,
        )
        with tarfile.open(archive, "r:gz") as bundle:
            root = bundle.getmembers()[0]
        self.assertEqual(root.name, ".")
        self.assertTrue(root.isdir())

        self.assertEqual(extractor.count(RUN14_TGZ_SHA256), 1)

        def run_extractor(selected_archive: Path, label: str):
            selected_script = extractor.replace(
                RUN14_TGZ_SHA256,
                hashlib.sha256(selected_archive.read_bytes()).hexdigest(),
            )
            destination = scratch / label
            destination.mkdir(mode=0o777)
            completed = subprocess.run(
                [
                    sys.executable,
                    "-c",
                    selected_script,
                    str(selected_archive),
                    str(destination),
                ],
                cwd=self.repository_root,
                check=False,
                capture_output=True,
                text=True,
                timeout=30,
            )
            return completed, destination

        completed, destination = run_extractor(archive, "extracted")
        self.assertEqual(
            completed.returncode,
            0,
            msg=f"stdout={completed.stdout!r}\nstderr={completed.stderr!r}",
        )
        self.assertEqual(
            {path.name for path in destination.iterdir()},
            allowed,
        )
        for name, payload in expected_payloads.items():
            self.assertEqual((destination / name).read_bytes(), payload)

        first_name = ordered_names[0]
        negative_cases = {
            "duplicate-root": (
                (directory_header("."), directory_header(".")),
                ordered_names,
                "unsafe run-14 archive root member",
            ),
            "slash-file-member": (
                (directory_header("."), special_header("./", tarfile.REGTYPE)),
                ordered_names,
                "unsafe run-14 archive member",
            ),
            "unexpected-directory": (
                (directory_header("."), directory_header("unexpected")),
                ordered_names,
                "unsafe run-14 archive member",
            ),
            "root-symlink": (
                (special_header(".", tarfile.SYMTYPE),),
                ordered_names,
                "unsafe run-14 archive root member",
            ),
            "nested-file": (
                (
                    directory_header("."),
                    special_header("./nested/file", tarfile.REGTYPE),
                ),
                ordered_names,
                "unsafe run-14 archive member",
            ),
            "duplicate-file": (
                (directory_header("."),),
                (*ordered_names, first_name),
                "run-14 archive member set changed",
            ),
            "missing-file": (
                (directory_header("."),),
                ordered_names[1:],
                "run-14 archive member set changed",
            ),
            "extra-file": (
                (directory_header("."),),
                (*ordered_names, "unexpected.json"),
                "run-14 archive member set changed",
            ),
            "missing-root": (
                (),
                ordered_names,
                "run-14 archive root member changed",
            ),
        }
        for label, (headers, names, expected_error) in negative_cases.items():
            with self.subTest(label=label):
                unsafe_archive = scratch / f"{label}.tgz"
                write_archive(
                    unsafe_archive,
                    leading_headers=headers,
                    file_names=names,
                )
                rejected, _ = run_extractor(unsafe_archive, f"{label}-extracted")
                self.assertNotEqual(rejected.returncode, 0)
                self.assertIn(expected_error, rejected.stderr)

    def test_reconstructed_240_candidate_contract_matches_original_batch(self):
        assert self.recovery_audit is not None
        assert self.recovery_contract is not None
        self.assertEqual(len(self.entry_specs), 240)
        self.assertEqual(
            self.recovery_audit["identity"]["entryBankFileSha256"],
            RUN14_ENTRY_BANK_FILE_SHA256,
        )
        self.assertEqual(self.recovery_audit["candidateOutcomeRecordCount"], 0)
        self.assertEqual(self.recovery_audit["laterRoleRecordCount"], 0)
        self.assertFalse(self.recovery_audit["holdoutAuthorizationPresent"])
        self.assertEqual(self.recovery_audit["maximumRecoveryAttempts"], 1)
        self.assertEqual(
            self.recovery_contract["audit"]["identity"]["candidateCount"], 240
        )

    def test_exact_completed_chain_and_sealed_batch_prove_holdout_recovery(self):
        proof = self.validate_completed_recovery()
        self.assertEqual(
            proof["schema"], "fresh-xauusd-run14-holdout-recovery-proof/v1"
        )
        self.assertEqual(proof["candidateOutcomeRecordCount"], 240)
        self.assertEqual(
            proof["orderedCandidateSequenceSha256"],
            RUN14_ORDERED_CANDIDATE_SHA256,
        )
        self.assertEqual(
            proof["recoveryContractSha256"],
            self.recovery_contract["recoveryContractSha256"],
        )
        self.assertEqual(
            proof["sealedBatchResultFileSha256"],
            hashlib.sha256(self.sealed_batch_path.read_bytes()).hexdigest(),
        )

    def test_completed_recovery_rejects_candidate_order_tampering(self):
        records = list(copy.deepcopy(self.completed_records))
        records[7], records[8] = records[8], records[7]
        with self.assertRaisesRegex(PermissionError, "candidate order"):
            self.validate_completed_recovery(records=records)

    def test_completed_recovery_rejects_resealed_evaluation_tampering(self):
        assert self.sealed_batch_path is not None
        batch_path = self.scratch_directory() / "tampered-sealed-batch.json"
        document = json.loads(self.sealed_batch_path.read_text(encoding="utf-8"))
        document["orderedResults"][0]["evaluation"]["metrics"]["candidateOrdinal"] = 999
        body = {
            key: value for key, value in document.items() if key != "batchResultSha256"
        }
        batch_sha = canonical_hash(body)
        document["batchResultSha256"] = batch_sha
        encoded = (
            json.dumps(
                document,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            )
            + "\n"
        ).encode("utf-8")
        batch_path.write_bytes(encoded)
        batch_file_sha = hashlib.sha256(encoded).hexdigest()

        records = copy.deepcopy(self.completed_records)
        for record in records:
            if record.get("recordKind") == "infrastructure-resume":
                status = record["status"]
                parameters = record["parameters"]["parameters"]
                if status in {"batch_resume_completed", "resume_stage_completed"}:
                    parameters["batchResultSha256"] = batch_sha
                if status == "batch_resume_completed":
                    parameters["batchResultFileSha256"] = batch_file_sha
                record["identitySha256"] = canonical_hash(record["parameters"])
                self.reseal_record(record)
            elif record.get("role") == "discovery" and record.get("candidateId") in {
                spec.candidate_id for spec in self.entry_specs
            }:
                record["parameters"]["sealedBatchResultSha256"] = batch_sha
                self.reseal_record(record)

        with self.assertRaisesRegex(PermissionError, "differs from the ledger"):
            self.validate_completed_recovery(records=records, batch_path=batch_path)

    def test_recovery_contract_and_equivalence_evidence_are_strictly_bound(self):
        assert self.repository_root is not None
        assert self.shared_output is not None
        assert self.recovery_contract is not None
        contract = copy.deepcopy(self.recovery_contract)
        contract["audit"]["identity"]["candidateCount"] = 239
        self.reseal_contract(contract)
        with self.assertRaisesRegex(PermissionError, "identities changed"):
            self.validate_completed_recovery(contract=contract)

        evidence_cases = []
        changed_command = self.equivalence_evidence(self.repository_root)
        changed_command["command"] = list(reversed(changed_command["command"]))
        evidence_cases.append((changed_command, "evidence is incomplete"))
        changed_source = self.equivalence_evidence(self.repository_root)
        source_name = sorted(changed_source["testSourceSha256"])[0]
        changed_source["testSourceSha256"][source_name] = "0" * 64
        evidence_cases.append((changed_source, "test-source bytes changed"))
        changed_fixture = self.equivalence_evidence(self.repository_root)
        changed_fixture["fixtureIdentity"]["runId"] = RUN14_RUN_ID + 1
        evidence_cases.append((changed_fixture, "evidence is incomplete"))

        for evidence, expected in evidence_cases:
            with self.subTest(expected=expected):
                with self.assertRaisesRegex(PermissionError, expected):
                    build_run14_recovery_contract(
                        self.bundle,
                        entry_specs=self.entry_specs,
                        recovery_implementation_manifest=(self.recovery_implementation),
                        generated_entry_bank_path=(
                            self.shared_output / "fresh_entry_bank_v1.json"
                        ),
                        equivalence_evidence=evidence,
                    )

    def copy_original_ledger(self) -> Path:
        assert self.fixture_path is not None
        ledger = self.scratch_directory() / "ledger.jsonl"
        shutil.copyfile(self.fixture_path / "fresh_experiment_ledger_v1.jsonl", ledger)
        return ledger

    def append_resume_record(
        self,
        ledger: Path,
        status: str,
        *,
        inner: dict[str, object] | None = None,
    ) -> None:
        original = [
            json.loads(line) for line in ledger.read_text(encoding="utf-8").splitlines()
        ]
        payload = {
            "kind": "fresh-infrastructure-recovery",
            "status": status,
            "stage": "discovery",
            "recoveryAttemptId": self.recovery_attempt_id,
            "parameters": inner or {"recoveryAttemptId": self.recovery_attempt_id},
        }
        append_fresh_record(
            ledger,
            {
                "recordKind": "infrastructure-resume",
                "candidateId": f"protocol-infrastructure-resume::{status}",
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
                "preregistrationSha256": RUN14_PREREGISTRATION_SHA256,
            },
        )

    def append_authorization_and_start(self, ledger: Path) -> None:
        for status in (
            "resume_eligibility_audit",
            "resume_authorized",
            "resume_identity_verified",
            "batch_resume_started",
        ):
            self.append_resume_record(ledger, status)

    def append_batch_completed(self, ledger: Path, batch_sha: str) -> None:
        self.append_resume_record(
            ledger,
            "batch_resume_completed",
            inner={
                "recoveryAttemptId": self.recovery_attempt_id,
                "batchResultSha256": batch_sha,
                "batchResultFileSha256": "b" * 64,
                "candidateCount": 240,
                "orderedCandidateSequenceSha256": (RUN14_ORDERED_CANDIDATE_SHA256),
            },
        )

    def append_candidate_outcome(
        self, ledger: Path, candidate_index: int, batch_sha: str
    ) -> None:
        records = [
            json.loads(line) for line in ledger.read_text(encoding="utf-8").splitlines()
        ]
        batch = records[1]["parameters"]
        candidate_id = batch["candidateIds"][candidate_index]
        candidate_sha = batch["candidateSha256"][candidate_index]
        append_fresh_record(
            ledger,
            {
                "candidateId": candidate_id,
                "family": "test-family",
                "stage": "discovery",
                "trainingWindow": "discovery",
                "evaluationWindow": "discovery",
                "parameters": {
                    "recoveryAttemptId": self.recovery_attempt_id,
                    "sealedBatchResultSha256": batch_sha,
                },
                "entryVariant": "test-entry",
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
                "preregistrationSha256": RUN14_PREREGISTRATION_SHA256,
            },
        )

    def test_external_finalizer_seals_an_abrupt_recovery_batch_once(self):
        ledger = self.copy_original_ledger()
        self.append_authorization_and_start(ledger)
        self.assertTrue(finalize_interrupted_run14_recovery(ledger, exit_status=137))
        records = [
            json.loads(line) for line in ledger.read_text(encoding="utf-8").splitlines()
        ]
        self.assertEqual(records[-1]["status"], "batch_resume_error")
        self.assertEqual(records[-1]["metrics"]["externalExitStatus"], 137)
        self.assertFalse(finalize_interrupted_run14_recovery(ledger, exit_status=137))

    def test_external_finalizer_covers_each_authorization_cut_point(self):
        statuses = (
            "resume_eligibility_audit",
            "resume_authorized",
            "resume_identity_verified",
        )
        for cut_point in range(len(statuses) + 1):
            with self.subTest(cut_point=cut_point):
                ledger = self.copy_original_ledger()
                for status in statuses[:cut_point]:
                    self.append_resume_record(ledger, status)
                self.assertTrue(
                    finalize_interrupted_run14_recovery(ledger, exit_status=137)
                )

    def test_external_finalizer_counts_outcomes_after_sealed_batch(self):
        ledger = self.copy_original_ledger()
        self.append_authorization_and_start(ledger)
        batch_sha = "a" * 64
        self.append_batch_completed(ledger, batch_sha)
        self.append_candidate_outcome(ledger, 0, batch_sha)
        self.assertTrue(finalize_interrupted_run14_recovery(ledger, exit_status=137))
        records = [
            json.loads(line) for line in ledger.read_text(encoding="utf-8").splitlines()
        ]
        error = records[-1]
        self.assertEqual(error["status"], "batch_resume_error")
        self.assertEqual(error["metrics"]["candidateOutcomesAppended"], 1)
        self.assertTrue(error["metrics"]["batchResultSealed"])

    def test_external_finalizer_does_not_change_a_completed_recovery(self):
        ledger = self.copy_original_ledger()
        self.append_authorization_and_start(ledger)
        batch_sha = "a" * 64
        self.append_batch_completed(ledger, batch_sha)
        for candidate_index in range(240):
            self.append_candidate_outcome(ledger, candidate_index, batch_sha)
        self.append_resume_record(
            ledger,
            "resume_stage_completed",
            inner={
                "recoveryAttemptId": self.recovery_attempt_id,
                "batchResultSha256": batch_sha,
                "candidateCount": 240,
                "candidateOutcomeRecordCount": 240,
                "promotedCandidateIds": [],
            },
        )
        before = ledger.read_bytes()
        self.assertFalse(finalize_interrupted_run14_recovery(ledger, exit_status=1))
        self.assertEqual(ledger.read_bytes(), before)


if __name__ == "__main__":
    unittest.main()
