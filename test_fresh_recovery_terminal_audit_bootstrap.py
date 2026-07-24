from __future__ import annotations

import hashlib
from pathlib import Path
import shutil
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import uuid

from datavis.research import fresh_recovery_terminal_audit_bootstrap as bootstrap


ROOT = Path(__file__).resolve().parent
AUDITOR = ROOT / "datavis/research/fresh_recovery_terminal_audit.py"


class FreshRecoveryAuditBootstrapTests(unittest.TestCase):
    def test_frozen_auditor_digest_matches_exact_source(self) -> None:
        self.assertEqual(
            bootstrap.FROZEN_AUDITOR_SHA256,
            hashlib.sha256(AUDITOR.read_bytes()).hexdigest(),
        )

    def test_verified_loader_executes_only_frozen_bytes(self) -> None:
        with patch.object(bootstrap, "_reject_preloaded_datavis"):
            module = bootstrap._load_verified_auditor(
                auditor_path=AUDITOR,
                expected_sha256=bootstrap.FROZEN_AUDITOR_SHA256,
            )
        self.assertTrue(
            callable(module.audit_recovery_scientific_gate)
        )
        self.assertEqual(
            module.AUDIT_SCHEMA,
            "fresh-xauusd-v5-recovery-scientific-gate-audit/v1",
        )

    def test_changed_auditor_and_wrong_requested_digest_fail_closed(self) -> None:
        temporary = ROOT / f".fresh-recovery-bootstrap-{uuid.uuid4().hex}"
        temporary.mkdir(mode=0o777)
        self.addCleanup(shutil.rmtree, temporary, True)
        changed = temporary / AUDITOR.name
        changed.write_bytes(AUDITOR.read_bytes() + b"\n")
        with (
            patch.object(bootstrap, "_reject_preloaded_datavis"),
            self.assertRaisesRegex(
                bootstrap.FreshRecoveryAuditBootstrapError,
                "bytes changed",
            ),
        ):
            bootstrap._load_verified_auditor(
                auditor_path=changed,
                expected_sha256=bootstrap.FROZEN_AUDITOR_SHA256,
            )
        with self.assertRaisesRegex(
            bootstrap.FreshRecoveryAuditBootstrapError,
            "differs from the frozen seal",
        ):
            bootstrap._sealed_auditor_sha256("0" * 64)

    def test_preloaded_scientific_modules_are_rejected(self) -> None:
        with self.assertRaisesRegex(
            bootstrap.FreshRecoveryAuditBootstrapError,
            "imported before auditor verification",
        ):
            bootstrap._reject_preloaded_datavis()

    def test_bootstrap_receipt_cannot_authorize_a_strategy_conclusion(self) -> None:
        gate = {
            "schema": (
                "fresh-xauusd-v5-recovery-scientific-gate-audit/v1"
            ),
            "status": "recovery_scientific_gate_verified",
            "scientificResultInterpreted": False,
            "finalStrategyConclusionAuthorized": False,
        }
        fake = SimpleNamespace(
            audit_recovery_scientific_gate=lambda *_args, **_kwargs: gate
        )
        with (
            patch.object(bootstrap, "_runtime_failures", return_value=()),
            patch.object(
                bootstrap,
                "_load_verified_auditor",
                return_value=fake,
            ),
        ):
            receipt = bootstrap.run_verified_auditor(
                terminal_output_directory="/terminal",
                launch_source_root="/launch",
                expected_launch_commit_sha="a" * 40,
                recorded_remote_repository_root=(
                    "/tmp/fresh-xauusd-v5-recovery-worktree.Sealed"
                ),
                auditor_sha256=bootstrap.FROZEN_AUDITOR_SHA256,
            )
        self.assertEqual(receipt["schema"], bootstrap.BOOTSTRAP_SCHEMA)
        self.assertEqual(receipt["audit"], gate)
        self.assertFalse(
            receipt["audit"]["finalStrategyConclusionAuthorized"]
        )

    def test_runtime_contract_requires_isolated_cpython_311(self) -> None:
        failures = bootstrap._runtime_failures()
        if failures:
            self.assertTrue(
                any(
                    "3.11" in failure
                    or "runtime flag" in failure
                    or "CPython" in failure
                    for failure in failures
                )
            )
        else:
            self.assertEqual((), failures)


if __name__ == "__main__":
    unittest.main()
