from __future__ import annotations

import json
import math
import unittest
import uuid
from datetime import date, timedelta
from pathlib import Path

from datavis.research.fresh_protocol import (
    FreshWindowPolicy,
    append_fresh_record,
    authorize_evaluation,
    build_fresh_split_manifest,
    canonical_hash,
    finite_metrics,
    frozen_research_fingerprint,
)


INVENTORY_HASH = "a" * 64
POLICY = FreshWindowPolicy(
    discovery_sessions=46,
    walk_forward_sessions=(12, 12, 12),
    validation_sessions=21,
    holdout_sessions=36,
)


def weekday_anchors(count: int) -> list[str]:
    result: list[str] = []
    current = date(2026, 1, 2)
    while len([value for value in result if date.fromisoformat(value).weekday() < 5]) < count:
        if current.weekday() < 5:
            result.append(current.isoformat())
        current += timedelta(days=1)
    return result


class FreshProtocolTests(unittest.TestCase):
    def test_split_is_exact_chronological_and_holdout_is_newest(self):
        days = weekday_anchors(139)
        manifest = build_fresh_split_manifest(
            days,
            inventory_sha256=INVENTORY_HASH,
            excluded_sessions=[{"sessionAnchor": "2026-01-01", "reason": "missing prefix"}],
            policy=POLICY,
        )
        self.assertEqual(manifest["sessionCount"], 139)
        self.assertEqual(manifest["windows"]["discovery"]["sessionCount"], 46)
        self.assertEqual(manifest["windows"]["validation"]["sessionCount"], 21)
        self.assertEqual(manifest["windows"]["holdout"]["sessionCount"], 36)
        self.assertEqual(
            manifest["windows"]["holdout"]["firstSessionAnchor"], days[-36]
        )
        self.assertEqual(manifest["windows"]["holdout"]["lastSessionAnchor"], days[-1])
        json.dumps(manifest, allow_nan=False)

    def test_weekend_and_nonchronological_anchors_are_rejected(self):
        days = weekday_anchors(139)
        saturday = "2026-01-03"
        with self.assertRaisesRegex(ValueError, "weekdays"):
            build_fresh_split_manifest(
                [saturday, *days[1:]],
                inventory_sha256=INVENTORY_HASH,
                excluded_sessions=[{"reason": "partial"}],
                policy=POLICY,
            )
        with self.assertRaisesRegex(ValueError, "chronological"):
            build_fresh_split_manifest(
                [days[1], days[0], *days[2:]],
                inventory_sha256=INVENTORY_HASH,
                excluded_sessions=[{"reason": "partial"}],
                policy=POLICY,
            )

    def test_wrong_session_count_and_missing_partial_evidence_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "requires 139"):
            build_fresh_split_manifest(
                weekday_anchors(138),
                inventory_sha256=INVENTORY_HASH,
                excluded_sessions=[{"reason": "partial"}],
                policy=POLICY,
            )
        with self.assertRaisesRegex(ValueError, "excluded_sessions"):
            build_fresh_split_manifest(
                weekday_anchors(139),
                inventory_sha256=INVENTORY_HASH,
                excluded_sessions=[],
                policy=POLICY,
            )

    def test_holdout_requires_one_frozen_authorized_unconsumed_attempt(self):
        manifest = build_fresh_split_manifest(
            weekday_anchors(139),
            inventory_sha256=INVENTORY_HASH,
            excluded_sessions=[{"reason": "partial"}],
            policy=POLICY,
        )
        with self.assertRaises(PermissionError):
            authorize_evaluation("holdout", split_manifest=manifest)
        authorization = authorize_evaluation(
            "holdout",
            split_manifest=manifest,
            frozen_strategy_sha256="b" * 64,
            explicit_holdout_authorization=True,
        )
        self.assertEqual(authorization["frozenStrategySha256"], "b" * 64)
        with self.assertRaisesRegex(PermissionError, "already been attempted"):
            authorize_evaluation(
                "holdout",
                split_manifest=manifest,
                access_records=[authorization],
                frozen_strategy_sha256="b" * 64,
                explicit_holdout_authorization=True,
            )

    def test_fingerprint_changes_with_execution_or_candidate_grid(self):
        common = dict(
            split_manifest={"split": 1},
            data_manifest={"data": 1},
            feature_specification={"feature": 1},
            execution_config={"latency": 300},
            candidate_grid={"family": [1, 2]},
            code_identifier="commit-a",
        )
        first = frozen_research_fingerprint(**common)
        second = frozen_research_fingerprint(
            **{**common, "execution_config": {"latency": 500}}
        )
        self.assertNotEqual(first["researchSha256"], second["researchSha256"])

    def test_ledger_is_numbered_append_only_and_json_safe(self):
        directory = Path(__file__).resolve().parent / "artifacts" / "test-fresh-protocol"
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{uuid.uuid4().hex}-ledger.jsonl"
        self.addCleanup(path.unlink, missing_ok=True)
        self.addCleanup(path.with_name(path.name + ".lock").unlink, missing_ok=True)
        record = {
            "candidateId": "fresh-1",
            "family": "acceleration",
            "stage": "discovery",
            "trainingWindow": "discovery",
            "evaluationWindow": "walk_forward_1",
            "parameters": {},
            "entryVariant": "onset",
            "exitVariant": "entry-edge-only",
            "metrics": {"netPnl": 0.0},
            "status": "tested",
            "leakageChecks": {"prefixInvariant": True},
        }
        first = append_fresh_record(path, record)
        second = append_fresh_record(path, {**record, "candidateId": "fresh-2"})
        self.assertEqual((first["recordNumber"], second["recordNumber"]), (1, 2))
        lines = [json.loads(line) for line in path.read_text().splitlines()]
        self.assertEqual(len(lines), 2)
        with self.assertRaises(ValueError):
            append_fresh_record(
                path, {**record, "metrics": {"netPnl": math.inf}}
            )

    def test_hash_and_metric_finiteness(self):
        self.assertEqual(canonical_hash({"a": 1}), canonical_hash({"a": 1}))
        self.assertTrue(finite_metrics({"a": [1.0, None]}))
        self.assertFalse(finite_metrics({"a": float("nan")}))


if __name__ == "__main__":
    unittest.main()
