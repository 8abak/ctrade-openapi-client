from __future__ import annotations

import json
import shutil
import unittest
from contextlib import contextmanager
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Iterator
from uuid import uuid4

from datavis.research.fresh_bootstrap import (
    FRESH_REGISTERED_ELIGIBLE_ANCHORS,
    FRESH_SOURCE_FIRST_ANCHOR,
    FreshBootstrapConfig,
    build_fresh_source_bootstrap,
    registered_fresh_bootstrap_config,
    write_fresh_source_bootstrap,
)
from datavis.research.fresh_data import FreshDataConfig
from datavis.research.fresh_bootstrap_cli import build_parser, main
from datavis.research.fresh_protocol import FreshWindowPolicy
from datavis.research.fresh_sessions import SessionAuditConfig, broker_session_bounds


class FakeCursor:
    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows
        self.position = 0
        self.itersize = 0
        self.arraysize = 0

    def execute(self, _query: str, _parameters: tuple[Any, ...]) -> None:
        return None

    def fetchmany(self, size: int):
        selected = self.rows[self.position : self.position + size]
        self.position += len(selected)
        return selected

    def close(self) -> None:
        return None


class FakeConnection:
    autocommit = False

    def __init__(self, rows: list[tuple[Any, ...]]) -> None:
        self.rows = rows

    def cursor(self, *, name: str) -> FakeCursor:
        if not name.startswith("fresh_inventory_"):
            raise AssertionError(name)
        return FakeCursor(self.rows)


def small_config() -> FreshBootstrapConfig:
    data = FreshDataConfig(
        session_audit=SessionAuditConfig(
            open_tolerance_seconds=120,
            close_tolerance_seconds=120,
            friday_close_tolerance_seconds=600,
            unexpected_gap_seconds=100_000,
        ),
        expected_symbol="XAUUSD",
        chunk_rows=2,
        expected_anchors=(),
        maximum_issue_samples=2,
    )
    return FreshBootstrapConfig(
        first_anchor=date(2026, 1, 2),
        last_anchor=date(2026, 1, 5),
        data_config=data,
        window_policy=FreshWindowPolicy(
            discovery_sessions=1,
            walk_forward_sessions=(1, 1, 1),
            validation_sessions=1,
            holdout_sessions=1,
        ),
        expected_eligible_sessions=6,
    )


class FreshBootstrapTests(unittest.TestCase):
    def test_cli_requires_an_explicit_execute_flag(self):
        arguments = build_parser().parse_args(["--output-dir", "unused"])
        self.assertFalse(arguments.execute)
        with self.assertRaisesRegex(SystemExit, "without --execute"):
            main(["--output-dir", "unused"])

    def test_registered_range_contains_the_actual_earliest_partial_session(self):
        config = registered_fresh_bootstrap_config()
        self.assertEqual(config.first_anchor, FRESH_SOURCE_FIRST_ANCHOR)
        self.assertEqual(config.first_anchor, date(2025, 12, 31))
        self.assertEqual(config.expected_eligible_sessions, 115)
        self.assertEqual(config.window_policy.required_sessions, 115)
        self.assertEqual(config.expected_eligible_anchors, FRESH_REGISTERED_ELIGIBLE_ANCHORS)
        self.assertEqual(len(config.expected_eligible_anchors), 115)
        self.assertEqual(config.expected_eligible_anchors[0], "2026-01-02")
        self.assertEqual(config.expected_eligible_anchors[-1], "2026-07-17")
        self.assertEqual(config.window_policy.discovery_sessions, 40)
        self.assertEqual(config.window_policy.walk_forward_sessions, (10, 10, 10))
        self.assertEqual(config.window_policy.validation_sessions, 15)
        self.assertEqual(config.window_policy.holdout_sessions, 30)

    def test_config_rejects_policy_count_disagreement(self):
        config = small_config()
        with self.assertRaisesRegex(ValueError, "must agree"):
            replace(config, expected_eligible_sessions=5)
        with self.assertRaisesRegex(ValueError, "chronological weekdays"):
            replace(
                config,
                expected_eligible_anchors=(
                    "2026-01-02",
                    "2026-01-05",
                    "2026-01-06",
                    "2026-01-07",
                    "2026-01-08",
                    "2026-01-08",
                ),
            )

    def test_scan_aborts_before_split_when_qc_count_differs(self):
        config = small_config()

        @contextmanager
        def empty_factory() -> Iterator[FakeConnection]:
            yield FakeConnection([])

        with self.assertRaisesRegex(RuntimeError, "Outcome analysis is forbidden"):
            build_fresh_source_bootstrap(empty_factory, config=config)

    def test_strategy_neutral_scan_freezes_split_and_progress(self):
        policy = FreshWindowPolicy(
            discovery_sessions=1,
            walk_forward_sessions=(1, 1, 1),
            validation_sessions=1,
            holdout_sessions=1,
        )
        anchors: list[date] = []
        cursor = date(2026, 1, 2)
        while len(anchors) < policy.required_sessions + 1:
            if cursor.weekday() < 5:
                anchors.append(cursor)
            cursor += timedelta(days=1)
        config = replace(
            small_config(),
            first_anchor=anchors[0],
            last_anchor=anchors[-1],
            expected_eligible_anchors=tuple(
                anchor.isoformat() for anchor in anchors[1:]
            ),
        )
        factory_anchors = iter(anchors)

        @contextmanager
        def factory() -> Iterator[FakeConnection]:
            anchor = next(factory_anchors)
            if anchor == anchors[0]:
                yield FakeConnection([])
                return
            bounds = broker_session_bounds(anchor)
            position = anchors.index(anchor)
            rows = [
                (1_000 + position * 2, "XAUUSD", bounds.start_utc + timedelta(seconds=1), 100.0, 100.2),
                (1_001 + position * 2, "XAUUSD", bounds.end_utc - timedelta(seconds=1), 100.1, 100.3),
            ]
            yield FakeConnection(rows)

        progress: list[dict[str, Any]] = []
        artifacts = build_fresh_source_bootstrap(
            factory,
            config=config,
            on_progress=lambda item: progress.append(dict(item)),
        )
        self.assertEqual(len(progress), 7)
        self.assertFalse(progress[0]["isComplete"])
        self.assertTrue(all(item["isComplete"] for item in progress[1:]))
        self.assertEqual(artifacts["inventory"]["eligibleSessionCount"], 6)
        self.assertEqual(artifacts["split"]["sessionCount"], 6)
        text = repr(artifacts).lower()
        for forbidden in ("pnl", "barrier", "signal", "trade_pnl"):
            self.assertNotIn(forbidden, text)

    def test_artifact_writer_refuses_overwrite_and_cleans_partial_publish(self):
        payload = {
            "inventory": {"a": 1},
            "corpus": {"b": 2},
            "split": {"c": 3},
        }
        temporary = Path.cwd() / f"fresh_bootstrap_test_{uuid4().hex}"
        try:
            output = temporary / "artifacts"
            written = write_fresh_source_bootstrap(output, payload)
            for path in written.values():
                self.assertTrue(Path(path).is_file())
                json.loads(Path(path).read_text(encoding="utf-8"))
            with self.assertRaises(FileExistsError):
                write_fresh_source_bootstrap(output, payload)
        finally:
            shutil.rmtree(temporary, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
