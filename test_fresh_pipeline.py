from __future__ import annotations

import unittest
from types import SimpleNamespace

import pandas as pd

from datavis.research.fresh_pipeline import (
    BASELINE_MINIMUM_UPLIFT,
    SESSION_CLOSE_SAFETY_MS,
    _baseline_events,
    _cluster_entry_edge,
)
from datavis.research.fresh_pipeline_cli import main
from datavis.research.fresh_preregistration import (
    required_fresh_implementation_files,
)
from datavis.research.fresh_session_eval import FreshSessionTape
from datavis.research.fresh_sessions import broker_session_bounds
from datavis.research.ticks import Tick


def result(*, successes_10: int, successes_30: int, count: int):
    diagnostics = []
    for position in range(count):
        diagnostics.append(
            SimpleNamespace(
                event=SimpleNamespace(side="long"),
                cost_covered_by_10s=position < successes_10,
                cost_covered_by_30s=position < successes_30,
            )
        )
    return SimpleNamespace(diagnostics=tuple(diagnostics))


class FreshPipelineTests(unittest.TestCase):
    def test_direction_matched_cluster_uplift_is_deterministic(self):
        candidates = [result(successes_10=8, successes_30=9, count=10) for _ in range(12)]
        baselines = [result(successes_10=4, successes_30=5, count=10) for _ in range(12)]
        first = _cluster_entry_edge(candidates, baselines, seed_text="frozen")
        second = _cluster_entry_edge(candidates, baselines, seed_text="frozen")
        self.assertEqual(first, second)
        self.assertGreater(first["10"]["uplift"], BASELINE_MINIMUM_UPLIFT)
        self.assertGreater(first["10"]["upliftInterval"][0], 0.0)
        self.assertGreater(first["30"]["upliftInterval"][0], 0.0)

    def test_baseline_is_stratified_and_excludes_session_close_buffer(self):
        bounds = broker_session_bounds("2026-01-02")
        inside = bounds.end_utc - pd.Timedelta(milliseconds=SESSION_CLOSE_SAFETY_MS + 1)
        excluded = bounds.end_utc - pd.Timedelta(milliseconds=SESSION_CLOSE_SAFETY_MS)
        ticks = (
            Tick(1, inside, 100.0, 100.2),
            Tick(2, excluded, 100.1, 100.3),
        )
        tape = FreshSessionTape("2026-01-02", bounds, ticks, "a" * 64)
        frame = pd.DataFrame(
            {
                "tick_id": [1, 2],
                "timestamp": pd.to_datetime([item.timestamp for item in ticks], utc=True),
                "feature_ready": [True, True],
                "gap_detected": [False, False],
            }
        )
        events = _baseline_events(frame, tape)
        self.assertEqual([(item.tick_id, item.side) for item in events], [(1, "long"), (1, "short")])

    def test_manifest_binding_covers_the_complete_runner(self):
        paths = required_fresh_implementation_files()
        self.assertIn("datavis/research/fresh_pipeline.py", paths)
        self.assertIn("datavis/research/fresh_scoring.py", paths)
        self.assertIn("datavis/research/fresh_candidate_grid.py", paths)

    def test_cli_requires_explicit_execute(self):
        with self.assertRaisesRegex(SystemExit, "without --execute"):
            main(["--output-dir", "unused"])


if __name__ == "__main__":
    unittest.main()
