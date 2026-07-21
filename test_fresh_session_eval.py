from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

import pandas as pd

from datavis.research.fresh_entry_diagnostics import (
    EntryDiagnosticRejection,
    FreshEntryDiagnosticsResult,
    FrozenSignalEvent,
)
from datavis.research.fresh_session_eval import (
    combine_entry_diagnostics,
    corpus_bindings_from_manifest,
    decision_feature_rows,
    volatility_rows,
)


UTC = timezone.utc


def event(index: int) -> FrozenSignalEvent:
    return FrozenSignalEvent(
        tick_index=index,
        tick_id=100 + index,
        timestamp=datetime(2026, 1, 2, tzinfo=UTC) + timedelta(seconds=index),
        side="long",
        metadata={},
    )


def rejection(position: int) -> EntryDiagnosticRejection:
    selected = event(position)
    return EntryDiagnosticRejection(
        event_position=position,
        event=selected,
        reason="test-rejection",
        observed_timestamp=selected.timestamp,
        ready_timestamp=selected.timestamp,
        expires_timestamp=selected.timestamp + timedelta(seconds=1),
        scheduling_release_timestamp=selected.timestamp,
    )


class FreshSessionEvaluationTests(unittest.TestCase):
    def test_corpus_bindings_require_unique_chronological_sessions(self):
        manifest = {
            "sessions": [
                {
                    "sessionAnchor": "2026-01-02",
                    "normalizedQuoteCount": 10,
                    "normalizedSha256": "a" * 64,
                    "eligible": True,
                },
                {
                    "sessionAnchor": "2026-01-05",
                    "normalizedQuoteCount": 20,
                    "normalizedSha256": "b" * 64,
                    "eligible": False,
                },
            ]
        }
        bindings = corpus_bindings_from_manifest(manifest)
        self.assertEqual(tuple(bindings), ("2026-01-02", "2026-01-05"))
        self.assertTrue(bindings["2026-01-02"].eligible)

        reversed_manifest = {"sessions": list(reversed(manifest["sessions"]))}
        with self.assertRaisesRegex(ValueError, "chronological"):
            corpus_bindings_from_manifest(reversed_manifest)

    def test_combiner_renumbers_events_and_reason_counts(self):
        first = FreshEntryDiagnosticsResult(
            diagnostics=(),
            rejections=(rejection(0),),
            rejected_reason_counts={"test-rejection": 1},
            event_count=1,
        )
        second = FreshEntryDiagnosticsResult(
            diagnostics=(),
            rejections=(rejection(0), rejection(1)),
            rejected_reason_counts={"test-rejection": 2},
            event_count=2,
        )
        combined = combine_entry_diagnostics((first, second))
        self.assertEqual(combined.event_count, 3)
        self.assertEqual(
            tuple(item.event_position for item in combined.rejections),
            (0, 1, 2),
        )
        self.assertEqual(combined.rejected_reason_counts, {"test-rejection": 3})

    def test_exact_feature_rows_convert_nan_to_unavailable(self):
        timestamps = pd.to_datetime(
            ["2026-01-02T00:00:00Z", "2026-01-02T00:00:01Z"], utc=True
        )
        frame = pd.DataFrame(
            {
                "tick_id": [1, 2],
                "timestamp": timestamps,
                "bid": [100.0, 100.1],
                "ask": [100.2, 100.3],
                "1s_mid_speed": [float("nan"), 0.5],
                "1s_mid_acceleration": [float("nan"), -0.2],
                "1s_bollinger_std": [float("nan"), 0.3],
            }
        )
        decision_rows = decision_feature_rows(
            frame,
            velocity_column="1s_mid_speed",
            acceleration_column="1s_mid_acceleration",
        )
        self.assertIsNone(decision_rows[0].velocity)
        self.assertEqual(decision_rows[1].velocity, 0.5)
        self.assertEqual(decision_rows[1].bid, 100.1)
        vol_rows = volatility_rows(frame, column="1s_bollinger_std")
        self.assertIsNone(vol_rows[0].value)
        self.assertEqual(vol_rows[1].value, 0.3)


if __name__ == "__main__":
    unittest.main()
