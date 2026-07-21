from __future__ import annotations

import copy
import unittest

import numpy as np
import pandas as pd

from datavis.research.fresh_thresholds import (
    FreshQuantileBankConfig,
    QuantileMeasurementSpec,
    SessionBalancedQuantileFitter,
    fit_session_balanced_quantiles,
    fresh_quantile_bank_from_payload,
    fresh_quantile_bank_payload,
)


CONFIG = FreshQuantileBankConfig(
    ranks=(0.25, 0.5, 0.75),
    minimum_finite_values_per_session=3,
    minimum_eligible_sessions=2,
)


def frame(values: list[float]) -> pd.DataFrame:
    return pd.DataFrame(
        {
            "feature_ready": [True] * len(values),
            "gap_detected": [False] * len(values),
            "speed": values,
            "signed": [-value for value in values],
        }
    )


class FreshThresholdTests(unittest.TestCase):
    def test_quantile_bank_payload_round_trip_and_tamper_detection(self):
        fitted = fit_session_balanced_quantiles(
            {
                "2026-01-02": frame([1, 2, 3, 4, 5]),
                "2026-01-05": frame([2, 3, 4, 5, 6]),
            },
            measurements=(
                QuantileMeasurementSpec("speed", "speed", "identity"),
            ),
            config=CONFIG,
        )
        payload = fresh_quantile_bank_payload(fitted)
        self.assertEqual(fresh_quantile_bank_from_payload(payload), fitted)
        tampered = copy.deepcopy(payload)
        tampered["thresholds"][0]["value"] += 0.1
        with self.assertRaisesRegex(ValueError, "hash"):
            fresh_quantile_bank_from_payload(tampered)

    def test_day_balanced_quantiles_do_not_weight_busy_session_more(self):
        frames = {
            "2026-01-02": frame([1, 2, 3, 4, 5]),
            "2026-01-05": frame([100, 200, 300]),
        }
        bank = fit_session_balanced_quantiles(
            frames,
            measurements=[QuantileMeasurementSpec("speed", "speed", "identity")],
            config=CONFIG,
        )
        # Session medians are 3 and 200; their day-balanced median is 101.5.
        self.assertEqual(bank.threshold("speed", 0.5), 101.5)
        self.assertEqual(bank.training_session_anchors, ("2026-01-02", "2026-01-05"))
        self.assertEqual(len(bank.bank_sha256), 64)

    def test_absolute_positive_and_readiness_filters_are_explicit(self):
        first = frame([-4, -3, -2, -1, 0, 1, 2, 3, 4])
        first.loc[0, "feature_ready"] = False
        first.loc[1, "gap_detected"] = True
        second = frame([-8, -6, -4, -2, 0, 2, 4, 6, 8])
        bank = fit_session_balanced_quantiles(
            {"2026-01-02": first, "2026-01-05": second},
            measurements=[
                QuantileMeasurementSpec("absolute", "speed", "absolute"),
                QuantileMeasurementSpec("positive", "speed", "positive"),
            ],
            config=CONFIG,
        )
        self.assertGreater(bank.threshold("absolute", 0.5), 0)
        self.assertGreater(bank.threshold("positive", 0.25), 0)

    def test_future_or_outcome_columns_and_insufficient_data_fail(self):
        with self.assertRaisesRegex(ValueError, "outcome"):
            QuantileMeasurementSpec("bad", "future_profit", "identity")
        with self.assertRaisesRegex(ValueError, "eligible sessions"):
            fit_session_balanced_quantiles(
                {"2026-01-02": frame([1, 2, np.nan])},
                measurements=[QuantileMeasurementSpec("speed", "speed", "identity")],
                config=CONFIG,
            )

    def test_incremental_fitter_matches_mapping_and_freezes_once(self):
        frames = {
            "2026-01-02": frame([1, 2, 3, 4]),
            "2026-01-05": frame([5, 6, 7, 8]),
        }
        spec = [QuantileMeasurementSpec("speed", "speed", "identity")]
        expected = fit_session_balanced_quantiles(
            frames, measurements=spec, config=CONFIG
        )
        fitter = SessionBalancedQuantileFitter(measurements=spec, config=CONFIG)
        for anchor, values in frames.items():
            fitter.add_session(anchor, values)
        actual = fitter.freeze()
        self.assertEqual(actual, expected)
        with self.assertRaisesRegex(RuntimeError, "frozen"):
            fitter.freeze()
        with self.assertRaisesRegex(RuntimeError, "frozen"):
            fitter.add_session("2026-01-06", frame([1, 2, 3]))
        with self.assertRaisesRegex(ValueError, "chronological"):
            fit_session_balanced_quantiles(
                {
                    "2026-01-05": frame([1, 2, 3]),
                    "2026-01-02": frame([1, 2, 3]),
                },
                measurements=[QuantileMeasurementSpec("speed", "speed", "identity")],
                config=CONFIG,
            )


if __name__ == "__main__":
    unittest.main()
