from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd


PROJECT_DIR = Path(__file__).resolve().parent / "jupyter" / "codexAnalyze"
sys.path.insert(0, str(PROJECT_DIR))

import hot_zone_study  # noqa: E402


class HotZoneCausalityTests(unittest.TestCase):
    def test_expanding_average_is_unchanged_when_only_future_quotes_change(self) -> None:
        timestamps = pd.date_range("2026-02-11T00:00:00Z", periods=10, freq="1s")
        ticks = pd.DataFrame({
            "id": range(1, 11),
            "timestamp_utc": timestamps,
            "bid": [100.0 + value for value in range(10)],
            "ask": [100.2 + value for value in range(10)],
            "spread": [0.2] * 10,
        })
        ticks["mid"] = (ticks["bid"] + ticks["ask"]) / 2
        changed = ticks.copy()
        changed.loc[6:, ["bid", "ask", "mid"]] += 1000

        original = hot_zone_study.second_bars(ticks)
        perturbed = hot_zone_study.second_bars(changed)

        pd.testing.assert_series_equal(
            original["quote_average"].iloc[:6], perturbed["quote_average"].iloc[:6]
        )
        pd.testing.assert_series_equal(
            original["time_average"].iloc[:6], perturbed["time_average"].iloc[:6]
        )


if __name__ == "__main__":
    unittest.main()
