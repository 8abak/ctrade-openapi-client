from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from datavis.market_profile import (
    DEFAULT_PROFILE_BIN_SIZE,
    MarketProfileConfig,
    MarketProfileProcessor,
    compute_profile_metrics,
    session_bounds,
)


def make_tick(tick_id: int, timestamp: datetime, mid: float) -> dict:
    return {
        "id": tick_id,
        "symbol": "XAUUSD",
        "timestamp": timestamp,
        "bid": mid - 0.05,
        "ask": mid + 0.05,
        "mid": mid,
        "price": mid,
    }


class MarketProfileValidation(unittest.TestCase):
    def test_dwell_time_is_capped(self) -> None:
        config = MarketProfileConfig(maxgapms=1500).normalized()
        processor = MarketProfileProcessor(config)
        base = datetime(2026, 4, 1, 0, 0, tzinfo=timezone.utc)
        self.assertIsNone(processor.process_tick(make_tick(1, base, 3300.0)))
        result = processor.process_tick(make_tick(2, base + timedelta(seconds=5), 3300.2))
        self.assertIsNotNone(result)
        self.assertEqual(int(result["weightms"]), 1500)

    def test_session_boundary_does_not_bleed_into_next_session(self) -> None:
        config = MarketProfileConfig(sessionstarthour=8).normalized()
        processor = MarketProfileProcessor(config)
        tick_time = datetime(2026, 4, 1, 20, 59, 59, tzinfo=timezone.utc)
        next_time = tick_time + timedelta(seconds=5)
        self.assertIsNone(processor.process_tick(make_tick(1, tick_time, 3300.0)))
        result = processor.process_tick(make_tick(2, next_time, 3300.2))
        self.assertIsNotNone(result)
        session_start, session_end = session_bounds(tick_time, config)
        expected_ms = int((session_end - tick_time).total_seconds() * 1000.0)
        self.assertEqual(int(result["weightms"]), expected_ms)
        self.assertTrue(result["sessionchanged"])
        self.assertEqual(result["sessionstart"], session_start)

    def test_value_area_expands_from_poc(self) -> None:
        metrics = compute_profile_metrics(
            [
                {"pricebin": 3299.9, "weightms": 1000, "tickcount": 10},
                {"pricebin": 3300.0, "weightms": 4000, "tickcount": 40},
                {"pricebin": 3300.1, "weightms": 2500, "tickcount": 25},
                {"pricebin": 3300.2, "weightms": 500, "tickcount": 5},
            ],
            binsize=DEFAULT_PROFILE_BIN_SIZE,
            valueareapercent=0.70,
            nodelimit=3,
        )
        self.assertEqual(metrics["poc"], 3300.0)
        self.assertEqual(metrics["val"], 3300.0)
        self.assertEqual(metrics["vah"], 3300.1)

    def test_nodes_are_derived_from_local_extrema(self) -> None:
        metrics = compute_profile_metrics(
            [
                {"pricebin": 3299.8, "weightms": 900, "tickcount": 9},
                {"pricebin": 3299.9, "weightms": 2500, "tickcount": 25},
                {"pricebin": 3300.0, "weightms": 800, "tickcount": 8},
                {"pricebin": 3300.1, "weightms": 2600, "tickcount": 26},
                {"pricebin": 3300.2, "weightms": 700, "tickcount": 7},
            ],
            binsize=DEFAULT_PROFILE_BIN_SIZE,
            valueareapercent=0.70,
            nodelimit=3,
        )
        hvn_prices = {round(node["price"], 2) for node in metrics["hvns"]}
        lvn_prices = {round(node["price"], 2) for node in metrics["lvns"]}
        self.assertIn(3299.9, hvn_prices)
        self.assertIn(3300.0, lvn_prices)


if __name__ == "__main__":
    unittest.main()
