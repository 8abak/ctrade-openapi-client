from __future__ import annotations

import unittest
from datetime import datetime, timezone

from datavis.motion_trade_spots import (
    DEFAULT_SIGNAL_RULE,
    MotionModelScenario,
    SignalSummaryAggregate,
    build_signal_candidate,
    build_named_signal_config,
)


class MotionTradeSpotScenarioTests(unittest.TestCase):
    def test_continuation_candidate_uses_named_config_thresholds(self):
        config = build_named_signal_config(DEFAULT_SIGNAL_RULE)
        candidate = build_signal_candidate(
            tick_row={
                "id": 101,
                "timestamp": datetime(2026, 4, 25, 0, 0, tzinfo=timezone.utc),
                "bid": 3300.10,
                "ask": 3300.30,
                "mid": 3300.20,
                "spread": 0.20,
            },
            points={
                3: {
                    "motionstate": "fast_up",
                    "efficiency": 0.61,
                    "spreadmultiple": 3.2,
                    "velocity": 0.12,
                    "acceleration": 0.03,
                },
                10: {
                    "motionstate": "building_up",
                    "velocity": 0.04,
                    "acceleration": -0.005,
                    "efficiency": 0.55,
                    "spreadmultiple": 2.9,
                },
                30: {"motionstate": "quiet"},
            },
            last_signal_at={},
            config=config,
        )

        self.assertIsNotNone(candidate)
        self.assertEqual(candidate.side, "buy")
        self.assertEqual(candidate.signalrule, DEFAULT_SIGNAL_RULE)
        self.assertAlmostEqual(candidate.riskfreeprice, 3300.60)
        self.assertAlmostEqual(candidate.targetprice, 3301.30)
        self.assertAlmostEqual(candidate.stopprice, 3299.30)

    def test_micro_burst_scenario_respects_ratio_and_state_filters(self):
        scenario = MotionModelScenario(
            id=1,
            scenarioname="strict_micro_burst eff=0.65",
            signalrule="scenario_strict_micro_burst",
            family="strict_micro_burst",
            min_efficiency3=0.65,
            min_spreadmultiple3=3.0,
            max_spreadmultiple3=5.0,
            require_state10="choppy",
            require_state30="choppy",
            allow_state3=frozenset({"fast_up", "fast_down"}),
            velocity10_ratio_max=0.5,
            cooldownsec=20,
            riskfreeusd=0.20,
            targetusd=0.70,
            stopusd=0.70,
            lookaheadsec=120,
            isactive=True,
            createdat=None,
        )
        candidate = build_signal_candidate(
            tick_row={
                "id": 202,
                "timestamp": datetime(2026, 4, 25, 0, 0, tzinfo=timezone.utc),
                "bid": 3300.10,
                "ask": 3300.30,
                "mid": 3300.20,
                "spread": 0.20,
            },
            points={
                3: {
                    "motionstate": "fast_up",
                    "efficiency": 0.66,
                    "spreadmultiple": 4.2,
                    "velocity": 0.18,
                    "acceleration": 0.08,
                },
                10: {
                    "motionstate": "choppy",
                    "velocity": 0.08,
                    "acceleration": 0.0,
                },
                30: {"motionstate": "choppy"},
            },
            last_signal_at={},
            config=scenario.signal_config(),
        )
        rejected = build_signal_candidate(
            tick_row={
                "id": 203,
                "timestamp": datetime(2026, 4, 25, 0, 1, tzinfo=timezone.utc),
                "bid": 3300.10,
                "ask": 3300.30,
                "mid": 3300.20,
                "spread": 0.20,
            },
            points={
                3: {
                    "motionstate": "fast_up",
                    "efficiency": 0.66,
                    "spreadmultiple": 4.2,
                    "velocity": 0.18,
                    "acceleration": 0.08,
                },
                10: {
                    "motionstate": "choppy",
                    "velocity": 0.12,
                    "acceleration": 0.0,
                },
                30: {"motionstate": "choppy"},
            },
            last_signal_at={},
            config=scenario.signal_config(),
        )

        self.assertIsNotNone(candidate)
        self.assertEqual(candidate.side, "buy")
        self.assertIsNone(rejected)

    def test_signal_summary_aggregate_applies_constraints_and_profit_proxy(self):
        summary = SignalSummaryAggregate()
        for index in range(40):
            summary.observe(
                {
                    "outcome": "target_before_stop",
                    "seconds_to_riskfree": 8 + (index % 3),
                    "maxadverse": 1.2,
                    "score": 80.0,
                }
            )
        for index in range(20):
            summary.observe(
                {
                    "outcome": "riskfree_before_stop",
                    "seconds_to_riskfree": 10 + (index % 2),
                    "maxadverse": 1.8,
                    "score": 35.0,
                }
            )
        for _ in range(10):
            summary.observe(
                {
                    "outcome": "stop_before_riskfree",
                    "seconds_to_riskfree": None,
                    "maxadverse": 2.5,
                    "score": -110.0,
                }
            )

        result = summary.as_result_row(
            scenarioid=1,
            signalrule="scenario_continuation",
            fromts=datetime(2026, 4, 24, 0, 0, tzinfo=timezone.utc),
            tots=datetime(2026, 4, 25, 0, 0, tzinfo=timezone.utc),
            riskfreeusd=0.30,
            targetusd=1.00,
            stopusd=0.70,
        )

        self.assertEqual(result["signals"], 70)
        self.assertEqual(result["targets"], 40)
        self.assertEqual(result["riskfree"], 20)
        self.assertEqual(result["stops"], 10)
        self.assertTrue(result["passedconstraints"])
        self.assertAlmostEqual(result["usefulpct"], 85.71428571428571)
        self.assertAlmostEqual(result["stoppct"], 14.285714285714286)
        self.assertAlmostEqual(result["profitproxy"], 39.0)


if __name__ == "__main__":
    unittest.main()
