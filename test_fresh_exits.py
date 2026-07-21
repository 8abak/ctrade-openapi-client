from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from datavis.research.fresh_exits import (
    BoundVolatilityRows,
    ExitDistance,
    FreshExitPolicyConfig,
    FreshProtectiveExitPolicy,
    VolatilityFrame,
    VolatilityRow,
    executable_break_even_quote,
)
from datavis.research.fresh_replay import (
    DecisionFrame,
    DecisionRow,
    FreshExecutionConfig,
    PositionView,
    ReplayDecision,
    run_fresh_replay,
)
from datavis.research.ticks import Tick


BASE = datetime(2026, 4, 7, 0, 0, tzinfo=timezone.utc)


def quote(
    tick_id: int,
    milliseconds: float,
    bid: float,
    ask: float,
) -> Tick:
    return Tick(
        id=tick_id,
        timestamp=BASE + timedelta(milliseconds=milliseconds),
        bid=bid,
        ask=ask,
    )


def execution(**changes) -> FreshExecutionConfig:
    values = {
        "entry_latency_ms": 0,
        "exit_latency_ms": 0,
        "maximum_entry_lag_ms": 1_000,
        "maximum_exit_lag_ms": 1_000,
        "maximum_intertick_gap_ms": 60_000,
        "actual_fill_deadline_ms": 60_000,
        "cooldown_ms": 0,
        "post_gap_rearm_ms": 0,
        "quantity": 1.0,
        "slippage_per_side": 0.0,
        "commission_per_unit_per_side": 0.0,
        "pnl_classification_tolerance": 1e-9,
    }
    values.update(changes)
    return FreshExecutionConfig(**values)


def exit_config(**changes) -> FreshExitPolicyConfig:
    values = {
        "initial_stop": ExitDistance("fixed", 0.5),
        "break_even_activation": None,
        "break_even_buffer_net_per_unit": 0.0,
        "trailing_activation": None,
        "trailing_distance": None,
        "trailing_volatility_basis": "entry",
        "maximum_holding_ms": 59_000,
    }
    values.update(changes)
    return FreshExitPolicyConfig(**values)


def entry_frame(points: list[Tick], side: str) -> DecisionFrame:
    action = "enter_long" if side == "long" else "enter_short"
    return DecisionFrame(
        [
            DecisionRow(
                tick_index=0,
                tick_id=points[0].id,
                timestamp=points[0].timestamp,
                decision=ReplayDecision(action, "frozen-entry"),
            )
        ]
    )


def managed(
    points: list[Tick],
    side: str,
    *,
    policy_config: FreshExitPolicyConfig,
    execution_config: FreshExecutionConfig,
    volatility: VolatilityFrame | None = None,
) -> FreshProtectiveExitPolicy:
    return FreshProtectiveExitPolicy(
        entry_frame(points, side),
        config=policy_config,
        execution=execution_config,
        volatility=volatility,
    )


class FreshProtectiveExitTests(unittest.TestCase):
    def test_wrapped_stale_decision_frame_is_rejected_during_preflight(self):
        points = [
            quote(1, 0, 100.0, 100.2),
            quote(2, 1, 100.0, 100.2),
        ]
        stale = DecisionFrame(
            [
                DecisionRow(
                    tick_index=0,
                    tick_id=999,
                    timestamp=points[0].timestamp,
                    decision=ReplayDecision("enter_long", "stale-entry"),
                )
            ]
        )
        settings = execution()
        policy = FreshProtectiveExitPolicy(
            stale,
            config=exit_config(),
            execution=settings,
            volatility=None,
        )
        with self.assertRaisesRegex(ValueError, "tick_id"):
            run_fresh_replay(points, policy, config=settings)

        outside = DecisionFrame(
            [
                DecisionRow(
                    tick_index=5,
                    tick_id=999,
                    timestamp=points[0].timestamp,
                    decision=ReplayDecision("enter_long", "outside-input"),
                )
            ]
        )
        with self.assertRaisesRegex(ValueError, "outside the tick sequence"):
            run_fresh_replay(
                points,
                FreshProtectiveExitPolicy(
                    outside,
                    config=exit_config(),
                    execution=settings,
                    volatility=None,
                ),
                config=settings,
            )

    def test_long_stop_uses_bid_and_gap_through_fills_later_bid(self):
        points = [
            quote(1, 0, 100.0, 100.2),
            quote(2, 1, 100.0, 100.2),
            # The bid crosses 99.70 even though the ask remains far above it.
            quote(3, 2, 99.0, 100.4),
            quote(4, 3, 98.8, 99.0),
        ]
        settings = execution()
        result = run_fresh_replay(
            points,
            managed(
                points,
                "long",
                policy_config=exit_config(),
                execution_config=settings,
            ),
            config=settings,
        )
        self.assertEqual(len(result.trades), 1)
        trade = result.trades[0]
        self.assertEqual(trade.entry_fill_price, 100.2)
        self.assertEqual(trade.exit_decision_tick_id, 3)
        self.assertEqual(trade.exit_fill_tick_id, 4)
        self.assertEqual(trade.exit_quote_price, 98.8)
        self.assertEqual(trade.exit_metadata["triggerExecutableSide"], "bid")
        self.assertEqual(trade.exit_metadata["triggerQuotePrice"], 99.0)
        self.assertAlmostEqual(trade.exit_metadata["activeStopQuote"], 99.7)
        self.assertAlmostEqual(trade.exit_metadata["gapThroughPrice"], 0.7)
        self.assertNotEqual(
            trade.exit_quote_price, trade.exit_metadata["activeStopQuote"]
        )

    def test_short_stop_uses_ask_and_gap_through_fills_later_ask(self):
        points = [
            quote(1, 0, 100.0, 100.2),
            quote(2, 1, 100.0, 100.2),
            # The ask crosses 100.50 while the bid remains below the stop.
            quote(3, 2, 99.0, 100.6),
            quote(4, 3, 100.8, 101.0),
        ]
        settings = execution()
        result = run_fresh_replay(
            points,
            managed(
                points,
                "short",
                policy_config=exit_config(),
                execution_config=settings,
            ),
            config=settings,
        )
        trade = result.trades[0]
        self.assertEqual(trade.entry_fill_price, 100.0)
        self.assertEqual(trade.exit_decision_tick_id, 3)
        self.assertEqual(trade.exit_fill_tick_id, 4)
        self.assertEqual(trade.exit_quote_price, 101.0)
        self.assertEqual(trade.exit_metadata["triggerExecutableSide"], "ask")
        self.assertEqual(trade.exit_metadata["triggerQuotePrice"], 100.6)
        self.assertAlmostEqual(trade.exit_metadata["activeStopQuote"], 100.5)
        self.assertAlmostEqual(trade.exit_metadata["gapThroughPrice"], 0.1)

    def test_executable_break_even_includes_exit_slippage_and_both_commissions(self):
        settings = execution(
            slippage_per_side=0.05,
            commission_per_unit_per_side=0.02,
        )
        long_position = PositionView(
            side="long",
            quantity=1.0,
            entry_tick_id=2,
            entry_timestamp=BASE,
            entry_quote_price=100.20,
            entry_fill_price=100.25,
        )
        short_position = PositionView(
            side="short",
            quantity=1.0,
            entry_tick_id=2,
            entry_timestamp=BASE,
            entry_quote_price=100.00,
            entry_fill_price=99.95,
        )
        self.assertAlmostEqual(
            executable_break_even_quote(
                long_position, settings, buffer_net_per_unit=0.0
            ),
            100.34,
        )
        self.assertAlmostEqual(
            executable_break_even_quote(
                short_position, settings, buffer_net_per_unit=0.0
            ),
            99.86,
        )

        points = [
            quote(1, 0, 100.0, 100.2),
            quote(2, 1, 100.0, 100.2),
            quote(3, 2, 100.8, 101.0),
            quote(4, 3, 100.34, 100.54),
            quote(5, 4, 100.34, 100.54),
        ]
        result = run_fresh_replay(
            points,
            managed(
                points,
                "long",
                policy_config=exit_config(
                    initial_stop=ExitDistance("fixed", 2.0),
                    break_even_activation=ExitDistance("fixed", 0.3),
                ),
                execution_config=settings,
            ),
            config=settings,
        )
        trade = result.trades[0]
        self.assertEqual(trade.exit_reason, "fresh-exit:executable-break-even")
        self.assertAlmostEqual(trade.exit_metadata["activeStopQuote"], 100.34)
        self.assertAlmostEqual(trade.net_pnl, 0.0)

    def test_fixed_trailing_activates_and_ratchets_from_best_bid(self):
        points = [
            quote(1, 0, 100.0, 100.2),
            quote(2, 1, 100.0, 100.2),
            quote(3, 2, 101.0, 101.2),
            quote(4, 3, 100.70, 100.90),
            quote(5, 4, 100.60, 100.80),
        ]
        settings = execution()
        result = run_fresh_replay(
            points,
            managed(
                points,
                "long",
                policy_config=exit_config(
                    initial_stop=ExitDistance("fixed", 2.0),
                    trailing_activation=ExitDistance("fixed", 0.5),
                    trailing_distance=ExitDistance("fixed", 0.25),
                ),
                execution_config=settings,
            ),
            config=settings,
        )
        trade = result.trades[0]
        self.assertEqual(trade.exit_reason, "fresh-exit:trailing-stop")
        self.assertAlmostEqual(trade.exit_metadata["bestExecutableQuote"], 101.0)
        self.assertAlmostEqual(trade.exit_metadata["activeStopQuote"], 100.75)
        self.assertAlmostEqual(trade.exit_metadata["triggerQuotePrice"], 100.70)
        self.assertEqual(trade.exit_fill_tick_id, 5)
        self.assertAlmostEqual(trade.exit_quote_price, 100.60)

    def test_short_fixed_trailing_ratchets_from_best_ask(self):
        points = [
            quote(1, 0, 100.0, 100.2),
            quote(2, 1, 100.0, 100.2),
            quote(3, 2, 99.0, 99.2),
            quote(4, 3, 99.15, 99.45),
            quote(5, 4, 99.30, 99.60),
        ]
        settings = execution()
        result = run_fresh_replay(
            points,
            managed(
                points,
                "short",
                policy_config=exit_config(
                    initial_stop=ExitDistance("fixed", 2.0),
                    trailing_activation=ExitDistance("fixed", 0.5),
                    trailing_distance=ExitDistance("fixed", 0.25),
                ),
                execution_config=settings,
            ),
            config=settings,
        )
        trade = result.trades[0]
        self.assertEqual(trade.exit_reason, "fresh-exit:trailing-stop")
        self.assertAlmostEqual(trade.exit_metadata["bestExecutableQuote"], 99.2)
        self.assertAlmostEqual(trade.exit_metadata["activeStopQuote"], 99.45)
        self.assertAlmostEqual(trade.exit_metadata["triggerQuotePrice"], 99.45)
        self.assertEqual(trade.exit_metadata["triggerExecutableSide"], "ask")
        self.assertAlmostEqual(trade.exit_quote_price, 99.60)

    def test_volatility_activation_and_current_distance_are_causal_and_never_loosen(self):
        points = [
            quote(1, 0, 100.0, 100.2),
            quote(2, 1, 100.0, 100.2),
            quote(3, 2, 100.8, 101.0),
            quote(4, 3, 100.75, 100.95),
            quote(5, 4, 100.69, 100.89),
            quote(6, 5, 100.60, 100.80),
        ]
        values = [0.4, 0.4, 0.4, 0.2, 0.2, 0.2]
        volatility = VolatilityFrame(
            VolatilityRow(index, point.id, point.timestamp, values[index])
            for index, point in enumerate(points)
        )
        settings = execution()
        result = run_fresh_replay(
            points,
            managed(
                points,
                "long",
                policy_config=exit_config(
                    initial_stop=ExitDistance("volatility", 5.0),
                    trailing_activation=ExitDistance("volatility", 1.0),
                    trailing_distance=ExitDistance("volatility", 0.5),
                    trailing_volatility_basis="current",
                ),
                execution_config=settings,
                volatility=volatility,
            ),
            config=settings,
        )
        trade = result.trades[0]
        # Entry volatility sets 0.40 activation.  Current volatility falling to
        # 0.20 tightens distance to 0.10, so best bid 100.80 produces 100.70.
        self.assertAlmostEqual(trade.exit_metadata["activeStopQuote"], 100.70)
        self.assertEqual(trade.exit_metadata["triggerTickId"], 5)
        self.assertEqual(trade.exit_fill_tick_id, 6)

    def test_equal_timestamps_use_increasing_ids_for_trigger_then_fill(self):
        points = [
            Tick(id=10, timestamp=BASE, bid=100.0, ask=100.2),
            Tick(id=11, timestamp=BASE, bid=100.0, ask=100.2),
            Tick(id=12, timestamp=BASE, bid=99.0, ask=99.2),
            Tick(id=13, timestamp=BASE, bid=98.8, ask=99.0),
        ]
        settings = execution()
        result = run_fresh_replay(
            points,
            managed(
                points,
                "long",
                policy_config=exit_config(),
                execution_config=settings,
            ),
            config=settings,
        )
        trade = result.trades[0]
        self.assertEqual(trade.entry_fill_tick_id, 11)
        self.assertEqual(trade.exit_metadata["triggerTickId"], 12)
        self.assertEqual(trade.exit_fill_tick_id, 13)
        self.assertEqual(trade.holding_ms, 0.0)

        values = VolatilityFrame(
            [
                VolatilityRow(0, 10, BASE, 0.1),
                VolatilityRow(1, 11, BASE, 0.1),
            ]
        )
        self.assertEqual(values.on_tick(0, points[0]), 0.1)
        self.assertEqual(values.on_tick(1, points[1]), 0.1)

    def test_bound_volatility_has_independent_cursors_and_exact_tuple_preflight(self):
        points = (
            Tick(id=10, timestamp=BASE, bid=100.0, ask=100.2),
            Tick(id=11, timestamp=BASE, bid=100.0, ask=100.2),
        )
        rows = (
            VolatilityRow(0, 10, BASE, 0.1),
            VolatilityRow(1, 11, BASE, 0.2),
        )
        binding = BoundVolatilityRows(points, rows)
        unvalidated = binding.cursor()
        with self.assertRaisesRegex(RuntimeError, "must be validated"):
            unvalidated.on_tick(0, points[0])

        first = binding.cursor()
        second = binding.cursor()
        first.validate(points)
        second.validate(points)
        self.assertEqual(
            [first.on_tick(index, point) for index, point in enumerate(points)],
            [0.1, 0.2],
        )
        self.assertEqual(
            [second.on_tick(index, point) for index, point in enumerate(points)],
            [0.1, 0.2],
        )

        equal_but_distinct_tuple = tuple(list(points))
        self.assertIsNot(equal_but_distinct_tuple, points)
        with self.assertRaisesRegex(ValueError, "different tick tuple"):
            binding.validate(equal_but_distinct_tuple)

    def test_bound_volatility_replay_is_identical_with_repeated_quote_volume(self):
        points = (
            Tick(id=1, timestamp=BASE, bid=100.0, ask=100.2),
            Tick(id=2, timestamp=BASE, bid=100.0, ask=100.2),
            quote(3, 1, 100.8, 101.0),
            quote(4, 2, 100.75, 100.95),
            quote(5, 3, 100.69, 100.89),
            quote(6, 4, 100.60, 100.80),
        )
        values = (0.4, 0.4, 0.4, 0.2, 0.2, 0.2)
        rows = tuple(
            VolatilityRow(index, point.id, point.timestamp, values[index])
            for index, point in enumerate(points)
        )
        settings = execution()
        policy_config = exit_config(
            initial_stop=ExitDistance("volatility", 5.0),
            trailing_activation=ExitDistance("volatility", 1.0),
            trailing_distance=ExitDistance("volatility", 0.5),
            trailing_volatility_basis="current",
        )

        legacy = run_fresh_replay(
            points,
            managed(
                points,
                "long",
                policy_config=policy_config,
                execution_config=settings,
                volatility=VolatilityFrame(rows),
            ),
            config=settings,
        )
        binding = BoundVolatilityRows(points, rows)
        bound = run_fresh_replay(
            points,
            managed(
                points,
                "long",
                policy_config=policy_config,
                execution_config=settings,
                volatility=binding.cursor(),
            ),
            config=settings,
        )

        self.assertEqual(bound, legacy)
        self.assertEqual(bound.trades[0].entry_fill_tick_id, 2)

    def test_bound_volatility_rejects_incomplete_shifted_or_stale_rows(self):
        points = (
            quote(1, 0, 100.0, 100.2),
            quote(2, 1, 100.1, 100.3),
        )
        first = VolatilityRow(0, 1, points[0].timestamp, 0.1)
        with self.assertRaisesRegex(ValueError, "one row per tick"):
            BoundVolatilityRows(points, (first,))
        shifted = VolatilityRow(1, 1, points[0].timestamp, 0.2)
        with self.assertRaisesRegex(ValueError, "exact tick"):
            BoundVolatilityRows(points, (first, shifted))
        wrong_index = VolatilityRow(2, 2, points[1].timestamp, 0.2)
        with self.assertRaisesRegex(ValueError, "contiguous"):
            BoundVolatilityRows(points, (first, wrong_index))

    def test_time_exit_and_configuration_remain_strictly_under_sixty_seconds(self):
        with self.assertRaisesRegex(ValueError, "strictly below 60 seconds"):
            exit_config(maximum_holding_ms=60_000)
        exit_config(maximum_holding_ms=59_999)
        with self.assertRaisesRegex(ValueError, "plus exit_latency_ms"):
            FreshProtectiveExitPolicy(
                DecisionFrame(),
                config=exit_config(maximum_holding_ms=59_900),
                execution=execution(exit_latency_ms=100),
                volatility=None,
            )

        points = [
            quote(1, 0, 100.0, 100.2),
            quote(2, 1, 100.0, 100.2),
            quote(3, 59_001, 100.1, 100.3),
            quote(4, 59_002, 100.1, 100.3),
        ]
        settings = execution(maximum_intertick_gap_ms=60_000)
        result = run_fresh_replay(
            points,
            managed(
                points,
                "long",
                policy_config=exit_config(
                    initial_stop=ExitDistance("fixed", 2.0),
                    maximum_holding_ms=59_000,
                ),
                execution_config=settings,
            ),
            config=settings,
        )
        trade = result.trades[0]
        self.assertEqual(trade.exit_reason, "fresh-exit:time-stop")
        self.assertEqual(trade.exit_metadata["holdingAtDecisionMs"], 59_000.0)
        self.assertLess(trade.holding_ms, 60_000)


if __name__ == "__main__":
    unittest.main()
