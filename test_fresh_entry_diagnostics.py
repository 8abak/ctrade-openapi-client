from __future__ import annotations

import math
import unittest
from datetime import datetime, timedelta, timezone

from datavis.research.fresh_entry_diagnostics import (
    DiagnosticBoundary,
    EntryDiagnosticConfig,
    EntrySchedulingConfig,
    FrozenSignalEvent,
    evaluate_frozen_entries,
)
from datavis.research.ticks import Tick


BASE = datetime(2026, 4, 7, 0, 0, tzinfo=timezone.utc)


def quote(index: int, milliseconds: int, bid: float, ask: float) -> Tick:
    return Tick(
        id=index + 1,
        timestamp=BASE + timedelta(milliseconds=milliseconds),
        bid=bid,
        ask=ask,
    )


def event(points: list[Tick], index: int, side: str, **metadata) -> FrozenSignalEvent:
    source = points[index]
    return FrozenSignalEvent(
        tick_index=index,
        tick_id=source.id,
        timestamp=source.timestamp,
        side=side,
        metadata=metadata,
    )


def settings(**overrides) -> EntryDiagnosticConfig:
    values = {
        "entry_latency_ms": 0,
        "maximum_entry_lag_ms": 1_000,
        "maximum_intertick_gap_ms": 5_000,
        "diagnostic_horizon_ms": 1_000,
        "quantity": 2.0,
        "entry_slippage_per_unit": 0.05,
        "exit_slippage_per_unit": 0.04,
        "entry_commission_per_unit": 0.01,
        "exit_commission_per_unit": 0.02,
        "profit_barrier_net_per_unit": None,
        "loss_barrier_net_per_unit": None,
    }
    values.update(overrides)
    return EntryDiagnosticConfig(**values)


class FreshEntryDiagnosticTests(unittest.TestCase):
    def test_long_uses_ask_entry_bid_value_and_all_explicit_costs(self):
        points = [
            quote(0, 0, 100.0, 100.2),
            quote(1, 100, 100.1, 100.3),
            quote(2, 500, 100.5, 100.7),
            quote(3, 1_100, 999.0, 999.2),
        ]
        result = evaluate_frozen_entries(
            points,
            [event(points, 0, "long", source="frozen")],
            config=settings(),
        )
        self.assertEqual(result.filled_count, 1)
        self.assertEqual(result.rejected_count, 0)
        diagnostic = result.diagnostics[0]
        self.assertEqual(diagnostic.event.metadata["source"], "frozen")
        self.assertEqual(diagnostic.fill_tick_id, 2)
        self.assertAlmostEqual(diagnostic.decision_spread, 0.2)
        self.assertAlmostEqual(diagnostic.fill_spread, 0.2)
        self.assertAlmostEqual(diagnostic.entry_quote_price, 100.3)
        self.assertAlmostEqual(diagnostic.entry_fill_price, 100.35)
        self.assertAlmostEqual(diagnostic.initial_executable_quote_price, 100.1)
        self.assertAlmostEqual(diagnostic.initial_executable_fill_price, 100.06)
        self.assertAlmostEqual(diagnostic.explicit_round_trip_cost_per_unit, 0.12)
        self.assertAlmostEqual(diagnostic.initial_net_pnl_per_unit, -0.32)
        self.assertAlmostEqual(diagnostic.initial_net_pnl, -0.64)
        self.assertAlmostEqual(diagnostic.break_even_executable_quote_price, 100.42)
        self.assertEqual(diagnostic.cost_coverage_tick_id, 3)
        self.assertEqual(diagnostic.time_to_cost_coverage_ms, 400.0)
        self.assertEqual(diagnostic.decision_to_cost_coverage_ms, 500.0)
        self.assertTrue(diagnostic.cost_covered_by_1s)
        self.assertTrue(diagnostic.cost_covered_by_60s)
        self.assertAlmostEqual(diagnostic.mae_horizon_per_unit, -0.32)
        self.assertAlmostEqual(diagnostic.mfe_horizon_per_unit, 0.08)
        self.assertAlmostEqual(diagnostic.entry_efficiency, 0.2)
        self.assertEqual(diagnostic.observed_quote_count, 2)
        self.assertTrue(diagnostic.horizon_complete)
        # The quote at the half-open horizon endpoint is not observed.
        self.assertLess(diagnostic.mfe_horizon_per_unit, 1.0)

    def test_short_uses_bid_entry_ask_value_symmetrically(self):
        points = [
            quote(0, 0, 100.0, 100.2),
            quote(1, 100, 99.9, 100.1),
            quote(2, 500, 99.4, 99.6),
            quote(3, 1_100, 1.0, 1.2),
        ]
        diagnostic = evaluate_frozen_entries(
            points,
            [event(points, 0, "short")],
            config=settings(),
        ).diagnostics[0]
        self.assertAlmostEqual(diagnostic.entry_quote_price, 99.9)
        self.assertAlmostEqual(diagnostic.entry_fill_price, 99.85)
        self.assertAlmostEqual(diagnostic.initial_executable_quote_price, 100.1)
        self.assertAlmostEqual(diagnostic.initial_executable_fill_price, 100.14)
        self.assertAlmostEqual(diagnostic.initial_net_pnl_per_unit, -0.32)
        self.assertAlmostEqual(diagnostic.break_even_executable_quote_price, 99.78)
        self.assertEqual(diagnostic.time_to_cost_coverage_ms, 400.0)
        self.assertAlmostEqual(diagnostic.mfe_horizon_per_unit, 0.18)

    def test_equal_timestamp_row_is_later_by_id_and_latency_uses_first_eligible_quote(self):
        points = [
            quote(0, 0, 100.0, 100.2),
            quote(1, 0, 100.1, 100.3),
            quote(2, 50, 100.2, 100.4),
            quote(3, 100, 100.3, 100.5),
        ]
        immediate = evaluate_frozen_entries(
            points,
            [event(points, 0, "long")],
            config=settings(
                diagnostic_horizon_ms=50,
                entry_slippage_per_unit=0.0,
                exit_slippage_per_unit=0.0,
                entry_commission_per_unit=0.0,
                exit_commission_per_unit=0.0,
            ),
        ).diagnostics[0]
        self.assertEqual(immediate.fill_tick_index, 1)
        self.assertEqual(immediate.decision_to_fill_ms, 0.0)

        delayed = evaluate_frozen_entries(
            points,
            [event(points, 0, "long")],
            config=settings(
                entry_latency_ms=50,
                maximum_entry_lag_ms=0,
                diagnostic_horizon_ms=50,
                entry_slippage_per_unit=0.0,
                exit_slippage_per_unit=0.0,
                entry_commission_per_unit=0.0,
                exit_commission_per_unit=0.0,
            ),
        ).diagnostics[0]
        self.assertEqual(delayed.fill_tick_index, 2)
        self.assertEqual(delayed.decision_to_fill_ms, 50.0)
        self.assertEqual(delayed.ready_to_fill_lag_ms, 0.0)

    def test_maximum_entry_lag_rejects_instead_of_inventing_a_fill(self):
        points = [
            quote(0, 0, 100.0, 100.2),
            quote(1, 100, 100.1, 100.3),
        ]
        result = evaluate_frozen_entries(
            points,
            [event(points, 0, "long")],
            config=settings(entry_latency_ms=50, maximum_entry_lag_ms=20),
        )
        self.assertEqual(result.filled_count, 0)
        self.assertEqual(result.rejected_count, 1)
        self.assertEqual(result.rejections[0].reason, "maximum_entry_lag_exceeded")
        self.assertEqual(result.rejected_reason_counts, {"maximum_entry_lag_exceeded": 1})

        no_later_quote = evaluate_frozen_entries(
            points[:1],
            [event(points, 0, "long")],
            config=settings(entry_latency_ms=50, maximum_entry_lag_ms=20),
        )
        self.assertEqual(no_later_quote.rejections[0].reason, "input_ended_before_fill")

    def test_barriers_are_net_pnl_diagnostics_and_preserve_first_hit_order(self):
        points = [
            quote(0, 0, 100.0, 100.2),
            quote(1, 100, 100.0, 100.2),
            quote(2, 200, 99.5, 99.7),
            quote(3, 300, 101.0, 101.2),
            quote(4, 1_100, 101.5, 101.7),
        ]
        diagnostic = evaluate_frozen_entries(
            points,
            [event(points, 0, "long")],
            config=settings(
                quantity=1.0,
                entry_slippage_per_unit=0.0,
                exit_slippage_per_unit=0.0,
                entry_commission_per_unit=0.0,
                exit_commission_per_unit=0.0,
                profit_barrier_net_per_unit=0.5,
                loss_barrier_net_per_unit=0.5,
            ),
        ).diagnostics[0]
        self.assertTrue(diagnostic.loss_barrier_hit)
        self.assertTrue(diagnostic.profit_barrier_hit)
        self.assertEqual(diagnostic.loss_barrier_first_hit_ms, 100.0)
        self.assertEqual(diagnostic.profit_barrier_first_hit_ms, 200.0)
        self.assertEqual(diagnostic.first_barrier_hit, "loss")
        self.assertEqual(diagnostic.first_barrier_hit_tick_id, 3)
        self.assertEqual(diagnostic.first_barrier_hit_ms, 100.0)
        self.assertAlmostEqual(diagnostic.mae_horizon_per_unit, -0.7)
        self.assertAlmostEqual(diagnostic.mfe_horizon_per_unit, 0.8)
        self.assertAlmostEqual(diagnostic.entry_efficiency, 0.8 / 1.5)

    def test_sixty_second_checkpoint_is_strictly_before_deadline(self):
        points = [
            quote(0, 0, 100.0, 100.2),
            quote(1, 1, 100.0, 100.2),
            quote(2, 59_001, 100.0, 100.2),
            quote(3, 60_001, 101.0, 101.2),
        ]
        config = settings(
            maximum_intertick_gap_ms=60_000,
            diagnostic_horizon_ms=60_000,
            entry_slippage_per_unit=0.0,
            exit_slippage_per_unit=0.0,
            entry_commission_per_unit=0.0,
            exit_commission_per_unit=0.0,
        )
        at_deadline = evaluate_frozen_entries(
            points, [event(points, 0, "long")], config=config
        ).diagnostics[0]
        self.assertTrue(at_deadline.horizon_complete)
        self.assertFalse(at_deadline.cost_covered_by_60s)
        self.assertIsNone(at_deadline.time_to_cost_coverage_ms)
        self.assertEqual(at_deadline.observed_quote_count, 2)
        self.assertLess(at_deadline.mfe_horizon_per_unit, 0.0)

        just_before_points = [
            quote(0, 0, 100.0, 100.2),
            quote(1, 1, 100.0, 100.2),
            quote(2, 60_000, 101.0, 101.2),
            quote(3, 60_001, 101.1, 101.3),
        ]
        just_before = evaluate_frozen_entries(
            just_before_points,
            [event(just_before_points, 0, "long")],
            config=config,
        ).diagnostics[0]
        self.assertEqual(just_before.time_to_cost_coverage_ms, 59_999.0)
        self.assertTrue(just_before.cost_covered_by_60s)
        with self.assertRaises(ValueError):
            settings(diagnostic_horizon_ms=60_001)

    def test_gap_and_fold_boundary_censor_without_using_the_right_quote(self):
        gap_points = [
            quote(0, 0, 100.0, 100.2),
            quote(1, 100, 100.0, 100.2),
            quote(2, 200, 100.1, 100.3),
            quote(3, 5_000, 200.0, 200.2),
        ]
        gap = evaluate_frozen_entries(
            gap_points,
            [event(gap_points, 0, "long")],
            config=settings(
                maximum_intertick_gap_ms=1_000,
                diagnostic_horizon_ms=10_000,
                entry_slippage_per_unit=0.0,
                exit_slippage_per_unit=0.0,
                entry_commission_per_unit=0.0,
                exit_commission_per_unit=0.0,
            ),
        ).diagnostics[0]
        self.assertTrue(gap.censored)
        self.assertEqual(gap.observation_end_reason, "intertick_gap")
        self.assertEqual(gap.observed_quote_count, 2)
        self.assertLess(gap.mfe_horizon_per_unit, 1.0)

        fold_points = [
            quote(0, 0, 100.0, 100.2),
            quote(1, 100, 100.0, 100.2),
            quote(2, 200, 100.1, 100.3),
            quote(3, 500, 200.0, 200.2),
        ]
        boundary = DiagnosticBoundary(
            start=BASE,
            end=BASE + timedelta(milliseconds=300),
            name="fold-1",
            end_reason="fold_end",
            input_complete_through_end=True,
        )
        fold = evaluate_frozen_entries(
            fold_points,
            [event(fold_points, 0, "long")],
            config=settings(
                diagnostic_horizon_ms=1_000,
                entry_slippage_per_unit=0.0,
                exit_slippage_per_unit=0.0,
                entry_commission_per_unit=0.0,
                exit_commission_per_unit=0.0,
            ),
            boundary=boundary,
        ).diagnostics[0]
        self.assertTrue(fold.censored)
        self.assertEqual(fold.observation_end_reason, "fold_end")
        self.assertEqual(fold.observation_end_timestamp, boundary.end)
        self.assertEqual(fold.observed_quote_count, 2)
        self.assertLess(fold.mfe_horizon_per_unit, 1.0)

    def test_gap_and_boundary_can_cancel_a_pending_entry(self):
        gap_points = [
            quote(0, 0, 100.0, 100.2),
            quote(1, 5_000, 100.1, 100.3),
        ]
        gap_result = evaluate_frozen_entries(
            gap_points,
            [event(gap_points, 0, "long")],
            config=settings(maximum_intertick_gap_ms=1_000),
        )
        self.assertEqual(
            gap_result.rejections[0].reason, "intertick_gap_before_fill"
        )

        boundary_points = [
            quote(0, 0, 100.0, 100.2),
            quote(1, 500, 100.1, 100.3),
        ]
        boundary = DiagnosticBoundary(
            start=BASE,
            end=BASE + timedelta(milliseconds=300),
            end_reason="session_end",
            input_complete_through_end=True,
        )
        boundary_result = evaluate_frozen_entries(
            boundary_points,
            [event(boundary_points, 0, "long")],
            config=settings(),
            boundary=boundary,
        )
        self.assertEqual(
            boundary_result.rejections[0].reason, "session_end_before_fill"
        )

    def test_nonoverlap_and_cooldown_are_explicit_and_reasons_are_counted(self):
        points = [
            quote(index, index * 100, 100.0 + index / 100.0, 100.2 + index / 100.0)
            for index in range(21)
        ]
        events = [
            event(points, 0, "long", name="first"),
            event(points, 2, "long", name="overlap"),
            event(points, 6, "long", name="cooldown"),
            event(points, 9, "long", name="second"),
        ]
        config = settings(
            diagnostic_horizon_ms=500,
            entry_slippage_per_unit=0.0,
            exit_slippage_per_unit=0.0,
            entry_commission_per_unit=0.0,
            exit_commission_per_unit=0.0,
        )
        selected = evaluate_frozen_entries(
            points,
            events,
            config=config,
            scheduling=EntrySchedulingConfig(
                mode="non_overlapping", cooldown_ms=300
            ),
        )
        self.assertEqual(selected.filled_count, 2)
        self.assertEqual(
            [item.event.metadata["name"] for item in selected.diagnostics],
            ["first", "second"],
        )
        self.assertEqual(
            [item.reason for item in selected.rejections],
            ["scheduling_overlap", "scheduling_cooldown"],
        )
        self.assertEqual(
            selected.rejected_reason_counts,
            {"scheduling_cooldown": 1, "scheduling_overlap": 1},
        )

        independent = evaluate_frozen_entries(points, events, config=config)
        self.assertEqual(independent.filled_count, 4)
        self.assertEqual(independent.rejected_count, 0)

    def test_event_binding_tick_order_and_configuration_are_validated(self):
        points = [
            quote(0, 0, 100.0, 100.2),
            quote(1, 100, 100.1, 100.3),
            quote(2, 200, 100.2, 100.4),
        ]
        mismatched = FrozenSignalEvent(
            tick_index=0,
            tick_id=999,
            timestamp=points[0].timestamp,
            side="long",
        )
        with self.assertRaisesRegex(ValueError, "does not match"):
            evaluate_frozen_entries(points, [mismatched], config=settings())
        with self.assertRaisesRegex(ValueError, "non-decreasing"):
            evaluate_frozen_entries(
                points,
                [event(points, 2, "long"), event(points, 0, "short")],
                config=settings(),
            )

        disordered_ticks = [
            Tick(id=2, timestamp=BASE, bid=100.0, ask=100.2),
            Tick(id=1, timestamp=BASE, bid=100.1, ask=100.3),
        ]
        with self.assertRaisesRegex(ValueError, r"\(timestamp, id\)"):
            evaluate_frozen_entries(disordered_ticks, [], config=settings())
        with self.assertRaises(ValueError):
            EntrySchedulingConfig(mode="independent", cooldown_ms=1)
        with self.assertRaises(ValueError):
            settings(quantity=math.nan)


if __name__ == "__main__":
    unittest.main()
