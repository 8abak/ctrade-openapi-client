from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from datavis.research.fresh_decisions import (
    CausalDecisionFeatureRow,
    FrozenSignalDecisionSource,
    MomentumWeakeningExitConfig,
)
from datavis.research.fresh_entry_diagnostics import FrozenSignalEvent
from datavis.research.fresh_replay import (
    FreshExecutionConfig,
    PositionView,
    ReplayContext,
)
from datavis.research.ticks import Tick


BASE = datetime(2026, 1, 2, tzinfo=timezone.utc)


def tick(index: int, seconds: float, mid: float, spread: float = 0.2) -> Tick:
    return Tick(
        id=index + 1,
        timestamp=BASE + timedelta(seconds=seconds),
        bid=mid - spread / 2.0,
        ask=mid + spread / 2.0,
    )


def execution() -> FreshExecutionConfig:
    return FreshExecutionConfig(
        entry_latency_ms=100,
        exit_latency_ms=100,
        maximum_entry_lag_ms=500,
        maximum_exit_lag_ms=500,
        maximum_intertick_gap_ms=5_000,
        actual_fill_deadline_ms=60_000,
        cooldown_ms=0,
        post_gap_rearm_ms=5_000,
        quantity=1.0,
        slippage_per_side=0.02,
        commission_per_unit_per_side=0.03,
        pnl_classification_tolerance=1e-12,
    )


def flat_context() -> ReplayContext:
    return ReplayContext(None, None, True, None, None)


def position_context(side: str, entry_tick: Tick, fill: float) -> ReplayContext:
    return ReplayContext(
        PositionView(
            side=side,  # type: ignore[arg-type]
            quantity=1.0,
            entry_tick_id=entry_tick.id,
            entry_timestamp=entry_tick.timestamp,
            entry_quote_price=fill,
            entry_fill_price=fill,
        ),
        None,
        False,
        None,
        None,
    )


class FreshDecisionTests(unittest.TestCase):
    def source(
        self,
        points: list[Tick],
        velocities: list[float | None],
        *,
        event_index: int = 0,
        side: str = "long",
        config: MomentumWeakeningExitConfig | None = None,
    ) -> FrozenSignalDecisionSource:
        event_tick = points[event_index]
        source = FrozenSignalDecisionSource(
            [
                FrozenSignalEvent(
                    event_index,
                    event_tick.id,
                    event_tick.timestamp,
                    side,  # type: ignore[arg-type]
                    {"candidateId": "candidate-a"},
                )
            ],
            feature_rows=[
                CausalDecisionFeatureRow(
                    index,
                    point.id,
                    point.timestamp,
                    point.bid,
                    point.ask,
                    velocities[index],
                    -0.5 if velocities[index] is not None else None,
                )
                for index, point in enumerate(points)
            ],
            weakening=config,
            execution=execution(),
            source_metadata={"family": "trend-acceleration"},
        )
        source.validate(points)
        return source

    def test_frozen_entry_is_exact_and_metadata_is_preserved(self):
        points = [tick(0, 0.0, 100), tick(1, 0.1, 100.1)]
        source = self.source(points, [1.0, 1.0])
        decision = source.on_tick(0, points[0], flat_context())
        self.assertEqual(decision.action, "enter_long")  # type: ignore[union-attr]
        self.assertEqual(decision.metadata["candidateId"], "candidate-a")  # type: ignore[union-attr]
        self.assertEqual(decision.metadata["family"], "trend-acceleration")  # type: ignore[union-attr]
        self.assertIsNone(source.on_tick(1, points[1], flat_context()))

    def test_long_weakening_uses_side_aligned_velocity_and_confirmation_time(self):
        points = [
            tick(0, 0.0, 100),
            tick(1, 0.2, 100.3),
            tick(2, 0.4, 100.4),
            tick(3, 0.7, 100.35),
        ]
        config = MomentumWeakeningExitConfig(
            minimum_holding_ms=0,
            weakening_confirmation_ms=250,
            velocity_exit_threshold=0.0,
            acceleration_exit_threshold=0.0,
            stall_deadline_ms=None,
            minimum_best_net_progress_per_unit=None,
        )
        source = self.source(points, [1.0, -0.1, -0.2, -0.3], config=config)
        self.assertIsNotNone(source.on_tick(0, points[0], flat_context()))
        context = position_context("long", points[0], 100.1)
        self.assertIsNone(source.on_tick(1, points[1], context))
        self.assertIsNone(source.on_tick(2, points[2], context))
        decision = source.on_tick(3, points[3], context)
        self.assertEqual(decision.reason, "fresh-exit:momentum-weakening")  # type: ignore[union-attr]
        self.assertEqual(decision.metadata["featureTickId"], points[3].id)  # type: ignore[union-attr]

    def test_short_side_is_mirrored(self):
        points = [tick(0, 0, 100), tick(1, 0.2, 99.8)]
        config = MomentumWeakeningExitConfig(0, 0, 0.0, None, None, None)
        source = self.source(points, [-1.0, 0.1], side="short", config=config)
        entry = source.on_tick(0, points[0], flat_context())
        self.assertEqual(entry.action, "enter_short")  # type: ignore[union-attr]
        decision = source.on_tick(
            1, points[1], position_context("short", points[0], 99.9)
        )
        self.assertEqual(decision.reason, "fresh-exit:momentum-weakening")  # type: ignore[union-attr]

    def test_stall_uses_best_executable_net_progress_after_costs(self):
        points = [tick(0, 0, 100), tick(1, 1.0, 100.05), tick(2, 2.0, 100.1)]
        config = MomentumWeakeningExitConfig(
            minimum_holding_ms=0,
            weakening_confirmation_ms=0,
            velocity_exit_threshold=-999.0,
            acceleration_exit_threshold=None,
            stall_deadline_ms=2_000,
            minimum_best_net_progress_per_unit=0.1,
        )
        source = self.source(points, [1.0, 1.0, 1.0], config=config)
        source.on_tick(0, points[0], flat_context())
        context = position_context("long", points[0], 100.1)
        self.assertIsNone(source.on_tick(1, points[1], context))
        decision = source.on_tick(2, points[2], context)
        self.assertEqual(decision.reason, "fresh-exit:acceleration-stall")  # type: ignore[union-attr]
        self.assertLess(decision.metadata["bestExecutableNetProgressPerUnit"], 0.1)  # type: ignore[union-attr]

    def test_binding_future_mutation_and_order_guards(self):
        points = [tick(0, 0, 100), tick(1, 0.1, 100.1)]
        source = self.source(points, [1.0, -1.0])
        changed = [points[0], tick(1, 0.1, 110.0)]
        with self.assertRaisesRegex(ValueError, "feature row"):
            source.validate(changed)

        source = self.source(points, [1.0, -1.0])
        source.on_tick(0, points[0], flat_context())
        with self.assertRaisesRegex(ValueError, "strict"):
            source.on_tick(0, points[0], flat_context())

    def test_invalid_timing_and_stall_pairs_are_rejected(self):
        with self.assertRaisesRegex(ValueError, "both"):
            MomentumWeakeningExitConfig(0, 0, 0.0, None, 1_000, None)
        with self.assertRaisesRegex(ValueError, "60 seconds"):
            FrozenSignalDecisionSource(
                [],
                feature_rows=[],
                weakening=MomentumWeakeningExitConfig(
                    0, 0, 0.0, None, 59_900, 0.0
                ),
                execution=execution(),
                source_metadata={},
            )


if __name__ == "__main__":
    unittest.main()
