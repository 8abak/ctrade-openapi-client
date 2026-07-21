from __future__ import annotations

import random
import unittest
from datetime import datetime, timedelta, timezone
from typing import Callable

from datavis.research.fresh_decisions import (
    BoundDecisionFeatureRows,
    CausalDecisionFeatureRow,
    FrozenSignalDecisionSource,
    MomentumWeakeningExitConfig,
)
from datavis.research.fresh_entry_diagnostics import FrozenSignalEvent
from datavis.research.fresh_exits import (
    BoundVolatilityRows,
    ExitDistance,
    FreshExitPolicyConfig,
    FreshProtectiveExitPolicy,
    VolatilityFrame,
    VolatilityRow,
)
from datavis.research.fresh_replay import (
    FreshExecutionConfig,
    ReplayBoundary,
    _prepare_replay_tape,
    run_fresh_replay,
)
from datavis.research.ticks import Tick


BASE = datetime(2026, 4, 7, tzinfo=timezone.utc)


def execution(**changes: object) -> FreshExecutionConfig:
    values: dict[str, object] = {
        "entry_latency_ms": 0,
        "exit_latency_ms": 0,
        "maximum_entry_lag_ms": 500,
        "maximum_exit_lag_ms": 500,
        "maximum_intertick_gap_ms": 500,
        "actual_fill_deadline_ms": 5_000,
        "cooldown_ms": 0,
        "post_gap_rearm_ms": 0,
        "quantity": 1.0,
        "slippage_per_side": 0.01,
        "commission_per_unit_per_side": 0.01,
        "pnl_classification_tolerance": 1e-12,
    }
    values.update(changes)
    return FreshExecutionConfig(**values)  # type: ignore[arg-type]


def exit_config(**changes: object) -> FreshExitPolicyConfig:
    values: dict[str, object] = {
        "initial_stop": ExitDistance("fixed", 0.5),
        "break_even_activation": None,
        "break_even_buffer_net_per_unit": 0.0,
        "trailing_activation": None,
        "trailing_distance": None,
        "trailing_volatility_basis": "entry",
        "maximum_holding_ms": 3_000,
    }
    values.update(changes)
    return FreshExitPolicyConfig(**values)  # type: ignore[arg-type]


def points_from_rows(rows: list[tuple[int, float, float]]) -> tuple[Tick, ...]:
    return tuple(
        Tick(
            id=index + 1,
            timestamp=BASE + timedelta(milliseconds=milliseconds),
            bid=bid,
            ask=ask,
        )
        for index, (milliseconds, bid, ask) in enumerate(rows)
    )


def make_stack(
    points: tuple[Tick, ...],
    event_specs: tuple[tuple[int, str], ...],
    settings: FreshExecutionConfig,
    policy_config: FreshExitPolicyConfig,
    *,
    velocities: tuple[float | None, ...] | None = None,
    accelerations: tuple[float | None, ...] | None = None,
    weakening: MomentumWeakeningExitConfig | None = None,
    volatility_values: tuple[float | None, ...] | None = None,
    custom_volatility: object | None = None,
    bound_rows: bool = False,
) -> FreshProtectiveExitPolicy:
    if velocities is None:
        velocities = tuple(0.1 for _ in points)
    if accelerations is None:
        accelerations = tuple(0.0 for _ in points)
    feature_rows = tuple(
        CausalDecisionFeatureRow(
            tick_index=index,
            tick_id=tick.id,
            timestamp=tick.timestamp,
            bid=tick.bid,
            ask=tick.ask,
            velocity=velocities[index],
            acceleration=accelerations[index],
        )
        for index, tick in enumerate(points)
    )
    source = FrozenSignalDecisionSource(
        (
            FrozenSignalEvent(
                tick_index=index,
                tick_id=points[index].id,
                timestamp=points[index].timestamp,
                side=side,  # type: ignore[arg-type]
                metadata={"candidate_id": "sparse-equivalence"},
            )
            for index, side in event_specs
        ),
        feature_rows=(
            BoundDecisionFeatureRows(points, feature_rows)
            if bound_rows
            else feature_rows
        ),
        weakening=weakening,
        execution=settings,
        source_metadata={"test": "dense-vs-sparse"},
    )
    volatility = custom_volatility
    if volatility_values is not None:
        rows = tuple(
            VolatilityRow(index, tick.id, tick.timestamp, volatility_values[index])
            for index, tick in enumerate(points)
        )
        volatility = (
            BoundVolatilityRows(points, rows).cursor()
            if bound_rows
            else VolatilityFrame(rows)
        )
    return FreshProtectiveExitPolicy(
        source,
        config=policy_config,
        execution=settings,
        volatility=volatility,  # type: ignore[arg-type]
    )


class FreshSparseReplayTests(unittest.TestCase):
    def assert_dense_sparse_equal(
        self,
        points: tuple[Tick, ...],
        factory: Callable[[], FreshProtectiveExitPolicy],
        settings: FreshExecutionConfig,
        *,
        boundary: ReplayBoundary | None = None,
    ):
        dense_policy = factory()
        sparse_policy = factory()
        dense = run_fresh_replay(
            points,
            dense_policy,
            config=settings,
            boundary=boundary,
        )
        prepared = _prepare_replay_tape(
            points,
            maximum_intertick_gap_ms=settings.maximum_intertick_gap_ms,
        )
        sparse = run_fresh_replay(
            points,
            sparse_policy,
            config=settings,
            boundary=boundary,
            _prepared_replay_tape=prepared,
        )
        self.assertEqual(sparse, dense)
        self.assertEqual(sparse.summary(), dense.summary())
        self.assertEqual(
            [trade.as_dict() for trade in sparse.trades],
            [trade.as_dict() for trade in dense.trades],
        )
        return dense, sparse, dense_policy, sparse_policy

    def test_repeats_multiple_gaps_rearm_cooldown_latency_boundary_and_censor(self):
        points = points_from_rows(
            [
                (0, 100.0, 100.2),
                (0, 100.0, 100.2),
                (1, 100.0, 100.2),
                (100, 100.0, 100.2),
                (100, 100.0, 100.2),
                (200, 100.4, 100.6),
                (300, 99.5, 99.7),
                (300, 99.4, 99.6),
                (1_000, 99.4, 99.6),
                (1_200, 99.4, 99.6),
                (1_500, 99.4, 99.6),
                (1_600, 99.4, 99.6),
                (1_600, 99.4, 99.6),
                (1_700, 99.0, 99.2),
                (1_800, 99.8, 100.0),
                (1_801, 99.9, 100.1),
                (3_000, 99.9, 100.1),
                (4_500, 99.9, 100.1),
                (5_000, 99.9, 100.1),
                (5_100, 99.9, 100.1),
                (5_100, 99.9, 100.1),
                (5_200, 99.9, 100.1),
            ]
        )
        settings = execution(cooldown_ms=1_000, post_gap_rearm_ms=600)
        events = (
            (3, "long"),
            (8, "short"),
            (9, "short"),
            (10, "short"),
            (11, "short"),
            (16, "long"),
            (17, "long"),
            (19, "long"),
        )
        boundary = ReplayBoundary(
            start=BASE,
            end=BASE + timedelta(milliseconds=5_200),
            name="sparse-boundary",
            input_complete_through_end=True,
        )
        dense, sparse, _, _ = self.assert_dense_sparse_equal(
            points,
            lambda: make_stack(points, events, settings, exit_config(),),
            settings,
            boundary=boundary,
        )
        self.assertEqual(sparse.ticks_seen, 21)
        self.assertTrue(sparse.boundary_reached)
        self.assertEqual(len(sparse.trades), 2)
        self.assertEqual(sparse.censors[-1].reason, "boundary-open")
        self.assertEqual(
            [item.disposition for item in sparse.decisions],
            [
                "scheduled-entry",
                "scheduled-exit",
                "scheduled-entry",
                "scheduled-exit",
                "scheduled-entry",
            ],
        )
        self.assertEqual(dense, sparse)

    def test_current_volatility_trailing_and_flat_suffix_cursor_are_exact(self):
        points = points_from_rows(
            [
                (0, 100.0, 100.2),
                (0, 100.0, 100.2),
                (20, 100.0, 100.2),
                (40, 100.0, 100.2),
                (60, 100.0, 100.2),
                (60, 100.0, 100.2),
                (80, 100.8, 101.0),
                (100, 100.75, 100.95),
                (120, 100.69, 100.89),
                (140, 100.60, 100.80),
                (200, 100.60, 100.80),
                (800, 100.60, 100.80),
                (800, 100.60, 100.80),
            ]
        )
        settings = execution(post_gap_rearm_ms=250)
        policy_config = exit_config(
            initial_stop=ExitDistance("volatility", 5.0),
            trailing_activation=ExitDistance("volatility", 1.0),
            trailing_distance=ExitDistance("volatility", 0.5),
            trailing_volatility_basis="current",
        )
        values = (0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.4, 0.2, 0.2, 0.2, 0.2, 0.3, 0.3)
        dense, sparse, dense_policy, sparse_policy = self.assert_dense_sparse_equal(
            points,
            lambda: make_stack(
                points,
                ((4, "long"),),
                settings,
                policy_config,
                volatility_values=values,
                bound_rows=True,
            ),
            settings,
        )
        self.assertEqual(len(sparse.trades), 1)
        self.assertEqual(sparse.trades[0].exit_metadata["triggerTickId"], 9)
        self.assertEqual(dense_policy._last_index, len(points) - 1)
        self.assertEqual(sparse_policy._last_index, len(points) - 1)
        self.assertEqual(dense, sparse)

    def test_weakening_confirmation_and_pending_fill_ticks_remain_dense(self):
        points = points_from_rows(
            [
                (0, 100.0, 100.2),
                (10, 100.0, 100.2),
                (100, 100.0, 100.2),
                (150, 100.0, 100.2),
                (200, 100.2, 100.4),
                (250, 100.3, 100.5),
                (300, 100.3, 100.5),
                (360, 100.25, 100.45),
                (420, 100.2, 100.4),
                (500, 100.2, 100.4),
                (510, 100.2, 100.4),
                (900, 100.2, 100.4),
            ]
        )
        settings = execution(
            entry_latency_ms=100,
            exit_latency_ms=50,
            maximum_entry_lag_ms=200,
            maximum_exit_lag_ms=200,
        )
        velocities = (0.2, 0.2, 0.2, 0.2, 0.2, 0.1, -0.1, -0.2, -0.3, -0.3, -0.3, -0.3)
        weakening = MomentumWeakeningExitConfig(
            minimum_holding_ms=0,
            weakening_confirmation_ms=100,
            velocity_exit_threshold=0.0,
            acceleration_exit_threshold=None,
            stall_deadline_ms=None,
            minimum_best_net_progress_per_unit=None,
        )
        dense, sparse, _, _ = self.assert_dense_sparse_equal(
            points,
            lambda: make_stack(
                points,
                ((2, "long"),),
                settings,
                exit_config(initial_stop=ExitDistance("fixed", 5.0)),
                velocities=velocities,
                weakening=weakening,
            ),
            settings,
        )
        self.assertEqual(len(sparse.trades), 1)
        self.assertEqual(sparse.trades[0].exit_reason, "fresh-exit:momentum-weakening")
        self.assertEqual(dense, sparse)

    def test_custom_volatility_declines_sparse_skipping_and_receives_every_tick(self):
        points = points_from_rows(
            [(index * 10, 100.0, 100.2) for index in range(40)]
        )
        settings = execution()

        class CustomVolatility:
            def __init__(self) -> None:
                self.calls: list[int] = []

            def on_tick(self, tick_index: int, tick: Tick) -> float:
                del tick
                self.calls.append(tick_index)
                return 0.2

        custom = CustomVolatility()
        policy = make_stack(
            points,
            (),
            settings,
            exit_config(),
            custom_volatility=custom,
        )
        prepared = _prepare_replay_tape(
            points,
            maximum_intertick_gap_ms=settings.maximum_intertick_gap_ms,
        )
        result = run_fresh_replay(
            points,
            policy,
            config=settings,
            _prepared_replay_tape=prepared,
        )
        self.assertEqual(result.ticks_seen, len(points))
        self.assertEqual(custom.calls, list(range(len(points))))

    def test_no_signal_flat_tape_skips_calls_but_preserves_every_volume_tick(self):
        points = tuple(
            Tick(
                id=index + 1,
                timestamp=BASE + timedelta(milliseconds=index // 3),
                bid=100.0,
                ask=100.2,
            )
            for index in range(1_000)
        )
        settings = execution()

        class CountingSource(FrozenSignalDecisionSource):
            def __init__(self) -> None:
                self.calls = 0
                super().__init__(
                    (),
                    feature_rows=(
                        CausalDecisionFeatureRow(
                            index,
                            tick.id,
                            tick.timestamp,
                            tick.bid,
                            tick.ask,
                            0.0,
                            0.0,
                        )
                        for index, tick in enumerate(points)
                    ),
                    weakening=None,
                    execution=settings,
                    source_metadata={},
                )

            def on_tick(self, tick_index, tick, context):
                self.calls += 1
                return super().on_tick(tick_index, tick, context)

        dense_source = CountingSource()
        sparse_source = CountingSource()
        dense_policy = FreshProtectiveExitPolicy(
            dense_source,
            config=exit_config(),
            execution=settings,
            volatility=None,
        )
        sparse_policy = FreshProtectiveExitPolicy(
            sparse_source,
            config=exit_config(),
            execution=settings,
            volatility=None,
        )
        dense = run_fresh_replay(points, dense_policy, config=settings)
        prepared = _prepare_replay_tape(
            points,
            maximum_intertick_gap_ms=settings.maximum_intertick_gap_ms,
        )
        sparse = run_fresh_replay(
            points,
            sparse_policy,
            config=settings,
            _prepared_replay_tape=prepared,
        )
        self.assertEqual(sparse, dense)
        self.assertEqual(sparse.ticks_seen, 1_000)
        self.assertEqual(dense_source.calls, 1_000)
        self.assertEqual(sparse_source.calls, 1)
        self.assertEqual(sparse_source._last_index, 999)

    def test_prepared_tape_rejects_wrong_tuple_and_gap_threshold(self):
        points = points_from_rows([(0, 100.0, 100.2), (1, 100.0, 100.2)])
        settings = execution()
        prepared = _prepare_replay_tape(
            points,
            maximum_intertick_gap_ms=settings.maximum_intertick_gap_ms,
        )
        policy = make_stack(points, (), settings, exit_config())
        with self.assertRaisesRegex(ValueError, "exact tuple"):
            run_fresh_replay(
                tuple(list(points)),
                policy,
                config=settings,
                _prepared_replay_tape=prepared,
            )
        policy = make_stack(points, (), settings, exit_config())
        with self.assertRaisesRegex(ValueError, "different gap"):
            run_fresh_replay(
                points,
                policy,
                config=execution(maximum_intertick_gap_ms=501),
                _prepared_replay_tape=prepared,
            )

    def test_seeded_randomized_dense_sparse_equivalence(self):
        rng = random.Random(0x5A17CE)
        for case in range(60):
            size = rng.randint(25, 80)
            timestamp_ms = 0
            mid = 100.0
            rows: list[tuple[int, float, float]] = []
            for _ in range(size):
                timestamp_ms += rng.choice((0, 1, 5, 25, 100, 700))
                mid += rng.choice((-0.25, -0.1, 0.0, 0.1, 0.25))
                spread = rng.choice((0.1, 0.2, 0.3))
                rows.append(
                    (
                        timestamp_ms,
                        round(mid - spread / 2.0, 6),
                        round(mid + spread / 2.0, 6),
                    )
                )
            points = points_from_rows(rows)
            event_indexes = sorted(
                rng.sample(range(size), k=rng.randint(0, min(10, size)))
            )
            events = tuple(
                (index, rng.choice(("long", "short")))
                for index in event_indexes
            )
            settings = execution(
                entry_latency_ms=rng.choice((0, 10, 50)),
                exit_latency_ms=rng.choice((0, 10, 50)),
                cooldown_ms=rng.choice((0, 100, 500)),
                post_gap_rearm_ms=rng.choice((0, 100, 400)),
            )
            velocities = tuple(
                rng.choice((None, -0.3, -0.1, 0.0, 0.1, 0.3))
                for _ in points
            )
            accelerations = tuple(
                rng.choice((None, -0.2, 0.0, 0.2)) for _ in points
            )
            weakening = (
                None
                if rng.random() < 0.5
                else MomentumWeakeningExitConfig(
                    minimum_holding_ms=rng.choice((0, 50)),
                    weakening_confirmation_ms=rng.choice((0, 50, 100)),
                    velocity_exit_threshold=0.0,
                    acceleration_exit_threshold=(
                        None if rng.random() < 0.5 else 0.0
                    ),
                    stall_deadline_ms=None,
                    minimum_best_net_progress_per_unit=None,
                )
            )
            use_volatility = rng.random() < 0.5
            if use_volatility:
                policy_config = exit_config(
                    initial_stop=ExitDistance("volatility", 3.0),
                    trailing_activation=ExitDistance("volatility", 2.0),
                    trailing_distance=ExitDistance("volatility", 1.0),
                    trailing_volatility_basis=rng.choice(("entry", "current")),
                )
                volatility_values = tuple(
                    rng.choice((0.1, 0.2, 0.4)) for _ in points
                )
            else:
                policy_config = exit_config(
                    initial_stop=ExitDistance("fixed", rng.choice((0.3, 0.6, 1.0)))
                )
                volatility_values = None

            boundary: ReplayBoundary | None
            boundary_kind = rng.randrange(4)
            if boundary_kind == 0:
                boundary = None
            elif boundary_kind == 1:
                end_index = rng.randrange(1, size)
                boundary = ReplayBoundary(
                    end=points[end_index].timestamp,
                    name=f"random-{case}-sentinel",
                )
            elif boundary_kind == 2:
                start_index = rng.randrange(0, size - 1)
                end_index = rng.randrange(start_index + 1, size)
                start = points[start_index].timestamp
                end = points[end_index].timestamp
                if end <= start:
                    end = start + timedelta(microseconds=1)
                boundary = ReplayBoundary(
                    start=start,
                    end=end,
                    name=f"random-{case}-window",
                )
            else:
                boundary = ReplayBoundary(
                    end=points[-1].timestamp + timedelta(milliseconds=1),
                    name=f"random-{case}-complete",
                    input_complete_through_end=True,
                )

            with self.subTest(case=case):
                self.assert_dense_sparse_equal(
                    points,
                    lambda: make_stack(
                        points,
                        events,
                        settings,
                        policy_config,
                        velocities=velocities,
                        accelerations=accelerations,
                        weakening=weakening,
                        volatility_values=volatility_values,
                    ),
                    settings,
                    boundary=boundary,
                )


if __name__ == "__main__":
    unittest.main()
