from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone

from datavis.research.fresh_replay import (
    DecisionFrame,
    DecisionRow,
    FreshExecutionConfig,
    ReplayBoundary,
    ReplayDecision,
    run_fresh_replay,
)
from datavis.research.ticks import Tick


BASE = datetime(2026, 4, 7, 0, 0, tzinfo=timezone.utc)


def tick(index: int, milliseconds: float, bid: float, ask: float | None = None) -> Tick:
    return Tick(
        id=index + 1,
        timestamp=BASE + timedelta(milliseconds=milliseconds),
        bid=bid,
        ask=bid if ask is None else ask,
    )


def frame(points, specifications):
    return DecisionFrame(
        DecisionRow(
            tick_index=index,
            tick_id=points[index].id,
            timestamp=points[index].timestamp,
            decision=ReplayDecision(action, reason),
        )
        for index, action, reason in specifications
    )


def execution(**changes):
    values = {
        "entry_latency_ms": 150,
        "exit_latency_ms": 150,
        "maximum_entry_lag_ms": 1_000,
        "maximum_exit_lag_ms": 1_000,
        "maximum_intertick_gap_ms": 5_000,
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


class FreshReplayTests(unittest.TestCase):
    def test_later_quotes_executable_sides_separate_latencies_and_costs(self):
        points = [
            tick(0, 0, 100.0, 100.2),
            tick(1, 100, 100.1, 100.3),
            tick(2, 150, 100.2, 100.4),
            tick(3, 200, 100.5, 100.7),
            tick(4, 300, 100.6, 100.8),
            tick(5, 400, 101.0, 101.2),
        ]
        decisions = frame(
            points,
            [(0, "enter_long", "entry"), (3, "exit", "momentum weakened")],
        )
        config = execution(
            entry_latency_ms=150,
            exit_latency_ms=200,
            slippage_per_side=0.05,
            commission_per_unit_per_side=0.02,
            quantity=2.0,
            cooldown_ms=0,
        )
        result = run_fresh_replay(points, decisions, config=config)
        self.assertEqual(len(result.trades), 1)
        trade = result.trades[0]
        self.assertEqual(trade.entry_fill_tick_id, 3)
        self.assertEqual(trade.entry_quote_price, 100.4)
        self.assertEqual(trade.entry_fill_price, 100.45)
        self.assertEqual(trade.exit_fill_tick_id, 6)
        self.assertEqual(trade.exit_quote_price, 101.0)
        self.assertEqual(trade.exit_fill_price, 100.95)
        self.assertAlmostEqual(trade.raw_quote_pnl, 1.2)
        self.assertAlmostEqual(trade.slippage_cost, 0.2)
        self.assertAlmostEqual(trade.pnl_after_slippage, 1.0)
        self.assertAlmostEqual(trade.commission, 0.08)
        self.assertAlmostEqual(trade.net_pnl, 0.92)
        self.assertEqual(trade.entry_decision_to_fill_ms, 150)
        self.assertEqual(trade.exit_decision_to_fill_ms, 200)

    def test_zero_latency_still_uses_a_later_row_and_short_is_symmetric(self):
        points = [
            tick(0, 0, 100.0, 100.2),
            tick(1, 1, 99.9, 100.1),
            tick(2, 2, 99.8, 100.0),
            tick(3, 3, 99.0, 99.2),
        ]
        result = run_fresh_replay(
            points,
            frame(points, [(0, "enter_short", "short"), (2, "exit", "exit")]),
            config=execution(
                entry_latency_ms=0,
                exit_latency_ms=0,
                cooldown_ms=0,
            ),
        )
        trade = result.trades[0]
        self.assertEqual(trade.entry_fill_tick_id, 2)
        self.assertEqual(trade.entry_quote_price, 99.9)
        self.assertEqual(trade.exit_fill_tick_id, 4)
        self.assertEqual(trade.exit_quote_price, 99.2)
        self.assertAlmostEqual(trade.net_pnl, 0.7)

    def test_stale_entry_is_canceled_without_creating_exposure(self):
        points = [tick(0, 0, 100), tick(1, 500, 101), tick(2, 600, 101)]
        result = run_fresh_replay(
            points,
            frame(points, [(0, "enter_long", "entry")]),
            config=execution(
                entry_latency_ms=150,
                maximum_entry_lag_ms=100,
                maximum_intertick_gap_ms=1_000,
            ),
        )
        self.assertFalse(result.trades)
        self.assertFalse(result.censors)
        self.assertEqual(result.entry_cancellations[0].reason, "stale-entry")
        self.assertFalse(result.halted)

    def test_stale_exit_censors_and_halts_before_later_decisions(self):
        points = [
            tick(0, 0, 100),
            tick(1, 10, 100),
            tick(2, 20, 100),
            tick(3, 500, 101),
            tick(4, 510, 101),
        ]
        decisions = frame(
            points,
            [
                (0, "enter_long", "entry"),
                (2, "exit", "exit"),
                (4, "enter_short", "must not replay"),
            ],
        )
        result = run_fresh_replay(
            points,
            decisions,
            config=execution(
                entry_latency_ms=0,
                exit_latency_ms=100,
                maximum_exit_lag_ms=100,
                maximum_intertick_gap_ms=1_000,
            ),
        )
        self.assertEqual(result.halt_reason, "stale-exit")
        self.assertEqual(result.censors[0].reason, "stale-exit")
        self.assertFalse(result.trades)
        self.assertNotIn(5, [item.tick_id for item in result.decisions])

    def test_open_position_gap_censors_and_halts(self):
        points = [tick(0, 0, 100), tick(1, 100, 100), tick(2, 1_000, 110)]
        result = run_fresh_replay(
            points,
            frame(points, [(0, "enter_long", "entry")]),
            config=execution(
                entry_latency_ms=0,
                maximum_intertick_gap_ms=200,
            ),
        )
        self.assertEqual(result.censors[0].reason, "intertick-gap")
        self.assertTrue(result.halted)
        self.assertFalse(result.trades)

    def test_boundary_quote_is_never_used_as_a_forced_fill(self):
        points = [tick(0, 0, 100), tick(1, 100, 100), tick(2, 1_000, 110)]
        boundary = ReplayBoundary(start=BASE, end=BASE + timedelta(seconds=1), name="fold-1")
        result = run_fresh_replay(
            points,
            frame(points, [(0, "enter_long", "entry")]),
            config=execution(entry_latency_ms=0),
            boundary=boundary,
        )
        self.assertTrue(result.boundary_reached)
        self.assertEqual(result.censors[0].reason, "boundary-open")
        self.assertFalse(result.trades)

    def test_eof_pending_entry_and_open_position_are_not_force_filled(self):
        pending_points = [tick(0, 0, 100)]
        pending_result = run_fresh_replay(
            pending_points,
            frame(pending_points, [(0, "enter_long", "entry")]),
            config=execution(),
        )
        self.assertEqual(
            pending_result.entry_cancellations[0].reason, "eof-before-entry"
        )
        self.assertFalse(pending_result.censors)

        open_points = [tick(0, 0, 100), tick(1, 200, 100)]
        open_result = run_fresh_replay(
            open_points,
            frame(open_points, [(0, "enter_long", "entry")]),
            config=execution(),
        )
        self.assertEqual(open_result.censors[0].reason, "eof-open")
        self.assertFalse(open_result.trades)

        exit_points = [tick(0, 0, 100), tick(1, 200, 100), tick(2, 300, 101)]
        exit_result = run_fresh_replay(
            exit_points,
            frame(
                exit_points,
                [(0, "enter_long", "entry"), (2, "exit", "pending exit")],
            ),
            config=execution(),
        )
        self.assertEqual(exit_result.censors[0].reason, "eof-pending-exit")
        self.assertFalse(exit_result.trades)

    def test_typed_policy_is_called_sequentially_with_current_state_only(self):
        points = [tick(0, 0, 100), tick(1, 1, 100), tick(2, 2, 101), tick(3, 3, 101)]

        class ScriptPolicy:
            def __init__(self):
                self.observed = []

            def on_tick(self, tick_index, current, context):
                self.observed.append(
                    (tick_index, current.id, context.position is not None)
                )
                if tick_index == 0:
                    return ReplayDecision("enter_long", "entry")
                if tick_index == 2:
                    return ReplayDecision("exit", "exit")
                return None

        policy = ScriptPolicy()
        result = run_fresh_replay(
            points,
            policy,
            config=execution(
                entry_latency_ms=0, exit_latency_ms=0, cooldown_ms=0
            ),
        )
        self.assertEqual(policy.observed, [(0, 1, False), (1, 2, True), (2, 3, True), (3, 4, False)])
        self.assertEqual(len(result.trades), 1)

    def test_actual_fill_deadline_is_strict_at_sixty_seconds(self):
        before = [
            tick(0, 0, 100),
            tick(1, 1, 100),
            tick(2, 2, 100),
            tick(3, 59_999.999, 101),
        ]
        base_config = execution(
            entry_latency_ms=0,
            exit_latency_ms=0,
            maximum_exit_lag_ms=60_000,
            maximum_intertick_gap_ms=120_000,
            cooldown_ms=0,
        )
        before_result = run_fresh_replay(
            before,
            frame(before, [(0, "enter_long", "entry"), (2, "exit", "exit")]),
            config=base_config,
        )
        self.assertEqual(len(before_result.trades), 1)
        self.assertLess(before_result.trades[0].holding_ms, 60_000)

        exact = [
            tick(0, 0, 100),
            tick(1, 1, 100),
            tick(2, 2, 100),
            tick(3, 60_001, 101),
        ]
        exact_result = run_fresh_replay(
            exact,
            frame(exact, [(0, "enter_long", "entry"), (2, "exit", "exit")]),
            config=base_config,
        )
        self.assertFalse(exact_result.trades)
        self.assertEqual(exact_result.censors[0].reason, "actual-fill-deadline")

    def test_one_position_and_cooldown_starts_at_actual_exit(self):
        points = [
            tick(0, 0, 100),
            tick(1, 1, 100),
            tick(2, 2, 100),
            tick(3, 3, 101),
            tick(4, 100, 101),
            tick(5, 1_003, 101),
            tick(6, 1_004, 101),
        ]
        result = run_fresh_replay(
            points,
            frame(
                points,
                [
                    (0, "enter_long", "first"),
                    (1, "enter_short", "busy"),
                    (2, "exit", "close"),
                    (4, "enter_short", "cooldown"),
                    (5, "enter_short", "eligible"),
                ],
            ),
            config=execution(
                entry_latency_ms=0,
                exit_latency_ms=0,
                cooldown_ms=1_000,
                maximum_intertick_gap_ms=2_000,
            ),
        )
        dispositions = [item.disposition for item in result.decisions]
        self.assertEqual(
            dispositions,
            [
                "scheduled-entry",
                "ignored-busy",
                "scheduled-exit",
                "ignored-cooldown",
                "scheduled-entry",
            ],
        )
        self.assertEqual(len(result.trades), 1)
        self.assertEqual(result.censors[-1].reason, "eof-open")

    def test_decision_frame_and_tick_integrity_are_enforced(self):
        points = [tick(0, 0, 100), tick(1, 1, 100)]
        wrong = DecisionFrame(
            [
                DecisionRow(
                    0,
                    999,
                    points[0].timestamp,
                    ReplayDecision("enter_long", "wrong id"),
                )
            ]
        )
        with self.assertRaisesRegex(ValueError, "tick_id"):
            run_fresh_replay(points, wrong, config=execution())

        duplicate_time = replace(points[1], timestamp=points[0].timestamp, id=points[0].id)
        with self.assertRaisesRegex(ValueError, "duplicate tick id"):
            run_fresh_replay(
                [points[0], duplicate_time], DecisionFrame(), config=execution()
            )

    def test_equal_timestamp_distinct_quotes_follow_id_order_and_latency(self):
        same = BASE
        points = [
            Tick(id=1, timestamp=same, bid=100.0, ask=100.2),
            Tick(id=2, timestamp=same, bid=100.1, ask=100.3),
            Tick(id=3, timestamp=same + timedelta(milliseconds=1), bid=100.2, ask=100.4),
            Tick(id=4, timestamp=same + timedelta(milliseconds=2), bid=100.5, ask=100.7),
        ]
        immediate = run_fresh_replay(
            points,
            frame(points, [(0, "enter_long", "entry"), (2, "exit", "exit")]),
            config=execution(entry_latency_ms=0, exit_latency_ms=0),
        )
        self.assertEqual(immediate.trades[0].entry_fill_tick_id, 2)
        self.assertEqual(immediate.trades[0].entry_fill_timestamp, same)

        delayed = run_fresh_replay(
            points,
            frame(points, [(0, "enter_long", "entry"), (2, "exit", "exit")]),
            config=execution(entry_latency_ms=1, exit_latency_ms=0),
        )
        self.assertEqual(delayed.trades[0].entry_fill_tick_id, 3)

        decreasing_id = [points[0], replace(points[1], id=0)]
        with self.assertRaisesRegex(ValueError, r"\(timestamp, id\)"):
            run_fresh_replay(decreasing_id, DecisionFrame(), config=execution())

    def test_exact_repeated_quote_is_a_separate_executable_tick(self):
        same = BASE
        points = [
            Tick(id=1, timestamp=same, bid=100.0, ask=100.2),
            Tick(id=2, timestamp=same, bid=100.0, ask=100.2),
            Tick(
                id=3,
                timestamp=same + timedelta(milliseconds=1),
                bid=100.3,
                ask=100.5,
            ),
            Tick(
                id=4,
                timestamp=same + timedelta(milliseconds=2),
                bid=100.4,
                ask=100.6,
            ),
        ]
        result = run_fresh_replay(
            points,
            frame(points, [(0, "enter_long", "entry"), (2, "exit", "exit")]),
            config=execution(entry_latency_ms=0, exit_latency_ms=0),
        )
        self.assertEqual(result.trades[0].entry_fill_tick_id, 2)
        self.assertEqual(result.trades[0].entry_fill_timestamp, same)

    def test_deadline_cannot_be_configured_past_one_minute(self):
        with self.assertRaisesRegex(ValueError, "60-second"):
            execution(actual_fill_deadline_ms=60_001)

    def test_post_gap_rearm_blocks_flat_entries_until_elapsed(self):
        points = [
            tick(0, 0, 100),
            tick(1, 1_000, 100),
            tick(2, 1_500, 100),
            tick(3, 2_000, 100),
            tick(4, 2_001, 100),
        ]
        result = run_fresh_replay(
            points,
            frame(
                points,
                [
                    (1, "enter_long", "blocked-at-gap"),
                    (2, "enter_long", "blocked-warmup"),
                    (3, "enter_long", "eligible"),
                ],
            ),
            config=execution(
                entry_latency_ms=0,
                maximum_intertick_gap_ms=600,
                post_gap_rearm_ms=1_000,
            ),
        )
        self.assertEqual(
            [item.disposition for item in result.decisions],
            ["ignored-gap-rearm", "ignored-gap-rearm", "scheduled-entry"],
        )
        self.assertEqual(result.censors[0].entry_tick_id, 5)

    def test_complete_boundary_without_sentinel_is_classified_at_end(self):
        points = [tick(0, 0, 100), tick(1, 1, 100)]
        boundary = ReplayBoundary(
            start=BASE,
            end=BASE + timedelta(seconds=1),
            name="complete-fold",
            input_complete_through_end=True,
        )
        result = run_fresh_replay(
            points,
            frame(points, [(0, "enter_long", "entry")]),
            config=execution(entry_latency_ms=0),
            boundary=boundary,
        )
        self.assertTrue(result.boundary_reached)
        self.assertEqual(result.censors[0].reason, "boundary-open")
        self.assertEqual(result.censors[0].censor_timestamp, boundary.end)

    def test_trusted_session_tuple_matches_default_with_repeat_and_gap(self):
        points = (
            Tick(id=1, timestamp=BASE, bid=100.0, ask=100.2),
            Tick(id=2, timestamp=BASE, bid=100.0, ask=100.2),
            Tick(
                id=3,
                timestamp=BASE + timedelta(milliseconds=1),
                bid=100.4,
                ask=100.6,
            ),
            Tick(
                id=4,
                timestamp=BASE + timedelta(milliseconds=2),
                bid=100.5,
                ask=100.7,
            ),
            Tick(
                id=5,
                timestamp=BASE + timedelta(milliseconds=1_000),
                bid=100.5,
                ask=100.7,
            ),
            Tick(
                id=6,
                timestamp=BASE + timedelta(milliseconds=1_500),
                bid=100.5,
                ask=100.7,
            ),
            Tick(
                id=7,
                timestamp=BASE + timedelta(milliseconds=2_000),
                bid=100.5,
                ask=100.7,
            ),
            Tick(
                id=8,
                timestamp=BASE + timedelta(milliseconds=2_001),
                bid=100.5,
                ask=100.7,
            ),
        )
        decisions = frame(
            points,
            [
                (0, "enter_long", "entry"),
                (2, "exit", "exit"),
                (4, "enter_long", "blocked-at-gap"),
                (5, "enter_long", "blocked-during-rearm"),
                (6, "enter_long", "eligible-after-rearm"),
            ],
        )
        config = execution(
            entry_latency_ms=0,
            exit_latency_ms=0,
            maximum_intertick_gap_ms=600,
            post_gap_rearm_ms=1_000,
        )

        checked = run_fresh_replay(points, decisions, config=config)
        trusted = run_fresh_replay(
            points,
            decisions,
            config=config,
            _trusted_validated_ticks=True,
        )

        self.assertEqual(trusted, checked)
        self.assertEqual(trusted.trades[0].entry_fill_tick_id, 2)
        self.assertEqual(
            [item.disposition for item in trusted.decisions],
            [
                "scheduled-entry",
                "scheduled-exit",
                "ignored-gap-rearm",
                "ignored-gap-rearm",
                "scheduled-entry",
            ],
        )

    def test_default_tape_validation_remains_enabled_and_trust_requires_tuple(self):
        malformed = (
            Tick(id=1, timestamp=BASE, bid=100.0, ask=100.2),
            Tick(id=1, timestamp=BASE, bid=100.0, ask=100.2),
        )
        with self.assertRaisesRegex(ValueError, "duplicate tick id"):
            run_fresh_replay(malformed, DecisionFrame(), config=execution())
        with self.assertRaisesRegex(TypeError, "validated tick tuple"):
            run_fresh_replay(
                list(malformed),
                DecisionFrame(),
                config=execution(),
                _trusted_validated_ticks=True,
            )


if __name__ == "__main__":
    unittest.main()
