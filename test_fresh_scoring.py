from __future__ import annotations

import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from datavis.research.fresh_preregistration import (
    FRESH_V2_WINDOW_POLICY,
    build_fresh_implementation_manifest,
    build_fresh_preregistration_v2,
    required_fresh_implementation_files,
)
from datavis.research.fresh_protocol import build_fresh_split_manifest
from datavis.research.fresh_scoring import (
    BALANCED_COMPONENT_NAMES,
    BalancedScoreSpecification,
    CandidateRankRecord,
    ChronologicalGateItem,
    EntryMetricConfig,
    EntryPromotionThresholds,
    FullStrategyThresholds,
    GateResult,
    MinimumSampleThresholds,
    RegisteredScoringConfig,
    SliceDimensions,
    TradeMetricConfig,
    build_candidate_scorecard,
    compute_balanced_score,
    evaluate_chronological_gates,
    evaluate_entry_gate,
    evaluate_full_strategy_gate,
    rank_candidates,
    score_entry_diagnostics,
    score_replay_result,
    score_trade_records,
    scoring_config_from_preregistration,
)


BASE = datetime(2026, 4, 7, 0, 0, tzinfo=timezone.utc)
DIMENSIONS = SliceDimensions(
    day_metadata_path="context.day",
    market_session_metadata_path="context.market_session",
    regime_metadata_path="context.regime",
)


def entry_metric_config() -> EntryMetricConfig:
    return EntryMetricConfig(
        coverage_checkpoints_seconds=(1, 2, 5, 10, 20, 30, 60),
        restricted_uncovered_milliseconds=60_000,
        profit_barrier_net_per_unit=0.25,
        loss_barrier_net_per_unit=0.25,
    )


def trade_metric_config() -> TradeMetricConfig:
    return TradeMetricConfig(
        pnl_classification_tolerance=1e-12,
        loss_tail_quantile_probability=0.95,
        require_boundary_reached=True,
    )


def metadata(day: str, session: str, regime: str) -> dict:
    return {
        "context": {
            "day": day,
            "market_session": session,
            "regime": regime,
        }
    }


def event(day: str, side: str, session: str = "london", regime: str = "fast"):
    return SimpleNamespace(
        side=side,
        metadata=metadata(day, session, regime),
    )


def diagnostic(
    position: int,
    day: str,
    side: str,
    *,
    coverage_ms: float | None,
    barrier: str | None,
    censored: bool = False,
    session: str = "london",
    regime: str = "fast",
):
    flags = {}
    for checkpoint in (1, 2, 5, 10, 20, 30, 60):
        flags[f"cost_covered_by_{checkpoint}s"] = bool(
            coverage_ms is not None
            and (
                coverage_ms < checkpoint * 1_000
                if checkpoint == 60
                else coverage_ms <= checkpoint * 1_000
            )
        )
    return SimpleNamespace(
        event_position=position,
        event=event(day, side, session, regime),
        time_to_cost_coverage_ms=coverage_ms,
        first_barrier_hit=barrier,
        censored=censored,
        horizon_complete=not censored,
        observation_end_reason="intertick_gap" if censored else "horizon_complete",
        **flags,
    )


def rejection(
    position: int,
    day: str,
    side: str,
    reason: str = "maximum_entry_lag_exceeded",
):
    return SimpleNamespace(
        event_position=position,
        event=event(day, side),
        reason=reason,
    )


def diagnostic_result(diagnostics, rejections=()):
    rejections = tuple(rejections)
    return SimpleNamespace(
        diagnostics=tuple(diagnostics),
        rejections=rejections,
        event_count=len(tuple(diagnostics)) + len(rejections),
        rejected_reason_counts={
            reason: sum(item.reason == reason for item in rejections)
            for reason in sorted({item.reason for item in rejections})
        },
    )


def trade(
    index: int,
    pnl: float,
    day: str,
    *,
    side: str = "long",
    session: str = "london",
    regime: str = "fast",
):
    entry_time = BASE + timedelta(seconds=index * 2)
    return SimpleNamespace(
        side=side,
        net_pnl=pnl,
        entry_fill_timestamp=entry_time,
        entry_fill_tick_id=index * 2 + 1,
        exit_fill_timestamp=entry_time + timedelta(seconds=1),
        exit_fill_tick_id=index * 2 + 2,
        entry_metadata=metadata(day, session, regime),
    )


def balanced_spec() -> BalancedScoreSpecification:
    return BalancedScoreSpecification(
        component_weights=(
            ("expectancyScaledByMedianAbsoluteTradePnl", 0.25),
            ("coverageProbabilityAndSpeed", 0.20),
            ("profitFactorCappedAtTwo", 0.15),
            ("inverseDrawdownToGrossProfit", 0.12),
            ("positiveSessionFraction", 0.10),
            ("requiredStressPassFraction", 0.08),
            ("inverseLargestSessionProfitConcentration", 0.05),
            ("tradeCountAdequacy", 0.05),
        ),
        coverage_probability_weights=(
            (2, 0.15),
            (5, 0.25),
            (10, 0.25),
            (30, 0.20),
            (60, 0.15),
        ),
        coverage_probability_share=0.75,
        restricted_median_speed_share=0.25,
    )


def minimum_sample() -> MinimumSampleThresholds:
    return MinimumSampleThresholds(
        filled_trades_per_session=1,
        absolute_filled_trades=1,
        active_session_fraction_minimum=1.0,
    )


def entry_thresholds() -> EntryPromotionThresholds:
    return EntryPromotionThresholds(
        fill_rate_minimum=0.90,
        coverage_10_seconds_minimum=0.50,
        coverage_30_seconds_minimum=0.60,
        coverage_60_seconds_minimum=0.65,
        restricted_median_coverage_milliseconds_maximum=30_000,
        censored_fraction_maximum=0.02,
        equal_barrier_distance_per_unit=0.25,
        equal_barrier_profit_first_rate_minimum=0.52,
    )


def full_thresholds() -> FullStrategyThresholds:
    return FullStrategyThresholds(
        reference_profit_factor_minimum=1.10,
        positive_session_fraction_minimum=0.50,
        maximum_drawdown_to_gross_profit_maximum=0.65,
        largest_trade_share_of_gross_profit_maximum=0.40,
        largest_session_share_of_gross_profit_maximum=0.60,
        loss_95_to_median_absolute_loss_maximum=3.0,
        required_stress_profit_factor_minimum=1.0,
        full_replay_censor_count_maximum=0,
        reference_net_pnl_strictly_positive=True,
        reference_expectancy_strictly_positive=True,
        required_stress_net_pnl_strictly_positive=True,
        profitability_valid_required=True,
        entry_promotion_gates_still_required=True,
    )


class EntryMetricTests(unittest.TestCase):
    def test_exact_counts_censor_failures_coverage_barrier_and_slices(self):
        result = diagnostic_result(
            [
                diagnostic(0, "d1", "long", coverage_ms=500, barrier="profit"),
                diagnostic(1, "d1", "short", coverage_ms=12_000, barrier="loss"),
                diagnostic(
                    2,
                    "d2",
                    "long",
                    coverage_ms=1_000,
                    barrier="profit",
                    censored=True,
                    session="new_york",
                    regime="wide",
                ),
            ],
            [rejection(3, "d2", "short")],
        )
        report = score_entry_diagnostics(
            result,
            config=entry_metric_config(),
            dimensions=DIMENSIONS,
            evaluated_sessions=("d1", "d2", "d3"),
        )
        metrics = report.overall
        self.assertEqual((metrics.signal_count, metrics.filled_count), (4, 3))
        self.assertEqual((metrics.rejected_count, metrics.censored_count), (1, 1))
        self.assertAlmostEqual(metrics.fill_rate, 0.75)
        self.assertAlmostEqual(metrics.censored_fraction, 1 / 3)
        self.assertAlmostEqual(metrics.coverage_probability(1), 1 / 3)
        self.assertAlmostEqual(metrics.coverage_probability(10), 1 / 3)
        self.assertAlmostEqual(metrics.coverage_probability(20), 2 / 3)
        self.assertAlmostEqual(metrics.coverage_probability(60), 2 / 3)
        self.assertEqual(metrics.restricted_median_coverage_milliseconds, 12_000)
        self.assertEqual(metrics.median_covered_time_milliseconds, 6_250)
        self.assertEqual(metrics.barrier_profit_first_count, 1)
        self.assertEqual(metrics.barrier_loss_first_count, 1)
        self.assertEqual(metrics.barrier_no_hit_count, 1)
        self.assertAlmostEqual(metrics.barrier_profit_first_rate, 1 / 3)
        self.assertEqual(metrics.rejection_reason_counts, (("maximum_entry_lag_exceeded", 1),))
        self.assertEqual(metrics.censor_reason_counts, (("intertick_gap", 1),))
        self.assertEqual(metrics.active_session_fraction, 2 / 3)
        self.assertEqual([name for name, _ in report.by_day], ["d1", "d2", "d3"])
        self.assertEqual(dict(report.by_day)["d3"].signal_count, 0)
        self.assertEqual([name for name, _ in report.by_side], ["long", "short"])
        self.assertEqual(
            [name for name, _ in report.by_market_session],
            ["london", "new_york"],
        )

    def test_inconsistent_event_or_coverage_audit_fields_are_rejected(self):
        bad = diagnostic(0, "d1", "long", coverage_ms=500, barrier="profit")
        bad.cost_covered_by_1s = False
        with self.assertRaisesRegex(ValueError, "inconsistent"):
            score_entry_diagnostics(
                diagnostic_result([bad]),
                config=entry_metric_config(),
                dimensions=DIMENSIONS,
                evaluated_sessions=("d1",),
            )
        result = diagnostic_result([], [rejection(0, "d1", "long")])
        result.event_count = 2
        with self.assertRaisesRegex(ValueError, "event_count"):
            score_entry_diagnostics(
                result,
                config=entry_metric_config(),
                dimensions=DIMENSIONS,
                evaluated_sessions=("d1",),
            )


class TradeMetricTests(unittest.TestCase):
    def test_pnl_drawdown_tail_concentration_and_all_slices(self):
        values = [
            trade(0, 2.0, "d1", side="long"),
            trade(1, -1.0, "d1", side="short"),
            trade(2, 1.0, "d2", side="long", session="new_york"),
            trade(3, -3.0, "d2", side="short", regime="wide"),
            trade(4, 0.0, "d3", side="long"),
        ]
        report = score_trade_records(
            values,
            config=trade_metric_config(),
            dimensions=DIMENSIONS,
            evaluated_sessions=("d1", "d2", "d3"),
            replay_censor_count=0,
            profitability_valid=True,
        )
        metrics = report.overall
        self.assertEqual((metrics.trade_count, metrics.win_count, metrics.loss_count), (5, 2, 2))
        self.assertEqual(metrics.flat_count, 1)
        self.assertAlmostEqual(metrics.win_rate, 0.4)
        self.assertAlmostEqual(metrics.net_pnl, -1.0)
        self.assertAlmostEqual(metrics.expectancy, -0.2)
        self.assertAlmostEqual(metrics.gross_profit, 3.0)
        self.assertAlmostEqual(metrics.gross_loss, 4.0)
        self.assertAlmostEqual(metrics.profit_factor, 0.75)
        self.assertAlmostEqual(metrics.maximum_drawdown, 3.0)
        self.assertAlmostEqual(metrics.maximum_drawdown_to_gross_profit, 1.0)
        self.assertAlmostEqual(metrics.loss_95_absolute, 2.9)
        self.assertAlmostEqual(metrics.median_absolute_loss, 2.0)
        self.assertAlmostEqual(metrics.loss_95_to_median_absolute_loss, 1.45)
        self.assertAlmostEqual(metrics.largest_trade_share_of_gross_profit, 2 / 3)
        self.assertAlmostEqual(metrics.largest_session_share_of_gross_profit, 1 / 3)
        self.assertAlmostEqual(metrics.positive_session_fraction, 1 / 3)
        self.assertEqual(dict(metrics.session_net_pnl), {"d1": 1.0, "d2": -2.0, "d3": 0.0})
        self.assertEqual([key for key, _ in report.by_day], ["d1", "d2", "d3"])
        self.assertEqual([key for key, _ in report.by_side], ["long", "short"])
        self.assertEqual([key for key, _ in report.by_regime], ["fast", "wide"])

    def test_zero_and_no_loss_cases_are_explicit_not_fabricated(self):
        empty = score_trade_records(
            (),
            config=trade_metric_config(),
            dimensions=DIMENSIONS,
            evaluated_sessions=("d1",),
            replay_censor_count=0,
            profitability_valid=True,
        ).overall
        self.assertEqual(empty.net_pnl, 0.0)
        self.assertEqual(empty.maximum_drawdown, 0.0)
        self.assertIsNone(empty.expectancy)
        self.assertIsNone(empty.profit_factor)
        self.assertIsNone(empty.maximum_drawdown_to_gross_profit)
        self.assertIsNone(empty.loss_95_to_median_absolute_loss)

        all_wins = score_trade_records(
            [trade(0, 1.0, "d1"), trade(1, 2.0, "d1")],
            config=trade_metric_config(),
            dimensions=DIMENSIONS,
            evaluated_sessions=("d1",),
            replay_censor_count=0,
            profitability_valid=True,
        ).overall
        self.assertEqual(all_wins.profit_factor, "Infinity")
        self.assertIsNone(all_wins.loss_95_to_median_absolute_loss)

    def test_replay_completeness_and_tolerance_binding(self):
        replay = SimpleNamespace(
            trades=(trade(0, 1.0, "d1"),),
            censors=(SimpleNamespace(reason="fold_end"),),
            halted=False,
            boundary_reached=True,
            config=SimpleNamespace(pnl_classification_tolerance=1e-12),
        )
        report = score_replay_result(
            replay,
            config=trade_metric_config(),
            dimensions=DIMENSIONS,
            evaluated_sessions=("d1",),
        )
        self.assertFalse(report.overall.profitability_valid)
        self.assertEqual(report.overall.replay_censor_count, 1)
        replay.censors = ()
        replay.config.pnl_classification_tolerance = 1e-6
        with self.assertRaisesRegex(ValueError, "tolerance"):
            score_replay_result(
                replay,
                config=trade_metric_config(),
                dimensions=DIMENSIONS,
                evaluated_sessions=("d1",),
            )

    def test_non_chronological_trades_are_not_sorted_silently(self):
        with self.assertRaisesRegex(ValueError, "chronological"):
            score_trade_records(
                [trade(1, 1.0, "d1"), trade(0, 1.0, "d1")],
                config=trade_metric_config(),
                dimensions=DIMENSIONS,
                evaluated_sessions=("d1",),
                replay_censor_count=0,
                profitability_valid=True,
            )


class GateAndScoreTests(unittest.TestCase):
    def setUp(self):
        self.days = ("d1", "d2")
        self.entry_report = score_entry_diagnostics(
            diagnostic_result(
                [
                    diagnostic(
                        index,
                        self.days[index % 2],
                        "long" if index % 2 == 0 else "short",
                        coverage_ms=500,
                        barrier="profit",
                    )
                    for index in range(6)
                ]
            ),
            config=entry_metric_config(),
            dimensions=DIMENSIONS,
            evaluated_sessions=self.days,
        )
        pnls = (1.0, -0.5, 1.0, -0.5, 1.0, -0.5)
        trades = [trade(index, pnl, self.days[index // 3]) for index, pnl in enumerate(pnls)]
        self.reference = score_trade_records(
            trades,
            config=trade_metric_config(),
            dimensions=DIMENSIONS,
            evaluated_sessions=self.days,
            replay_censor_count=0,
            profitability_valid=True,
        )

    def test_hard_gates_and_balanced_score_pass_complete_inputs(self):
        sample = minimum_sample()
        entry_gate = evaluate_entry_gate(
            self.entry_report.overall,
            minimum_sample=sample,
            thresholds=entry_thresholds(),
        )
        self.assertTrue(entry_gate.passed, entry_gate.failed_check_names)
        stress_metrics = {
            "latency": self.reference.overall,
            "friction": self.reference.overall,
        }
        full_gate = evaluate_full_strategy_gate(
            self.reference.overall,
            stress_metrics,
            entry_gate,
            minimum_sample=sample,
            thresholds=full_thresholds(),
            required_stress_scenario_ids=("latency", "friction"),
        )
        self.assertTrue(full_gate.passed, full_gate.failed_check_names)
        score = compute_balanced_score(
            self.entry_report.overall,
            self.reference.overall,
            stress_metrics,
            minimum_sample=sample,
            specification=balanced_spec(),
            required_stress_scenario_ids=("latency", "friction"),
        )
        self.assertTrue(score.valid)
        self.assertGreaterEqual(score.score, -1.0)
        self.assertLessEqual(score.score, 1.0)
        self.assertEqual(tuple(name for name, _ in score.components), BALANCED_COMPONENT_NAMES)

    def test_scorecard_does_not_let_score_override_failed_hard_gate(self):
        config = RegisteredScoringConfig(
            entry_metrics=entry_metric_config(),
            trade_metrics=trade_metric_config(),
            minimum_sample=minimum_sample(),
            entry_gate=entry_thresholds(),
            full_gate=full_thresholds(),
            balanced_score=balanced_spec(),
            required_stress_scenario_ids=("latency", "friction"),
        )
        bad_stress = score_trade_records(
            [trade(0, -1.0, "d1"), trade(1, -1.0, "d2")],
            config=trade_metric_config(),
            dimensions=DIMENSIONS,
            evaluated_sessions=self.days,
            replay_censor_count=0,
            profitability_valid=True,
        )
        scorecard = build_candidate_scorecard(
            self.entry_report,
            self.reference,
            {"latency": self.reference, "friction": bad_stress},
            config=config,
        )
        self.assertFalse(scorecard.full_gate.passed)
        self.assertIn("stress.friction.net_pnl_positive", scorecard.full_gate.failed_check_names)
        self.assertIsNotNone(scorecard.balanced_score.score)

    def test_profitable_but_sparse_required_stress_fails_sample_support(self):
        sample = minimum_sample()
        entry_gate = evaluate_entry_gate(
            self.entry_report.overall,
            minimum_sample=sample,
            thresholds=entry_thresholds(),
        )
        sparse = score_trade_records(
            [trade(0, 1.0, "d1")],
            config=trade_metric_config(),
            dimensions=DIMENSIONS,
            evaluated_sessions=self.days,
            replay_censor_count=0,
            profitability_valid=True,
        ).overall

        gate = evaluate_full_strategy_gate(
            self.reference.overall,
            {"latency": sparse, "friction": self.reference.overall},
            entry_gate,
            minimum_sample=sample,
            thresholds=full_thresholds(),
            required_stress_scenario_ids=("latency", "friction"),
        )

        self.assertFalse(gate.passed)
        self.assertIn(
            "stress.latency.minimum_trade_count", gate.failed_check_names
        )
        self.assertIn(
            "stress.latency.minimum_active_session_fraction",
            gate.failed_check_names,
        )

    def test_undefined_zero_trade_components_produce_no_balanced_score(self):
        empty = score_trade_records(
            (),
            config=trade_metric_config(),
            dimensions=DIMENSIONS,
            evaluated_sessions=self.days,
            replay_censor_count=0,
            profitability_valid=True,
        )
        score = compute_balanced_score(
            self.entry_report.overall,
            empty.overall,
            {"latency": empty.overall, "friction": empty.overall},
            minimum_sample=minimum_sample(),
            specification=balanced_spec(),
            required_stress_scenario_ids=("latency", "friction"),
        )
        self.assertIsNone(score.score)
        self.assertIn(
            "expectancyScaledByMedianAbsoluteTradePnl", score.invalid_components
        )
        self.assertIn("profitFactorCappedAtTwo", score.invalid_components)


class ChronologyAndRankingTests(unittest.TestCase):
    def test_required_windows_must_pass_independently_in_order(self):
        yes = GateResult(True, ())
        no = GateResult(False, ())
        items = (
            ChronologicalGateItem("wf1", BASE, BASE + timedelta(days=1), yes),
            ChronologicalGateItem(
                "wf2", BASE + timedelta(days=1), BASE + timedelta(days=2), no
            ),
            ChronologicalGateItem(
                "validation", BASE + timedelta(days=2), BASE + timedelta(days=3), yes
            ),
        )
        result = evaluate_chronological_gates(
            items, required_windows=("wf1", "wf2")
        )
        self.assertFalse(result.passed)
        self.assertEqual(result.failed_windows, ("wf2",))
        with self.assertRaisesRegex(ValueError, "overlap"):
            evaluate_chronological_gates(
                (
                    items[0],
                    ChronologicalGateItem(
                        "bad",
                        BASE + timedelta(hours=12),
                        BASE + timedelta(days=2),
                        yes,
                    ),
                ),
                required_windows=("wf1", "bad"),
            )

    def test_ranking_uses_every_tie_break_then_candidate_id(self):
        base = dict(
            hard_gate_passed=True,
            balanced_score=0.5,
            per_window_expectancies=(0.1, 0.2),
            required_stress_expectancies=(0.05, 0.1),
            maximum_drawdown=2.0,
            rule_complexity=3,
        )
        records = [
            CandidateRankRecord(candidate_id="z", **base),
            CandidateRankRecord(candidate_id="a", **base),
            CandidateRankRecord(
                candidate_id="better-window",
                **{**base, "per_window_expectancies": (0.11, 0.2)},
            ),
            CandidateRankRecord(
                candidate_id="failed-high-score",
                **{**base, "hard_gate_passed": False, "balanced_score": 0.9},
            ),
        ]
        ranked = rank_candidates(reversed(records))
        self.assertEqual(
            [item.candidate_id for item in ranked],
            ["better-window", "a", "z", "failed-high-score"],
        )


class PreregistrationParserTests(unittest.TestCase):
    def test_validated_preregistration_materializes_all_scoring_thresholds(self):
        root = Path(__file__).resolve().parent
        implementation_files = required_fresh_implementation_files()
        manifest = build_fresh_implementation_manifest(
            repository_root=root,
            relative_paths=implementation_files,
        )
        anchors = []
        cursor = date(2025, 1, 2)
        while len(anchors) < FRESH_V2_WINDOW_POLICY.required_sessions:
            if cursor.weekday() < 5:
                anchors.append(cursor.isoformat())
            cursor += timedelta(days=1)
        split = build_fresh_split_manifest(
            anchors,
            inventory_sha256="a" * 64,
            excluded_sessions=[
                {"sessionAnchor": "2025-01-01", "reason": "known partial source"}
            ],
            policy=FRESH_V2_WINDOW_POLICY,
        )
        preregistration = build_fresh_preregistration_v2(
            split_manifest=split,
            corpus_manifest_sha256="b" * 64,
            protocol_code_identifier="fresh-scoring-test",
            implementation_manifest=manifest,
            experiment_ledger_path=root / "artifacts" / "unused-score-ledger.jsonl",
            holdout_authorization_registry_path=root
            / "artifacts"
            / "unused-score-holdout.json",
        )
        parsed = scoring_config_from_preregistration(preregistration)
        self.assertEqual(
            parsed.entry_metrics.coverage_checkpoints_seconds,
            (1, 2, 5, 10, 20, 30, 60),
        )
        self.assertEqual(parsed.entry_metrics.profit_barrier_net_per_unit, 0.25)
        self.assertEqual(parsed.minimum_sample.absolute_filled_trades, 30)
        self.assertEqual(
            parsed.required_stress_scenario_ids,
            ("latency-stress", "friction-stress"),
        )
        self.assertAlmostEqual(
            sum(weight for _, weight in parsed.balanced_score.component_weights), 1.0
        )


if __name__ == "__main__":
    unittest.main()
