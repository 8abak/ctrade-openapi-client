from __future__ import annotations

import dataclasses
import re
import unittest

from datavis.research.fresh_exit_grid import (
    FRESH_EXIT_GRID_SCHEMA,
    FRESH_EXIT_QUANTILE_RANKS,
    FRESH_EXIT_RANK_NEIGHBOURHOOD,
    FRESH_EXIT_VOLATILITY_COLUMN,
    MAXIMUM_EXIT_VARIANTS,
    build_fresh_exit_grid,
    fresh_exit_quantile_measurements,
)
from datavis.research.fresh_replay import FreshExecutionConfig
from datavis.research.fresh_thresholds import (
    FreshQuantileBank,
    FreshQuantileBankConfig,
    QuantileThreshold,
)


SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def threshold_value(measurement: str, rank: float) -> float:
    if measurement == "identity::spread":
        return 0.10 + rank / 10.0
    if measurement == "absolute::1s_mid_speed":
        return 0.50 + rank
    if measurement == "absolute::1s_mid_acceleration":
        return 1.00 + 2.0 * rank
    raise AssertionError(measurement)


def bank() -> FreshQuantileBank:
    measurements = fresh_exit_quantile_measurements()
    thresholds = tuple(
        QuantileThreshold(
            measurement=spec.name,
            rank=rank,
            value=threshold_value(spec.name, rank),
            eligible_session_count=46,
            finite_value_count=460_000,
            minimum_session_quantile=threshold_value(spec.name, rank) * 0.8,
            maximum_session_quantile=threshold_value(spec.name, rank) * 1.2,
        )
        for spec in measurements
        for rank in FRESH_EXIT_QUANTILE_RANKS
    )
    return FreshQuantileBank(
        config=FreshQuantileBankConfig(
            ranks=FRESH_EXIT_QUANTILE_RANKS,
            minimum_finite_values_per_session=1_000,
            minimum_eligible_sessions=40,
        ),
        training_session_anchors=("2026-01-02",),
        measurements=measurements,
        thresholds=thresholds,
        bank_sha256="a" * 64,
    )


def execution(
    *,
    entry_latency_ms: int,
    exit_latency_ms: int,
    maximum_exit_lag_ms: int,
    actual_fill_deadline_ms: int = 60_000,
    slippage_per_side: float = 0.02,
    commission_per_unit_per_side: float = 0.035,
) -> FreshExecutionConfig:
    return FreshExecutionConfig(
        entry_latency_ms=entry_latency_ms,
        exit_latency_ms=exit_latency_ms,
        maximum_entry_lag_ms=1_000,
        maximum_exit_lag_ms=maximum_exit_lag_ms,
        maximum_intertick_gap_ms=5_000,
        actual_fill_deadline_ms=actual_fill_deadline_ms,
        cooldown_ms=0,
        post_gap_rearm_ms=5_000,
        quantity=1.0,
        slippage_per_side=slippage_per_side,
        commission_per_unit_per_side=commission_per_unit_per_side,
        pnl_classification_tolerance=1e-12,
    )


def executions() -> dict[str, FreshExecutionConfig]:
    return {
        "reference-provisional": execution(
            entry_latency_ms=250,
            exit_latency_ms=250,
            maximum_exit_lag_ms=1_000,
        ),
        "latency-stress": execution(
            entry_latency_ms=500,
            exit_latency_ms=500,
            maximum_exit_lag_ms=1_000,
        ),
        "friction-stress": execution(
            entry_latency_ms=250,
            exit_latency_ms=250,
            maximum_exit_lag_ms=1_000,
            slippage_per_side=0.05,
            commission_per_unit_per_side=0.07,
        ),
    }


class FreshExitGridTests(unittest.TestCase):
    def test_grid_is_deterministic_unique_and_within_registered_budget(self):
        first = build_fresh_exit_grid(bank(), execution_configs=executions())
        second = build_fresh_exit_grid(
            bank(),
            execution_configs=dict(reversed(tuple(executions().items()))),
        )
        self.assertEqual(first, second)
        self.assertEqual(first.schema, FRESH_EXIT_GRID_SCHEMA)
        self.assertEqual(len(first.variants), 72)
        self.assertLessEqual(len(first.variants), MAXIMUM_EXIT_VARIANTS)
        self.assertTrue(SHA256.fullmatch(first.grid_sha256))
        self.assertTrue(SHA256.fullmatch(first.execution_scenarios_sha256))
        self.assertEqual(
            tuple(item.scenario_id for item in first.execution_scenarios),
            tuple(sorted(executions())),
        )
        ids = [item.variant_id for item in first.variants]
        hashes = [item.variant_sha256 for item in first.variants]
        self.assertEqual(len(ids), len(set(ids)))
        self.assertEqual(len(hashes), len(set(hashes)))
        self.assertTrue(all(SHA256.fullmatch(value) for value in hashes))

    def test_bank_covers_every_registered_exit_structure(self):
        grid = build_fresh_exit_grid(bank(), execution_configs=executions())
        policies = [item.policy for item in grid.variants]
        self.assertEqual(
            {item.initial_stop.mode for item in policies},
            {"fixed", "volatility"},
        )
        self.assertTrue(any(item.break_even_activation is None for item in policies))
        self.assertTrue(
            any(
                item.break_even_activation is not None
                and item.break_even_buffer_net_per_unit == 0.0
                for item in policies
            )
        )
        self.assertTrue(
            any(item.break_even_buffer_net_per_unit > 0.0 for item in policies)
        )
        self.assertEqual(
            {
                item.trailing_distance.mode
                for item in policies
                if item.trailing_distance
            },
            {"fixed", "volatility"},
        )
        self.assertEqual(
            {item.maximum_holding_ms for item in policies},
            {5_000, 10_000, 20_000, 30_000},
        )

        weakening = [item.weakening for item in grid.variants]
        self.assertTrue(any(item is None for item in weakening))
        self.assertTrue(
            any(
                item is not None and item.acceleration_exit_threshold is None
                for item in weakening
            )
        )
        self.assertTrue(
            any(
                item is not None and item.acceleration_exit_threshold is not None
                for item in weakening
            )
        )
        self.assertTrue(
            any(
                item is not None and item.stall_deadline_ms is not None
                for item in weakening
            )
        )

        for variant in grid.variants:
            if variant.policy.requires_volatility:
                self.assertEqual(
                    variant.volatility_feature_column,
                    FRESH_EXIT_VOLATILITY_COLUMN,
                )
            else:
                self.assertIsNone(variant.volatility_feature_column)

    def test_every_fitted_field_has_exact_bank_provenance(self):
        selected_bank = bank()
        grid = build_fresh_exit_grid(
            selected_bank,
            execution_configs=executions(),
        )
        required = {item.name: item for item in grid.required_measurements}
        self.assertEqual(
            grid.required_measurements,
            fresh_exit_quantile_measurements(),
        )
        for variant in grid.variants:
            self.assertIn(
                variant.rank_offset,
                FRESH_EXIT_RANK_NEIGHBOURHOOD,
            )
            for provenance in variant.threshold_provenance:
                spec = required[provenance.measurement]
                self.assertEqual(provenance.column, spec.column)
                self.assertEqual(provenance.transform, spec.transform)
                self.assertAlmostEqual(
                    provenance.resolved_rank,
                    provenance.base_rank + provenance.rank_offset,
                )
                self.assertEqual(
                    provenance.bank_value,
                    selected_bank.threshold(
                        provenance.measurement,
                        provenance.resolved_rank,
                    ),
                )
                self.assertAlmostEqual(
                    provenance.final_value,
                    provenance.additive_floor
                    + provenance.bank_value * provenance.multiplier,
                )
            if variant.policy.initial_stop.mode == "fixed":
                matches = [
                    item
                    for item in variant.threshold_provenance
                    if item.parameter == "policy.initial_stop.value"
                ]
                self.assertEqual(len(matches), 1)
                self.assertEqual(
                    variant.policy.initial_stop.value,
                    matches[0].final_value,
                )

    def test_true_break_even_activation_covers_worst_registered_cost(self):
        selected_executions = executions()
        grid = build_fresh_exit_grid(
            bank(),
            execution_configs=selected_executions,
        )
        worst_cost = max(
            item.slippage_per_side + 2.0 * item.commission_per_unit_per_side
            for item in selected_executions.values()
        )
        managed = [
            item
            for item in grid.variants
            if item.policy.break_even_activation is not None
        ]
        self.assertTrue(managed)
        for variant in managed:
            activation = variant.policy.break_even_activation
            assert activation is not None
            required = worst_cost + variant.policy.break_even_buffer_net_per_unit
            self.assertGreater(activation.value, required)

    def test_every_deadline_is_fillable_inside_sixty_seconds(self):
        selected_executions = executions()
        grid = build_fresh_exit_grid(
            bank(),
            execution_configs=selected_executions,
        )
        for variant in grid.variants:
            for selected_execution in selected_executions.values():
                self.assertLessEqual(
                    variant.policy.maximum_holding_ms
                    + selected_execution.exit_latency_ms
                    + selected_execution.maximum_exit_lag_ms,
                    60_000,
                )
                self.assertLess(
                    variant.policy.maximum_holding_ms
                    + selected_execution.exit_latency_ms,
                    selected_execution.actual_fill_deadline_ms,
                )
                if variant.weakening is not None:
                    deadlines = [variant.weakening.minimum_holding_ms]
                    if variant.weakening.stall_deadline_ms is not None:
                        deadlines.append(variant.weakening.stall_deadline_ms)
                        self.assertLessEqual(
                            variant.weakening.stall_deadline_ms,
                            variant.policy.maximum_holding_ms,
                        )
                    self.assertLessEqual(
                        max(deadlines)
                        + selected_execution.exit_latency_ms
                        + selected_execution.maximum_exit_lag_ms,
                        60_000,
                    )

    def test_missing_bank_contract_and_incompatible_execution_are_rejected(self):
        complete = bank()
        missing_measurement = dataclasses.replace(
            complete,
            measurements=complete.measurements[:-1],
        )
        with self.assertRaisesRegex(ValueError, "exact required measurement"):
            build_fresh_exit_grid(
                missing_measurement,
                execution_configs=executions(),
            )

        missing_rank = dataclasses.replace(
            complete,
            config=dataclasses.replace(
                complete.config,
                ranks=FRESH_EXIT_QUANTILE_RANKS[1:],
            ),
        )
        with self.assertRaisesRegex(ValueError, "missing required ranks"):
            build_fresh_exit_grid(
                missing_rank,
                execution_configs=executions(),
            )

        zero_thresholds = tuple(
            dataclasses.replace(item, value=0.0)
            if item.measurement == "identity::spread"
            else item
            for item in complete.thresholds
        )
        nonpositive = dataclasses.replace(complete, thresholds=zero_thresholds)
        with self.assertRaisesRegex(ValueError, "must resolve positive"):
            build_fresh_exit_grid(
                nonpositive,
                execution_configs=executions(),
            )

        too_short = {
            "too-short": execution(
                entry_latency_ms=500,
                exit_latency_ms=1_000,
                maximum_exit_lag_ms=1_000,
                actual_fill_deadline_ms=6_000,
            )
        }
        with self.assertRaisesRegex(ValueError, "replay fill deadline"):
            build_fresh_exit_grid(
                complete,
                execution_configs=too_short,
            )


if __name__ == "__main__":
    unittest.main()
