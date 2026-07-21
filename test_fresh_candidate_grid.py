from __future__ import annotations

import dataclasses
import unittest
from collections import Counter, defaultdict

from datavis.research.fresh_candidate_grid import (
    FRESH_CANDIDATE_GRID_SCHEMA,
    FRESH_CANDIDATE_QUANTILE_RANKS,
    FRESH_RANK_NEIGHBOURHOOD,
    MAXIMUM_CANDIDATES_PER_FAMILY,
    MAXIMUM_TOTAL_CANDIDATES,
    SYMMETRIC_DIRECTION_POLICY,
    build_fresh_candidate_grid,
    fresh_candidate_quantile_measurements,
)
from datavis.research.fresh_signals import (
    COMPRESSION_EXPANSION_BREAKOUT,
    COUNTERTREND_PIVOT,
    PULLBACK_RESUMPTION,
    QUOTE_TRANSLATION_PRESSURE,
    TREND_ACCELERATION,
    CompressionExpansionBreakoutSignalConfig,
    CountertrendPivotSignalConfig,
    PullbackResumptionSignalConfig,
    QuoteTranslationPressureSignalConfig,
    TrendAccelerationSignalConfig,
    signal_config_fingerprint,
)
from datavis.research.fresh_thresholds import (
    FreshQuantileBank,
    FreshQuantileBankConfig,
    QuantileThreshold,
)


MODELS = tuple(
    f"kalman-q{q:g}-r{r:g}"
    for q in (0.04, 0.16, 0.64)
    for r in (0.01, 0.04, 0.16)
)


def threshold_value(measurement: str, rank: float) -> float:
    if any(
        token in measurement
        for token in (
            "translation_pressure",
            "translation_coherence",
            "persistence",
        )
    ):
        return 0.15 + 0.55 * rank
    if measurement == "identity::spread":
        return 0.08 + 0.08 * rank
    return 0.05 + 0.75 * rank


def bank(model_ids: tuple[str, ...] = MODELS) -> FreshQuantileBank:
    specs = fresh_candidate_quantile_measurements(kalman_model_ids=model_ids)
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
        for spec in specs
        for rank in FRESH_CANDIDATE_QUANTILE_RANKS
    )
    return FreshQuantileBank(
        config=FreshQuantileBankConfig(
            ranks=FRESH_CANDIDATE_QUANTILE_RANKS,
            minimum_finite_values_per_session=1_000,
            minimum_eligible_sessions=40,
        ),
        training_session_anchors=("2026-01-02",),
        measurements=specs,
        thresholds=thresholds,
        bank_sha256="a" * 64,
    )


EXPECTED_THRESHOLD_PARAMETERS = {
    TrendAccelerationSignalConfig: {
        "minimum_trend",
        "minimum_velocity",
        "reset_velocity",
        "minimum_acceleration",
        "minimum_translation_coherence",
    },
    PullbackResumptionSignalConfig: {
        "minimum_established_trend",
        "minimum_residual_trend",
        "minimum_pullback_speed",
        "minimum_resumption_speed",
        "minimum_resumption_acceleration",
    },
    CountertrendPivotSignalConfig: {
        "minimum_established_trend",
        "minimum_residual_trend",
        "minimum_pullback_speed",
        "minimum_pivot_speed",
        "minimum_velocity_improvement",
        "minimum_pivot_acceleration",
    },
    CompressionExpansionBreakoutSignalConfig: {
        "minimum_breakout_speed",
        "breakout_buffer",
    },
    QuoteTranslationPressureSignalConfig: {
        "minimum_translation_pressure",
        "reset_translation_pressure",
        "minimum_translation_coherence",
        "minimum_movement_speed",
        "minimum_persistence",
    },
}


class FreshCandidateGridTests(unittest.TestCase):
    def test_registered_nine_model_grid_is_balanced_and_within_budgets(self):
        grid = build_fresh_candidate_grid(bank(), kalman_model_ids=MODELS)
        self.assertEqual(grid.schema, FRESH_CANDIDATE_GRID_SCHEMA)
        self.assertEqual(len(grid.candidates), 93)
        self.assertLessEqual(len(grid.candidates), MAXIMUM_TOTAL_CANDIDATES)
        self.assertGreaterEqual(len(grid.candidates), 60)
        self.assertLessEqual(len(grid.candidates), 120)

        counts = Counter(candidate.family for candidate in grid.candidates)
        self.assertEqual(
            counts,
            {
                TREND_ACCELERATION: 39,
                PULLBACK_RESUMPTION: 15,
                COUNTERTREND_PIVOT: 15,
                COMPRESSION_EXPANSION_BREAKOUT: 12,
                QUOTE_TRANSLATION_PRESSURE: 12,
            },
        )
        self.assertTrue(
            all(value <= MAXIMUM_CANDIDATES_PER_FAMILY for value in counts.values())
        )

        identifiers = [item.config.candidate_id for item in grid.candidates]
        fingerprints = [item.config_sha256 for item in grid.candidates]
        self.assertEqual(len(identifiers), len(set(identifiers)))
        self.assertEqual(len(fingerprints), len(set(fingerprints)))
        self.assertTrue(all(len(value) == 64 for value in fingerprints))
        self.assertEqual(len(grid.grid_sha256), 64)

    def test_grid_is_deterministic_and_kalman_input_order_is_irrelevant(self):
        first_bank = bank(MODELS)
        second_bank = bank(tuple(reversed(MODELS)))
        first = build_fresh_candidate_grid(first_bank, kalman_model_ids=MODELS)
        second = build_fresh_candidate_grid(
            second_bank,
            kalman_model_ids=tuple(reversed(MODELS)),
        )
        self.assertEqual(first, second)
        self.assertEqual(first.grid_sha256, second.grid_sha256)
        self.assertEqual(first.kalman_model_ids, tuple(sorted(MODELS)))
        for candidate in first.candidates:
            self.assertEqual(
                candidate.config_sha256,
                signal_config_fingerprint(candidate.config),
            )

    def test_every_fitted_value_has_exact_bank_rank_provenance(self):
        selected_bank = bank()
        grid = build_fresh_candidate_grid(selected_bank, kalman_model_ids=MODELS)
        specs = {item.name: item for item in grid.required_measurements}
        self.assertEqual(
            grid.required_measurements,
            fresh_candidate_quantile_measurements(kalman_model_ids=MODELS),
        )

        for candidate in grid.candidates:
            self.assertEqual(candidate.direction_policy, SYMMETRIC_DIRECTION_POLICY)
            actual_parameters = {
                item.parameter for item in candidate.threshold_provenance
            }
            self.assertEqual(
                actual_parameters,
                EXPECTED_THRESHOLD_PARAMETERS[type(candidate.config)],
            )
            for provenance in candidate.threshold_provenance:
                spec = specs[provenance.measurement]
                self.assertEqual(spec.column, provenance.column)
                self.assertEqual(spec.transform, provenance.transform)
                self.assertAlmostEqual(
                    provenance.resolved_rank,
                    provenance.base_rank + provenance.rank_offset,
                )
                self.assertIn(
                    round(provenance.resolved_rank, 12),
                    {round(value, 12) for value in FRESH_CANDIDATE_QUANTILE_RANKS},
                )
                self.assertEqual(
                    provenance.bank_value,
                    selected_bank.threshold(
                        provenance.measurement,
                        provenance.resolved_rank,
                    ),
                )
                self.assertEqual(
                    provenance.final_value,
                    provenance.bank_value * provenance.multiplier,
                )
                self.assertEqual(
                    getattr(candidate.config, provenance.parameter),
                    provenance.final_value,
                )

    def test_each_structure_is_an_explicit_three_point_rank_neighbourhood(self):
        grid = build_fresh_candidate_grid(bank(), kalman_model_ids=MODELS)
        grouped: dict[str, list[float]] = defaultdict(list)
        for candidate in grid.candidates:
            grouped[candidate.neighbourhood_id].append(candidate.rank_offset)
        self.assertTrue(grouped)
        for offsets in grouped.values():
            self.assertEqual(tuple(sorted(offsets)), FRESH_RANK_NEIGHBOURHOOD)

    def test_measurement_contract_uses_only_causal_feature_names(self):
        specs = fresh_candidate_quantile_measurements(
            kalman_model_ids=tuple(reversed(MODELS))
        )
        self.assertEqual(len(specs), len({item.name for item in specs}))
        self.assertTrue(any(item.column == "250ms_mid_speed" for item in specs))
        self.assertTrue(any(item.column == "30s_mid_speed" for item in specs))
        for model_id in MODELS:
            self.assertTrue(
                any(item.column == f"{model_id}__velocity" for item in specs)
            )
            self.assertTrue(
                any(item.column == f"{model_id}__velocity_change" for item in specs)
            )
        forbidden = ("future", "label", "target", "outcome", "profit", "mfe", "mae")
        self.assertFalse(
            any(token in item.column.casefold() for item in specs for token in forbidden)
        )

    def test_missing_bank_contract_and_excess_model_budget_are_rejected(self):
        complete = bank(())
        missing_measurement = dataclasses.replace(
            complete,
            measurements=complete.measurements[:-1],
        )
        with self.assertRaisesRegex(ValueError, "exact required measurement"):
            build_fresh_candidate_grid(missing_measurement)

        missing_rank = dataclasses.replace(
            complete,
            config=dataclasses.replace(
                complete.config,
                ranks=FRESH_CANDIDATE_QUANTILE_RANKS[1:],
            ),
        )
        with self.assertRaisesRegex(ValueError, "missing required ranks"):
            build_fresh_candidate_grid(missing_rank)

        too_many = tuple(f"model-{index}" for index in range(17))
        with self.assertRaisesRegex(ValueError, "too many optional Kalman"):
            fresh_candidate_quantile_measurements(kalman_model_ids=too_many)


if __name__ == "__main__":
    unittest.main()
