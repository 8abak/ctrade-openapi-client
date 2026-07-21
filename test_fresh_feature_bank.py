from __future__ import annotations

import math
import unittest
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pandas as pd

import datavis.research.fresh_feature_bank as bank_module
from datavis.research.fresh_feature_bank import (
    FEATURE_BANK_BINDING_COLUMNS,
    KALMAN_MEASUREMENT_COLUMNS,
    FreshFeatureBankConfig,
    FreshFeatureBankOutputSelection,
    FreshKalmanBankMember,
    NamedFeatureFamily,
    compute_fresh_feature_bank,
    feature_bank_columns,
    kalman_bank_column,
    preflight_feature_bank_bindings,
)
from datavis.research.fresh_features import (
    FreshFeatureConfig,
    compute_fresh_features,
)
from datavis.research.ticks import Tick


BASE = datetime(2026, 2, 12, 1, 0, tzinfo=timezone.utc)
HORIZONS = (0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0)
KALMAN_Q = (0.04, 0.16, 0.64)
KALMAN_R = (0.01, 0.04, 0.16)


def quote(
    tick_id: int,
    seconds: float,
    mid: float,
    spread: float = 0.18,
) -> Tick:
    return Tick(
        id=tick_id,
        timestamp=BASE + timedelta(seconds=seconds),
        bid=mid - spread / 2.0,
        ask=mid + spread / 2.0,
    )


def irregular_points(duration: float = 38.0) -> list[Tick]:
    pattern = (0.07, 0.13, 0.04, 0.19, 0.0, 0.08, 0.11, 0.05)
    seconds = [0.0]
    position = 0
    while seconds[-1] < duration:
        seconds.append(round(seconds[-1] + pattern[position % len(pattern)], 6))
        position += 1
    return [
        quote(
            tick_id=10_000 + index,
            seconds=elapsed,
            mid=(
                2_900.0
                + 0.035 * elapsed
                + 0.21 * math.sin(elapsed * 1.4)
                + 0.04 * math.sin(elapsed * 8.1)
            ),
            spread=0.14 + 0.025 * (1.0 + math.sin(elapsed * 0.7)),
        )
        for index, elapsed in enumerate(seconds)
    ]


def members() -> tuple[FreshKalmanBankMember, ...]:
    output: list[FreshKalmanBankMember] = []
    for q in KALMAN_Q:
        for r in KALMAN_R:
            identifier = f"kalman-q{q:g}-r{r:g}"
            output.append(
                FreshKalmanBankMember(
                    model_id=identifier,
                    feature_config=FreshFeatureConfig(
                        horizons_seconds=HORIZONS,
                        maximum_intertick_gap_ms=5_000,
                        ewma_half_lives_seconds=HORIZONS,
                        kalman_acceleration_variance=q,
                        kalman_measurement_variance=r,
                        bollinger_width=2.0,
                    ),
                )
            )
    return tuple(output)


def full_config() -> FreshFeatureBankConfig:
    return FreshFeatureBankConfig(
        members=members(),
        output_selection=FreshFeatureBankOutputSelection(
            include_all_columns=True,
            candidate_families=(),
            selected_candidate_families=(),
        ),
    )


def compact_config() -> FreshFeatureBankConfig:
    selected_model = members()[4].model_id
    return FreshFeatureBankConfig(
        members=members(),
        output_selection=FreshFeatureBankOutputSelection(
            include_all_columns=False,
            candidate_families=(
                NamedFeatureFamily(
                    family_name="trend-acceleration",
                    required_columns=(
                        "30s_mid_speed",
                        kalman_bank_column(selected_model, "kalman_velocity"),
                        "1s_mid_acceleration",
                    ),
                ),
                NamedFeatureFamily(
                    family_name="quote-translation-pressure",
                    required_columns=(
                        "500ms_translation_pressure",
                        "500ms_translation_coherence",
                    ),
                ),
            ),
            selected_candidate_families=("trend-acceleration",),
        ),
    )


class FreshFeatureBankTests(unittest.TestCase):
    def test_nine_model_bank_exactly_matches_nine_separate_runs(self):
        points = irregular_points()
        config = full_config()
        with patch.object(
            bank_module,
            "compute_fresh_features",
            wraps=compute_fresh_features,
        ) as shared_spy, patch.object(
            bank_module,
            "_constant_velocity_kalman",
            wraps=bank_module._constant_velocity_kalman,
        ) as kalman_spy:
            bank = compute_fresh_feature_bank(points, config=config)

        self.assertEqual(shared_spy.call_count, 1)
        self.assertEqual(kalman_spy.call_count, 8)
        self.assertEqual(len(config.members), 9)

        shared_columns: list[str] | None = None
        for member in config.members:
            separate = compute_fresh_features(
                points, config=member.feature_config
            )
            current_shared = [
                column
                for column in separate.columns
                if column not in KALMAN_MEASUREMENT_COLUMNS
            ]
            if shared_columns is None:
                shared_columns = current_shared
                pd.testing.assert_frame_equal(
                    bank.loc[:, shared_columns],
                    separate.loc[:, shared_columns],
                    check_exact=True,
                )
            else:
                self.assertEqual(current_shared, shared_columns)
            for measurement in KALMAN_MEASUREMENT_COLUMNS:
                pd.testing.assert_series_equal(
                    bank[kalman_bank_column(member.model_id, measurement)],
                    separate[measurement],
                    check_exact=True,
                    check_names=False,
                )

    def test_prefix_invariance_is_exact_for_full_and_compact_outputs(self):
        points = irregular_points(43.0)
        cut = next(
            index
            for index, point in enumerate(points)
            if (point.timestamp - BASE).total_seconds() >= 35.0
        )
        changed_future = list(points)
        for position in range(cut, len(changed_future)):
            point = changed_future[position]
            changed_future[position] = quote(
                point.id,
                (point.timestamp - BASE).total_seconds(),
                point.mid + 50.0 + position / 10.0,
                point.spread + 0.2,
            )

        for config in (full_config(), compact_config()):
            with self.subTest(include_all=config.output_selection.include_all_columns):
                full = compute_fresh_feature_bank(points, config=config)
                prefix = compute_fresh_feature_bank(points[:cut], config=config)
                changed = compute_fresh_feature_bank(changed_future, config=config)
                pd.testing.assert_frame_equal(
                    full.iloc[:cut].reset_index(drop=True),
                    prefix,
                    check_exact=True,
                )
                pd.testing.assert_frame_equal(
                    full.iloc[:cut].reset_index(drop=True),
                    changed.iloc[:cut].reset_index(drop=True),
                    check_exact=True,
                )

    def test_equal_time_rows_are_causal_ordered_and_exactly_bound(self):
        points = [
            quote(1, 0.0, 2_900.0),
            quote(2, 0.0, 2_900.1),
            quote(3, 0.1, 2_900.15),
            quote(4, 0.1, 2_900.12),
            quote(5, 0.3, 2_900.2),
        ]
        config = full_config()
        frame = compute_fresh_feature_bank(points, config=config)
        preflight_feature_bank_bindings(points, frame)
        self.assertEqual(frame["tick_id"].tolist(), [1, 2, 3, 4, 5])
        self.assertEqual(float(frame.loc[1, "interarrival_seconds"]), 0.0)
        for member in config.members:
            self.assertTrue(
                math.isnan(
                    float(
                        frame.loc[
                            1,
                            kalman_bank_column(
                                member.model_id, "kalman_velocity_change"
                            ),
                        ]
                    )
                )
            )
        with self.assertRaisesRegex(ValueError, r"\(timestamp, id\)"):
            compute_fresh_feature_bank(
                [quote(2, 0.0, 2_900.0), quote(1, 0.0, 2_900.1)],
                config=config,
            )
        with self.assertRaisesRegex(ValueError, "duplicate tick id"):
            compute_fresh_feature_bank(
                [quote(1, 0.0, 2_900.0), quote(1, 0.1, 2_900.1)],
                config=config,
            )

    def test_every_model_and_shared_measurement_reset_at_feed_gap(self):
        before = [
            quote(index + 1, float(index), 2_900.0 + index)
            for index in range(36)
        ]
        after = [
            quote(100 + index, 100.0 + index, 3_100.0 + 2.0 * index)
            for index in range(33)
        ]
        changed_before = [
            quote(point.id, (point.timestamp - BASE).total_seconds(), point.mid + 500.0)
            for point in before
        ]
        config = full_config()
        original = compute_fresh_feature_bank(before + after, config=config)
        changed = compute_fresh_feature_bank(changed_before + after, config=config)
        gap_index = len(before)
        self.assertTrue(bool(original.loc[gap_index, "gap_detected"]))
        self.assertEqual(float(original.loc[gap_index, "segment_age_seconds"]), 0.0)
        self.assertFalse(bool(original.loc[gap_index, "feature_ready"]))
        for member in config.members:
            self.assertEqual(
                float(
                    original.loc[
                        gap_index,
                        kalman_bank_column(member.model_id, "kalman_price"),
                    ]
                ),
                after[0].mid,
            )
            self.assertEqual(
                float(
                    original.loc[
                        gap_index,
                        kalman_bank_column(member.model_id, "kalman_velocity"),
                    ]
                ),
                0.0,
            )
            self.assertTrue(
                math.isnan(
                    float(
                        original.loc[
                            gap_index,
                            kalman_bank_column(
                                member.model_id, "kalman_velocity_change"
                            ),
                        ]
                    )
                )
            )
        pd.testing.assert_frame_equal(
            original.iloc[gap_index:].reset_index(drop=True),
            changed.iloc[gap_index:].reset_index(drop=True),
            check_exact=True,
        )

    def test_named_family_projection_is_small_and_computes_only_its_model(self):
        points = irregular_points()
        compact = compact_config()
        selected_model = compact.members[4]
        with patch.object(
            bank_module,
            "compute_fresh_features",
            wraps=compute_fresh_features,
        ) as shared_spy, patch.object(
            bank_module,
            "_constant_velocity_kalman",
            wraps=bank_module._constant_velocity_kalman,
        ) as kalman_spy:
            projected = compute_fresh_feature_bank(points, config=compact)
        self.assertEqual(shared_spy.call_count, 1)
        self.assertIs(
            shared_spy.call_args.kwargs["config"],
            selected_model.feature_config,
        )
        self.assertEqual(kalman_spy.call_count, 0)
        expected = (
            *FEATURE_BANK_BINDING_COLUMNS,
            "30s_mid_speed",
            kalman_bank_column(selected_model.model_id, "kalman_velocity"),
            "1s_mid_acceleration",
        )
        self.assertEqual(tuple(projected.columns), expected)
        self.assertEqual(feature_bank_columns(compact), expected)

        full = compute_fresh_feature_bank(points, config=full_config())
        pd.testing.assert_frame_equal(
            projected,
            full.loc[:, list(expected)],
            check_exact=True,
        )
        self.assertLess(
            int(projected.memory_usage(index=True, deep=True).sum()),
            int(full.memory_usage(index=True, deep=True).sum()),
        )

    def test_empty_schema_and_binding_tampering_are_rejected(self):
        config = compact_config()
        empty = compute_fresh_feature_bank((), config=config)
        self.assertTrue(empty.empty)
        self.assertEqual(tuple(empty.columns), feature_bank_columns(config))
        preflight_feature_bank_bindings((), empty)

        points = irregular_points(1.0)
        frame = compute_fresh_feature_bank(points, config=config)
        for column, changed_value, message in (
            ("tick_id", 999_999, "tick_id mismatch"),
            ("timestamp", BASE + timedelta(days=1), "timestamp mismatch"),
            ("bid", 1.0, "bid mismatch"),
            ("ask", 1.0, "ask mismatch"),
            ("mid", 1.0, "mid mismatch"),
        ):
            with self.subTest(column=column):
                tampered = frame.copy()
                tampered.loc[0, column] = changed_value
                with self.assertRaisesRegex(ValueError, message):
                    preflight_feature_bank_bindings(points, tampered)

    def test_configuration_is_explicit_and_rejects_ambiguous_banks(self):
        with self.assertRaises(TypeError):
            FreshFeatureBankConfig()  # type: ignore[call-arg]
        with self.assertRaises(TypeError):
            FreshFeatureBankOutputSelection()  # type: ignore[call-arg]
        with self.assertRaisesRegex(ValueError, "selected_candidate_families"):
            FreshFeatureBankOutputSelection(
                include_all_columns=True,
                candidate_families=(),
                selected_candidate_families=("x",),
            )
        with self.assertRaisesRegex(ValueError, "not defined"):
            FreshFeatureBankOutputSelection(
                include_all_columns=False,
                candidate_families=(NamedFeatureFamily("x", ("mid",)),),
                selected_candidate_families=("y",),
            )

        one = members()[0]
        with self.assertRaisesRegex(ValueError, "model IDs"):
            FreshFeatureBankConfig(
                members=(one, one),
                output_selection=FreshFeatureBankOutputSelection(
                    include_all_columns=True,
                    candidate_families=(),
                    selected_candidate_families=(),
                ),
            )
        mismatched = FreshKalmanBankMember(
            model_id="mismatched",
            feature_config=replace(
                members()[1].feature_config,
                horizons_seconds=(0.5, 1.0),
                ewma_half_lives_seconds=(0.5, 1.0),
            ),
        )
        with self.assertRaisesRegex(ValueError, "non-Kalman settings"):
            FreshFeatureBankConfig(
                members=(one, mismatched),
                output_selection=FreshFeatureBankOutputSelection(
                    include_all_columns=True,
                    candidate_families=(),
                    selected_candidate_families=(),
                ),
            )
        unavailable = FreshFeatureBankConfig(
            members=members(),
            output_selection=FreshFeatureBankOutputSelection(
                include_all_columns=False,
                candidate_families=(
                    NamedFeatureFamily("bad", ("future_outcome",)),
                ),
                selected_candidate_families=("bad",),
            ),
        )
        with self.assertRaisesRegex(ValueError, "unavailable"):
            feature_bank_columns(unavailable)


if __name__ == "__main__":
    unittest.main()
