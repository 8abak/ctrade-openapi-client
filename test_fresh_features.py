from __future__ import annotations

import math
import unittest
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

from datavis.research.fresh_features import (
    FreshFeatureConfig,
    compute_fresh_features,
)
from datavis.research.ticks import Tick


BASE = datetime(2026, 7, 15, 0, 0, tzinfo=timezone.utc)
TEST_CONFIG = FreshFeatureConfig(
    horizons_seconds=(0.5, 1.0, 3.0, 10.0, 30.0),
    maximum_intertick_gap_ms=5_000,
    ewma_half_lives_seconds=(0.5, 1.0, 3.0, 10.0, 30.0),
    kalman_acceleration_variance=0.25,
    kalman_measurement_variance=0.04,
    bollinger_width=2.0,
)


def quote(
    index: int,
    seconds: float,
    mid: float,
    spread: float = 0.2,
    *,
    base: datetime = BASE,
) -> Tick:
    return Tick(
        id=index + 1,
        timestamp=base + timedelta(seconds=seconds),
        bid=mid - spread / 2.0,
        ask=mid + spread / 2.0,
    )


def irregular_seconds(duration: float = 42.0) -> list[float]:
    pattern = (0.07, 0.11, 0.08, 0.14, 0.09, 0.12, 0.06, 0.13)
    values = [0.0]
    index = 0
    while values[-1] < duration:
        values.append(round(values[-1] + pattern[index % len(pattern)], 6))
        index += 1
    return values


class FreshFeatureTests(unittest.TestCase):
    def test_prefix_invariance_including_stateful_filters(self):
        times = irregular_seconds(44.0)
        points = [
            quote(
                index,
                seconds,
                100.0
                + 0.04 * seconds
                + 0.22 * math.sin(seconds * 1.7)
                + 0.05 * math.sin(seconds * 7.0),
                0.16 + 0.02 * (1.0 + math.sin(seconds * 0.8)),
            )
            for index, seconds in enumerate(times)
        ]
        cut = next(index for index, seconds in enumerate(times) if seconds >= 35.0)
        full = compute_fresh_features(points, config=TEST_CONFIG)
        prefix = compute_fresh_features(points[:cut], config=TEST_CONFIG)
        pd.testing.assert_frame_equal(
            full.iloc[:cut].reset_index(drop=True),
            prefix,
            check_exact=True,
        )

        changed_future = list(points)
        for index in range(cut, len(changed_future)):
            old = changed_future[index]
            changed_future[index] = quote(
                index,
                times[index],
                old.mid + 20.0 + index / 100.0,
                old.spread + 0.1,
            )
        changed = compute_fresh_features(changed_future, config=TEST_CONFIG)
        pd.testing.assert_frame_equal(
            full.iloc[:cut].reset_index(drop=True),
            changed.iloc[:cut].reset_index(drop=True),
            check_exact=True,
        )

    def test_price_mirror_symmetry(self):
        times = irregular_seconds(38.0)
        points = [
            quote(
                index,
                seconds,
                100.0
                + 0.08 * seconds
                + 0.35 * math.sin(seconds * 1.2)
                + 0.03 * math.cos(seconds * 5.0),
                0.12 + 0.025 * (1.0 + math.sin(seconds * 0.6)),
            )
            for index, seconds in enumerate(times)
        ]
        center = 200.0
        mirrored = [
            Tick(
                id=point.id,
                timestamp=point.timestamp,
                bid=2.0 * center - point.ask,
                ask=2.0 * center - point.bid,
            )
            for point in points
        ]
        original = compute_fresh_features(points, config=TEST_CONFIG)
        reflected = compute_fresh_features(mirrored, config=TEST_CONFIG)

        np.testing.assert_allclose(
            original["mid"] + reflected["mid"], 2.0 * center, atol=1e-12
        )
        np.testing.assert_allclose(original["spread"], reflected["spread"], atol=1e-12)
        for tag in ("500ms", "1s", "3s", "10s", "30s"):
            ready = original[f"{tag}_ready"].to_numpy(dtype=bool)
            for suffix in (
                "mid_displacement",
                "mid_speed",
                "mid_acceleration",
                "mid_jerk",
                "translation_pressure",
                "bollinger_zscore",
            ):
                np.testing.assert_allclose(
                    original.loc[ready, f"{tag}_{suffix}"],
                    -reflected.loc[ready, f"{tag}_{suffix}"],
                    rtol=1e-8,
                    atol=1e-8,
                    equal_nan=True,
                )
            for suffix in (
                "persistence",
                "path_efficiency",
                "noise",
                "translation_coherence",
                "spread_mean",
                "spread_std",
                "spread_regime_zscore",
                "spread_regime_ratio",
            ):
                np.testing.assert_allclose(
                    original.loc[ready, f"{tag}_{suffix}"],
                    reflected.loc[ready, f"{tag}_{suffix}"],
                    rtol=1e-8,
                    atol=1e-8,
                    equal_nan=True,
                )
            np.testing.assert_allclose(
                original.loc[ready, f"{tag}_range_position"]
                + reflected.loc[ready, f"{tag}_range_position"],
                1.0,
                rtol=1e-8,
                atol=1e-8,
                equal_nan=True,
            )
            np.testing.assert_allclose(
                original.loc[ready, f"{tag}_bollinger_position"]
                + reflected.loc[ready, f"{tag}_bollinger_position"],
                1.0,
                rtol=1e-8,
                atol=1e-8,
                equal_nan=True,
            )

        for tag in ("500ms", "1s", "3s", "10s", "30s"):
            np.testing.assert_allclose(
                original[f"ewma_{tag}_mid"] + reflected[f"ewma_{tag}_mid"],
                2.0 * center,
                rtol=1e-9,
                atol=1e-9,
            )
            np.testing.assert_allclose(
                original[f"ewma_{tag}_slope"],
                -reflected[f"ewma_{tag}_slope"],
                rtol=1e-8,
                atol=1e-8,
                equal_nan=True,
            )
        np.testing.assert_allclose(
            original["kalman_price"] + reflected["kalman_price"],
            2.0 * center,
            rtol=1e-9,
            atol=1e-9,
        )
        for name in (
            "kalman_velocity",
            "kalman_innovation",
            "kalman_price_separation",
            "kalman_velocity_change",
        ):
            np.testing.assert_allclose(
                original[name],
                -reflected[name],
                rtol=1e-8,
                atol=1e-8,
                equal_nan=True,
            )

    def test_irregular_ticks_use_elapsed_time_not_row_count(self):
        times = irregular_seconds(36.0)
        points = [
            quote(index, seconds, 100.0 + 2.0 * seconds, 0.2)
            for index, seconds in enumerate(times)
        ]
        frame = compute_fresh_features(points, config=TEST_CONFIG)
        row = frame.iloc[-1]
        self.assertTrue(bool(row["feature_ready"]))
        for tag in ("500ms", "1s", "3s", "10s", "30s"):
            self.assertTrue(bool(row[f"{tag}_ready"]))
            self.assertAlmostEqual(float(row[f"{tag}_bid_speed"]), 2.0, places=9)
            self.assertAlmostEqual(float(row[f"{tag}_ask_speed"]), 2.0, places=9)
            self.assertAlmostEqual(float(row[f"{tag}_mid_speed"]), 2.0, places=9)
            self.assertAlmostEqual(float(row[f"{tag}_mid_acceleration"]), 0.0, places=7)
            self.assertAlmostEqual(float(row[f"{tag}_mid_jerk"]), 0.0, places=6)
            self.assertAlmostEqual(float(row[f"{tag}_persistence"]), 1.0, places=12)
            self.assertAlmostEqual(float(row[f"{tag}_path_efficiency"]), 1.0, places=12)
            self.assertAlmostEqual(
                float(row[f"{tag}_translation_coherence"]), 1.0, places=12
            )
            self.assertAlmostEqual(
                float(row[f"{tag}_translation_pressure"]), 1.0, places=12
            )
            self.assertGreater(float(row[f"{tag}_arrival_rate"]), 0.0)
            self.assertGreater(float(row[f"{tag}_noise"]), 0.0)

    def test_gap_resets_and_requires_a_new_full_warmup(self):
        points = [quote(index, float(index), 100.0 + index) for index in range(36)]
        offset = len(points)
        points.extend(
            quote(offset + age, 100.0 + age, 500.0 + 3.0 * age)
            for age in range(32)
        )
        config = FreshFeatureConfig(
            horizons_seconds=TEST_CONFIG.horizons_seconds,
            maximum_intertick_gap_ms=1_500,
            ewma_half_lives_seconds=TEST_CONFIG.ewma_half_lives_seconds,
            kalman_acceleration_variance=TEST_CONFIG.kalman_acceleration_variance,
            kalman_measurement_variance=TEST_CONFIG.kalman_measurement_variance,
            bollinger_width=TEST_CONFIG.bollinger_width,
        )
        frame = compute_fresh_features(points, config=config)
        gap_index = offset

        self.assertTrue(bool(frame.loc[gap_index, "gap_detected"]))
        self.assertEqual(float(frame.loc[gap_index, "segment_age_seconds"]), 0.0)
        self.assertFalse(bool(frame.loc[gap_index, "feature_ready"]))
        self.assertFalse(bool(frame.loc[gap_index + 29, "feature_ready"]))
        self.assertTrue(bool(frame.loc[gap_index + 30, "feature_ready"]))
        self.assertFalse(bool(frame.loc[gap_index + 29, "30s_ready"]))
        self.assertTrue(bool(frame.loc[gap_index + 30, "30s_ready"]))
        self.assertAlmostEqual(
            float(frame.loc[gap_index + 30, "30s_mid_displacement"]), 90.0
        )
        self.assertTrue(math.isnan(float(frame.loc[gap_index, "interarrival_seconds"])))
        self.assertAlmostEqual(
            float(frame.loc[gap_index, "ewma_500ms_mid"]),
            float(frame.loc[gap_index, "mid"]),
        )
        self.assertAlmostEqual(
            float(frame.loc[gap_index, "kalman_price"]),
            float(frame.loc[gap_index, "mid"]),
        )
        self.assertEqual(float(frame.loc[gap_index, "kalman_velocity"]), 0.0)
        self.assertTrue(math.isnan(float(frame.loc[gap_index, "kalman_velocity_change"])))

    def test_iana_sessions_follow_london_and_new_york_dst(self):
        def flags(moment: datetime) -> pd.Series:
            return compute_fresh_features(
                [quote(0, 0.0, 100.0, base=moment)], config=TEST_CONFIG
            ).iloc[0]

        london_winter = flags(datetime(2026, 1, 15, 8, 30, tzinfo=timezone.utc))
        london_summer = flags(datetime(2026, 7, 15, 7, 30, tzinfo=timezone.utc))
        self.assertTrue(bool(london_winter["london_open"]))
        self.assertTrue(bool(london_winter["london_opening_hour"]))
        self.assertTrue(bool(london_summer["london_open"]))
        self.assertFalse(
            bool(flags(datetime(2026, 1, 15, 7, 30, tzinfo=timezone.utc))["london_open"])
        )
        self.assertFalse(
            bool(flags(datetime(2026, 7, 15, 6, 30, tzinfo=timezone.utc))["london_open"])
        )

        self.assertTrue(
            bool(flags(datetime(2026, 1, 15, 13, 30, tzinfo=timezone.utc))["new_york_open"])
        )
        self.assertTrue(
            bool(flags(datetime(2026, 1, 15, 13, 30, tzinfo=timezone.utc))["new_york_opening_hour"])
        )
        self.assertTrue(
            bool(flags(datetime(2026, 7, 15, 12, 30, tzinfo=timezone.utc))["new_york_open"])
        )
        self.assertTrue(
            bool(flags(datetime(2026, 1, 15, 0, 30, tzinfo=timezone.utc))["tokyo_open"])
        )
        self.assertTrue(
            bool(flags(datetime(2026, 7, 15, 0, 30, tzinfo=timezone.utc))["tokyo_open"])
        )
        overlap = flags(datetime(2026, 7, 15, 13, 0, tzinfo=timezone.utc))
        self.assertTrue(bool(overlap["london_open"]))
        self.assertTrue(bool(overlap["new_york_open"]))
        self.assertTrue(bool(overlap["london_new_york_overlap"]))
        self.assertTrue(bool(overlap["any_major_session_overlap"]))
        weekend = flags(datetime(2026, 7, 18, 13, 0, tzinfo=timezone.utc))
        self.assertFalse(bool(weekend["any_major_session_open"]))

    def test_input_integrity_empty_schema_and_no_outcome_columns(self):
        frame = compute_fresh_features([], config=TEST_CONFIG)
        self.assertTrue(frame.empty)
        self.assertIn("30s_mid_speed", frame.columns)
        self.assertIn("feature_ready", frame.columns)
        forbidden = ("signal", "label", "target", "profit", "trade")
        self.assertFalse(
            any(word in column.lower() for column in frame.columns for word in forbidden)
        )

        same_time = [quote(0, 0.0, 100.0), quote(1, 0.0, 100.1)]
        same_time_frame = compute_fresh_features(same_time, config=TEST_CONFIG)
        self.assertEqual(float(same_time_frame.loc[1, "interarrival_seconds"]), 0.0)
        self.assertTrue(math.isfinite(float(same_time_frame.loc[1, "kalman_price"])))
        self.assertTrue(
            math.isnan(float(same_time_frame.loc[1, "kalman_velocity_change"]))
        )
        repeated_quote_events = [
            quote(0, 0.0, 100.0),
            quote(1, 0.25, 100.0),
            quote(2, 0.25, 100.0),
            quote(3, 0.5, 100.0),
        ]
        repeated_frame = compute_fresh_features(
            repeated_quote_events, config=TEST_CONFIG
        )
        self.assertEqual(float(repeated_frame.loc[3, "500ms_tick_count"]), 4.0)
        self.assertEqual(float(repeated_frame.loc[3, "500ms_arrival_rate"]), 6.0)
        decreasing_same_time = [quote(1, 0.0, 100.0), quote(0, 0.0, 100.1)]
        with self.assertRaisesRegex(ValueError, r"\(timestamp, id\)"):
            compute_fresh_features(decreasing_same_time, config=TEST_CONFIG)
        duplicate_ids = [quote(0, 0.0, 100.0), quote(0, 1.0, 100.1)]
        with self.assertRaisesRegex(ValueError, "duplicate tick id"):
            compute_fresh_features(duplicate_ids, config=TEST_CONFIG)
        later_lower_unique_id = [quote(1, 0.0, 100.0), quote(0, 1.0, 100.1)]
        later_frame = compute_fresh_features(later_lower_unique_id, config=TEST_CONFIG)
        self.assertEqual(later_frame["tick_id"].tolist(), [2, 1])
        reverse_time = [quote(0, 1.0, 100.0), quote(1, 0.0, 100.1)]
        with self.assertRaisesRegex(ValueError, r"\(timestamp, id\)"):
            compute_fresh_features(reverse_time, config=TEST_CONFIG)
        with self.assertRaises(ValueError):
            FreshFeatureConfig(
                horizons_seconds=(1.0, 1.0),
                maximum_intertick_gap_ms=5_000,
                ewma_half_lives_seconds=(1.0,),
                kalman_acceleration_variance=0.25,
                kalman_measurement_variance=0.04,
                bollinger_width=2.0,
            )
        with self.assertRaises(ValueError):
            FreshFeatureConfig(
                horizons_seconds=(1.0,),
                maximum_intertick_gap_ms=0,
                ewma_half_lives_seconds=(1.0,),
                kalman_acceleration_variance=0.25,
                kalman_measurement_variance=0.04,
                bollinger_width=2.0,
            )


if __name__ == "__main__":
    unittest.main()
