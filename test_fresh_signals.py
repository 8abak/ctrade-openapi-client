from __future__ import annotations

import dataclasses
import time
import unittest
from datetime import datetime, timedelta, timezone

import numpy as np
import pandas as pd

from datavis.research.fresh_entry_diagnostics import FrozenSignalEvent
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
    generate_frozen_signal_events,
    preflight_signal_bindings,
    signal_config_fingerprint,
    signal_required_columns,
)
from datavis.research.ticks import Tick


BASE = datetime(2026, 4, 7, tzinfo=timezone.utc)


def frame(
    mids: list[float],
    *,
    milliseconds: list[int] | None = None,
    ids: list[int] | None = None,
    gap: list[bool] | None = None,
    ready: list[bool] | None = None,
    segment: list[int] | None = None,
    **columns: list[float],
) -> pd.DataFrame:
    size = len(mids)
    milliseconds = milliseconds or [index * 100 for index in range(size)]
    ids = ids or [index + 1 for index in range(size)]
    gap = gap or [False] * size
    ready = ready or [True] * size
    if segment is None:
        current = 0
        segment = []
        for is_gap in gap:
            if is_gap:
                current += 1
            segment.append(current)
    data: dict[str, object] = {
        "tick_id": ids,
        "timestamp": [BASE + timedelta(milliseconds=value) for value in milliseconds],
        "bid": [value - 0.1 for value in mids],
        "ask": [value + 0.1 for value in mids],
        "mid": mids,
        "gap_detected": gap,
        "segment_id": segment,
        "feature_ready": ready,
    }
    data.update(columns)
    return pd.DataFrame(data)


def trend_config(candidate_id: str = "trend-a") -> TrendAccelerationSignalConfig:
    return TrendAccelerationSignalConfig(
        candidate_id=candidate_id,
        trend_column="trend",
        velocity_column="move",
        acceleration_column="accel",
        translation_coherence_column="coherence",
        minimum_trend=0.5,
        minimum_velocity=0.6,
        reset_velocity=0.2,
        minimum_acceleration=0.4,
        minimum_translation_coherence=0.7,
    )


def pullback_config(candidate_id: str = "pullback-a") -> PullbackResumptionSignalConfig:
    return PullbackResumptionSignalConfig(
        candidate_id=candidate_id,
        trend_column="trend",
        movement_column="move",
        acceleration_column="accel",
        depth_normalizer_column="normalizer",
        minimum_established_trend=0.5,
        minimum_residual_trend=0.2,
        minimum_pullback_speed=0.4,
        minimum_pullback_depth_fraction=0.1,
        maximum_pullback_depth_fraction=0.8,
        minimum_resumption_speed=0.5,
        minimum_resumption_acceleration=0.4,
        minimum_pullback_duration_ms=50,
        maximum_pullback_duration_ms=2_000,
    )


def pivot_config(candidate_id: str = "pivot-a") -> CountertrendPivotSignalConfig:
    return CountertrendPivotSignalConfig(
        candidate_id=candidate_id,
        trend_column="trend",
        movement_column="move",
        acceleration_column="accel",
        depth_normalizer_column="normalizer",
        minimum_established_trend=0.5,
        minimum_residual_trend=0.2,
        minimum_pullback_speed=0.4,
        minimum_pullback_depth_fraction=0.2,
        maximum_pullback_depth_fraction=0.9,
        minimum_rebound_fraction=0.05,
        minimum_pivot_speed=-0.2,
        minimum_velocity_improvement=0.3,
        minimum_pivot_acceleration=0.4,
        minimum_pullback_duration_ms=50,
        maximum_pullback_duration_ms=2_000,
    )


def compression_config(
    candidate_id: str = "compression-a",
) -> CompressionExpansionBreakoutSignalConfig:
    return CompressionExpansionBreakoutSignalConfig(
        candidate_id=candidate_id,
        short_volatility_column="short_vol",
        long_volatility_column="long_vol",
        short_arrival_rate_column="short_arrival",
        long_arrival_rate_column="long_arrival",
        movement_column="move",
        maximum_compression_ratio=0.5,
        minimum_expansion_ratio=1.2,
        minimum_arrival_rate_ratio=1.5,
        minimum_breakout_speed=0.5,
        breakout_buffer=0.0,
        minimum_compression_rows=3,
        maximum_breakout_wait_ms=500,
    )


def pressure_config(candidate_id: str = "pressure-a") -> QuoteTranslationPressureSignalConfig:
    return QuoteTranslationPressureSignalConfig(
        candidate_id=candidate_id,
        translation_pressure_column="pressure",
        translation_coherence_column="coherence",
        movement_column="move",
        persistence_column="persistence",
        short_arrival_rate_column="short_arrival",
        long_arrival_rate_column="long_arrival",
        minimum_translation_pressure=0.7,
        reset_translation_pressure=0.3,
        minimum_translation_coherence=0.8,
        minimum_movement_speed=0.5,
        minimum_persistence=0.7,
        minimum_arrival_rate_ratio=1.5,
    )


def signature(events: tuple[FrozenSignalEvent, ...]) -> list[tuple[object, ...]]:
    return [
        (
            event.tick_index,
            event.tick_id,
            event.timestamp,
            event.side,
            event.metadata["candidate_id"],
            event.metadata["family"],
        )
        for event in events
    ]


class FreshSignalTests(unittest.TestCase):
    def test_required_columns_are_explicit_unique_and_match_config(self):
        config = trend_config()
        columns = signal_required_columns(config)
        self.assertEqual(
            columns,
            (
                config.trend_column,
                config.velocity_column,
                config.acceleration_column,
                config.translation_coherence_column,
            ),
        )
        self.assertEqual(len(columns), len(set(columns)))

    def test_trend_acceleration_emits_only_on_hysteretic_onset_and_is_symmetric(self):
        long_frame = frame(
            [100.0] * 7,
            trend=[1.0] * 7,
            move=[0.1, 0.7, 0.8, 0.5, 0.1, 0.7, 0.8],
            accel=[0.0, 0.5, 0.6, 0.5, 0.0, 0.5, 0.6],
            coherence=[0.9] * 7,
        )
        events = generate_frozen_signal_events(long_frame, configs=[trend_config()])
        self.assertEqual([(item.tick_index, item.side) for item in events], [(1, "long"), (5, "long")])
        self.assertTrue(all(item.metadata["family"] == TREND_ACCELERATION for item in events))

        reflected = long_frame.copy()
        reflected["trend"] *= -1
        reflected["move"] *= -1
        reflected["accel"] *= -1
        reflected["mid"] = 200.0 - reflected["mid"]
        reflected["bid"] = reflected["mid"] - 0.1
        reflected["ask"] = reflected["mid"] + 0.1
        short_events = generate_frozen_signal_events(reflected, configs=[trend_config()])
        self.assertEqual([(item.tick_index, item.side) for item in short_events], [(1, "short"), (5, "short")])

    def test_pullback_resumption_requires_a_prior_trend_and_pullback_state(self):
        features = frame(
            [100.0, 99.5, 99.3, 99.7, 100.0],
            trend=[1.0] * 5,
            move=[0.6, -0.7, -0.3, 0.8, 0.9],
            accel=[0.0, -0.5, 0.1, 0.7, 0.8],
            normalizer=[2.0] * 5,
        )
        events = generate_frozen_signal_events(features, configs=[pullback_config()])
        self.assertEqual([(item.tick_index, item.side) for item in events], [(3, "long")])
        self.assertEqual(events[0].metadata["family"], PULLBACK_RESUMPTION)
        self.assertAlmostEqual(float(events[0].metadata["pullback_depth_fraction"]), 0.15)

        # A nominal resumption on the first usable row cannot fabricate a
        # trend/pullback history.
        no_history = generate_frozen_signal_events(features.iloc[3:].reset_index(drop=True), configs=[pullback_config()])
        self.assertEqual(no_history, ())

    def test_countertrend_pivot_uses_a_seen_extreme_and_current_deceleration_only(self):
        features = frame(
            [100.0, 99.5, 99.0, 99.2, 99.4],
            trend=[1.0] * 5,
            move=[0.6, -0.8, -0.7, -0.1, 0.2],
            accel=[0.0, -0.4, 0.1, 0.8, 0.8],
            normalizer=[2.0] * 5,
        )
        events = generate_frozen_signal_events(features, configs=[pivot_config()])
        self.assertEqual([(item.tick_index, item.side) for item in events], [(3, "long")])
        event = events[0]
        self.assertEqual(event.metadata["family"], COUNTERTREND_PIVOT)
        self.assertAlmostEqual(float(event.metadata["pullback_depth_fraction"]), 0.5)
        self.assertAlmostEqual(float(event.metadata["rebound_fraction"]), 0.1)
        self.assertAlmostEqual(float(event.metadata["signed_velocity_improvement"]), 0.6)

    def test_compression_breakout_level_excludes_the_decision_row(self):
        features = frame(
            [100.0, 100.1, 100.2, 100.6, 100.8],
            short_vol=[0.4, 0.4, 0.4, 1.5, 1.6],
            long_vol=[1.0] * 5,
            short_arrival=[1.0, 1.0, 1.0, 2.0, 2.0],
            long_arrival=[1.0] * 5,
            move=[0.0, 0.1, 0.1, 0.8, 0.9],
        )
        events = generate_frozen_signal_events(features, configs=[compression_config()])
        self.assertEqual([(item.tick_index, item.side) for item in events], [(3, "long")])
        event = events[0]
        self.assertEqual(event.metadata["family"], COMPRESSION_EXPANSION_BREAKOUT)
        # Highest prior bid is 100.1 (mid 100.2 less the 0.1 spread half),
        # not the decision row's 100.5 bid.
        self.assertAlmostEqual(float(event.metadata["fixed_prior_executable_level"]), 100.1)
        self.assertLess(float(event.metadata["fixed_prior_executable_level"]), float(features.loc[3, "bid"]))

    def test_quote_translation_pressure_latches_until_pressure_weakens(self):
        features = frame(
            [100.0] * 6,
            pressure=[0.2, 0.9, 0.95, 0.2, -0.9, -0.95],
            coherence=[0.9] * 6,
            move=[0.1, 0.8, 0.9, 0.1, -0.8, -0.9],
            persistence=[0.8] * 6,
            short_arrival=[2.0] * 6,
            long_arrival=[1.0] * 6,
        )
        events = generate_frozen_signal_events(features, configs=[pressure_config()])
        self.assertEqual([(item.tick_index, item.side) for item in events], [(1, "long"), (4, "short")])
        self.assertTrue(all(item.metadata["family"] == QUOTE_TRANSLATION_PRESSURE for item in events))

    def test_all_families_are_prefix_invariant_and_ignore_future_mutation(self):
        features = frame(
            [100.0, 99.5, 99.0, 99.2, 100.0, 100.1, 100.2, 100.6, 100.7],
            trend=[1.0] * 9,
            move=[0.1, -0.8, -0.7, -0.1, 0.1, 0.1, 0.1, 0.8, 0.9],
            accel=[0.0, -0.4, 0.1, 0.8, 0.0, 0.0, 0.0, 0.8, 0.9],
            coherence=[0.9] * 9,
            normalizer=[2.0] * 9,
            short_vol=[1.0, 1.0, 1.0, 1.0, 0.4, 0.4, 0.4, 1.5, 1.5],
            long_vol=[1.0] * 9,
            short_arrival=[2.0] * 9,
            long_arrival=[1.0] * 9,
            pressure=[0.0, -0.9, -0.8, -0.1, 0.1, 0.1, 0.1, 0.9, 0.95],
            persistence=[0.8] * 9,
        )
        configs = [
            trend_config(),
            pullback_config(),
            pivot_config(),
            compression_config(),
            pressure_config(),
        ]
        full = generate_frozen_signal_events(features, configs=configs)
        for stop in range(1, len(features) + 1):
            prefix = features.iloc[:stop].copy()
            prefix_events = generate_frozen_signal_events(prefix, configs=configs)
            expected = tuple(item for item in full if item.tick_index < stop)
            self.assertEqual(signature(prefix_events), signature(expected), f"prefix {stop}")

        cut = 5
        mutated = features.copy()
        mutation_columns = [
            "trend", "move", "accel", "coherence", "normalizer",
            "short_vol", "long_vol", "short_arrival", "long_arrival",
            "pressure", "persistence",
        ]
        mutated.loc[cut:, mutation_columns] = 12345.0
        mutated.loc[cut:, ["bid", "ask", "mid"]] += 250.0
        changed = generate_frozen_signal_events(mutated, configs=configs)
        self.assertEqual(
            signature(tuple(item for item in full if item.tick_index < cut)),
            signature(tuple(item for item in changed if item.tick_index < cut)),
        )

    def test_batch_engine_is_exactly_equal_to_reference_on_randomized_state(self):
        rng = np.random.default_rng(20260720)
        size = 2_000
        gaps = [False] * size
        for index in (137, 611, 1_401):
            gaps[index] = True
        segments: list[int] = []
        current_segment = 0
        for is_gap in gaps:
            if is_gap:
                current_segment += 1
            segments.append(current_segment)
        ready = [True] * size
        for index in (137, 611, 1_401):
            ready[index : index + 3] = [False, False, False]
        milliseconds = [index // 2 * 10 for index in range(size)]
        mids = (100.0 + np.cumsum(rng.normal(0.0, 0.03, size))).tolist()
        columns = {
            "trend": rng.normal(0.0, 1.0, size),
            "move": rng.normal(0.0, 1.0, size),
            "accel": rng.normal(0.0, 1.0, size),
            "coherence": rng.uniform(-1.0, 1.0, size),
            "normalizer": rng.uniform(0.2, 3.0, size),
            "short_vol": rng.uniform(0.1, 2.0, size),
            "long_vol": rng.uniform(0.1, 2.0, size),
            "short_arrival": rng.uniform(0.0, 4.0, size),
            "long_arrival": rng.uniform(0.0, 4.0, size),
            "pressure": rng.uniform(-1.0, 1.0, size),
            "persistence": rng.uniform(0.0, 1.0, size),
        }
        # Missing causal measurements reset state in both engines.
        for column, index in zip(columns, range(200, 211)):
            columns[column][index] = np.nan
        features = frame(
            mids,
            milliseconds=milliseconds,
            gap=gaps,
            ready=ready,
            segment=segments,
            **columns,
        )
        configs = [
            trend_config(),
            pullback_config(),
            pivot_config(),
            compression_config(),
            pressure_config(),
            dataclasses.replace(
                trend_config(),
                candidate_id="trend-random-neighbor",
                minimum_velocity=0.75,
                reset_velocity=-0.1,
            ),
            dataclasses.replace(
                pressure_config(),
                candidate_id="pressure-random-neighbor",
                minimum_translation_pressure=0.8,
                reset_translation_pressure=0.1,
            ),
        ]
        batch = generate_frozen_signal_events(
            features, configs=configs, engine="batch"
        )
        reference = generate_frozen_signal_events(
            features, configs=configs, engine="reference"
        )
        self.assertEqual(batch, reference)

    def test_batch_onset_engine_has_material_synthetic_speedup(self):
        size = 10_000
        features = frame(
            [100.0] * size,
            trend=[1.0] * size,
            move=[0.1] * size,
            accel=[0.0] * size,
            coherence=[0.9] * size,
            pressure=[0.1] * size,
            persistence=[0.8] * size,
            short_arrival=[2.0] * size,
            long_arrival=[1.0] * size,
        )
        configs = []
        for index in range(8):
            configs.extend(
                (
                    dataclasses.replace(
                        trend_config(),
                        candidate_id=f"trend-benchmark-{index}",
                        minimum_velocity=0.6 + index * 0.01,
                    ),
                    dataclasses.replace(
                        pressure_config(),
                        candidate_id=f"pressure-benchmark-{index}",
                        minimum_translation_pressure=0.7 + index * 0.01,
                    ),
                )
            )

        started = time.perf_counter()
        batch = generate_frozen_signal_events(
            features, configs=configs, engine="batch"
        )
        batch_seconds = time.perf_counter() - started
        started = time.perf_counter()
        reference = generate_frozen_signal_events(
            features, configs=configs, engine="reference"
        )
        reference_seconds = time.perf_counter() - started
        self.assertEqual(batch, reference)
        speedup = reference_seconds / max(batch_seconds, 1e-12)
        self.assertGreaterEqual(
            speedup,
            2.0,
            f"expected >=2x speedup, observed {speedup:.2f}x "
            f"(batch={batch_seconds:.3f}s, reference={reference_seconds:.3f}s)",
        )

    def test_equal_timestamp_id_order_gap_reset_and_exact_tick_preflight(self):
        features = frame(
            [100.0, 100.1, 100.2, 100.3, 100.4],
            milliseconds=[0, 0, 100, 5_500, 6_000],
            ids=[10, 11, 12, 1, 2],
            gap=[False, False, False, True, False],
            ready=[True, True, True, False, True],
            segment=[0, 0, 0, 1, 1],
            trend=[1.0] * 5,
            move=[0.1, 0.8, 0.9, 0.9, 0.8],
            accel=[0.0, 0.8, 0.9, 0.9, 0.8],
            coherence=[0.9] * 5,
        )
        events = generate_frozen_signal_events(features, configs=[trend_config()])
        self.assertEqual([(item.tick_index, item.tick_id) for item in events], [(1, 11), (4, 2)])
        self.assertEqual(events[0].timestamp, events[1].timestamp - timedelta(seconds=6))

        ticks = [
            Tick(
                id=int(row.tick_id),
                timestamp=pd.Timestamp(row.timestamp).to_pydatetime(),
                bid=float(row.bid),
                ask=float(row.ask),
            )
            for row in features.itertuples(index=False)
        ]
        preflight_signal_bindings(features, events, ticks=ticks)
        bad_ticks = list(ticks)
        bad_ticks[1] = Tick(
            id=999,
            timestamp=bad_ticks[1].timestamp,
            bid=bad_ticks[1].bid,
            ask=bad_ticks[1].ask,
        )
        with self.assertRaisesRegex(ValueError, "ID mismatch"):
            preflight_signal_bindings(features, events, ticks=bad_ticks)

        reordered = features.copy()
        reordered.loc[1, "tick_id"] = 9
        with self.assertRaisesRegex(ValueError, r"\(timestamp, id\)"):
            generate_frozen_signal_events(reordered, configs=[trend_config()])

    def test_configuration_and_frame_leakage_guards_are_explicit(self):
        config_classes = (
            TrendAccelerationSignalConfig,
            PullbackResumptionSignalConfig,
            CountertrendPivotSignalConfig,
            CompressionExpansionBreakoutSignalConfig,
            QuoteTranslationPressureSignalConfig,
        )
        for config_class in config_classes:
            for item in dataclasses.fields(config_class):
                self.assertIs(item.default, dataclasses.MISSING)
                self.assertIs(item.default_factory, dataclasses.MISSING)

        first = trend_config()
        self.assertEqual(signal_config_fingerprint(first), signal_config_fingerprint(first))
        self.assertNotEqual(signal_config_fingerprint(first), signal_config_fingerprint(trend_config("trend-b")))
        with self.assertRaisesRegex(ValueError, "candidate_id values must be unique"):
            generate_frozen_signal_events(
                frame(
                    [100.0],
                    trend=[1.0],
                    move=[0.0],
                    accel=[0.0],
                    coherence=[1.0],
                ),
                configs=[first, first],
            )
        with self.assertRaisesRegex(ValueError, "forbidden outcome-like"):
            TrendAccelerationSignalConfig(
                candidate_id="bad",
                trend_column="future_profit_label",
                velocity_column="move",
                acceleration_column="accel",
                translation_coherence_column="coherence",
                minimum_trend=0.5,
                minimum_velocity=0.6,
                reset_velocity=0.2,
                minimum_acceleration=0.4,
                minimum_translation_coherence=0.7,
            )

        invalid = frame(
            [100.0, 100.1],
            trend=[1.0, np.inf],
            move=[0.0, 0.8],
            accel=[0.0, 0.8],
            coherence=[0.9, 0.9],
        )
        with self.assertRaisesRegex(ValueError, "contains infinity"):
            generate_frozen_signal_events(invalid, configs=[trend_config()])

        valid = frame(
            [100.0, 100.1],
            trend=[1.0, 1.0],
            move=[0.0, 0.8],
            accel=[0.0, 0.8],
            coherence=[0.9, 0.9],
        )
        event = generate_frozen_signal_events(valid, configs=[trend_config()])[0]
        mismatched = FrozenSignalEvent(
            tick_index=event.tick_index,
            tick_id=999,
            timestamp=event.timestamp,
            side=event.side,
            metadata=event.metadata,
        )
        with self.assertRaisesRegex(ValueError, "tick_id does not match"):
            preflight_signal_bindings(valid, [mismatched])


if __name__ == "__main__":
    unittest.main()
