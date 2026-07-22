from __future__ import annotations

import unittest
from dataclasses import replace
from datetime import datetime, timezone
from unittest.mock import patch

import pandas as pd

import datavis.research.fresh_event_filters as event_filter_module
from datavis.research.fresh_entry_diagnostics import FrozenSignalEvent
from datavis.research.fresh_event_filters import (
    FRESH_REGIME_QUINTILE_RANKS,
    EventFilterVariantSource,
    FreshEventFilterConfig,
    FreshEventFilterRequest,
    FreshRegimeDefinition,
    derive_bounded_post_discovery_variant_bank,
    enrich_and_filter_frozen_event_batch,
    enrich_and_filter_frozen_events,
    fresh_event_filter_config_fingerprint,
    fresh_regime_quantile_measurements,
)
from datavis.research.fresh_thresholds import (
    FreshQuantileBankConfig,
    fit_session_balanced_quantiles,
)


UTC = timezone.utc
DEFINITION = FreshRegimeDefinition(
    volatility_column="2s_noise",
    spread_column="spread",
    trend_column="10s_mid_speed",
    arrival_column="1s_arrival_rate",
)


def training_bank():
    frames = {}
    for anchor in ("2026-01-02", "2026-01-05"):
        frames[anchor] = pd.DataFrame(
            {
                "feature_ready": [True] * 5,
                "gap_detected": [False] * 5,
                "2s_noise": [1.0, 2.0, 3.0, 4.0, 5.0],
                "spread": [0.1, 0.2, 0.3, 0.4, 0.5],
                "10s_mid_speed": [-1.0, 2.0, -3.0, 4.0, -5.0],
                "1s_arrival_rate": [1.0, 2.0, 3.0, 4.0, 5.0],
            }
        )
    return fit_session_balanced_quantiles(
        frames,
        measurements=fresh_regime_quantile_measurements(DEFINITION),
        config=FreshQuantileBankConfig(
            ranks=FRESH_REGIME_QUINTILE_RANKS,
            minimum_finite_values_per_session=5,
            minimum_eligible_sessions=2,
        ),
    )


def feature_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "tick_id": [101, 102, 103, 104],
            "timestamp": pd.to_datetime(
                [
                    "2026-01-05T23:30:00Z",  # valid broker session, off-major
                    "2026-01-06T01:00:00Z",  # Tokyo active, not opening
                    "2026-01-06T08:30:00Z",  # London opening
                    "2026-01-06T13:30:00Z",  # London/NY overlap, NY opening
                ],
                utc=True,
            ),
            "bid": [2000.0, 2000.1, 2000.2, 2000.3],
            "ask": [2000.3, 2000.4, 2000.7, 2000.4],
            "feature_ready": [True, True, True, True],
            "gap_detected": [False, False, False, False],
            "2s_noise": [3.0, 1.0, 3.0, 5.0],
            "spread": [0.3, 0.3, 0.5, 0.1],
            "10s_mid_speed": [-3.0, 1.0, -5.0, 4.0],
            "1s_arrival_rate": [3.0, 1.0, 5.0, 4.0],
        }
    )


def events_for(frame: pd.DataFrame) -> tuple[FrozenSignalEvent, ...]:
    events = []
    for index, row in frame.iterrows():
        events.append(
            FrozenSignalEvent(
                tick_index=int(index),
                tick_id=int(row["tick_id"]),
                timestamp=pd.Timestamp(row["timestamp"]).to_pydatetime(),
                side="long" if index % 2 == 0 else "short",
                metadata={"candidate_id": "candidate-a", "ordinal": int(index)},
            )
        )
    return tuple(events)


def config(
    *,
    variant_id: str = "all",
    activity: str = "all",
    spread_rank: float | None = None,
    volatility_rank: float | None = None,
) -> FreshEventFilterConfig:
    return FreshEventFilterConfig(
        variant_id=variant_id,
        regime_definition=DEFINITION,
        activity_filter=activity,  # type: ignore[arg-type]
        spread_ceiling_rank=spread_rank,
        volatility_floor_rank=volatility_rank,
    )


class FreshEventFilterTests(unittest.TestCase):
    def test_measurement_specs_are_explicit_and_outcome_guarded(self):
        specs = fresh_regime_quantile_measurements(DEFINITION)
        self.assertEqual(FRESH_REGIME_QUINTILE_RANKS, (0.2, 0.4, 0.6, 0.8))
        self.assertEqual(
            [(item.name, item.column, item.transform) for item in specs],
            [
                ("regime::volatility", "2s_noise", "positive"),
                ("regime::spread", "spread", "identity"),
                ("regime::trend", "10s_mid_speed", "absolute"),
                ("regime::arrival", "1s_arrival_rate", "positive"),
            ],
        )
        with self.assertRaisesRegex(ValueError, "outcome-like"):
            FreshRegimeDefinition(
                volatility_column="future_profit",
                spread_column="spread",
                trend_column="trend",
                arrival_column="arrival",
            )

    def test_enrichment_preserves_exact_bindings_order_and_existing_metadata(self):
        frame = feature_frame()
        original = events_for(frame)
        result = enrich_and_filter_frozen_events(
            frame,
            original,
            config=config(),
            quantile_bank=training_bank(),
        )
        self.assertEqual(result.input_count, 4)
        self.assertEqual(result.retained_count, 4)
        self.assertEqual(result.rejected_count, 0)
        self.assertEqual(
            [
                (event.tick_index, event.tick_id, event.timestamp, event.side)
                for event in result.events
            ],
            [
                (event.tick_index, event.tick_id, event.timestamp, event.side)
                for event in original
            ],
        )
        self.assertEqual(
            [event.metadata["ordinal"] for event in result.events],
            [0, 1, 2, 3],
        )

        first = result.events[0].metadata["context"]
        self.assertEqual(first["sessionAnchor"], "2026-01-06")
        self.assertEqual(first["day"], "2026-01-06")
        self.assertEqual(first["session"], "off-major")
        self.assertEqual(first["timestampSydney"], "2026-01-06T10:30:00+11:00")
        self.assertEqual(first["timestampNewYork"], "2026-01-05T18:30:00-05:00")
        self.assertEqual(
            first["brokerSessionStartSydney"],
            "2026-01-06T10:00:00+11:00",
        )
        self.assertEqual(
            first["brokerSessionEndNewYork"],
            "2026-01-06T17:00:00-05:00",
        )
        self.assertEqual(
            first["regime"],
            "volatility:q3|spread:q3|trend:q3|arrival:q3",
        )
        self.assertEqual(first["regimes"]["trend"]["value"], 3.0)
        self.assertEqual(first["regimes"]["trend"]["quintile"], "q3")
        self.assertEqual(
            result.events[3].metadata["context"]["session"],
            "london+new-york-overlap|new-york-opening",
        )

    def test_batch_matches_scalar_order_counts_and_prepares_rows_once(self):
        frame = feature_frame()
        events = events_for(frame)
        bank = training_bank()
        requests = (
            FreshEventFilterRequest(events=events, config=config()),
            FreshEventFilterRequest(
                events=events,
                config=config(
                    variant_id="active",
                    activity="any-major-active",
                ),
            ),
            FreshEventFilterRequest(
                events=events,
                config=config(
                    variant_id="regime-filter",
                    spread_rank=0.8,
                    volatility_rank=0.4,
                ),
            ),
        )
        expected = tuple(
            enrich_and_filter_frozen_events(
                frame,
                request.events,
                config=request.config,
                quantile_bank=bank,
            )
            for request in requests
        )

        with patch.object(
            event_filter_module,
            "_prepare_rows",
            wraps=event_filter_module._prepare_rows,
        ) as prepare_rows:
            actual = enrich_and_filter_frozen_event_batch(
                frame,
                requests,
                quantile_bank=bank,
            )

        self.assertEqual(actual, expected)
        self.assertEqual(prepare_rows.call_count, 1)
        self.assertEqual([item.input_count for item in actual], [4, 4, 4])
        self.assertEqual(
            [[event.tick_id for event in item.events] for item in actual],
            [[101, 102, 103, 104], [102, 103, 104], [101, 104]],
        )

    def test_batch_outputs_do_not_alias_metadata_between_requests(self):
        frame = feature_frame()
        original = events_for(frame)[:1]
        shared_config = config()
        requests = (
            FreshEventFilterRequest(events=original, config=shared_config),
            FreshEventFilterRequest(events=original, config=shared_config),
        )

        first, second = enrich_and_filter_frozen_event_batch(
            frame,
            requests,
            quantile_bank=training_bank(),
        )

        self.assertEqual(first, second)
        first_metadata = first.events[0].metadata
        second_metadata = second.events[0].metadata
        first_context = first_metadata["context"]
        second_context = second_metadata["context"]
        self.assertIsNot(first_metadata, second_metadata)
        self.assertIsNot(first_context, second_context)
        self.assertIsNot(first_context["regimes"], second_context["regimes"])
        self.assertIsNot(
            first_context["regimes"]["volatility"],
            second_context["regimes"]["volatility"],
        )
        self.assertIsNot(
            first_context["eventFilter"], second_context["eventFilter"]
        )
        self.assertNotIn("context", original[0].metadata)

    def test_batch_ignores_rows_after_its_latest_requested_event(self):
        frame = feature_frame()
        events = events_for(frame)[:1]
        requests = (
            FreshEventFilterRequest(events=events, config=config()),
            FreshEventFilterRequest(
                events=events,
                config=config(
                    variant_id="spread",
                    spread_rank=0.8,
                ),
            ),
        )
        bank = training_bank()
        baseline = enrich_and_filter_frozen_event_batch(
            frame,
            requests,
            quantile_bank=bank,
        )

        malformed_future = frame.copy()
        for column in ("2s_noise", "spread", "10s_mid_speed", "1s_arrival_rate"):
            malformed_future.loc[1:, column] = float("inf")
        repeated = enrich_and_filter_frozen_event_batch(
            malformed_future,
            requests,
            quantile_bank=bank,
        )

        self.assertEqual(repeated, baseline)

    def test_activity_filters_use_civil_opening_and_overlap(self):
        frame = feature_frame()
        events = events_for(frame)
        bank = training_bank()
        active = enrich_and_filter_frozen_events(
            frame,
            events,
            config=config(variant_id="active", activity="any-major-active"),
            quantile_bank=bank,
        )
        self.assertEqual([event.tick_id for event in active.events], [102, 103, 104])
        self.assertEqual(active.rejected_activity_count, 1)

        opening = enrich_and_filter_frozen_events(
            frame,
            events,
            config=config(
                variant_id="opening", activity="opening-or-overlap"
            ),
            quantile_bank=bank,
        )
        self.assertEqual([event.tick_id for event in opening.events], [103, 104])
        self.assertEqual(opening.rejected_activity_count, 2)

    def test_spread_and_volatility_filters_have_fixed_rejection_precedence(self):
        frame = feature_frame()
        result = enrich_and_filter_frozen_events(
            frame,
            events_for(frame),
            config=config(
                variant_id="regime-filter",
                spread_rank=0.8,
                volatility_rank=0.4,
            ),
            quantile_bank=training_bank(),
        )
        # q80 spread is 0.42 and q40 volatility is 2.6.  Row 103 fails
        # spread before volatility is considered; row 102 then fails volatility.
        self.assertEqual([event.tick_id for event in result.events], [101, 104])
        self.assertEqual(result.rejected_activity_count, 0)
        self.assertEqual(result.rejected_spread_count, 1)
        self.assertEqual(result.rejected_volatility_count, 1)
        self.assertEqual(result.rejected_count, 2)

    def test_future_columns_and_appended_rows_cannot_change_prior_event_context(self):
        frame = feature_frame()
        first_event = events_for(frame)[:1]
        bank = training_bank()
        baseline = enrich_and_filter_frozen_events(
            frame,
            first_event,
            config=config(),
            quantile_bank=bank,
        )

        extended = frame.copy()
        extended["future_profit"] = [99.0, -99.0, 88.0, -88.0]
        future = pd.DataFrame(
            {
                "tick_id": [999],
                "timestamp": pd.to_datetime(["2026-01-06T14:00:00Z"], utc=True),
                "bid": [3000.0],
                "ask": [3000.1],
                "feature_ready": [True],
                "gap_detected": [False],
                # These malformed future measurements would fail validation
                # if the filter inspected rows after the requested event.
                "2s_noise": [float("inf")],
                "spread": [0.1],
                "10s_mid_speed": [float("inf")],
                "1s_arrival_rate": [float("inf")],
                "future_profit": [1_000_000.0],
            }
        )
        extended = pd.concat([extended, future], ignore_index=True)
        repeated = enrich_and_filter_frozen_events(
            extended,
            first_event,
            config=config(),
            quantile_bank=bank,
        )
        self.assertEqual(repeated.events, baseline.events)
        self.assertEqual(repeated.config_sha256, baseline.config_sha256)

    def test_bindings_readiness_order_and_reserved_metadata_are_guarded(self):
        frame = feature_frame()
        events = events_for(frame)
        bank = training_bank()
        wrong_id = replace(events[0], tick_id=999)
        with self.assertRaisesRegex(ValueError, "tick_id does not match"):
            enrich_and_filter_frozen_events(
                frame,
                (wrong_id,),
                config=config(),
                quantile_bank=bank,
            )
        with self.assertRaisesRegex(ValueError, "non-decreasing"):
            enrich_and_filter_frozen_events(
                frame,
                (events[2], events[1]),
                config=config(),
                quantile_bank=bank,
            )
        unready = frame.copy()
        unready.loc[0, "feature_ready"] = False
        with self.assertRaisesRegex(ValueError, "unusable"):
            enrich_and_filter_frozen_events(
                unready,
                events[:1],
                config=config(),
                quantile_bank=bank,
            )
        reserved = replace(events[0], metadata={"context": {"outcome": 1}})
        with self.assertRaisesRegex(ValueError, "reserved context"):
            enrich_and_filter_frozen_events(
                frame,
                (reserved,),
                config=config(),
                quantile_bank=bank,
            )

    def test_events_outside_broker_session_are_rejected(self):
        frame = feature_frame().iloc[:1].copy()
        frame.loc[0, "timestamp"] = pd.Timestamp("2026-01-05T22:30:00Z")
        event = FrozenSignalEvent(
            tick_index=0,
            tick_id=101,
            timestamp=datetime(2026, 1, 5, 22, 30, tzinfo=UTC),
            side="long",
        )
        with self.assertRaisesRegex(ValueError, "scheduled broker session"):
            enrich_and_filter_frozen_events(
                frame,
                (event,),
                config=config(),
                quantile_bank=training_bank(),
            )

    def test_dst_aware_activity_and_sydney_new_york_conversion(self):
        frame = feature_frame().iloc[:1].copy()
        frame.loc[0, "timestamp"] = pd.Timestamp("2026-03-30T12:30:00Z")
        event = FrozenSignalEvent(
            tick_index=0,
            tick_id=101,
            timestamp=datetime(2026, 3, 30, 12, 30, tzinfo=UTC),
            side="long",
        )
        result = enrich_and_filter_frozen_events(
            frame,
            (event,),
            config=config(),
            quantile_bank=training_bank(),
        )
        context = result.events[0].metadata["context"]
        self.assertEqual(context["sessionAnchor"], "2026-03-30")
        self.assertEqual(
            context["session"],
            "london+new-york-overlap|new-york-opening",
        )
        self.assertEqual(context["timestampSydney"], "2026-03-30T23:30:00+11:00")
        self.assertEqual(context["timestampNewYork"], "2026-03-30T08:30:00-04:00")

    def test_config_hash_binds_bank_and_every_filter_choice(self):
        bank = training_bank()
        baseline = config()
        baseline_hash = fresh_event_filter_config_fingerprint(baseline, bank)
        self.assertEqual(len(baseline_hash), 64)
        self.assertNotEqual(
            baseline_hash,
            fresh_event_filter_config_fingerprint(
                config(variant_id="active", activity="any-major-active"), bank
            ),
        )
        self.assertNotEqual(
            baseline_hash,
            fresh_event_filter_config_fingerprint(
                config(variant_id="spread", spread_rank=0.8), bank
            ),
        )

        altered_threshold = replace(bank.thresholds[0], value=123.0)
        tampered = replace(
            bank,
            thresholds=(altered_threshold, *bank.thresholds[1:]),
        )
        with self.assertRaisesRegex(ValueError, "hash does not match"):
            fresh_event_filter_config_fingerprint(baseline, tampered)

    def test_variant_bank_never_exceeds_total_or_family_budget(self):
        bank = training_bank()
        sources = (
            EventFilterVariantSource("ta-1", "trend"),
            EventFilterVariantSource("pr-1", "pullback"),
            EventFilterVariantSource("cp-1", "pivot"),
            EventFilterVariantSource("ce-1", "compression"),
        )
        counts = {"trend": 58, "pullback": 58, "pivot": 58, "compression": 56}
        variants = derive_bounded_post_discovery_variant_bank(
            bank,
            regime_definition=DEFINITION,
            source_candidates=sources,
            already_registered_candidate_count=230,
            registered_family_counts=counts,
            requested_additional_candidates=100,
            maximum_total_candidates=240,
            maximum_candidates_per_family=60,
        )
        self.assertEqual(variants.total_candidate_count, 240)
        self.assertEqual(len(variants.variants), 10)
        added = {family: 0 for family in counts}
        for item in variants.variants:
            added[item.family] += 1
            self.assertEqual(len(item.filter_config_sha256), 64)
            self.assertEqual(len(item.candidate_sha256), 64)
            self.assertTrue(
                item.filter_config.activity_filter != "all"
                or item.filter_config.spread_ceiling_rank is not None
                or item.filter_config.volatility_floor_rank is not None
            )
        for family, existing in counts.items():
            self.assertLessEqual(existing + added[family], 60)
        self.assertEqual(
            dict(variants.final_family_counts),
            {family: existing + added[family] for family, existing in counts.items()},
        )
        self.assertEqual(len(variants.variant_bank_sha256), 64)

        reordered = derive_bounded_post_discovery_variant_bank(
            bank,
            regime_definition=DEFINITION,
            source_candidates=tuple(reversed(sources)),
            already_registered_candidate_count=230,
            registered_family_counts=dict(reversed(tuple(counts.items()))),
            requested_additional_candidates=100,
            maximum_total_candidates=240,
            maximum_candidates_per_family=60,
        )
        self.assertEqual(reordered, variants)

    def test_production_shape_allocates_complete_groups_across_full_catalog(self):
        family_group_counts = {
            "trend": 13,
            "pullback": 5,
            "pivot": 5,
            "compression": 4,
            "pressure": 4,
        }
        family_counts = {
            family: group_count * 3
            for family, group_count in family_group_counts.items()
        }
        sources = tuple(
            EventFilterVariantSource(
                candidate_id=f"{family}-{group_index:02d}-rank-{rank_index}",
                family=family,
                robustness_group=f"{family}-{group_index:02d}",
            )
            for family, group_count in family_group_counts.items()
            for group_index in range(group_count)
            for rank_index in range(3)
        )
        bank = derive_bounded_post_discovery_variant_bank(
            training_bank(),
            regime_definition=DEFINITION,
            source_candidates=sources,
            already_registered_candidate_count=93,
            registered_family_counts=family_counts,
            requested_additional_candidates=147,
            maximum_total_candidates=240,
            maximum_candidates_per_family=60,
        )

        self.assertEqual(bank.total_candidate_count, 240)
        self.assertEqual(len(bank.variants), 147)
        self.assertEqual(
            len({item.filter_config.variant_id for item in bank.variants}),
            18,
        )
        selected = {
            (
                item.source_candidate_id.rsplit("-rank-", 1)[0],
                item.filter_config.variant_id,
            )
            for item in bank.variants
        }
        for group, filter_id in selected:
            matching = [
                item
                for item in bank.variants
                if item.source_candidate_id.rsplit("-rank-", 1)[0] == group
                and item.filter_config.variant_id == filter_id
            ]
            self.assertEqual(len(matching), 3)
        self.assertTrue(all(count <= 60 for _, count in bank.final_family_counts))

    def test_variant_budget_inputs_are_audited(self):
        with self.assertRaisesRegex(ValueError, "sum to the registered total"):
            derive_bounded_post_discovery_variant_bank(
                training_bank(),
                regime_definition=DEFINITION,
                source_candidates=(EventFilterVariantSource("a", "trend"),),
                already_registered_candidate_count=10,
                registered_family_counts={"trend": 9},
                requested_additional_candidates=5,
            )


if __name__ == "__main__":
    unittest.main()
