 spool,
            baseline_by_candidate,
        ):
            baseline_summaries: dict[
                str, tuple[_BaselineCoverageSummary, ...]
            ] = {}
            for fingerprint in sorted(set(baseline_by_candidate.values())):
                summaries: list[_BaselineCoverageSummary] = []
                with spool.load(self._baseline_spool_key(fingerprint)) as loaded:
                    for _anchor in anchors:
                        try:
                            result = next(loaded)
                        except StopIteration as exc:
                            raise RuntimeError(
                                "baseline spool session count changed"
                            ) from exc
                        summaries.append(_baseline_coverage_summary(result))
                        del result
                    try:
                        next(loaded)
                    except StopIteration:
                        pass
                    else:
                        raise RuntimeError("baseline spool session count changed")
                baseline_summaries[fingerprint] = tuple(summaries)

            for candidate in candidates:
                baseline_fingerprint = baseline_by_candidate[candidate.candidate_id]
                seed_text = (
                    f"{context.stage}:{candidate.entry_sha256}:"
                    f"{canonical_hash(list(anchors))}"
                )
                with FloatSeriesSpool(
                    self.spool_directory,
                    maximum_bytes=self.numeric_spool_maximum_bytes,
                ) as values:
                    scorer = EntryDiagnosticSessionScorer(
                        config=self.scoring.entry_metrics,
                        dimensions=self.dimensions,
                        evaluated_sessions=anchors,
                        values=values,
                        key_prefix="entry-score",
                    )
                    edge_reducer = _StreamingEntryEdgeReducer(
                        values=values,
                        key_prefix="entry-edge",
                        seed_text=seed_text,
                    )
                    with spool.load(
                        self._candidate_spool_key(candidate.candidate_id)
                    ) as loaded:
                        for anchor, baseline in zip(
                            anchors,
                            baseline_summaries[baseline_fingerprint],
                        ):
                            try:
                                result = next(loaded)
                            except StopIteration as exc:
                                raise RuntimeError(
                                    "candidate spool session count changed"
                                ) from exc
                            scorer.add_session(result, session_anchor=anchor)
                            edge_reducer.add_session(result, baseline)
                            del result
                        try:
                            next(loaded)
                        except StopIteration:
                            pass
                        else:
                            raise RuntimeError(
                                "candidate spool session count changed"
                            )
                    report = scorer.finish()
                    gate = evaluate_entry_gate(
                        report.overall,
                        minimum_sample=self.scoring.minimum_sample,
                        thresholds=self.scoring.entry_gate,
                    )
                    edge = edge_reducer.finish()
                provisional[candidate.candidate_id] = (report, gate, edge)

        return self._finalize_entry_evaluations(
            candidates=candidates,
            context=context,
            provisional=provisional,
        )

    def score_entries_batch_materialized_reference(
        self,
        candidates: tuple[FrozenEntryCandidate, ...],
        context: EvaluationContext,
    ) -> Mapping[str, CandidateEvaluation]:
        """Test oracle for proving the disk-spooled scorer is outcome-equivalent."""

        anchors = _context_anchors(context)
        runtimes = tuple(self.entry_runtime[item.candidate_id] for item in candidates)
        session_results, baselines = self._entry_session_batch_materialized(
            runtimes, anchors, stage=context.stage
        )
        provisional = {
            candidate.candidate_id: self._entry_provisional_evaluation(
                candidate=candidate,
                context=context,
                anchors=anchors,
                session_results=session_results[candidate.candidate_id],
                baseline_results=baselines[candidate.candidate_id],
            )
            for candidate in candidates
        }
        return self._finalize_entry_evaluations(
            candidates=candidates,
            context=context,
            provisional=provisional,
        )

    def _finalize_entry_evaluations(
        self,
        *,
        candidates: Sequence[FrozenEntryCandidate],
        context: EvaluationContext,
        provisional: Mapping[
            str, tuple[EntryScoreReport, GateResult, _EntryEdgeSummary]
        ],
    ) -> Mapping[str, CandidateEvaluation]:
        grouped_candidates: dict[str, list[FrozenEntryCandidate]] = defaultdict(list)
        for candidate in candidates:
            runtime = self.entry_runtime[candidate.candidate_id]
            grouped_candidates[runtime.robustness_group].append(candidate)
        neighbourhood_spec = self.preregistration["robustnessAndGates"][
            "parameterNeighborhood"
        ]
        neighbourhood_audits: dict[str, dict[str, Any]] = {}
        if context.stage == "discovery":
            for group, members in grouped_candidates.items():
                neighbourhood_audits[group] = _parameter_neighbourhood_audit(
                    tuple(
                        (
                            candidate.candidate_id,
                            self.entry_runtime[
                                candidate.candidate_id
                            ].source.rank_offset,
                            bool(
                                provisional[candidate.candidate_id][1].passed
                                and provisional[candidate.candidate_id][
                                    2
                                ].baseline_gate_passed
                            ),
                            provisional[candidate.candidate_id][
                                2
                            ].expected_barrier_pnl_per_fill,
                            provisional[candidate.candidate_id][
                                0
                            ].overall.coverage_probability(30),
                            canonical_hash(
                                [
                                    (
                                        item.parameter,
                                        item.final_value,
                                    )
                                    for item in self.entry_runtime[
                                        candidate.candidate_id
                                    ].source.threshold_provenance
                                ]
                            ),
                        )
                        for candidate in members
                    ),
                    minimum_valid_neighbor_fraction=float(
                        neighbourhood_spec["minimumValidNeighborFraction"]
                    ),
                    minimum_positive_expectancy_neighbor_fraction=float(
                        neighbourhood_spec["minimumPositiveExpectancyNeighborFraction"]
                    ),
                    minimum_neighbor_expectancy_retention=float(
                        neighbourhood_spec["minimumNeighborExpectancyRetention"]
                    ),
                    maximum_absolute_coverage_30_drop=float(
                        neighbourhood_spec["maximumAbsoluteCoverage30SecondDrop"]
                    ),
                )

        output: dict[str, CandidateEvaluation] = {}
        for candidate in candidates:
            runtime = self.entry_runtime[candidate.candidate_id]
            report, gate, edge = provisional[candidate.candidate_id]
            neighbourhood_required = context.stage == "discovery"
            neighbourhood = neighbourhood_audits.get(
                runtime.robustness_group,
                {
                    "centerCandidateId": candidate.candidate_id,
                    "evaluatedCount": 1,
                    "passed": True,
                },
            )
            neighbourhood_passed = bool(neighbourhood["passed"])
            is_center = runtime.source.rank_offset == 0.0
            passed = bool(
                gate.passed
                and edge.baseline_gate_passed
                and (not neighbourhood_required or (is_center and neighbourhood_passed))
            )
            output[candidate.candidate_id] = CandidateEvaluation(
                identity_sha256=candidate.entry_sha256,
                passed=passed,
                metrics={
                    "entry": _compact_entry_slices(report),
                    "registeredGate": asdict(gate),
                    "entryEdge": asdict(edge),
                    "parameterNeighbourhood": {
                        **neighbourhood,
                        "group": runtime.robustness_group,
                        "requiredDuringStage": neighbourhood_required,
                        "candidateIsCenter": is_center,
                    },
                },
                leakage_checks={
                    "sessionCorpusFingerprintVerifiedBeforeFeatures": True,
                    "featureCalculationPrefixCausal": True,
                    "signalsUseCurrentOrEarlierRowsOnly": True,
                    "strictlyLaterBidAskFill": True,
                    "holdoutRolePresent": "holdout" in context.evaluation_roles,
                },
                score=_entry_rank_score(report, edge),
            )
        return output

    def build_exit_variants(
        self,
        entries: tuple[FrozenEntryCandidate, ...],
        context: EvaluationContext,
    ) -> Iterable[StrategyCandidateSpec]:
        if context.stage != "exit_search" or len(entries) != 1:
            raise PermissionError(
                "exit variants require exactly one promoted frozen entry"
            )
        if self.quantile_bank is None:
            raise RuntimeError("quantile bank has not been frozen")
        selected_entry = entries[0]
        grid = build_fresh_exit_grid(
            self.quantile_bank, execution_configs=self.executions
        )
        preflight = self.threshold_preflight
        if preflight is None or any(
            (
                preflight["quantileBankSha256"] != self.quantile_bank.bank_sha256,
                preflight["exitGridSha256"] != grid.grid_sha256,
                preflight["executionScenariosSha256"]
                != grid.execution_scenarios_sha256,
            )
        ):
            raise RuntimeError("runtime exit bank differs from threshold preflight")
        self.exit_runtime = {}
        artifact = {
            "schema": "fresh-xauusd-runtime-exit-bank/v1",
            "selectedEntryCandidateId": selected_entry.candidate_id,
            "selectedEntrySha256": selected_entry.entry_sha256,
            "exitGridSha256": grid.grid_sha256,
            "executionScenariosSha256": grid.execution_scenarios_sha256,
            "variantCount": len(grid.variants),
            "variants": [asdict(item) for item in grid.variants],
        }
        _write_new_json(self.output / "fresh_exit_bank_v1.json", artifact)
        for variant in grid.variants:
            strategy_id = f"{selected_entry.candidate_id}::{variant.variant_id}"
            self.exit_runtime[strategy_id] = variant
            yield StrategyCandidateSpec(
                strategy_id=strategy_id,
                entry_candidate_id=selected_entry.candidate_id,
                exit_config={
                    "schema": "fresh-xauusd-exit-runtime/v1",
                    "variant": asdict(variant),
                    "variantSha256": variant.variant_sha256,
                    "exitGridSha256": grid.grid_sha256,
                },
                execution_config={
                    "schema": "fresh-xauusd-execution-scenarios/v1",
                    "scenarioIds": [
                        item.scenario_id for item in grid.execution_scenarios
                    ],
                    "scenarioConfigSha256": {
                        item.scenario_id: item.config_sha256
                        for item in grid.execution_scenarios
                    },
                    "selectionScenario": REFERENCE_SCENARIO_ID,
                    "requiredStressScenarioIds": list(
                        self.scoring.required_stress_scenario_ids
                    ),
                    "scenarioEvaluationPolicy": dict(
                        self.preregistration["execution"]["scenarioEvaluationPolicy"]
                    ),
                },
                exit_variant=variant.variant_id,
            )

    def score_strategies_batch(
        self,
        candidates: tuple[FrozenStrategyCandidate, ...],
        context: EvaluationContext,
    ) -> Mapping[str, CandidateEvaluation]:
        with KeyedObjectSpool[tuple[Any, ...]](
            self.spool_directory,
            maximum_bytes=self.spool_maximum_bytes,
        ) as trade_spool:
            return self._score_strategies_batch_spooled(
                candidates, context, trade_spool
            )

    @staticmethod
    def _strategy_trade_spool_key(strategy_id: str, scenario_id: str) -> str:
        return f"strategy-trades\x00{strategy_id}\x00{scenario_id}"

    def _score_strategies_batch_spooled(
        self,
        candidates: tuple[FrozenStrategyCandidate, ...],
        context: EvaluationContext,
        trade_spool: KeyedObjectSpool[tuple[Any, ...]],
    ) -> Mapping[str, CandidateEvaluation]:
        if self.quantile_bank is None:
            raise RuntimeError("quantile bank has not been frozen")
        anchors = _context_anchors(context)
        runtimes = {
            candidate.entry.candidate_id: self.entry_runtime[
                candidate.entry.candidate_id
            ]
            for candidate in candidates
        }
        source_configs = {
            item.source.config.candidate_id: item.source.config
            for item in runtimes.values()
        }
        columns = list(
            dict.fromkeys(
                column
                for config in source_configs.values()
                for column in signal_required_columns(config)
            )
        )
        columns.extend(
            (
                "1s_mid_speed",
                "1s_mid_acceleration",
                FRESH_EXIT_VOLATILITY_COLUMN,
                *(
                    spec.column
                    for spec in fresh_regime_quantile_measurements(
                        self.regime_definition
                    )
                ),
            )
        )
        scenario_ids = (
            self.later_full_scenario_ids
            if context.stage in LATER_SENSITIVITY_STAGES
            else self.exit_search_scenario_ids
        )
        for candidate in candidates:
            for scenario_id in scenario_ids:
                trade_spool.register_key(
                    self._strategy_trade_spool_key(candidate.strategy_id, scenario_id)
                )
        censors: dict[str, Counter[str]] = {
            candidate.strategy_id: Counter() for candidate in candidates
        }
        complete: dict[str, dict[str, bool]] = {
            candidate.strategy_id: {scenario: True for scenario in scenario_ids}
            for candidate in candidates
        }
        entry_results: dict[str, list[FreshEntryDiagnosticsResult]] = {
            identifier: [] for identifier in runtimes
        }
        filter_baselines: dict[str, list[FreshEntryDiagnosticsResult]] = {
            identifier: [] for identifier in runtimes
        }

        for ordinal, anchor in enumerate(anchors, start=1):
            tape = self.source.load_session(anchor)
            frame = self._features(tape, columns)
            raw_events = generate_frozen_signal_events(
                frame, configs=tuple(source_configs.values()), engine="batch"
            )
            grouped = _events_by_candidate(raw_events)
            raw_baseline = _baseline_events(frame, tape)
            events_by_entry: dict[str, tuple[FrozenSignalEvent, ...]] = {}
            baseline_by_filter: dict[str, FreshEntryDiagnosticsResult] = {}
            runtime_items = tuple(runtimes.items())
            filters_by_sha: dict[str, FreshEventFilterConfig] = {}
            for _, runtime in runtime_items:
                filter_sha = fresh_event_filter_config_fingerprint(
                    runtime.event_filter, self.quantile_bank
                )
                filters_by_sha.setdefault(filter_sha, runtime.event_filter)
            filter_items = tuple(filters_by_sha.items())
            filtered_batch = enrich_and_filter_frozen_event_batch(
                frame,
                (
                    *(
                        FreshEventFilterRequest(
                            grouped.get(runtime.source.config.candidate_id, ()),
                            runtime.event_filter,
                        )
                        for _, runtime in runtime_items
                    ),
                    *(
                        FreshEventFilterRequest(raw_baseline, event_filter)
                        for _, event_filter in filter_items
                    ),
                ),
                quantile_bank=self.quantile_bank,
            )
            runtime_results = filtered_batch[: len(runtime_items)]
            filter_results = filtered_batch[len(runtime_items) :]
            for (identifier, _), filtered in zip(runtime_items, runtime_results):
                events = filtered.events
                events = _before_close(events, tape)
                if len({event.tick_index for event in events}) != len(events):
                    raise ValueError(
                        "a frozen entry emitted both directions on one tick"
                    )
                events_by_entry[identifier] = events
                entry_results[identifier].append(
                    _diagnose(tape, events, config=self.entry_diagnostic_config)
                )
            for (filter_sha, _), filtered in zip(filter_items, filter_results):
                baseline_by_filter[filter_sha] = _diagnose(
                    tape,
                    _before_close(filtered.events, tape),
                    config=self.entry_diagnostic_config,
                )
            for identifier, runtime in runtime_items:
                filter_sha = fresh_event_filter_config_fingerprint(
                    runtime.event_filter, self.quantile_bank
                )
                filter_baselines[identifier].append(baseline_by_filter[filter_sha])

            feature_rows = BoundDecisionFeatureRows(
                tape.ticks,
                decision_feature_rows(
                    frame,
                    velocity_column="1s_mid_speed",
                    acceleration_column="1s_mid_acceleration",
                ),
            )
            volatility_feature_rows = BoundVolatilityRows(
                tape.ticks,
                volatility_rows(frame, column=FRESH_EXIT_VOLATILITY_COLUMN),
            )
            prepared_by_gap: dict[int, Any] = {}
            prepared_by_scenario: dict[str, Any] = {}
            for scenario_id in scenario_ids:
                execution = self.executions[scenario_id]
                gap_ms = execution.maximum_intertick_gap_ms
                if gap_ms not in prepared_by_gap:
                    prepared_by_gap[gap_ms] = _prepare_replay_tape(
                        tape.ticks,
                        maximum_intertick_gap_ms=gap_ms,
                        _trusted_validated_ticks=True,
                    )
                prepared_by_scenario[scenario_id] = prepared_by_gap[gap_ms]
            for candidate in candidates:
                variant = self.exit_runtime[candidate.strategy_id]
                events = events_by_entry[candidate.entry.candidate_id]
                for scenario_id in scenario_ids:
                    execution = self.executions[scenario_id]
                    source = FrozenSignalDecisionSource(
                        events,
                        feature_rows=feature_rows,
                        weakening=variant.weakening,
                        execution=execution,
                        source_metadata={
                            "frozenStrategyId": candidate.strategy_id,
                            "frozenStrategySha256": candidate.strategy_sha256,
                            "frozenEntryCandidateId": candidate.entry.candidate_id,
                        },
                    )
                    volatility = (
                        volatility_feature_rows.cursor()
                        if variant.policy.requires_volatility
                        else None
                    )
                    policy = FreshProtectiveExitPolicy(
                        source,
                        config=variant.policy,
                        execution=execution,
                        volatility=volatility,
                    )
                    replay = _replay_session(
                        tape,
                        policy,
                        config=execution,
                        prepared_replay_tape=prepared_by_scenario[scenario_id],
                    )
                    trade_spool.append(
                        self._strategy_trade_spool_key(
                            candidate.strategy_id, scenario_id
                        ),
                        tuple(replay.trades),
                    )
                    censors[candidate.strategy_id][scenario_id] += len(replay.censors)
                    complete[candidate.strategy_id][scenario_id] &= bool(
                        replay.boundary_reached
                        and not replay.halted
                        and not replay.censors
                    )
            self._emit(
                stage=context.stage,
                sessionOrdinal=ordinal,
                sessionCount=len(anchors),
                sessionAnchor=anchor,
            )
            del frame, tape, raw_events, grouped, raw_baseline

        if any(count != len(anchors) for _, count in trade_spool.inventory):
            raise RuntimeError("strategy trade spool session inventory is incomplete")
        entry_summaries: dict[
            tuple[str, str], tuple[EntryScoreReport, _EntryEdgeSummary]
        ] = {}
        for candidate in candidates:
            entry_id = candidate.entry.candidate_id
            summary_key = (entry_id, candidate.entry.entry_sha256)
            if summary_key in entry_summaries:
                continue
            combined_entry = combine_entry_diagnostics(tuple(entry_results[entry_id]))
            entry_report = score_entry_diagnostics(
                combined_entry,
                config=self.scoring.entry_metrics,
                dimensions=self.dimensions,
                evaluated_sessions=anchors,
            )
            entry_edge = _entry_edge_summary(
                entry_results[entry_id],
                filter_baselines[entry_id],
                seed_text=f"{context.stage}:{candidate.entry.entry_sha256}",
            )
            entry_summaries[summary_key] = (entry_report, entry_edge)

        provisional: dict[
            str,
            tuple[CandidateEvaluation, bool, TradeScoreReport, EntryScoreReport],
        ] = {}
        for candidate in candidates:
            entry_id = candidate.entry.candidate_id
            entry_report, entry_edge = entry_summaries[
                (entry_id, candidate.entry.entry_sha256)
            ]
            reports: dict[str, TradeScoreReport] = {}
            for scenario_id in scenario_ids:
                censor_count = int(censors[candidate.strategy_id][scenario_id])
                with trade_spool.load(
                    self._strategy_trade_spool_key(candidate.strategy_id, scenario_id)
                ) as trade_batches:
                    reports[scenario_id] = score_trade_records(
                        (
                            trade
                            for session_trades in trade_batches
                            for trade in session_trades
                        ),
                        config=self.scoring.trade_metrics,
                        dimensions=self.dimensions,
                        evaluated_sessions=anchors,
                        replay_censor_count=censor_count,
                        profitability_valid=bool(
                            complete[candidate.strategy_id][scenario_id]
                            and censor_count == 0
                        ),
                    )
            reference = reports.pop(REFERENCE_SCENARIO_ID)
            required_reports = {
                scenario_id: reports[scenario_id]
                for scenario_id in self.scoring.required_stress_scenario_ids
            }
            sensitivity_reports = {
                scenario_id: report
                for scenario_id, report in reports.items()
                if scenario_id not in required_reports
            }
            scorecard = build_candidate_scorecard(
                entry_report,
                reference,
                required_reports,
                config=self.scoring,
            )
            passed = bool(
                scorecard.full_gate.passed and entry_edge.baseline_gate_passed
            )
            evaluation = CandidateEvaluation(
                identity_sha256=candidate.strategy_sha256,
                passed=passed,
                metrics={
                    "entry": _compact_entry_slices(entry_report),
                    "entryEdge": asdict(entry_edge),
                    "reference": _compact_trade_slices(reference),
                    "stresses": {
                        key: _compact_trade_slices(value)
                        for key, value in sorted(required_reports.items())
                    },
                    "sensitivities": {
                        key: _compact_trade_slices(value)
                        for key, value in sorted(sensitivity_reports.items())
                    },
                    "registeredEntryGate": asdict(scorecard.entry_gate),
                    "registeredFullGate": asdict(scorecard.full_gate),
                    "balancedScore": asdict(scorecard.balanced_score),
                },
                leakage_checks={
                    "entryDefinitionUnchangedDuringExitSearch": True,
                    "sessionCorpusFingerprintVerifiedBeforeReplay": True,
                    "causalFeatureRowsBoundToEveryReplayTick": True,
                    "stopsTriggerOnObservedExecutableQuote": True,
                    "allFillsUseStrictlyLaterObservedQuotes": True,
                },
                score=scorecard.balanced_score.score,
            )
            provisional[candidate.strategy_id] = (
                evaluation,
                passed,
                reference,
                entry_report,
            )

        grouped_strategies: dict[str, list[FrozenStrategyCandidate]] = defaultdict(list)
        for candidate in candidates:
            variant = self.exit_runtime[candidate.strategy_id]
            group = "::".join(
                (
                    variant.stop_structure_id,
                    variant.management_structure_id,
                    variant.invalidation_structure_id,
                )
            )
            grouped_strategies[group].append(candidate)

        neighbourhood_spec = self.preregistration["robustnessAndGates"][
            "parameterNeighborhood"
        ]
        neighbourhood_audits: dict[str, dict[str, Any]] = {}
        if context.stage == "exit_search":
            for group, members in grouped_strategies.items():
                neighbourhood_audits[group] = _parameter_neighbourhood_audit(
                    tuple(
                        (
                            candidate.strategy_id,
                            self.exit_runtime[candidate.strategy_id].rank_offset,
                            provisional[candidate.strategy_id][1],
                            provisional[candidate.strategy_id][2].overall.expectancy,
                            None,
                            canonical_hash(
                                {
                                    "policy": asdict(
                                        self.exit_runtime[candidate.strategy_id].policy
                                    ),
                                    "weakening": (
                                        asdict(
                                            self.exit_runtime[
                                                candidate.strategy_id
                                            ].weakening
                                        )
                                        if self.exit_runtime[
                                            candidate.strategy_id
                                        ].weakening
                                        is not None
                                        else None
                                    ),
                                }
                            ),
                        )
                        for candidate in members
                    ),
                    minimum_valid_neighbor_fraction=float(
                        neighbourhood_spec["minimumValidNeighborFraction"]
                    ),
                    minimum_positive_expectancy_neighbor_fraction=float(
                        neighbourhood_spec["minimumPositiveExpectancyNeighborFraction"]
                    ),
                    minimum_neighbor_expectancy_retention=float(
                        neighbourhood_spec["minimumNeighborExpectancyRetention"]
                    ),
                    maximum_absolute_coverage_30_drop=None,
                )

        output: dict[str, CandidateEvaluation] = {}
        for candidate in candidates:
            evaluation, base_passed, _, _ = provisional[candidate.strategy_id]
            variant = self.exit_runtime[candidate.strategy_id]
            group = "::".join(
                (
                    variant.stop_structure_id,
                    variant.management_structure_id,
                    variant.invalidation_structure_id,
                )
            )
            required = context.stage == "exit_search"
            neighbourhood = neighbourhood_audits.get(
                group,
                {
                    "centerCandidateId": candidate.strategy_id,
                    "evaluatedCount": 1,
                    "passed": True,
                },
            )
            neighbourhood_passed = bool(neighbourhood["passed"])
            is_center = variant.rank_offset == 0.0
            metrics = dict(evaluation.metrics)
            metrics["exitParameterNeighbourhood"] = {
                **neighbourhood,
                "group": group,
                "requiredDuringStage": required,
                "candidateIsCenter": is_center,
            }
            output[candidate.strategy_id] = CandidateEvaluation(
                identity_sha256=evaluation.identity_sha256,
                passed=bool(
                    base_passed
                    and (not required or (is_center and neighbourhood_passed))
                ),
                metrics=metrics,
                leakage_checks=evaluation.leakage_checks,
                score=evaluation.score,
            )
        return output

    def authorize_holdout(
        self,
        winner: FrozenStrategyCandidate,
        records: tuple[Mapping[str, Any], ...],
        explicit: bool,
    ) -> Mapping[str, Any]:
        if explicit is not True:
            raise PermissionError("holdout authorization must be explicit")
        matching_wf3 = [
            item
            for item in records
            if item.get("role") == "walk_forward_3"
            and item.get("frozenStrategySha256") == winner.strategy_sha256
            and item.get("gatePassed") is True
        ]
        matching_validation = [
            item
            for item in records
            if item.get("role") == "validation"
            and item.get("frozenStrategySha256") == winner.strategy_sha256
            and item.get("gatePassed") is True
        ]
        if len(matching_wf3) != 1 or len(matching_validation) != 1:
            raise PermissionError(
                "holdout requires exact passed WF3 and validation records"
            )
        frozen = {
            "schema": "fresh-xauusd-final-strategy/v1",
            "strategyId": winner.strategy_id,
            "strategySha256": winner.strategy_sha256,
            "entryCandidateId": winner.entry.candidate_id,
            "entrySha256": winner.entry.entry_sha256,
            "entryConfig": winner.entry.config,
            "exitConfig": winner.exit_config,
            "executionConfig": winner.execution_config,
            "noPostHoldoutTuning": True,
        }
        _write_new_json(self.output / "fresh_final_strategy_frozen_v1.json", frozen)
        return authorize_registered_holdout(
            preregistration=self.preregistration,
            split_manifest=self.split_manifest,
            frozen_strategy_sha256=winner.strategy_sha256,
            walk_forward_3_record_number=int(matching_wf3[0]["recordNumber"]),
            validation_record_number=int(matching_validation[0]["recordNumber"]),
            explicit_holdout_authorization=True,
            verify_current_implementation_files=(
                self.verify_preregistration_implementation_files
            ),
            infrastructure_recovery_contract=self.recovery_contract,
            recovery_implementation_manifest=self.recovery_implementation_manifest,
            recovery_batch_result_path=self.recovery_batch_result_path,
        )

    @staticmethod
    def _unused_signal_generator(
        _candidate: FrozenEntryCandidate, _context: EvaluationContext
    ) -> None:
        raise RuntimeError("batched entry scorer is required")

    @staticmethod
    def _unused_entry_scorer(
        _candidate: FrozenEntryCandidate,
        _context: EvaluationContext,
        _signals: Any,
    ) -> CandidateEvaluation:
        raise RuntimeError("batched entry scorer is required")

    @staticmethod
    def _unused_scenario_runner(
        _candidate: FrozenStrategyCandidate,
        _context: EvaluationContext,
        _signals: Any,
    ) -> None:
        raise RuntimeError("batched strategy scorer is required")

    @staticmethod
    def _unused_strategy_scorer(
        _candidate: FrozenStrategyCandidate,
        _context: EvaluationContext,
        _results: Any,
    ) -> CandidateEvaluation:
        raise RuntimeError("batched strategy scorer is required")

    def _search_budgets(self) -> FreshSearchBudgets:
        budgets = self.preregistration["candidateSearch"]["budgets"]
        return FreshSearchBudgets(
            discovery_distinct_candidates=int(budgets["discoveryDistinctCandidates"]),
            discovery_per_family_maximum=int(budgets["discoveryPerFamilyMaximum"]),
            walk_forward_1_frozen_candidates=int(
                budgets["walkForward1FrozenCandidates"]
            ),
            walk_forward_2_frozen_candidates=int(
                budgets["walkForward2FrozenCandidates"]
            ),
            exit_variants_after_entry_gate=int(budgets["exitVariantsAfterEntryGate"]),
            walk_forward_3_full_strategies=int(budgets["walkForward3FullStrategies"]),
            validation_full_strategies=int(budgets["validationFullStrategies"]),
            holdout_full_strategies=int(budgets["holdoutFullStrategies"]),
            exit_search_frozen_entries=int(budgets["exitSearchFrozenEntries"]),
        )

    def _search_callbacks(self) -> FreshSearchCallbacks:
        return FreshSearchCallbacks(
            fit_thresholds=self.fit_thresholds,
            build_entry_candidates=self.build_entry_candidates,
            generate_signals=self._unused_signal_generator,
            score_entry=self._unused_entry_scorer,
            build_exit_variants=self.build_exit_variants,
            run_execution_scenarios=self._unused_scenario_runner,
            score_strategy=self._unused_strategy_scorer,
            authorize_holdout=self.authorize_holdout,
            score_entries_batch=self.score_entries_batch,
            score_strategies_batch=self.score_strategies_batch,
        )

    def build_search(self) -> FreshChronologicalSearch:
        return FreshChronologicalSearch(
            split_manifest=self.split_manifest,
            ledger_path=self.preregistration["sourceBindings"]["experimentLedgerPath"],
            budgets=self._search_budgets(),
            callbacks=self._search_callbacks(),
            preregistration_sha256=str(self.preregistration["preregistrationSha256"]),
        )

    def resume_incomplete_discovery_search(
        self,
        *,
        entry_specs: Sequence[EntryCandidateSpec],
        recovery_audit: Mapping[str, Any],
        recovery_batch_result_path: str | Path,
    ) -> FreshChronologicalSearch:
        if self.quantile_bank is None:
            raise RuntimeError("recovery quantile bank has not been restored")
        return FreshChronologicalSearch.resume_incomplete_discovery(
            split_manifest=self.split_manifest,
            ledger_path=self.preregistration["sourceBindings"]["experimentLedgerPath"],
            budgets=self._search_budgets(),
            callbacks=self._search_callbacks(),
            preregistration_sha256=str(self.preregistration["preregistrationSha256"]),
            threshold_bank=fresh_quantile_bank_payload(self.quantile_bank),
            entry_specs=entry_specs,
            recovery_audit=recovery_audit,
            recovery_batch_result_path=recovery_batch_result_path,
        )


def _strongest_record(records: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    stage_order = {
        "discovery": 0,
        "walk_forward_1": 1,
        "walk_forward_2": 2,
        "exit_search": 3,
        "walk_forward_3": 4,
        "validation": 5,
        "holdout": 6,
    }
    eligible = [
        item
        for item in records
        if item.get("stage") in stage_order
        and isinstance(item.get("balancedScore"), (int, float))
        and not isinstance(item.get("balancedScore"), bool)
    ]
    if not eligible:
        return None
    furthest = max(stage_order[str(item["stage"])] for item in eligible)
    selected = [
        item for item in eligible if stage_order[str(item["stage"])] == furthest
    ]
    return min(
        selected,
        key=lambda item: (-float(item["balancedScore"]), str(item["candidateId"])),
    )


def run_registered_fresh_research(
    connection_context_factory: Any,
    *,
    repository_root: str | Path,
    output_directory: str | Path,
    research_state_directory: str | Path,
    scratch_directory: str | Path | None = None,
    progress: ProgressCallback | None = None,
    resume_artifact_directory: str | Path | None = None,
    resume_v5_artifact_directory: str | Path | None = None,
    infrastructure_restart_artifact_directory: str | Path | None = None,
    infrastructure_restart_v4_artifact_directory: str | Path | None = None,
    infrastructure_restart_v5_artifact_directory: str | Path | None = None,
) -> dict[str, Any]:
    """Run a registered study or one exact, audited same-lineage recovery."""

    root = Path(repository_root).resolve()
    output_argument = Path(output_directory).expanduser()
    state_argument = Path(research_state_directory).expanduser()
    scratch_argument = (
        output_argument
        if scratch_directory is None
        else Path(scratch_directory).expanduser()
    )
    if (
        (
            infrastructure_restart_v4_artifact_directory is not None
            or infrastructure_restart_v5_artifact_directory is not None
            or resume_v5_artifact_directory is not None
        )
        and scratch_directory is None
    ):
        raise ValueError(
            "the v4/v5 restart or V5 recovery requires an explicit separate scratch"
        )
    for name, path in (
        ("output", output_argument),
        ("durable research state", state_argument),
        ("scratch", scratch_argument),
    ):
        if path.is_symlink():
            raise PermissionError(f"{name} directory cannot be a symbolic link")
    output = output_argument.resolve()
    state_root = state_argument.resolve()
    scratch = scratch_argument.resolve()
    if output.exists() and any(output.iterdir()):
        raise FileExistsError("fresh research output directory must be empty")
    if state_root == output or output in state_root.parents:
        raise ValueError("durable research state cannot be inside temporary output")
    if (
        (
            infrastructure_restart_v4_artifact_directory is not None
            or infrastructure_restart_v5_artifact_directory is not None
            or resume_v5_artifact_directory is not None
        )
        and scratch == output
    ):
        raise ValueError(
            "the v4/v5 restart or V5 recovery requires an explicit separate scratch"
        )
    if scratch != output and (
        scratch == state_root
        or scratch in state_root.parents
        or state_root in scratch.parents
        or scratch in output.parents
        or output in scratch.parents
    ):
        raise ValueError(
            "scratch must be separate from output and durable research state"
        )
    if scratch.exists() and any(scratch.iterdir()):
        raise FileExistsError("fresh research scratch directory must be empty")
    output.mkdir(parents=True, exist_ok=True)
    if resume_v5_artifact_directory is not None:
        if not state_root.is_dir():
            raise PermissionError(
                "the frozen V5 durable state root is unavailable"
            )
    else:
        state_root.mkdir(parents=True, exist_ok=True)
    scratch.mkdir(parents=True, exist_ok=True)

    recovery_used = resume_artifact_directory is not None
    recovery_v5_used = resume_v5_artifact_directory is not None
    restart_used = infrastructure_restart_artifact_directory is not None
    restart_v4_used = infrastructure_restart_v4_artifact_directory is not None
    restart_v5_used = infrastructure_restart_v5_artifact_directory is not None
    if (
        sum(
            (
                recovery_used,
                recovery_v5_used,
                restart_used,
                restart_v4_used,
                restart_v5_used,
            )
        )
        > 1
    ):
        raise ValueError(
            "recovery and new-study restart modes are mutually exclusive"
        )
    recovery_manifest: Mapping[str, Any] | None = None
    recovery_equivalence_evidence: Mapping[str, Any] | None = None
    recovery_bundle = None
    recovery_v5_bundle = None
    restart_bundle = None
    restart_v4_bundle = None
    restart_v5_bundle = None
    if recovery_used:
        recovery_bundle = load_run14_recovery_bundle(resume_artifact_directory)
        bootstrap = {
            "inventory": recovery_bundle.inventory,
            "corpus": recovery_bundle.corpus,
            "split": recovery_bundle.split,
        }
    elif recovery_v5_used:
        recovery_v5_bundle = load_fresh_v5_recovery_bundle(
            resume_v5_artifact_directory
        )
        bootstrap = {
            "inventory": recovery_v5_bundle.inventory,
            "corpus": recovery_v5_bundle.corpus,
            "split": recovery_v5_bundle.split,
        }
    elif restart_v5_used:
        restart_v5_bundle = load_fresh_v5_restart_bundle(
            infrastructure_restart_v5_artifact_directory
        )
        bootstrap = {
            "inventory": restart_v5_bundle.inventory,
            "corpus": restart_v5_bundle.corpus,
            "split": restart_v5_bundle.split,
        }
        for source_name, destination_name in (
            ("fresh_source_inventory_v1.json", "fresh_source_inventory_v1.json"),
            ("fresh_corpus_manifest_v1.json", "fresh_corpus_manifest_v1.json"),
            ("fresh_split_manifest_v2.json", "fresh_split_manifest_v2.json"),
        ):
            _snapshot_new_file(
                restart_v5_bundle.paths[source_name],
                output / destination_name,
            )
    elif restart_v4_used:
        restart_v4_bundle = load_fresh_v4_restart_bundle(
            infrastructure_restart_v4_artifact_directory
        )
        bootstrap = {
            "inventory": restart_v4_bundle.inventory,
            "corpus": restart_v4_bundle.corpus,
            "split": restart_v4_bundle.split,
        }
        for source_name, destination_name in (
            ("fresh_source_inventory_v1.json", "fresh_source_inventory_v1.json"),
            ("fresh_corpus_manifest_v1.json", "fresh_corpus_manifest_v1.json"),
            ("fresh_split_manifest_v2.json", "fresh_split_manifest_v2.json"),
        ):
            _snapshot_new_file(
                restart_v4_bundle.paths[source_name],
                output / destination_name,
            )
    elif restart_used:
        restart_bundle = load_fresh_v3_restart_bundle(
            infrastructure_restart_artifact_directory
        )
        bootstrap = {
            "inventory": restart_bundle.inventory,
            "corpus": restart_bundle.corpus,
            "split": restart_bundle.split,
        }
        for source_name, destination_name in (
            ("fresh_source_inventory_v1.json", "fresh_source_inventory_v1.json"),
            ("fresh_corpus_manifest_v1.json", "fresh_corpus_manifest_v1.json"),
            ("fresh_split_manifest_v2.json", "fresh_split_manifest_v2.json"),
        ):
            _snapshot_new_file(
                restart_bundle.paths[source_name],
                output / destination_name,
            )
    else:
        bootstrap = build_fresh_source_bootstrap(
            connection_context_factory,
            config=registered_fresh_bootstrap_config(),
            on_progress=progress,
        )
        write_fresh_source_bootstrap(output, bootstrap)

    state_binding = (
        dict(recovery_v5_bundle.state_binding)
        if recovery_v5_bundle is not None
        else _research_state_binding_v5(
            state_root,
            bootstrap["split"],
            restart_v5_bundle.provenance,
            restart_v5_bundle.predecessor_state_binding,
        )
        if restart_v5_bundle is not None
        else _research_state_binding_v4(
            state_root,
            bootstrap["split"],
            restart_v4_bundle.provenance,
            restart_v4_bundle.predecessor_state_binding,
        )
        if restart_v4_bundle is not None
        else _research_state_binding_v3(
            state_root,
            bootstrap["split"],
            restart_bundle.provenance,
        )
        if restart_bundle is not None
        else _research_state_binding(state_root, bootstrap["split"])
    )
    if recovery_v5_bundle is not None:
        pinned_state_root = Path(
            str(state_binding["stateDirectory"])
        ).expanduser().resolve()
        if state_root != pinned_state_root:
            raise PermissionError(
                "the supplied durable state root differs from the frozen V5 binding"
            )
    ledger = Path(state_binding["experimentLedgerPath"])
    holdout_registry = Path(state_binding["holdoutAuthorizationRegistryPath"])
    for durable_path in (ledger, holdout_registry):
        if durable_path.is_symlink():
            raise PermissionError("durable research state cannot be a symbolic link")
    if holdout_registry.exists():
        raise PermissionError("this frozen holdout window has already been consumed")
    if recovery_v5_bundle is None:
        ledger.parent.mkdir(parents=True, exist_ok=True)
        holdout_registry.parent.mkdir(parents=True, exist_ok=True)
    elif not ledger.parent.is_dir() or not holdout_registry.parent.is_dir():
        raise PermissionError(
            "the frozen V5 durable state directories are unavailable"
        )
    if (
        restart_bundle is not None
        or restart_v4_bundle is not None
        or restart_v5_bundle is not None
        or recovery_v5_bundle is not None
    ):
        predecessor_ledger = Path(
            state_binding["predecessorExperimentLedgerPath"]
        )
        expected_predecessor_ledger_sha = (
            RUN19_LEDGER_SHA256
            if (
                restart_v5_bundle is not None
                or recovery_v5_bundle is not None
            )
            else (
                RUN17_LEDGER_SHA256
                if restart_v4_bundle is not None
                else RUN16_LEDGER_SHA256
            )
        )
        predecessor_label = (
            "v4"
            if (
                restart_v5_bundle is not None
                or recovery_v5_bundle is not None
            )
            else ("v3" if restart_v4_bundle is not None else "v2")
        )
        if (
            predecessor_ledger.is_symlink()
            or not predecessor_ledger.is_file()
            or _file_sha256(predecessor_ledger)
            != expected_predecessor_ledger_sha
        ):
            raise PermissionError(
                f"the terminal {predecessor_label} ledger was not preserved "
                "byte-for-byte"
            )

    if recovery_used:
        if recovery_bundle is None:
            raise RuntimeError("run-14 recovery bundle was not loaded")
        if state_binding != recovery_bundle.state_binding:
            raise PermissionError("durable run-14 state binding changed")
        if not ledger.is_file() or _file_sha256(ledger) != RUN14_LEDGER_SHA256:
            raise PermissionError(
                "the original durable run-14 ledger was not preserved byte-for-byte"
            )
        for source_name, destination_name in (
            ("fresh_source_inventory_v1.json", "fresh_source_inventory_v1.json"),
            ("fresh_corpus_manifest_v1.json", "fresh_corpus_manifest_v1.json"),
            ("fresh_split_manifest_v2.json", "fresh_split_manifest_v2.json"),
            (
                "fresh_research_state_binding_v1.json",
                "fresh_research_state_binding_v1.json",
            ),
            (
                "fresh_implementation_manifest_v1.json",
                "fresh_implementation_manifest_v1.json",
            ),
            ("fresh_preregistration_v2.json", "fresh_preregistration_v2.json"),
            ("fresh_quantile_bank_v1.json", "fresh_quantile_bank_v1.json"),
            (
                "fresh_threshold_domain_preflight_v1.json",
                "fresh_threshold_domain_preflight_v1.json",
            ),
            ("server-run.log", "run14_server-run.log"),
            ("remote-exit-status.txt", "run14_remote-exit-status.txt"),
        ):
            _snapshot_new_file(
                recovery_bundle.paths[source_name], output / destination_name
            )
        implementation = recovery_bundle.implementation
        preregistration = recovery_bundle.preregistration
        recovery_manifest = build_fresh_implementation_manifest(
            repository_root=root,
            relative_paths=(
                *required_fresh_implementation_files(),
                "datavis/research/fresh_recovery.py",
                "datavis/research/fresh_spool.py",
                "test_fresh_pipeline.py",
                "test_fresh_preregistration.py",
                "test_fresh_recovery.py",
                "test_fresh_search.py",
                "test_fresh_spool.py",
            ),
        )
        _write_new_json(
            output / "fresh_recovery_implementation_manifest_v1.json",
            recovery_manifest,
        )
        recovery_equivalence_evidence = run_run14_recovery_equivalence_preflight(
            root,
            resume_artifact_directory,
            recovery_implementation_manifest=recovery_manifest,
        )
        if progress is not None:
            progress(
                {
                    "stage": "recovery_equivalence_preflight",
                    "status": "passed",
                    "testModuleCount": len(
                        recovery_equivalence_evidence["testModules"]
                    ),
                    "evidenceSha256": canonical_hash(recovery_equivalence_evidence),
                }
            )
    elif recovery_v5_bundle is not None:
        if (
            not ledger.is_file()
            or _file_sha256(ledger) != V5_LEDGER_SHA256
        ):
            raise PermissionError(
                "the original durable V5 ledger was not preserved byte-for-byte"
            )
        for source_name, destination_name in (
            ("fresh_source_inventory_v1.json", "fresh_source_inventory_v1.json"),
            ("fresh_corpus_manifest_v1.json", "fresh_corpus_manifest_v1.json"),
            ("fresh_split_manifest_v2.json", "fresh_split_manifest_v2.json"),
            (
                "fresh_research_state_binding_v4.json",
                "fresh_research_state_binding_v4.json",
            ),
            (
                "fresh_implementation_manifest_v1.json",
                "fresh_implementation_manifest_v1.json",
            ),
            ("fresh_preregistration_v5.json", "fresh_preregistration_v5.json"),
            ("fresh_quantile_bank_v1.json", "fresh_quantile_bank_v1.json"),
            (
                "fresh_threshold_domain_preflight_v1.json",
                "fresh_threshold_domain_preflight_v1.json",
            ),
            (
                "fresh_experiment_ledger_v1.jsonl",
                "original_v5_fresh_experiment_ledger_v1.jsonl",
            ),
            (
                "predecessor_fresh_experiment_ledger_v1.jsonl",
                "predecessor_fresh_experiment_ledger_v1.jsonl",
            ),
            (
                "predecessor_fresh_implementation_manifest_v1.json",
                "predecessor_fresh_implementation_manifest_v1.json",
            ),
            (
                "predecessor_fresh_preregistration_v4.json",
                "predecessor_fresh_preregistration_v4.json",
            ),
            (
                "predecessor_fresh_research_state_binding_v3.json",
                "predecessor_fresh_research_state_binding_v3.json",
            ),
            ("server-run.log", "original_v5_server-run.log"),
            (
                "remote-exit-status.txt",
                "original_v5_remote-exit-status.txt",
            ),
        ):
            _snapshot_new_file(
                recovery_v5_bundle.paths[source_name],
                output / destination_name,
            )
        implementation = recovery_v5_bundle.implementation
        preregistration = recovery_v5_bundle.preregistration
        recovery_manifest = build_fresh_implementation_manifest(
            repository_root=root,
            relative_paths=(
                required_fresh_v5_recovery_implementation_files()
            ),
        )
        _write_new_json(
            output / "fresh_recovery_implementation_manifest_v1.json",
            recovery_manifest,
        )
        recovery_equivalence_evidence = (
            run_fresh_v5_recovery_equivalence_preflight(
                root,
                resume_v5_artifact_directory,
                recovery_implementation_manifest=recovery_manifest,
            )
        )
        if progress is not None:
            progress(
                {
                    "stage": "v5_recovery_equivalence_preflight",
                    "status": "passed",
                    "testModuleCount": len(
                        recovery_equivalence_evidence["testModules"]
                    ),
                    "evidenceSha256": canonical_hash(
                        recovery_equivalence_evidence
                    ),
                }
            )
    elif restart_v5_bundle is not None:
        if ledger.exists() and ledger.stat().st_size:
            raise PermissionError(
                "the v5 study lineage already has a durable experiment ledger"
            )
        _write_new_json(
            output / "fresh_research_state_binding_v4.json",
            state_binding,
        )
        for source_name, destination_name in (
            (
                "fresh_research_state_binding_v3.json",
                "predecessor_fresh_research_state_binding_v3.json",
            ),
            (
                "fresh_experiment_ledger_v1.jsonl",
                "predecessor_fresh_experiment_ledger_v1.jsonl",
            ),
            (
                "fresh_preregistration_v4.json",
                "predecessor_fresh_preregistration_v4.json",
            ),
            (
                "fresh_implementation_manifest_v1.json",
                "predecessor_fresh_implementation_manifest_v1.json",
            ),
        ):
            _snapshot_new_file(
                restart_v5_bundle.paths[source_name],
                output / destination_name,
            )
        implementation = build_fresh_implementation_manifest(
            repository_root=root,
            relative_paths=required_fresh_v5_implementation_files(),
        )
        _write_new_json(
            output / "fresh_implementation_manifest_v1.json",
            implementation,
        )
        preregistration = build_fresh_preregistration_v5(
            split_manifest=bootstrap["split"],
            corpus_manifest_sha256=str(
                bootstrap["corpus"]["corpusManifestSha256"]
            ),
            protocol_code_identifier=(
                "fresh-pipeline-v5:" + implementation["manifestSha256"]
            ),
            implementation_manifest=implementation,
            experiment_ledger_path=ledger,
            holdout_authorization_registry_path=holdout_registry,
            infrastructure_restart_provenance=restart_v5_bundle.provenance,
        )
        _write_new_json(
            output / "fresh_preregistration_v5.json",
            preregistration,
        )
    elif restart_v4_bundle is not None:
        if ledger.exists() and ledger.stat().st_size:
            raise PermissionError(
                "the v4 study lineage already has a durable experiment ledger"
            )
        _write_new_json(
            output / "fresh_research_state_binding_v3.json",
            state_binding,
        )
        for source_name, destination_name in (
            (
                "fresh_research_state_binding_v2.json",
                "predecessor_fresh_research_state_binding_v2.json",
            ),
            (
                "fresh_experiment_ledger_v1.jsonl",
                "predecessor_fresh_experiment_ledger_v1.jsonl",
            ),
            (
                "fresh_preregistration_v3.json",
                "predecessor_fresh_preregistration_v3.json",
            ),
            (
                "fresh_implementation_manifest_v1.json",
                "predecessor_fresh_implementation_manifest_v1.json",
            ),
        ):
            _snapshot_new_file(
                restart_v4_bundle.paths[source_name],
                output / destination_name,
            )
        implementation = build_fresh_implementation_manifest(
            repository_root=root,
            relative_paths=required_fresh_v4_implementation_files(),
        )
        _write_new_json(
            output / "fresh_implementation_manifest_v1.json",
            implementation,
        )
        preregistration = build_fresh_preregistration_v4(
            split_manifest=bootstrap["split"],
            corpus_manifest_sha256=str(
                bootstrap["corpus"]["corpusManifestSha256"]
            ),
            protocol_code_identifier=(
                "fresh-pipeline-v4:" + implementation["manifestSha256"]
            ),
            implementation_manifest=implementation,
            experiment_ledger_path=ledger,
            holdout_authorization_registry_path=holdout_registry,
            infrastructure_restart_provenance=restart_v4_bundle.provenance,
        )
        _write_new_json(
            output / "fresh_preregistration_v4.json",
            preregistration,
        )
    elif restart_bundle is not None:
        if ledger.exists() and ledger.stat().st_size:
            raise PermissionError(
                "the v3 study lineage already has a durable experiment ledger"
            )
        _write_new_json(
            output / "fresh_research_state_binding_v2.json",
            state_binding,
        )
        for source_name, destination_name in (
            (
                "fresh_research_state_binding_v1.json",
                "predecessor_fresh_research_state_binding_v1.json",
            ),
            (
                "fresh_experiment_ledger_v1.jsonl",
                "predecessor_fresh_experiment_ledger_v1.jsonl",
            ),
            (
                "fresh_preregistration_v2.json",
                "predecessor_fresh_preregistration_v2.json",
            ),
            (
                "fresh_implementation_manifest_v1.json",
                "predecessor_fresh_implementation_manifest_v1.json",
            ),
        ):
            _snapshot_new_file(
                restart_bundle.paths[source_name],
                output / destination_name,
            )
        implementation = build_fresh_implementation_manifest(
            repository_root=root,
            relative_paths=tuple(
                sorted(
                    {
                        *required_fresh_implementation_files(),
                        "datavis/research/fresh_restart.py",
                        "datavis/research/fresh_spool.py",
                    }
                )
            ),
        )
        _write_new_json(
            output / "fresh_implementation_manifest_v1.json",
            implementation,
        )
        preregistration = build_fresh_preregistration_v3(
            split_manifest=bootstrap["split"],
            corpus_manifest_sha256=str(
                bootstrap["corpus"]["corpusManifestSha256"]
            ),
            protocol_code_identifier=(
                "fresh-pipeline-v3:" + implementation["manifestSha256"]
            ),
            implementation_manifest=implementation,
            experiment_ledger_path=ledger,
            holdout_authorization_registry_path=holdout_registry,
            infrastructure_restart_provenance=restart_bundle.provenance,
        )
        _write_new_json(
            output / "fresh_preregistration_v3.json",
            preregistration,
        )
    else:
        if ledger.exists() and ledger.stat().st_size:
            raise PermissionError(
                "this frozen split already has a durable experiment ledger"
            )
        _write_new_json(output / "fresh_research_state_binding_v1.json", state_binding)
        implementation = build_fresh_implementation_manifest(
            repository_root=root,
            relative_paths=required_fresh_implementation_files(),
        )
        _write_new_json(
            output / "fresh_implementation_manifest_v1.json", implementation
        )
        preregistration = build_fresh_preregistration_v2(
            split_manifest=bootstrap["split"],
            corpus_manifest_sha256=str(bootstrap["corpus"]["corpusManifestSha256"]),
            protocol_code_identifier=(
                "fresh-pipeline-v1:" + implementation["manifestSha256"]
            ),
            implementation_manifest=implementation,
            experiment_ledger_path=ledger,
            holdout_authorization_registry_path=holdout_registry,
        )
        _write_new_json(output / "fresh_preregistration_v2.json", preregistration)

    pipeline = RegisteredFreshResearchPipeline(
        repository_root=root,
        output_directory=output,
        spool_directory=scratch,
        spool_maximum_bytes=FRESH_SPOOL_MAXIMUM_BYTES,
        connection_context_factory=connection_context_factory,
        corpus_manifest=bootstrap["corpus"],
        split_manifest=bootstrap["split"],
        preregistration=preregistration,
        progress=progress,
        verify_preregistration_implementation_files=not (
            recovery_used or recovery_v5_used
        ),
    )
    if recovery_used:
        if (
            recovery_bundle is None
            or recovery_manifest is None
            or recovery_equivalence_evidence is None
        ):
            raise RuntimeError("run-14 recovery identities were not frozen")
        pipeline.quantile_bank = fresh_quantile_bank_from_payload(
            recovery_bundle.quantile_bank
        )
        pipeline.threshold_preflight = dict(recovery_bundle.threshold_preflight)
        discovery_window = bootstrap["split"]["windows"]["discovery"]
        recovery_context = EvaluationContext(
            stage="discovery",
            training_roles=("discovery",),
            evaluation_roles=("discovery",),
            windows=(
                FrozenResearchWindow(
                    role="discovery",
                    session_anchors=tuple(discovery_window["sessionAnchors"]),
                    window_sha256=canonical_hash(discovery_window),
                ),
            ),
        )
        entry_specs = tuple(
            pipeline.build_entry_candidates(
                recovery_bundle.quantile_bank, recovery_context
            )
        )
        entry_bank_path = output / "fresh_entry_bank_v1.json"
        if _file_sha256(entry_bank_path) != RUN14_ENTRY_BANK_FILE_SHA256:
            raise PermissionError("reconstructed run-14 entry-bank bytes changed")
        recovery_audit, recovery_contract = build_run14_recovery_contract(
            recovery_bundle,
            entry_specs=entry_specs,
            recovery_implementation_manifest=recovery_manifest,
            generated_entry_bank_path=entry_bank_path,
            equivalence_evidence=recovery_equivalence_evidence,
        )
        _write_new_json(output / "fresh_recovery_contract_v1.json", recovery_contract)
        pipeline.recovery_contract = recovery_contract
        pipeline.recovery_implementation_manifest = recovery_manifest
        pipeline.recovery_batch_result_path = (
            output / "fresh_recovery_discovery_batch_v1.json"
        )
        search = pipeline.resume_incomplete_discovery_search(
            entry_specs=entry_specs,
            recovery_audit=recovery_audit,
            recovery_batch_result_path=(pipeline.recovery_batch_result_path),
        )
        operations = (
            search.resume_discovery,
            search.run_walk_forward_1,
            search.run_walk_forward_2,
            search.run_exit_search,
            search.run_walk_forward_3,
            search.run_validation,
        )
    elif recovery_v5_bundle is not None:
        if (
            recovery_manifest is None
            or recovery_equivalence_evidence is None
        ):
            raise RuntimeError("V5 recovery identities were not frozen")
        pipeline.quantile_bank = fresh_quantile_bank_from_payload(
            recovery_v5_bundle.quantile_bank
        )
        pipeline.threshold_preflight = dict(
            recovery_v5_bundle.threshold_preflight
        )
        discovery_window = bootstrap["split"]["windows"]["discovery"]
        recovery_context = EvaluationContext(
            stage="discovery",
            training_roles=("discovery",),
            evaluation_roles=("discovery",),
            windows=(
                FrozenResearchWindow(
                    role="discovery",
                    session_anchors=tuple(
                        discovery_window["sessionAnchors"]
                    ),
                    window_sha256=canonical_hash(discovery_window),
                ),
            ),
        )
        entry_specs = tuple(
            pipeline.build_entry_candidates(
                recovery_v5_bundle.quantile_bank,
                recovery_context,
            )
        )
        entry_bank_path = output / "fresh_entry_bank_v1.json"
        if _file_sha256(entry_bank_path) != V5_ENTRY_BANK_FILE_SHA256:
            raise PermissionError("reconstructed V5 entry-bank bytes changed")
        recovery_audit, recovery_contract = (
            build_fresh_v5_recovery_contract(
                recovery_v5_bundle,
                entry_specs=entry_specs,
                recovery_implementation_manifest=recovery_manifest,
                generated_entry_bank_path=entry_bank_path,
                equivalence_evidence=recovery_equivalence_evidence,
            )
        )
        _write_new_json(
            output / "fresh_recovery_contract_v1.json",
            recovery_contract,
        )
        pipeline.recovery_contract = recovery_contract
        pipeline.recovery_implementation_manifest = recovery_manifest
        pipeline.recovery_batch_result_path = (
            output / "fresh_recovery_discovery_batch_v1.json"
        )
        search = pipeline.resume_incomplete_discovery_search(
            entry_specs=entry_specs,
            recovery_audit=recovery_audit,
            recovery_batch_result_path=pipeline.recovery_batch_result_path,
        )
        operations = (
            search.resume_discovery,
            search.run_walk_forward_1,
            search.run_walk_forward_2,
            search.run_exit_search,
            search.run_walk_forward_3,
            search.run_validation,
        )
    elif restart_v5_bundle is not None:
        for source_name, destination_name in (
            ("fresh_quantile_bank_v1.json", "fresh_quantile_bank_v1.json"),
            (
                "fresh_threshold_domain_preflight_v1.json",
                "fresh_threshold_domain_preflight_v1.json",
            ),
        ):
            _snapshot_new_file(
                restart_v5_bundle.paths[source_name],
                output / destination_name,
            )
        pipeline.quantile_bank = fresh_quantile_bank_from_payload(
            restart_v5_bundle.quantile_bank
        )
        pipeline.threshold_preflight = dict(
            restart_v5_bundle.threshold_preflight
        )
        discovery_window = bootstrap["split"]["windows"]["discovery"]
        restart_context = EvaluationContext(
            stage="discovery",
            training_roles=("discovery",),
            evaluation_roles=("discovery",),
            windows=(
                FrozenResearchWindow(
                    role="discovery",
                    session_anchors=tuple(
                        discovery_window["sessionAnchors"]
                    ),
                    window_sha256=canonical_hash(discovery_window),
                ),
            ),
        )
        entry_specs = tuple(
            pipeline.build_entry_candidates(
                restart_v5_bundle.quantile_bank,
                restart_context,
            )
        )
        entry_bank_path = output / "fresh_entry_bank_v1.json"
        expected_entry_bank_sha = restart_v5_bundle.provenance[
            "reusedOutcomeBlindInputs"
        ]["fresh_entry_bank_v1.json"]
        if _file_sha256(entry_bank_path) != expected_entry_bank_sha:
            raise PermissionError("reconstructed v5 entry-bank bytes changed")
        search = pipeline.build_search()

        def run_v5_discovery() -> StageRunResult:
            return search.run_frozen_discovery(
                threshold_bank=restart_v5_bundle.quantile_bank,
                entry_specs=entry_specs,
            )

        operations = (
            run_v5_discovery,
            search.run_walk_forward_1,
            search.run_walk_forward_2,
            search.run_exit_search,
            search.run_walk_forward_3,
            search.run_validation,
        )
    elif restart_v4_bundle is not None:
        for source_name, destination_name in (
            ("fresh_quantile_bank_v1.json", "fresh_quantile_bank_v1.json"),
            (
                "fresh_threshold_domain_preflight_v1.json",
                "fresh_threshold_domain_preflight_v1.json",
            ),
        ):
            _snapshot_new_file(
                restart_v4_bundle.paths[source_name],
                output / destination_name,
            )
        pipeline.quantile_bank = fresh_quantile_bank_from_payload(
            restart_v4_bundle.quantile_bank
        )
        pipeline.threshold_preflight = dict(
            restart_v4_bundle.threshold_preflight
        )
        discovery_window = bootstrap["split"]["windows"]["discovery"]
        restart_context = EvaluationContext(
            stage="discovery",
            training_roles=("discovery",),
            evaluation_roles=("discovery",),
            windows=(
                FrozenResearchWindow(
                    role="discovery",
                    session_anchors=tuple(
                        discovery_window["sessionAnchors"]
                    ),
                    window_sha256=canonical_hash(discovery_window),
                ),
            ),
        )
        entry_specs = tuple(
            pipeline.build_entry_candidates(
                restart_v4_bundle.quantile_bank,
                restart_context,
            )
        )
        entry_bank_path = output / "fresh_entry_bank_v1.json"
        expected_entry_bank_sha = restart_v4_bundle.provenance[
            "reusedOutcomeBlindInputs"
        ]["fresh_entry_bank_v1.json"]
        if _file_sha256(entry_bank_path) != expected_entry_bank_sha:
            raise PermissionError("reconstructed v4 entry-bank bytes changed")
        search = pipeline.build_search()

        def run_v4_discovery() -> StageRunResult:
            return search.run_frozen_discovery(
                threshold_bank=restart_v4_bundle.quantile_bank,
                entry_specs=entry_specs,
            )

        operations = (
            run_v4_discovery,
            search.run_walk_forward_1,
            search.run_walk_forward_2,
            search.run_exit_search,
            search.run_walk_forward_3,
            search.run_validation,
        )
    elif restart_bundle is not None:
        for source_name, destination_name in (
            ("fresh_quantile_bank_v1.json", "fresh_quantile_bank_v1.json"),
            (
                "fresh_threshold_domain_preflight_v1.json",
                "fresh_threshold_domain_preflight_v1.json",
            ),
        ):
            _snapshot_new_file(
                restart_bundle.paths[source_name],
                output / destination_name,
            )
        pipeline.quantile_bank = fresh_quantile_bank_from_payload(
            restart_bundle.quantile_bank
        )
        pipeline.threshold_preflight = dict(
            restart_bundle.threshold_preflight
        )
        discovery_window = bootstrap["split"]["windows"]["discovery"]
        restart_context = EvaluationContext(
            stage="discovery",
            training_roles=("discovery",),
            evaluation_roles=("discovery",),
            windows=(
                FrozenResearchWindow(
                    role="discovery",
                    session_anchors=tuple(
                        discovery_window["sessionAnchors"]
                    ),
                    window_sha256=canonical_hash(discovery_window),
                ),
            ),
        )
        entry_specs = tuple(
            pipeline.build_entry_candidates(
                restart_bundle.quantile_bank,
                restart_context,
            )
        )
        entry_bank_path = output / "fresh_entry_bank_v1.json"
        expected_entry_bank_sha = restart_bundle.provenance[
            "reusedOutcomeBlindInputs"
        ]["fresh_entry_bank_v1.json"]
        if _file_sha256(entry_bank_path) != expected_entry_bank_sha:
            raise PermissionError(
                "reconstructed v3 entry-bank bytes changed"
            )
        search = pipeline.build_search()

        def run_v3_discovery() -> StageRunResult:
            return search.run_frozen_discovery(
                threshold_bank=restart_bundle.quantile_bank,
                entry_specs=entry_specs,
            )

        operations = (
            run_v3_discovery,
            search.run_walk_forward_1,
            search.run_walk_forward_2,
            search.run_exit_search,
            search.run_walk_forward_3,
            search.run_validation,
        )
    else:
        search = pipeline.build_search()
        operations = (
            search.run_discovery,
            search.run_walk_forward_1,
            search.run_walk_forward_2,
            search.run_exit_search,
            search.run_walk_forward_3,
            search.run_validation,
        )
    for operation in operations:
        result = operation()
        pipeline.stage_results.append(result)
        if progress is not None:
            progress(
                {
                    "stage": result.stage,
                    "evaluatedCount": len(result.evaluated_ids),
                    "promotedCount": len(result.promoted_ids),
                    "studyFailed": result.study_failed,
                }
            )
        if result.study_failed:
            break
    if pipeline.stage_results and pipeline.stage_results[-1].stage == "validation":
        if not pipeline.stage_results[-1].study_failed:
            search.authorize_holdout(explicit_holdout_authorization=True)
            holdout = search.run_holdout()
            pipeline.stage_results.append(holdout)
            if progress is not None:
                progress(
                    {
                        "stage": holdout.stage,
                        "evaluatedCount": len(holdout.evaluated_ids),
                        "promotedCount": len(holdout.promoted_ids),
                        "studyFailed": holdout.study_failed,
                    }
                )

    _snapshot_new_file(ledger, output / "fresh_experiment_ledger_v1.jsonl")
    if holdout_registry.is_file():
        _snapshot_new_file(
            holdout_registry,
            output / "fresh_holdout_authorization_v1.json",
        )
    records = search.audit_records
    strongest = _strongest_record(records)
    terminal_stage = pipeline.stage_results[-1] if pipeline.stage_results else None
    validated = bool(
        terminal_stage is not None
        and terminal_stage.stage == "holdout"
        and not terminal_stage.study_failed
    )
    summary = {
        "schema": FRESH_PIPELINE_SCHEMA,
        "status": (
            "validated_holdout_pass"
            if validated
            else "no_robust_setup_survived_frozen_validation"
        ),
        "preregistrationSha256": preregistration["preregistrationSha256"],
        "implementationManifestSha256": implementation["manifestSha256"],
        "recoveryUsed": recovery_used or recovery_v5_used,
        "recoveryVersion": (
            "v5-same-lineage"
            if recovery_v5_used
            else ("run14" if recovery_used else None)
        ),
        "recoveryOriginalRunId": (
            V5_ORIGINAL_GITHUB_RUN_ID
            if recovery_v5_used
            else (RUN14_RUN_ID if recovery_used else None)
        ),
        "recoveryImplementationManifestSha256": (
            recovery_manifest["manifestSha256"]
            if recovery_manifest is not None
            else None
        ),
        "infrastructureRestartUsed": (
            restart_used
            or restart_v4_used
            or restart_v5_used
            or recovery_v5_used
        ),
        "infrastructureRestartVersion": (
            5
            if (restart_v5_used or recovery_v5_used)
            else (4 if restart_v4_used else (3 if restart_used else None))
        ),
        "predecessorRunId": (
            recovery_v5_bundle.preregistration["infrastructureRestart"][
                "predecessorRunId"
            ]
            if recovery_v5_bundle is not None
            else (
                restart_v5_bundle.provenance["predecessorRunId"]
                if restart_v5_bundle is not None
                else (
                    restart_v4_bundle.provenance["predecessorRunId"]
                    if restart_v4_bundle is not None
                    else (
                        restart_bundle.provenance["predecessorRunId"]
                        if restart_bundle is not None
                        else None
                    )
                )
            )
        ),
        "studyId": state_binding["studyId"],
        "studyLineageSha256": state_binding.get("studyLineageSha256"),
        "splitManifestSha256": bootstrap["split"]["manifestSha256"],
        "corpusManifestSha256": bootstrap["corpus"]["corpusManifestSha256"],
        "holdoutOpened": any(
            item.stage == "holdout" for item in pipeline.stage_results
        ),
        "stageResults": [asdict(item) for item in pipeline.stage_results],
        "strongestRecord": dict(strongest) if strongest is not None else None,
        "artifactFiles": sorted(
            path.name for path in output.iterdir() if path.is_file()
        ),
    }
    _write_new_json(output / "fresh_run_summary_v1.json", summary)
    return summary


__all__ = [
    "BASELINE_BOOTSTRAP_REPLICATES",
    "BASELINE_CLUSTER_CONFIDENCE",
    "BASELINE_EVENTS_PER_SIDE_PER_SESSION",
    "BASELINE_MINIMUM_UPLIFT",
    "FRESH_PIPELINE_SCHEMA",
    "RegisteredFreshResearchPipeline",
    "run_registered_fresh_research",
]
