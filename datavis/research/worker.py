from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional

import psycopg2.extras
from psycopg2.extras import Json

from datavis.control.config import load_settings as load_control_settings
from datavis.control.panel import _setup_fingerprint
from datavis.control.panel_state import resolve_research_runtime
from datavis.research.config import ResearchSettings
from datavis.research.entry import execute_entry_research
from datavis.research.guardrails import SearchGuardrails, sanitize_parameters
from datavis.research.journal import ResearchJournal, write_run_artifacts
from datavis.research.models import JobRecord
from datavis.research.state import ensure_control_state, get_state
from datavis.separation import brokerday_for_timestamp


CONTROL_SETTINGS = load_control_settings()


class ResearchWorker:
    def __init__(self, settings: ResearchSettings) -> None:
        self._settings = settings
        self._journal = ResearchJournal(settings, "worker")
        self._limits = SearchGuardrails(
            max_slice_rows=settings.max_slice_rows,
            max_warmup_rows=settings.max_warmup_rows,
            max_slice_offset_rows=settings.max_slice_offset_rows,
            max_next_actions=settings.max_next_jobs,
        )

    def run_forever(self, conn_factory: Any) -> None:
        self._journal.write(level="INFO", event_type="worker.start", message="worker loop started")
        while True:
            with conn_factory(readonly=False, autocommit=False) as conn:
                did_work = self.run_once(conn)
                conn.commit()
            if not did_work:
                time.sleep(self._settings.worker_poll_seconds)

    def run_once(self, conn: Any) -> bool:
        runtime_policy = resolve_research_runtime(conn, CONTROL_SETTINGS, self._settings)
        if not runtime_policy.get("enabled", True):
            return False
        control = ensure_control_state(conn, self._settings)
        if control.get("paused") or control.get("stop_requested") or control.get("final_verdict"):
            return False
        job = self._claim_next_job(conn)
        if job is None:
            return False
        run_id = self._create_run(conn, job)
        self._journal.write(
            level="INFO",
            event_type="worker.job.claimed",
            message=f"claimed job {job.id}",
            job_id=job.id,
            run_id=run_id,
            conn=conn,
        )
        try:
            params = sanitize_parameters(job.config, limits=self._limits)
            if params.candidate_family == "divergence_sweep":
                self._journal.write(
                    level="INFO",
                    event_type="worker.job.divergence.start",
                    message=f"Running divergence_sweep on broker day {params.study_brokerday or 'latest available'}",
                    job_id=job.id,
                    run_id=run_id,
                    payload={
                        "family": params.candidate_family,
                        "studyBrokerday": params.study_brokerday,
                        "indicators": ["rsi14", "macd_line", "macd_hist", "roc8", "kal_gap"],
                    },
                    conn=conn,
                )
            result = execute_entry_research(conn, params=params, settings=self._settings)
            self._persist_result(conn, job=job, run_id=run_id, params=params.model_dump(), result=result)
            self._mark_job_completed(conn, job_id=job.id, run_id=run_id, summary=result["summaryPayload"])
            if params.candidate_family == "divergence_sweep":
                best_candidate = dict((result.get("summaryPayload") or {}).get("bestCandidate") or {})
                best_rule = dict(best_candidate.get("rule") or {})
                divergence_meta = dict(best_rule.get("divergence") or {})
                verdict_hint = str((result["summaryPayload"] or {}).get("verdictHint") or "no_robust_edge_found")
                if verdict_hint == "no_robust_edge_found":
                    completion_message = "Completed divergence_sweep: no robust divergence edge found"
                else:
                    completion_message = (
                        "Completed divergence_sweep: "
                        f"{divergence_meta.get('eventSubtype') or best_candidate.get('candidateName') or 'best candidate'} "
                        f"improved precision with {divergence_meta.get('indicator') or 'bounded indicators'}"
                    )
                self._journal.write(
                    level="INFO",
                    event_type="worker.job.divergence.completed",
                    message=completion_message,
                    job_id=job.id,
                    run_id=run_id,
                    payload={
                        "bestCandidate": best_candidate.get("candidateName"),
                        "eventSubtype": divergence_meta.get("eventSubtype"),
                        "indicator": divergence_meta.get("indicator"),
                        "signalStyle": divergence_meta.get("style"),
                        "verdictHint": verdict_hint,
                        "cleanPrecision": (best_candidate.get("validationMetrics") or {}).get("cleanPrecision"),
                    },
                    conn=conn,
                )
            self._journal.write(
                level="INFO",
                event_type="worker.job.completed",
                message=f"completed job {job.id}",
                job_id=job.id,
                run_id=run_id,
                payload={"caseCount": result["summaryPayload"]["caseCount"], "verdictHint": result["summaryPayload"]["verdictHint"]},
                conn=conn,
            )
        except Exception as exc:
            self._handle_failure(conn, job=job, run_id=run_id, error_text=str(exc))
            self._journal.write(
                level="ERROR",
                event_type="worker.job.failed",
                message=f"job {job.id} failed",
                job_id=job.id,
                run_id=run_id,
                payload={"error": str(exc)},
                conn=conn,
            )
        return True

    def _claim_next_job(self, conn: Any) -> Optional[JobRecord]:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, job_type, status, requested_by, config, attempt_count, max_attempts, parent_decision_id, parent_job_id
                FROM research.job
                WHERE status = 'pending'
                  AND scheduled_at <= NOW()
                ORDER BY priority ASC, scheduled_at ASC, id ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
                """
            )
            row = cur.fetchone()
            if not row:
                return None
            payload = dict(row)
            cur.execute(
                """
                UPDATE research.job
                SET status = 'running',
                    started_at = NOW(),
                    last_heartbeat_at = NOW(),
                    worker_name = %s,
                    attempt_count = attempt_count + 1,
                    error_text = NULL
                WHERE id = %s
                """,
                (self._settings.worker_name, int(payload["id"])),
            )
            payload["attempt_count"] = int(payload.get("attempt_count") or 0) + 1
        return JobRecord.model_validate(payload)

    def _create_run(self, conn: Any, job: JobRecord) -> int:
        params = sanitize_parameters(job.config, limits=self._limits)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO research.run (job_id, run_kind, status, symbol, iteration, config, started_at, worker_name)
                VALUES (%s, %s, %s, %s, %s, %s, NOW(), %s)
                RETURNING id
                """,
                (
                    job.id,
                    "entry_research",
                    "running",
                    params.symbol,
                    params.iteration,
                    Json(params.model_dump()),
                    self._settings.worker_name,
                ),
            )
            return int(cur.fetchone()[0])

    def _persist_result(
        self,
        conn: Any,
        *,
        job: JobRecord,
        run_id: int,
        params: Dict[str, Any],
        result: Dict[str, Any],
    ) -> None:
        summary_payload = dict(result["summaryPayload"])
        summary_payload["config"] = params
        label_rows = self._materialize_rows(result["labelRows"], run_id=run_id)
        feature_rows = self._materialize_rows(result["featureRows"], run_id=run_id)
        candidate_rows = self._materialize_rows(result["candidateRows"], run_id=run_id)
        divergence_event_rows = self._materialize_divergence_rows(
            result.get("divergenceEventRows") or [],
            run_id=run_id,
            job_id=job.id,
            symbol=str(params.get("symbol") or self._settings.symbol),
        )
        self._insert_feature_rows(conn, feature_rows)
        self._insert_label_rows(conn, label_rows)
        self._insert_candidate_rows(conn, candidate_rows)
        self._insert_divergence_rows(conn, divergence_event_rows)
        self._update_run_metadata(conn, run_id=run_id, slice_bounds=result["sliceBounds"], result=result)
        self._insert_run_summary(conn, run_id=run_id, summary_payload=summary_payload)
        artifact_paths = write_run_artifacts(self._settings, run_id=run_id, summary_payload=summary_payload)
        self._insert_artifacts(conn, run_id=run_id, artifact_paths=artifact_paths, job_id=job.id)

    def _materialize_rows(self, rows: Iterable[Dict[str, Any]], *, run_id: int) -> List[Dict[str, Any]]:
        materialized = []
        for row in rows:
            payload = dict(row)
            payload["runId"] = run_id
            materialized.append(payload)
        return materialized

    def _materialize_divergence_rows(
        self,
        rows: Iterable[Dict[str, Any]],
        *,
        run_id: int,
        job_id: int,
        symbol: str,
    ) -> List[Dict[str, Any]]:
        materialized = []
        for row in rows:
            payload = dict(row)
            payload["runId"] = run_id
            payload["jobId"] = job_id
            payload["symbol"] = symbol
            materialized.append(payload)
        return materialized

    def _insert_feature_rows(self, conn: Any, rows: List[Dict[str, Any]]) -> None:
        for batch in batched(rows, self._settings.write_batch_rows):
            payload = [
                (
                    row["runId"],
                    row["tickId"],
                    row["timestamp"],
                    row["sessionBucket"],
                    float(row["features"]["spread"]),
                    float(row["features"]["short_momentum"]),
                    float(row["features"]["short_acceleration"]),
                    float(row["features"]["recent_tick_imbalance"]),
                    float(row["features"]["burst_persistence"]),
                    float(row["features"]["micro_breakout"]),
                    float(row["features"]["breakout_failure"]),
                    float(row["features"]["pullback_depth"]),
                    float(row["features"]["distance_recent_high"]),
                    float(row["features"]["distance_recent_low"]),
                    float(row["features"]["flip_frequency"]),
                    Json(row["features"]),
                )
                for row in batch
            ]
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO research.feature_snapshot (
                        run_id, tick_id, tick_timestamp, session_bucket,
                        spread, short_momentum, short_acceleration, recent_tick_imbalance, burst_persistence,
                        micro_breakout, breakout_failure, pullback_depth, distance_recent_high,
                        distance_recent_low, flip_frequency, feature_json
                    )
                    VALUES %s
                    ON CONFLICT (run_id, tick_id)
                    DO NOTHING
                    """,
                    payload,
                    page_size=min(len(payload), self._settings.write_batch_rows),
                )
            time.sleep(self._settings.chunk_sleep_seconds)

    def _insert_label_rows(self, conn: Any, rows: List[Dict[str, Any]]) -> None:
        for batch in batched(rows, self._settings.write_batch_rows):
            payload = [
                (
                    row["runId"],
                    row["tickId"],
                    row["timestamp"],
                    row["sessionBucket"],
                    row["side"],
                    row["entryPrice"],
                    row["spreadAtEntry"],
                    row["targetPrice"],
                    row["targetMultiplier"],
                    row["adversePrice"],
                    row["adverseMultiplier"],
                    row["horizonTicks"],
                    row["horizonSeconds"],
                    row["hit2x"],
                    row["hitTicks"],
                    row["hitSeconds"],
                    row["maxFavorable"],
                    row["maxAdverse"],
                    row["adverseHit"],
                    row["targetBeforeAdverse"],
                )
                for row in batch
            ]
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO research.entry_label (
                        run_id, tick_id, tick_timestamp, session_bucket, side,
                        entry_price, spread_at_entry, target_price, target_multiplier, adverse_price,
                        adverse_multiplier, horizon_ticks, horizon_seconds, hit_2x, hit_ticks,
                        hit_seconds, max_favorable, max_adverse, adverse_hit, target_before_adverse
                    )
                    VALUES %s
                    ON CONFLICT (run_id, tick_id, side)
                    DO NOTHING
                    """,
                    payload,
                    page_size=min(len(payload), self._settings.write_batch_rows),
                )
            time.sleep(self._settings.chunk_sleep_seconds)

    def _insert_candidate_rows(self, conn: Any, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO research.candidate_result (
                    run_id, rank, candidate_name, family, side, is_selected, rule_json, train_metrics, validation_metrics, setup_fingerprint
                )
                VALUES %s
                """,
                [
                    (
                        row["runId"],
                        row["rank"],
                        row["candidateName"],
                        row["family"],
                        row["side"],
                        row["selected"],
                        Json(row["rule"]),
                        Json(row["trainMetrics"]),
                        Json(row["validationMetrics"]),
                        str(row.get("setupFingerprint") or _setup_fingerprint(row["rule"], fallback_name=str(row["candidateName"] or ""))),
                    )
                    for row in rows
                ],
                page_size=min(len(rows), self._settings.write_batch_rows),
            )

    def _insert_divergence_rows(self, conn: Any, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        for batch in batched(rows, self._settings.write_batch_rows):
            payload = [
                (
                    row["runId"],
                    row["jobId"],
                    row["setupFingerprint"],
                    row["fingerprint"],
                    row["brokerday"],
                    row["symbol"],
                    row["tickId"],
                    row["timestamp"],
                    row["eventFamily"],
                    row["eventSubtype"],
                    row["indicatorName"],
                    row["side"],
                    row["signalStyle"],
                    row["pivotMethod"],
                    row["structurePack"],
                    row.get("pivotLeftTickId"),
                    row.get("pivotRightTickId"),
                    row["entryPrice"],
                    row.get("priceValue1"),
                    row.get("priceValue2"),
                    row.get("indicatorValue1"),
                    row.get("indicatorValue2"),
                    Json(row.get("indicatorPayload") or {}),
                    row["spreadAtEvent"],
                    row["targetAmount"],
                    row["targetHit"],
                    row["firstSideHit"],
                    row.get("hitSeconds"),
                    row.get("hitTicks"),
                    row["maxAdverse"],
                    row["maxFavorable"],
                    row["sessionBucket"],
                    row["scalpQualified"],
                    Json(row.get("eventJson") or {}),
                )
                for row in batch
            ]
            with conn.cursor() as cur:
                psycopg2.extras.execute_values(
                    cur,
                    """
                    INSERT INTO research.divergence_event (
                        run_id, job_id, setup_fingerprint, fingerprint, brokerday, symbol, tick_id, event_timestamp,
                        event_family, event_subtype, indicator_name, side, signal_style, pivot_method, structure_pack,
                        pivot_left_tick_id, pivot_right_tick_id, entry_price, price_value_1, price_value_2,
                        indicator_value_1, indicator_value_2, indicator_payload, spread_at_event, target_amount,
                        target_hit, first_side_hit, hit_seconds, hit_ticks, max_adverse, max_favorable,
                        session_bucket, scalp_qualified, event_json
                    )
                    VALUES %s
                    ON CONFLICT (run_id, fingerprint)
                    DO NOTHING
                    """,
                    payload,
                    page_size=min(len(payload), self._settings.write_batch_rows),
                )
            time.sleep(self._settings.chunk_sleep_seconds)

    def _update_run_metadata(self, conn: Any, *, run_id: int, slice_bounds: Dict[str, Any], result: Dict[str, Any]) -> None:
        brokerday = slice_bounds.get("study_brokerday")
        cases = list(result.get("cases") or [])
        if cases and not brokerday:
            brokerday = brokerday_for_timestamp(cases[-1]["timestamp"])
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE research.run
                SET slice_start_tick_id = %s,
                    slice_end_tick_id = %s,
                    brokerday = COALESCE(%s, brokerday)
                WHERE id = %s
                """,
                (slice_bounds["start_tick_id"], slice_bounds["end_tick_id"], brokerday, run_id),
            )

    def _insert_run_summary(self, conn: Any, *, run_id: int, summary_payload: Dict[str, Any]) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO research.runsummary (
                    run_id, verdict_hint, headline, metrics_json, briefing_json,
                    top_candidates_json, positive_examples_json, false_positive_examples_json
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    run_id,
                    summary_payload["verdictHint"],
                    summary_payload["headline"],
                    Json((summary_payload.get("bestCandidate") or {}).get("validationMetrics") or {}),
                    Json(summary_payload.get("briefing") or {}),
                    Json(summary_payload.get("candidateResults") or []),
                    Json((summary_payload.get("bestCandidate") or {}).get("positiveExamples") or []),
                    Json((summary_payload.get("bestCandidate") or {}).get("falsePositiveExamples") or []),
                ),
            )

    def _insert_artifacts(self, conn: Any, *, run_id: int, artifact_paths: Dict[str, str], job_id: int) -> None:
        with conn.cursor() as cur:
            psycopg2.extras.execute_values(
                cur,
                """
                INSERT INTO research.artifact (run_id, artifact_type, path, metadata)
                VALUES %s
                """,
                [
                    (run_id, artifact_type, path, Json({"jobId": job_id}))
                    for artifact_type, path in artifact_paths.items()
                ],
                page_size=len(artifact_paths),
            )

    def _mark_job_completed(self, conn: Any, *, job_id: int, run_id: int, summary: Dict[str, Any]) -> None:
        control = get_state(conn, "entry_loop_control")
        control["iterations_completed"] = int(control.get("iterations_completed") or 0) + 1
        control["last_run_id"] = run_id
        control["last_selected_fingerprint"] = (summary.get("config") or {}).get("config_fingerprint")
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE research.job
                SET status = 'completed',
                    run_id = %s,
                    finished_at = NOW(),
                    last_heartbeat_at = NOW()
                WHERE id = %s
                """,
                (run_id, job_id),
            )
            cur.execute(
                """
                UPDATE research.run
                SET status = 'completed',
                    finished_at = NOW()
                WHERE id = %s
                """,
                (run_id,),
            )
            cur.execute(
                """
                INSERT INTO research.state (key, value)
                VALUES (%s, %s)
                ON CONFLICT (key)
                DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
                """,
                ("entry_loop_control", Json(control)),
            )

    def _handle_failure(self, conn: Any, *, job: JobRecord, run_id: int, error_text: str) -> None:
        should_retry = int(job.attempt_count or 0) < int(job.max_attempts or 1)
        next_status = "pending" if should_retry else "failed"
        scheduled_at = datetime.now(timezone.utc) + timedelta(seconds=(15 * max(1, int(job.attempt_count or 0))))
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE research.job
                SET status = %s,
                    finished_at = CASE WHEN %s = 'failed' THEN NOW() ELSE finished_at END,
                    last_heartbeat_at = NOW(),
                    error_text = %s,
                    scheduled_at = CASE WHEN %s = 'pending' THEN %s ELSE scheduled_at END
                WHERE id = %s
                """,
                (next_status, next_status, error_text[:4000], next_status, scheduled_at, job.id),
            )
            cur.execute(
                """
                UPDATE research.run
                SET status = 'failed',
                    finished_at = NOW()
                WHERE id = %s
                """,
                (run_id,),
            )


def batched(items: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    for start in range(0, len(items), max(1, size)):
        yield items[start:start + max(1, size)]
