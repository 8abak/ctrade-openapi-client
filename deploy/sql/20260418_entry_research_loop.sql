BEGIN;

CREATE SCHEMA IF NOT EXISTS research;

CREATE TABLE IF NOT EXISTS research.job (
    id BIGSERIAL PRIMARY KEY,
    job_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    priority INTEGER NOT NULL DEFAULT 100,
    requested_by TEXT NOT NULL,
    run_id BIGINT,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    guardrails JSONB NOT NULL DEFAULT '{}'::jsonb,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    max_attempts INTEGER NOT NULL DEFAULT 1,
    parent_decision_id BIGINT,
    parent_job_id BIGINT,
    worker_name TEXT,
    error_text TEXT,
    scheduled_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    last_heartbeat_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS research.run (
    id BIGSERIAL PRIMARY KEY,
    job_id BIGINT NOT NULL REFERENCES research.job (id),
    run_kind TEXT NOT NULL,
    status TEXT NOT NULL,
    symbol TEXT NOT NULL,
    iteration INTEGER NOT NULL DEFAULT 1,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    slice_start_tick_id BIGINT,
    slice_end_tick_id BIGINT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    worker_name TEXT
);

CREATE TABLE IF NOT EXISTS research.runsummary (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES research.run (id) ON DELETE CASCADE,
    verdict_hint TEXT,
    headline TEXT,
    metrics_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    briefing_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    top_candidates_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    positive_examples_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    false_positive_examples_json JSONB NOT NULL DEFAULT '[]'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS research.decision (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES research.run (id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'pending',
    orchestrator_name TEXT,
    supervisor_name TEXT,
    briefing JSONB NOT NULL DEFAULT '{}'::jsonb,
    raw_response TEXT,
    decision_json JSONB,
    decision TEXT,
    reason TEXT,
    stop_reason TEXT,
    validation_error TEXT,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    applied_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS research.artifact (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT REFERENCES research.run (id) ON DELETE CASCADE,
    decision_id BIGINT REFERENCES research.decision (id) ON DELETE CASCADE,
    artifact_type TEXT NOT NULL,
    path TEXT NOT NULL,
    checksum TEXT,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS research.state (
    key TEXT PRIMARY KEY,
    value JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS research.journal (
    id BIGSERIAL PRIMARY KEY,
    component TEXT NOT NULL,
    level TEXT NOT NULL,
    event_type TEXT NOT NULL,
    job_id BIGINT,
    run_id BIGINT,
    decision_id BIGINT,
    message TEXT NOT NULL,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS research.entry_label (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES research.run (id) ON DELETE CASCADE,
    tick_id BIGINT NOT NULL,
    tick_timestamp TIMESTAMPTZ NOT NULL,
    session_bucket TEXT NOT NULL,
    side TEXT NOT NULL,
    entry_price NUMERIC(18, 8) NOT NULL,
    spread_at_entry NUMERIC(18, 8) NOT NULL,
    target_price NUMERIC(18, 8) NOT NULL,
    target_multiplier NUMERIC(10, 4) NOT NULL,
    adverse_price NUMERIC(18, 8) NOT NULL,
    adverse_multiplier NUMERIC(10, 4) NOT NULL,
    horizon_ticks INTEGER NOT NULL,
    horizon_seconds INTEGER NOT NULL,
    hit_2x BOOLEAN NOT NULL,
    hit_ticks INTEGER,
    hit_seconds NUMERIC(18, 6),
    max_favorable NUMERIC(18, 8) NOT NULL,
    max_adverse NUMERIC(18, 8) NOT NULL,
    adverse_hit BOOLEAN NOT NULL,
    target_before_adverse BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS research.feature_snapshot (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES research.run (id) ON DELETE CASCADE,
    tick_id BIGINT NOT NULL,
    tick_timestamp TIMESTAMPTZ NOT NULL,
    session_bucket TEXT NOT NULL,
    spread NUMERIC(18, 8) NOT NULL,
    short_momentum NUMERIC(18, 8) NOT NULL,
    short_acceleration NUMERIC(18, 8) NOT NULL,
    recent_tick_imbalance NUMERIC(18, 8) NOT NULL,
    burst_persistence NUMERIC(18, 8) NOT NULL,
    micro_breakout NUMERIC(18, 8) NOT NULL,
    breakout_failure NUMERIC(18, 8) NOT NULL,
    pullback_depth NUMERIC(18, 8) NOT NULL,
    distance_recent_high NUMERIC(18, 8) NOT NULL,
    distance_recent_low NUMERIC(18, 8) NOT NULL,
    flip_frequency NUMERIC(18, 8) NOT NULL,
    feature_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS research.candidate_result (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES research.run (id) ON DELETE CASCADE,
    rank INTEGER NOT NULL,
    candidate_name TEXT NOT NULL,
    family TEXT NOT NULL,
    side TEXT NOT NULL,
    is_selected BOOLEAN NOT NULL DEFAULT FALSE,
    rule_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    train_metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    validation_metrics JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS research_job_single_running_idx
    ON research.job ((status))
    WHERE status = 'running';

CREATE INDEX IF NOT EXISTS research_job_status_schedule_idx
    ON research.job (status, scheduled_at, priority, id);

CREATE INDEX IF NOT EXISTS research_run_status_started_idx
    ON research.run (status, started_at DESC, id DESC);

CREATE UNIQUE INDEX IF NOT EXISTS research_runsummary_run_idx
    ON research.runsummary (run_id);

CREATE INDEX IF NOT EXISTS research_decision_status_requested_idx
    ON research.decision (status, requested_at, id);

CREATE INDEX IF NOT EXISTS research_artifact_run_idx
    ON research.artifact (run_id, created_at DESC);

CREATE INDEX IF NOT EXISTS research_journal_component_created_idx
    ON research.journal (component, created_at DESC);

CREATE INDEX IF NOT EXISTS research_journal_run_idx
    ON research.journal (run_id, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS research_entry_label_run_tick_side_idx
    ON research.entry_label (run_id, tick_id, side);

CREATE INDEX IF NOT EXISTS research_entry_label_run_side_idx
    ON research.entry_label (run_id, side, tick_id);

CREATE UNIQUE INDEX IF NOT EXISTS research_feature_snapshot_run_tick_idx
    ON research.feature_snapshot (run_id, tick_id);

CREATE INDEX IF NOT EXISTS research_candidate_result_run_rank_idx
    ON research.candidate_result (run_id, rank);

INSERT INTO research.state (key, value)
VALUES ('entry_loop_control', '{"paused": false, "stop_requested": false}'::jsonb)
ON CONFLICT (key) DO NOTHING;

CREATE OR REPLACE VIEW research.vw_loop_status AS
SELECT
    s.key,
    s.value,
    s.updated_at,
    (
        SELECT jsonb_build_object(
            'id', j.id,
            'status', j.status,
            'jobType', j.job_type,
            'scheduledAt', j.scheduled_at,
            'startedAt', j.started_at,
            'finishedAt', j.finished_at
        )
        FROM research.job j
        ORDER BY j.id DESC
        LIMIT 1
    ) AS latest_job,
    (
        SELECT jsonb_build_object(
            'id', r.id,
            'status', r.status,
            'symbol', r.symbol,
            'iteration', r.iteration,
            'startedAt', r.started_at,
            'finishedAt', r.finished_at
        )
        FROM research.run r
        ORDER BY r.id DESC
        LIMIT 1
    ) AS latest_run,
    (
        SELECT jsonb_build_object(
            'id', d.id,
            'status', d.status,
            'decision', d.decision,
            'stopReason', d.stop_reason,
            'requestedAt', d.requested_at,
            'completedAt', d.completed_at
        )
        FROM research.decision d
        ORDER BY d.id DESC
        LIMIT 1
    ) AS latest_decision
FROM research.state s
WHERE s.key = 'entry_loop_control';

COMMIT;
