BEGIN;

CREATE SCHEMA IF NOT EXISTS research;

CREATE TABLE IF NOT EXISTS research.divergence_event (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES research.run (id) ON DELETE CASCADE,
    job_id BIGINT NOT NULL REFERENCES research.job (id) ON DELETE CASCADE,
    setup_fingerprint TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    brokerday DATE NOT NULL,
    symbol TEXT NOT NULL,
    tick_id BIGINT NOT NULL,
    event_timestamp TIMESTAMPTZ NOT NULL,
    event_family TEXT NOT NULL,
    event_subtype TEXT NOT NULL,
    indicator_name TEXT NOT NULL,
    side TEXT NOT NULL,
    signal_style TEXT NOT NULL,
    pivot_method TEXT NOT NULL,
    structure_pack TEXT NOT NULL,
    pivot_left_tick_id BIGINT,
    pivot_right_tick_id BIGINT,
    entry_price NUMERIC(18, 8) NOT NULL,
    price_value_1 NUMERIC(18, 8),
    price_value_2 NUMERIC(18, 8),
    indicator_value_1 NUMERIC(18, 8),
    indicator_value_2 NUMERIC(18, 8),
    indicator_payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    spread_at_event NUMERIC(18, 8) NOT NULL,
    target_amount NUMERIC(18, 8) NOT NULL,
    target_hit BOOLEAN NOT NULL,
    first_side_hit TEXT NOT NULL,
    hit_seconds NUMERIC(18, 6),
    hit_ticks INTEGER,
    max_adverse NUMERIC(18, 8) NOT NULL,
    max_favorable NUMERIC(18, 8) NOT NULL,
    session_bucket TEXT NOT NULL,
    scalp_qualified BOOLEAN NOT NULL,
    event_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS research_divergence_event_run_fingerprint_idx
    ON research.divergence_event (run_id, fingerprint);

CREATE INDEX IF NOT EXISTS research_divergence_event_run_setup_idx
    ON research.divergence_event (run_id, setup_fingerprint, event_timestamp DESC, id DESC);

CREATE INDEX IF NOT EXISTS research_divergence_event_brokerday_idx
    ON research.divergence_event (brokerday, event_timestamp DESC, id DESC);

COMMIT;
