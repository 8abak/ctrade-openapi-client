CREATE TABLE IF NOT EXISTS public.regression_snapshot (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('live', 'review')),
    series TEXT NOT NULL CHECK (series IN ('ask', 'bid', 'mid')),
    source_start_tick_id BIGINT NOT NULL,
    source_end_tick_id BIGINT NOT NULL,
    source_start_ts TIMESTAMPTZ,
    source_end_ts TIMESTAMPTZ,
    visible_window_ticks INTEGER NOT NULL CHECK (visible_window_ticks > 0),
    row_count INTEGER NOT NULL CHECK (row_count >= 0),
    fast_window_ticks INTEGER NOT NULL CHECK (fast_window_ticks > 0),
    slow_window_ticks INTEGER NOT NULL CHECK (slow_window_ticks > 0),
    advanced_from_tick_id BIGINT,
    new_row_count INTEGER NOT NULL DEFAULT 0 CHECK (new_row_count >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT regression_snapshot_identity_uk
        UNIQUE (symbol, mode, series, source_end_tick_id, visible_window_ticks, fast_window_ticks, slow_window_ticks)
);

CREATE TABLE IF NOT EXISTS public.regression_metric (
    snapshot_id BIGINT PRIMARY KEY REFERENCES public.regression_snapshot(id) ON DELETE CASCADE,
    fast_slope DOUBLE PRECISION,
    fast_intercept DOUBLE PRECISION,
    fast_angle_deg DOUBLE PRECISION,
    fast_r2 DOUBLE PRECISION,
    fast_mae DOUBLE PRECISION,
    fast_residual_std DOUBLE PRECISION,
    fast_sse DOUBLE PRECISION,
    fast_start_tick_id BIGINT,
    fast_end_tick_id BIGINT,
    fast_price_change DOUBLE PRECISION,
    fast_duration_ms BIGINT,
    fast_tick_count INTEGER,
    fast_efficiency DOUBLE PRECISION,
    slow_slope DOUBLE PRECISION,
    slow_intercept DOUBLE PRECISION,
    slow_angle_deg DOUBLE PRECISION,
    slow_r2 DOUBLE PRECISION,
    slow_mae DOUBLE PRECISION,
    slow_residual_std DOUBLE PRECISION,
    slow_sse DOUBLE PRECISION,
    slow_start_tick_id BIGINT,
    slow_end_tick_id BIGINT,
    slow_price_change DOUBLE PRECISION,
    slow_duration_ms BIGINT,
    slow_tick_count INTEGER,
    slow_efficiency DOUBLE PRECISION,
    slope_difference DOUBLE PRECISION,
    slope_ratio DOUBLE PRECISION,
    angle_difference_deg DOUBLE PRECISION,
    current_fast_slow_distance DOUBLE PRECISION,
    alignment_state TEXT,
    directional_agreement INTEGER,
    fast_acceleration DOUBLE PRECISION,
    fast_accelerating BOOLEAN,
    fast_dominance_ratio DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.regression_break_pressure (
    snapshot_id BIGINT PRIMARY KEY REFERENCES public.regression_snapshot(id) ON DELETE CASCADE,
    recent_residual_window_ticks INTEGER,
    recent_residual_sign_imbalance DOUBLE PRECISION,
    recent_residual_run_length INTEGER,
    recent_positive_residual_ratio DOUBLE PRECISION,
    recent_negative_residual_ratio DOUBLE PRECISION,
    slow_fit_deterioration DOUBLE PRECISION,
    slow_fit_deterioration_pct DOUBLE PRECISION,
    fast_slow_disagreement_score DOUBLE PRECISION,
    best_candidate_split_tick_id BIGINT,
    best_two_line_improvement_pct DOUBLE PRECISION,
    best_two_line_left_sse DOUBLE PRECISION,
    best_two_line_right_sse DOUBLE PRECISION,
    best_two_line_total_sse DOUBLE PRECISION,
    break_pressure_score DOUBLE PRECISION,
    pressure_state TEXT,
    confidence_state TEXT,
    split_probe_window_ticks INTEGER,
    split_probe_min_segment_ticks INTEGER,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS regression_snapshot_symbol_mode_created_idx
    ON public.regression_snapshot (symbol, mode, created_at DESC);

CREATE INDEX IF NOT EXISTS regression_snapshot_source_end_tick_idx
    ON public.regression_snapshot (source_end_tick_id DESC);

CREATE INDEX IF NOT EXISTS regression_break_pressure_split_tick_idx
    ON public.regression_break_pressure (best_candidate_split_tick_id);

