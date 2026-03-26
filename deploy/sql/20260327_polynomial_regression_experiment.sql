CREATE TABLE IF NOT EXISTS public.regression_poly_metric (
    snapshot_id BIGINT NOT NULL REFERENCES public.regression_snapshot(id) ON DELETE CASCADE,
    fast_poly_order INTEGER NOT NULL CHECK (fast_poly_order >= 1),
    slow_poly_order INTEGER NOT NULL CHECK (slow_poly_order >= 1),
    fast_current_fitted_value DOUBLE PRECISION,
    fast_end_slope DOUBLE PRECISION,
    fast_end_curvature DOUBLE PRECISION,
    fast_distance_norm DOUBLE PRECISION,
    fast_r2 DOUBLE PRECISION,
    fast_mae DOUBLE PRECISION,
    fast_residual_std DOUBLE PRECISION,
    fast_sse DOUBLE PRECISION,
    slow_current_fitted_value DOUBLE PRECISION,
    slow_end_slope DOUBLE PRECISION,
    slow_end_curvature DOUBLE PRECISION,
    slow_distance_norm DOUBLE PRECISION,
    slow_r2 DOUBLE PRECISION,
    slow_mae DOUBLE PRECISION,
    slow_residual_std DOUBLE PRECISION,
    slow_sse DOUBLE PRECISION,
    slope_agreement_state TEXT,
    curvature_agreement_state TEXT,
    fast_slow_fit_spread DOUBLE PRECISION,
    fast_slow_slope_spread DOUBLE PRECISION,
    residual_compression_ratio DOUBLE PRECISION,
    residual_regime_state TEXT,
    aligned_with_both BOOLEAN,
    stretch_state TEXT,
    move_direction TEXT,
    move_quality_score DOUBLE PRECISION,
    move_quality_state TEXT,
    signal_candidate BOOLEAN NOT NULL DEFAULT FALSE,
    signal_threshold DOUBLE PRECISION,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (snapshot_id, fast_poly_order, slow_poly_order)
);

CREATE TABLE IF NOT EXISTS public.regression_poly_signal (
    id BIGSERIAL PRIMARY KEY,
    snapshot_id BIGINT NOT NULL REFERENCES public.regression_snapshot(id) ON DELETE CASCADE,
    symbol TEXT NOT NULL,
    mode TEXT NOT NULL CHECK (mode IN ('live', 'review')),
    series TEXT NOT NULL CHECK (series IN ('ask', 'bid', 'mid')),
    signal_tick_id BIGINT NOT NULL,
    signal_timestamp TIMESTAMPTZ,
    signal_price DOUBLE PRECISION NOT NULL,
    direction_guess TEXT NOT NULL,
    fast_window_ticks INTEGER NOT NULL CHECK (fast_window_ticks > 0),
    slow_window_ticks INTEGER NOT NULL CHECK (slow_window_ticks > 0),
    fast_poly_order INTEGER NOT NULL CHECK (fast_poly_order >= 1),
    slow_poly_order INTEGER NOT NULL CHECK (slow_poly_order >= 1),
    fast_slope DOUBLE PRECISION,
    slow_slope DOUBLE PRECISION,
    fast_curvature DOUBLE PRECISION,
    slow_curvature DOUBLE PRECISION,
    fast_distance_norm DOUBLE PRECISION,
    slow_distance_norm DOUBLE PRECISION,
    fit_spread DOUBLE PRECISION,
    slope_spread DOUBLE PRECISION,
    residual_regime_state TEXT,
    move_quality_score DOUBLE PRECISION,
    move_quality_state TEXT,
    score_threshold DOUBLE PRECISION,
    aligned_with_both BOOLEAN,
    outcome_state TEXT,
    outcome_ticks INTEGER,
    outcome_price_delta DOUBLE PRECISION,
    max_favorable_excursion DOUBLE PRECISION,
    max_adverse_excursion DOUBLE PRECISION,
    feature_payload JSONB,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT regression_poly_signal_identity_uk
        UNIQUE (snapshot_id, fast_poly_order, slow_poly_order, signal_tick_id, score_threshold)
);

CREATE INDEX IF NOT EXISTS regression_poly_metric_score_idx
    ON public.regression_poly_metric (move_quality_score DESC, created_at DESC);

CREATE INDEX IF NOT EXISTS regression_poly_signal_tick_idx
    ON public.regression_poly_signal (signal_tick_id DESC);

CREATE INDEX IF NOT EXISTS regression_poly_signal_created_idx
    ON public.regression_poly_signal (created_at DESC);
