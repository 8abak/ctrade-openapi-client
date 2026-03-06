-- flow60s signal engine state + signal tables

CREATE TABLE IF NOT EXISTS public.flow_state (
    id bigserial PRIMARY KEY,
    symbol text NOT NULL UNIQUE,
    last_tick_id bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT now(),
    prev_ts timestamptz,
    session_id bigint NOT NULL DEFAULT 0,
    session_start_ts timestamptz,
    session_sum_mid double precision NOT NULL DEFAULT 0,
    session_n bigint NOT NULL DEFAULT 0,
    atvwap double precision,
    win60_hi double precision,
    win60_lo double precision,
    range60 double precision,
    tick_count60 bigint NOT NULL DEFAULT 0,
    tick_rate60 double precision,
    ret60 double precision,
    ema_fast double precision,
    ema_slow double precision,
    ema15m double precision,
    ema1h double precision,
    ema1h_slope double precision,
    range_ewm_mean double precision,
    range_ewm_var double precision,
    tickrate_ewm_mean double precision,
    tickrate_ewm_var double precision,
    range_z double precision,
    tickrate_z double precision,
    state text,
    side text,
    impulse_high double precision,
    impulse_low double precision,
    cooldown_until timestamptz
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_flow_state_symbol
    ON public.flow_state(symbol);

CREATE TABLE IF NOT EXISTS public.flow_signals (
    id bigserial PRIMARY KEY,
    symbol text NOT NULL,
    tick_id bigint NOT NULL,
    timestamp timestamptz NOT NULL,
    side text NOT NULL,
    price double precision NOT NULL,
    atvwap double precision,
    range60 double precision,
    tick_rate60 double precision,
    range_z double precision,
    tickrate_z double precision,
    reason jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS ix_flow_signals_symbol_tickid_desc
    ON public.flow_signals(symbol, tick_id DESC);
