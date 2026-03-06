-- outcomes journal for flow_signals backtest evaluation

CREATE TABLE IF NOT EXISTS public.flow_signal_outcomes (
    id bigserial PRIMARY KEY,
    symbol text NOT NULL,
    signal_id bigint NOT NULL,
    entry_tick_id bigint NOT NULL,
    entry_ts timestamptz NOT NULL,
    side text NOT NULL,
    entry_px double precision NOT NULL,
    exit_tick_id bigint,
    exit_ts timestamptz,
    exit_px double precision,
    outcome text NOT NULL,
    pnl integer NOT NULL,
    seconds_to_close double precision,
    tp_px double precision NOT NULL,
    sl_px double precision NOT NULL,
    max_hold_seconds integer NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_flow_signal_outcomes_symbol_signal
    ON public.flow_signal_outcomes(symbol, signal_id);

CREATE INDEX IF NOT EXISTS ix_flow_signal_outcomes_symbol_entry_ts_desc
    ON public.flow_signal_outcomes(symbol, entry_ts DESC);

CREATE INDEX IF NOT EXISTS ix_flow_signal_outcomes_symbol_entry_tick_desc
    ON public.flow_signal_outcomes(symbol, entry_tick_id DESC);
