-- backtest daily summary table
-- one row per (symbol, trading_day)

CREATE TABLE IF NOT EXISTS public.backtest (
    id bigserial PRIMARY KEY,
    trading_day date NOT NULL,
    session_start_ts timestamptz NOT NULL,
    session_end_ts timestamptz NOT NULL,
    symbol text NOT NULL,
    config jsonb NOT NULL,
    trades_count int NOT NULL,
    wins_count int NOT NULL,
    losses_count int NOT NULL,
    win_rate numeric NOT NULL,
    total_profit numeric NOT NULL,
    avg_hold_sec numeric NOT NULL,
    max_hold_sec int NOT NULL,
    stopouts_count int NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    notes text
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_backtest_symbol_day
    ON public.backtest(symbol, trading_day);

CREATE INDEX IF NOT EXISTS ix_backtest_trading_day
    ON public.backtest(trading_day);
