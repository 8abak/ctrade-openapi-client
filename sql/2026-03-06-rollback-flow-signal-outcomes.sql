-- rollback outcomes journal for flow_signals backtest evaluation

DROP INDEX IF EXISTS public.ix_flow_signal_outcomes_symbol_entry_tick_desc;
DROP INDEX IF EXISTS public.ix_flow_signal_outcomes_symbol_entry_ts_desc;
DROP INDEX IF EXISTS public.ux_flow_signal_outcomes_symbol_signal;
DROP TABLE IF EXISTS public.flow_signal_outcomes;
