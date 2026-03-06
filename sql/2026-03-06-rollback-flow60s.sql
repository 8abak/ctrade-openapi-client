-- rollback flow60s signal engine tables

DROP INDEX IF EXISTS public.ix_flow_signals_symbol_tickid_desc;
DROP TABLE IF EXISTS public.flow_signals;

DROP INDEX IF EXISTS public.ux_flow_state_symbol;
DROP TABLE IF EXISTS public.flow_state;
