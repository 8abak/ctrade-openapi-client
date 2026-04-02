BEGIN;

DROP TABLE IF EXISTS public.marketprofilebin;
DROP TABLE IF EXISTS public.marketprofile;
DROP TABLE IF EXISTS public.marketprofilestate;

DROP TABLE IF EXISTS public.envelopezigpoint;
DROP TABLE IF EXISTS public.envelopezigstate;

DROP TABLE IF EXISTS public.zigmicro;
DROP TABLE IF EXISTS public.zigmed;
DROP TABLE IF EXISTS public.zigmaxi;
DROP TABLE IF EXISTS public.zigmacro;
DROP TABLE IF EXISTS public.zigstate;

DROP TABLE IF EXISTS public.envelopetick;
DROP TABLE IF EXISTS public.envelopejobstate;

DROP TABLE IF EXISTS public.ottbacktesttrade;
DROP TABLE IF EXISTS public.ottbacktestrun;
DROP TABLE IF EXISTS public.otttick;
DROP TABLE IF EXISTS public.ottjobstate;

CREATE INDEX IF NOT EXISTS ticks_symbol_id_idx
    ON public.ticks (symbol, id DESC);

CREATE INDEX IF NOT EXISTS ticks_symbol_timestamp_id_idx
    ON public.ticks (symbol, timestamp DESC, id DESC);

COMMIT;
