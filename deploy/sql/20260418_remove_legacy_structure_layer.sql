BEGIN;

DROP TABLE IF EXISTS public.supresareaevent;
DROP TABLE IF EXISTS public.supresarea;
DROP TABLE IF EXISTS public.supresstate;

DROP TABLE IF EXISTS public.zoneboxstate;
DROP TABLE IF EXISTS public.zonebox;

DROP TABLE IF EXISTS public.fast_zig_pivots;
DROP TABLE IF EXISTS public.fast_zig_state;

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

CREATE INDEX IF NOT EXISTS auctionhistorysession_symbol_endts_startts_idx
    ON public.auctionhistorysession (symbol, endts DESC, startts DESC);

COMMIT;
