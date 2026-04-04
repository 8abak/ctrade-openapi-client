BEGIN;

CREATE TABLE IF NOT EXISTS public.zonebox (
    id bigserial PRIMARY KEY,
    symbol text NOT NULL,
    level integer NOT NULL,
    state text NOT NULL DEFAULT 'provisional',
    pattern text NOT NULL,
    pricesource text NOT NULL DEFAULT 'mid',
    startpivotid bigint NOT NULL,
    middlepivotid bigint NOT NULL,
    endpivotid bigint NOT NULL,
    startpivottickid bigint NOT NULL,
    middlepivottickid bigint NOT NULL,
    endpivottickid bigint NOT NULL,
    startpivottime timestamptz NOT NULL,
    middlepivottime timestamptz NOT NULL,
    endpivottime timestamptz NOT NULL,
    startpivotprice double precision NOT NULL,
    middlepivotprice double precision NOT NULL,
    endpivotprice double precision NOT NULL,
    starttickid bigint NOT NULL,
    endtickid bigint NULL,
    starttime timestamptz NOT NULL,
    endtime timestamptz NULL,
    initialzonehigh double precision NOT NULL,
    initialzonelow double precision NOT NULL,
    zonehigh double precision NOT NULL,
    zonelow double precision NOT NULL,
    zoneheight double precision NOT NULL,
    samesidedistance double precision NOT NULL,
    samesidetoleranceused double precision NOT NULL,
    tickcountinside bigint NOT NULL DEFAULT 0,
    durationms bigint NOT NULL DEFAULT 0,
    continuationovershootused double precision NOT NULL,
    breakticksused integer NOT NULL,
    breaktoleranceused double precision NOT NULL,
    breakdirection text NULL,
    breaktickid bigint NULL,
    lasttickid bigint NOT NULL,
    lasttime timestamptz NOT NULL,
    lastinsidetickid bigint NOT NULL,
    lastinsidetime timestamptz NOT NULL,
    touchcount integer NOT NULL DEFAULT 0,
    revisitcount integer NOT NULL DEFAULT 0,
    lasttouchside text NULL,
    outsidestreak integer NOT NULL DEFAULT 0,
    outsidedirection text NULL,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    CONSTRAINT zonebox_level_check CHECK (level >= 0),
    CONSTRAINT zonebox_state_check CHECK (state IN ('provisional', 'active', 'closed')),
    CONSTRAINT zonebox_pattern_check CHECK (pattern IN ('H-L-H', 'L-H-L')),
    CONSTRAINT zonebox_breakdirection_check CHECK (breakdirection IS NULL OR breakdirection IN ('up', 'down')),
    CONSTRAINT zonebox_lasttouchside_check CHECK (lasttouchside IS NULL OR lasttouchside IN ('low', 'high')),
    CONSTRAINT zonebox_outsidedirection_check CHECK (outsidedirection IS NULL OR outsidedirection IN ('up', 'down'))
);

CREATE TABLE IF NOT EXISTS public.zoneboxstate (
    id bigserial PRIMARY KEY,
    symbol text NOT NULL,
    level integer NOT NULL,
    lastprocessedtickid bigint NOT NULL DEFAULT 0,
    lastprocessedpivotid bigint NOT NULL DEFAULT 0,
    activezoneid bigint NULL,
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    CONSTRAINT zoneboxstate_level_check CHECK (level >= 0),
    CONSTRAINT zoneboxstate_symbol_level_unique UNIQUE (symbol, level),
    CONSTRAINT zoneboxstate_activezone_fk FOREIGN KEY (activezoneid)
        REFERENCES public.zonebox (id)
        ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS zonebox_symbol_level_state_idx
    ON public.zonebox (symbol, level, state, id DESC);

CREATE INDEX IF NOT EXISTS zonebox_symbol_level_starttime_idx
    ON public.zonebox (symbol, level, starttime DESC);

CREATE INDEX IF NOT EXISTS zonebox_symbol_level_endtime_idx
    ON public.zonebox (symbol, level, endtime DESC);

CREATE INDEX IF NOT EXISTS zonebox_symbol_level_lasttickid_idx
    ON public.zonebox (symbol, level, lasttickid DESC);

CREATE INDEX IF NOT EXISTS zonebox_symbol_level_active_idx
    ON public.zonebox (symbol, level, id DESC)
    WHERE state IN ('provisional', 'active');

CREATE INDEX IF NOT EXISTS zonebox_symbol_level_starttickid_idx
    ON public.zonebox (symbol, level, starttickid DESC);

CREATE INDEX IF NOT EXISTS zoneboxstate_activezoneid_idx
    ON public.zoneboxstate (activezoneid)
    WHERE activezoneid IS NOT NULL;

COMMIT;
