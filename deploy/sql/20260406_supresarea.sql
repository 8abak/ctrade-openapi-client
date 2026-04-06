BEGIN;

CREATE TABLE IF NOT EXISTS public.supresstate (
    id bigserial PRIMARY KEY,
    symbol text NOT NULL,
    lastprocessedtickid bigint NOT NULL DEFAULT 0,
    lastprocessedpivotid bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    CONSTRAINT supresstate_symbol_unique UNIQUE (symbol)
);

CREATE TABLE IF NOT EXISTS public.supresarea (
    id bigserial PRIMARY KEY,
    symbol text NOT NULL,
    pattern text NOT NULL,
    side text NOT NULL,
    state text NOT NULL DEFAULT 'active',
    sourcelevel integer NOT NULL DEFAULT 0,
    sourcepivotid bigint NOT NULL,
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
    parentl1pivotid bigint NULL,
    parentl2pivotid bigint NULL,
    isl1extreme boolean NOT NULL DEFAULT false,
    isl2extreme boolean NOT NULL DEFAULT false,
    birthtickid bigint NOT NULL,
    birthtime timestamptz NOT NULL,
    originallow double precision NOT NULL,
    originalhigh double precision NOT NULL,
    currentlow double precision NOT NULL,
    currenthigh double precision NOT NULL,
    originalheight double precision NOT NULL,
    activeheight double precision NOT NULL,
    firsttouchtickid bigint NULL,
    firsttouchtime timestamptz NULL,
    fullusetickid bigint NULL,
    fullusetime timestamptz NULL,
    touchcount integer NOT NULL DEFAULT 0,
    maxpenetration double precision NOT NULL DEFAULT 0,
    firstbreaktickid bigint NULL,
    firstbreaktime timestamptz NULL,
    closetickid bigint NULL,
    closetime timestamptz NULL,
    closereason text NULL,
    priorityscore double precision NOT NULL DEFAULT 0,
    initialdeparturedistance double precision NOT NULL DEFAULT 0,
    untoucheddurationms bigint NOT NULL DEFAULT 0,
    breakticksused integer NOT NULL DEFAULT 4,
    breaktoleranceused double precision NOT NULL DEFAULT 0,
    outsidestreak integer NOT NULL DEFAULT 0,
    outsidedirection text NULL,
    insideactive boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW(),
    CONSTRAINT supresarea_pattern_check CHECK (pattern IN ('H-L-H', 'L-H-L')),
    CONSTRAINT supresarea_side_check CHECK (side IN ('top', 'bottom')),
    CONSTRAINT supresarea_state_check CHECK (state IN ('active', 'used', 'closed')),
    CONSTRAINT supresarea_sourcelevel_check CHECK (sourcelevel = 0),
    CONSTRAINT supresarea_outsidedirection_check CHECK (outsidedirection IS NULL OR outsidedirection IN ('up', 'down')),
    CONSTRAINT supresarea_symbol_sourcepivot_unique UNIQUE (symbol, sourcepivotid)
);

CREATE TABLE IF NOT EXISTS public.supresareaevent (
    id bigserial PRIMARY KEY,
    areaid bigint NOT NULL,
    symbol text NOT NULL,
    eventtype text NOT NULL,
    tickid bigint NOT NULL,
    eventtime timestamptz NOT NULL,
    price double precision NULL,
    lowprice double precision NULL,
    highprice double precision NULL,
    penetration double precision NULL,
    statebefore text NULL,
    stateafter text NULL,
    details text NULL,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    CONSTRAINT supresareaevent_area_fk FOREIGN KEY (areaid)
        REFERENCES public.supresarea (id)
        ON DELETE CASCADE,
    CONSTRAINT supresareaevent_type_check CHECK (eventtype IN ('birth', 'touch', 'partialuse', 'fulluse', 'break', 'close')),
    CONSTRAINT supresareaevent_statebefore_check CHECK (statebefore IS NULL OR statebefore IN ('active', 'used', 'closed')),
    CONSTRAINT supresareaevent_stateafter_check CHECK (stateafter IS NULL OR stateafter IN ('active', 'used', 'closed'))
);

CREATE INDEX IF NOT EXISTS supresarea_symbol_state_birthtickid_idx
    ON public.supresarea (symbol, state, birthtickid DESC, id DESC);

CREATE INDEX IF NOT EXISTS supresarea_symbol_side_state_idx
    ON public.supresarea (symbol, side, state, id DESC);

CREATE INDEX IF NOT EXISTS supresarea_symbol_closebirth_idx
    ON public.supresarea (symbol, birthtickid DESC, closetickid DESC, id DESC);

CREATE INDEX IF NOT EXISTS supresarea_symbol_priority_idx
    ON public.supresarea (symbol, priorityscore DESC, id DESC);

CREATE INDEX IF NOT EXISTS supresarea_symbol_higher_idx
    ON public.supresarea (symbol, isl2extreme DESC, isl1extreme DESC, id DESC);

CREATE INDEX IF NOT EXISTS supresareaevent_area_tickid_idx
    ON public.supresareaevent (areaid, tickid ASC, id ASC);

CREATE INDEX IF NOT EXISTS supresareaevent_symbol_time_idx
    ON public.supresareaevent (symbol, eventtime DESC, id DESC);

COMMIT;
