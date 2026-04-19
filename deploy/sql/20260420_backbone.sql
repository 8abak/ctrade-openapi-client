BEGIN;

CREATE TABLE IF NOT EXISTS public.backbonepivots (
    id BIGSERIAL PRIMARY KEY,
    dayid BIGINT NOT NULL,
    tickid BIGINT NOT NULL,
    ticktime TIMESTAMPTZ NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    pivottype TEXT NOT NULL,
    threshold DOUBLE PRECISION,
    source TEXT NOT NULL DEFAULT 'adaptivehysteresis',
    createdat TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.backbonemoves (
    id BIGSERIAL PRIMARY KEY,
    dayid BIGINT NOT NULL,
    starttickid BIGINT NOT NULL,
    endtickid BIGINT NOT NULL,
    starttime TIMESTAMPTZ NOT NULL,
    endtime TIMESTAMPTZ NOT NULL,
    startprice DOUBLE PRECISION NOT NULL,
    endprice DOUBLE PRECISION NOT NULL,
    direction TEXT NOT NULL,
    pricedelta DOUBLE PRECISION NOT NULL,
    tickcount INTEGER NOT NULL,
    thresholdatconfirm DOUBLE PRECISION,
    source TEXT NOT NULL DEFAULT 'adaptivehysteresis',
    createdat TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.backbonestate (
    id BIGSERIAL PRIMARY KEY,
    dayid BIGINT NOT NULL,
    symbol TEXT NOT NULL,
    lastprocessedtickid BIGINT,
    confirmedpivottickid BIGINT,
    confirmedpivottime TIMESTAMPTZ,
    confirmedpivotprice DOUBLE PRECISION,
    direction TEXT,
    candidateextremetickid BIGINT,
    candidateextremetime TIMESTAMPTZ,
    candidateextremeprice DOUBLE PRECISION,
    currentthreshold DOUBLE PRECISION,
    statejson JSONB NOT NULL DEFAULT '{}'::jsonb,
    updatedat TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS backbonepivots_dayid_tickid_type_source_idx
    ON public.backbonepivots (dayid, tickid, pivottype, source);

CREATE INDEX IF NOT EXISTS backbonepivots_dayid_tickid_idx
    ON public.backbonepivots (dayid, tickid ASC, ticktime ASC);

CREATE INDEX IF NOT EXISTS backbonepivots_dayid_ticktime_idx
    ON public.backbonepivots (dayid, ticktime ASC, tickid ASC);

CREATE UNIQUE INDEX IF NOT EXISTS backbonemoves_dayid_start_end_dir_source_idx
    ON public.backbonemoves (dayid, starttickid, endtickid, direction, source);

CREATE INDEX IF NOT EXISTS backbonemoves_dayid_endtickid_idx
    ON public.backbonemoves (dayid, endtickid ASC, endtime ASC);

CREATE INDEX IF NOT EXISTS backbonemoves_dayid_starttickid_idx
    ON public.backbonemoves (dayid, starttickid ASC, starttime ASC);

CREATE UNIQUE INDEX IF NOT EXISTS backbonestate_dayid_symbol_idx
    ON public.backbonestate (dayid, symbol);

CREATE INDEX IF NOT EXISTS backbonestate_symbol_updatedat_idx
    ON public.backbonestate (symbol, updatedat DESC);

COMMIT;
