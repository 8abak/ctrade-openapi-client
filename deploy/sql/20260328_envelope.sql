CREATE TABLE IF NOT EXISTS public.envelopetick (
    id BIGSERIAL PRIMARY KEY,
    tickid BIGINT NOT NULL,
    symbol TEXT NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('ask', 'bid', 'mid')),
    length INTEGER NOT NULL CHECK (length > 0),
    bandwidth DOUBLE PRECISION NOT NULL CHECK (bandwidth > 0),
    mult DOUBLE PRECISION NOT NULL CHECK (mult >= 0),
    timestamp TIMESTAMPTZ NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    basis DOUBLE PRECISION,
    mae DOUBLE PRECISION,
    upper DOUBLE PRECISION,
    lower DOUBLE PRECISION,
    createdat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updatedat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT envelopetick_identity_uk UNIQUE (tickid, symbol, source, length, bandwidth, mult)
);

CREATE TABLE IF NOT EXISTS public.envelopejobstate (
    id BIGSERIAL PRIMARY KEY,
    jobname TEXT NOT NULL,
    jobtype TEXT NOT NULL CHECK (jobtype IN ('worker', 'backfill')),
    symbol TEXT NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('ask', 'bid', 'mid')),
    length INTEGER NOT NULL CHECK (length > 0),
    bandwidth DOUBLE PRECISION NOT NULL CHECK (bandwidth > 0),
    mult DOUBLE PRECISION NOT NULL CHECK (mult >= 0),
    starttickid BIGINT,
    endtickid BIGINT,
    startts TIMESTAMPTZ,
    endts TIMESTAMPTZ,
    lasttickid BIGINT NOT NULL DEFAULT 0,
    lastts TIMESTAMPTZ,
    statejson JSONB NOT NULL DEFAULT '{}'::jsonb,
    createdat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updatedat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT envelopejobstate_jobname_uk UNIQUE (jobname)
);

CREATE INDEX IF NOT EXISTS envelopetick_lookup_idx
    ON public.envelopetick (symbol, source, length, bandwidth, mult, tickid DESC);

CREATE INDEX IF NOT EXISTS envelopetick_timestamp_idx
    ON public.envelopetick (timestamp DESC);

CREATE INDEX IF NOT EXISTS envelopejobstate_lookup_idx
    ON public.envelopejobstate (jobtype, symbol, source, length, bandwidth, mult, updatedat DESC);
