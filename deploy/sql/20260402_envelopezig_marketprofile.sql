CREATE TABLE IF NOT EXISTS public.envelopezigpoint (
    id BIGSERIAL PRIMARY KEY,
    tickid BIGINT NOT NULL,
    confirmtickid BIGINT NOT NULL,
    sourceid BIGINT,
    symbol TEXT NOT NULL,
    level TEXT NOT NULL CHECK (level IN ('micro', 'med', 'maxi', 'macro')),
    length INTEGER NOT NULL CHECK (length > 0),
    bandwidth DOUBLE PRECISION NOT NULL CHECK (bandwidth > 0),
    mult DOUBLE PRECISION NOT NULL CHECK (mult >= 0),
    timestamp TIMESTAMPTZ NOT NULL,
    confirmtime TIMESTAMPTZ NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    basis DOUBLE PRECISION,
    mae DOUBLE PRECISION,
    upper DOUBLE PRECISION,
    lower DOUBLE PRECISION,
    createdat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updatedat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT envelopezigpoint_identity_uk UNIQUE (tickid, confirmtickid, symbol, level, length, bandwidth, mult)
);

CREATE TABLE IF NOT EXISTS public.envelopezigstate (
    id BIGSERIAL PRIMARY KEY,
    jobname TEXT NOT NULL,
    jobtype TEXT NOT NULL CHECK (jobtype IN ('worker', 'backfill')),
    symbol TEXT NOT NULL,
    level TEXT NOT NULL CHECK (level IN ('micro', 'med', 'maxi', 'macro')),
    length INTEGER NOT NULL CHECK (length > 0),
    bandwidth DOUBLE PRECISION NOT NULL CHECK (bandwidth > 0),
    mult DOUBLE PRECISION NOT NULL CHECK (mult >= 0),
    starttickid BIGINT,
    endtickid BIGINT,
    startts TIMESTAMPTZ,
    endts TIMESTAMPTZ,
    lasttickid BIGINT NOT NULL DEFAULT 0,
    lastconfirmtickid BIGINT NOT NULL DEFAULT 0,
    lastts TIMESTAMPTZ,
    statejson JSONB NOT NULL DEFAULT '{}'::jsonb,
    createdat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updatedat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT envelopezigstate_jobname_uk UNIQUE (jobname)
);

CREATE INDEX IF NOT EXISTS envelopezigpoint_lookup_idx
    ON public.envelopezigpoint (symbol, level, length, bandwidth, mult, tickid DESC);

CREATE INDEX IF NOT EXISTS envelopezigpoint_confirm_idx
    ON public.envelopezigpoint (symbol, level, length, bandwidth, mult, confirmtickid DESC);

CREATE INDEX IF NOT EXISTS envelopezigstate_lookup_idx
    ON public.envelopezigstate (jobtype, symbol, level, length, bandwidth, mult, updatedat DESC);

CREATE TABLE IF NOT EXISTS public.marketprofile (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('ask', 'bid', 'mid')),
    binsize DOUBLE PRECISION NOT NULL CHECK (binsize > 0),
    maxgapms INTEGER NOT NULL CHECK (maxgapms > 0),
    sessionlabel TEXT NOT NULL,
    sessionstart TIMESTAMPTZ NOT NULL,
    sessionend TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'open' CHECK (status IN ('open', 'closed')),
    firsttickid BIGINT,
    lasttickid BIGINT,
    firstts TIMESTAMPTZ,
    lastts TIMESTAMPTZ,
    totalweightms DOUBLE PRECISION NOT NULL DEFAULT 0,
    totalticks BIGINT NOT NULL DEFAULT 0,
    poc DOUBLE PRECISION,
    vah DOUBLE PRECISION,
    val DOUBLE PRECISION,
    hvns JSONB NOT NULL DEFAULT '[]'::jsonb,
    lvns JSONB NOT NULL DEFAULT '[]'::jsonb,
    createdat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updatedat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT marketprofile_identity_uk UNIQUE (symbol, source, binsize, maxgapms, sessionstart)
);

CREATE TABLE IF NOT EXISTS public.marketprofilebin (
    id BIGSERIAL PRIMARY KEY,
    profileid BIGINT NOT NULL REFERENCES public.marketprofile(id) ON DELETE CASCADE,
    pricebin DOUBLE PRECISION NOT NULL,
    weightms DOUBLE PRECISION NOT NULL DEFAULT 0,
    tickcount BIGINT NOT NULL DEFAULT 0,
    ispoc BOOLEAN NOT NULL DEFAULT FALSE,
    isvah BOOLEAN NOT NULL DEFAULT FALSE,
    isval BOOLEAN NOT NULL DEFAULT FALSE,
    ishvn BOOLEAN NOT NULL DEFAULT FALSE,
    islvn BOOLEAN NOT NULL DEFAULT FALSE,
    createdat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updatedat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT marketprofilebin_identity_uk UNIQUE (profileid, pricebin)
);

CREATE TABLE IF NOT EXISTS public.marketprofilestate (
    id BIGSERIAL PRIMARY KEY,
    jobname TEXT NOT NULL,
    jobtype TEXT NOT NULL CHECK (jobtype IN ('worker', 'backfill')),
    symbol TEXT NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('ask', 'bid', 'mid')),
    binsize DOUBLE PRECISION NOT NULL CHECK (binsize > 0),
    maxgapms INTEGER NOT NULL CHECK (maxgapms > 0),
    lasttickid BIGINT NOT NULL DEFAULT 0,
    lastts TIMESTAMPTZ,
    statejson JSONB NOT NULL DEFAULT '{}'::jsonb,
    createdat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updatedat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT marketprofilestate_jobname_uk UNIQUE (jobname)
);

CREATE INDEX IF NOT EXISTS marketprofile_lookup_idx
    ON public.marketprofile (symbol, source, binsize, maxgapms, sessionstart DESC);

CREATE INDEX IF NOT EXISTS marketprofile_lasttick_idx
    ON public.marketprofile (symbol, source, binsize, maxgapms, lasttickid DESC);

CREATE INDEX IF NOT EXISTS marketprofilebin_profile_idx
    ON public.marketprofilebin (profileid, pricebin);

CREATE INDEX IF NOT EXISTS marketprofilestate_lookup_idx
    ON public.marketprofilestate (symbol, source, binsize, maxgapms, updatedat DESC);
