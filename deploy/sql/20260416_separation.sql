BEGIN;

CREATE TABLE IF NOT EXISTS public.separationsegments (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    brokerday DATE NOT NULL,
    level TEXT NOT NULL,
    status TEXT NOT NULL,
    sourcemode TEXT NOT NULL,
    starttickid BIGINT,
    endtickid BIGINT,
    starttime TIMESTAMPTZ NOT NULL,
    endtime TIMESTAMPTZ NOT NULL,
    startprice NUMERIC(18, 8) NOT NULL,
    endprice NUMERIC(18, 8) NOT NULL,
    highprice NUMERIC(18, 8) NOT NULL,
    lowprice NUMERIC(18, 8) NOT NULL,
    tickcount INTEGER NOT NULL DEFAULT 0,
    netmove NUMERIC(18, 8) NOT NULL DEFAULT 0,
    rangeprice NUMERIC(18, 8) NOT NULL DEFAULT 0,
    pathlength NUMERIC(18, 8) NOT NULL DEFAULT 0,
    efficiency NUMERIC(18, 8) NOT NULL DEFAULT 0,
    thickness NUMERIC(18, 8) NOT NULL DEFAULT 0,
    direction TEXT NOT NULL,
    shapetype TEXT NOT NULL,
    angle NUMERIC(18, 8) NOT NULL DEFAULT 0,
    unitprice NUMERIC(18, 8) NOT NULL DEFAULT 0,
    version INTEGER NOT NULL DEFAULT 1,
    createdat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updatedat TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.separationstate (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    brokerday DATE NOT NULL,
    level TEXT NOT NULL,
    lastsourceid BIGINT,
    opentickid BIGINT,
    starttime TIMESTAMPTZ,
    startprice NUMERIC(18, 8),
    lasttime TIMESTAMPTZ,
    lastprice NUMERIC(18, 8),
    highprice NUMERIC(18, 8),
    lowprice NUMERIC(18, 8),
    tickcount INTEGER NOT NULL DEFAULT 0,
    pathlength NUMERIC(18, 8) NOT NULL DEFAULT 0,
    directioncandidate TEXT,
    unitprice NUMERIC(18, 8) NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'open',
    statejson JSONB NOT NULL DEFAULT '{}'::jsonb,
    updatedat TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE public.separationstate
    ADD COLUMN IF NOT EXISTS statejson JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE TABLE IF NOT EXISTS public.separationruns (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    brokerday DATE,
    mode TEXT NOT NULL,
    starttickid BIGINT,
    endtickid BIGINT,
    tickcount INTEGER NOT NULL DEFAULT 0,
    microcount INTEGER NOT NULL DEFAULT 0,
    mediancount INTEGER NOT NULL DEFAULT 0,
    macrocount INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL,
    message TEXT,
    startedat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finishedat TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS separationstate_symbol_brokerday_level_idx
    ON public.separationstate (symbol, brokerday, level);

CREATE INDEX IF NOT EXISTS separationsegments_symbol_brokerday_level_idx
    ON public.separationsegments (symbol, brokerday, level, status, starttime DESC);

CREATE INDEX IF NOT EXISTS separationsegments_symbol_level_starttime_idx
    ON public.separationsegments (symbol, level, starttime DESC);

CREATE INDEX IF NOT EXISTS separationsegments_symbol_level_endtime_idx
    ON public.separationsegments (symbol, level, endtime DESC);

CREATE INDEX IF NOT EXISTS separationsegments_symbol_level_endtickid_idx
    ON public.separationsegments (symbol, level, endtickid DESC);

CREATE INDEX IF NOT EXISTS separationsegments_status_idx
    ON public.separationsegments (status, updatedat DESC);

CREATE UNIQUE INDEX IF NOT EXISTS separationsegments_one_open_idx
    ON public.separationsegments (symbol, brokerday, level)
    WHERE status = 'open';

CREATE INDEX IF NOT EXISTS separationruns_symbol_brokerday_startedat_idx
    ON public.separationruns (symbol, brokerday, startedat DESC);

COMMIT;
