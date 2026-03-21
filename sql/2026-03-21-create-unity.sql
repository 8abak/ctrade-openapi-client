CREATE TABLE IF NOT EXISTS public.unitystate (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL UNIQUE,
    tickid BIGINT NOT NULL DEFAULT 0,
    time TIMESTAMPTZ NULL,
    mode TEXT NOT NULL DEFAULT 'live',
    status TEXT NOT NULL DEFAULT 'idle',
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS public.unitypivot (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    tickid BIGINT NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    kind TEXT NOT NULL,
    noise DOUBLE PRECISION NOT NULL,
    thresh DOUBLE PRECISION NOT NULL,
    state TEXT NOT NULL,
    legtick BIGINT NOT NULL,
    UNIQUE (symbol, tickid, kind)
);

CREATE TABLE IF NOT EXISTS public.unityswing (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    starttick BIGINT NOT NULL,
    endtick BIGINT NOT NULL,
    starttime TIMESTAMPTZ NOT NULL,
    endtime TIMESTAMPTZ NOT NULL,
    startprice DOUBLE PRECISION NOT NULL,
    endprice DOUBLE PRECISION NOT NULL,
    state TEXT NOT NULL,
    ticks BIGINT NOT NULL,
    move DOUBLE PRECISION NOT NULL,
    efficiency DOUBLE PRECISION NOT NULL,
    multiple DOUBLE PRECISION NOT NULL,
    conviction DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS public.unitytick (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    tickid BIGINT NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    spread DOUBLE PRECISION NOT NULL,
    noise DOUBLE PRECISION NOT NULL,
    thresh DOUBLE PRECISION NOT NULL,
    legtick BIGINT NOT NULL,
    legdir INTEGER NOT NULL,
    legeff DOUBLE PRECISION NOT NULL,
    legmultiple DOUBLE PRECISION NOT NULL,
    causalscore DOUBLE PRECISION NOT NULL,
    causalstate TEXT NOT NULL,
    causalzone BIGINT NOT NULL,
    cleanstate TEXT NOT NULL,
    cleanzone BIGINT NOT NULL,
    swingtick BIGINT NULL,
    cleanconviction DOUBLE PRECISION NOT NULL,
    revised TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (symbol, tickid)
);

CREATE TABLE IF NOT EXISTS public.unitysignal (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    tickid BIGINT NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    side TEXT NOT NULL,
    state TEXT NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    score DOUBLE PRECISION NOT NULL,
    favored BOOLEAN NOT NULL,
    reason TEXT NOT NULL,
    detail JSONB NOT NULL DEFAULT '{}'::jsonb,
    context JSONB NOT NULL DEFAULT '{}'::jsonb,
    used BOOLEAN NOT NULL DEFAULT false,
    skipreason TEXT NULL,
    status TEXT NOT NULL DEFAULT 'seen',
    UNIQUE (symbol, tickid, side)
);

CREATE TABLE IF NOT EXISTS public.unitytrade (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    signaltickid BIGINT NOT NULL,
    side TEXT NOT NULL,
    state TEXT NOT NULL,
    opentick BIGINT NOT NULL,
    opentime TIMESTAMPTZ NOT NULL,
    openprice DOUBLE PRECISION NOT NULL,
    pivottickid BIGINT NOT NULL,
    pivotprice DOUBLE PRECISION NOT NULL,
    buffer DOUBLE PRECISION NOT NULL,
    risk DOUBLE PRECISION NOT NULL,
    stopprice DOUBLE PRECISION NOT NULL,
    targetprice DOUBLE PRECISION NOT NULL,
    bearmed BOOLEAN NOT NULL DEFAULT false,
    trailarmed BOOLEAN NOT NULL DEFAULT false,
    bestprice DOUBLE PRECISION NULL,
    bestfavor DOUBLE PRECISION NOT NULL DEFAULT 0,
    bestadverse DOUBLE PRECISION NOT NULL DEFAULT 0,
    closetick BIGINT NULL,
    closetime TIMESTAMPTZ NULL,
    closeprice DOUBLE PRECISION NULL,
    pnl DOUBLE PRECISION NULL,
    exitreason TEXT NULL,
    status TEXT NOT NULL DEFAULT 'open',
    UNIQUE (symbol, signaltickid)
);

CREATE TABLE IF NOT EXISTS public.unityevent (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    signaltickid BIGINT NOT NULL,
    tickid BIGINT NOT NULL,
    time TIMESTAMPTZ NOT NULL,
    kind TEXT NOT NULL,
    price DOUBLE PRECISION NULL,
    stopprice DOUBLE PRECISION NULL,
    targetprice DOUBLE PRECISION NULL,
    reason TEXT NOT NULL,
    detail JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS unitypivottickidx
ON public.unitypivot (symbol, tickid);

CREATE INDEX IF NOT EXISTS unityswingstartidx
ON public.unityswing (symbol, starttick);

CREATE INDEX IF NOT EXISTS unityswingendidx
ON public.unityswing (symbol, endtick);

CREATE INDEX IF NOT EXISTS unityticktimeidx
ON public.unitytick (symbol, time);

CREATE INDEX IF NOT EXISTS unitytickstateidx
ON public.unitytick (symbol, tickid, causalstate, cleanstate);

CREATE INDEX IF NOT EXISTS unitysignalfavidx
ON public.unitysignal (symbol, tickid, favored, used);

CREATE INDEX IF NOT EXISTS unitytradestatusidx
ON public.unitytrade (symbol, status, opentick);

CREATE UNIQUE INDEX IF NOT EXISTS unitytradeopenuniq
ON public.unitytrade (symbol)
WHERE status = 'open';

CREATE INDEX IF NOT EXISTS unityeventtickidx
ON public.unityevent (symbol, tickid);

CREATE INDEX IF NOT EXISTS unityeventsignalidx
ON public.unityevent (symbol, signaltickid, id);

CREATE OR REPLACE VIEW public.unityopen AS
SELECT *
FROM public.unitytrade
WHERE status = 'open';

CREATE OR REPLACE VIEW public.unityrecent AS
SELECT
    s.id,
    s.symbol,
    s.tickid,
    s.time,
    s.side,
    s.state,
    s.price,
    s.score,
    s.favored,
    s.used,
    s.skipreason,
    s.status,
    t.openprice,
    t.closeprice,
    t.pnl,
    t.exitreason,
    t.status AS tradestatus
FROM public.unitysignal s
LEFT JOIN public.unitytrade t
  ON t.symbol = s.symbol
 AND t.signaltickid = s.tickid;
