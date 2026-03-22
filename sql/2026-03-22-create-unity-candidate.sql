CREATE TABLE IF NOT EXISTS public.unitycandidate (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    signaltickid BIGINT NOT NULL,
    tradeid BIGINT NULL REFERENCES public.unitytrade(id) ON DELETE SET NULL,
    time TIMESTAMPTZ NOT NULL,
    side TEXT NOT NULL,
    regimefrom TEXT NOT NULL,
    regimeto TEXT NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    spread DOUBLE PRECISION NOT NULL,
    causalstate TEXT NOT NULL,
    cleanstate TEXT NOT NULL,
    score DOUBLE PRECISION NOT NULL,
    reason TEXT NOT NULL,
    detail JSONB NOT NULL DEFAULT '{}'::jsonb,
    context JSONB NOT NULL DEFAULT '{}'::jsonb,
    pivottickid BIGINT NULL,
    pivotkind TEXT NULL,
    pivotprice DOUBLE PRECISION NULL,
    entryprice DOUBLE PRECISION NULL,
    buffer DOUBLE PRECISION NULL,
    risk DOUBLE PRECISION NULL,
    baselinetp DOUBLE PRECISION NULL,
    baselinesl DOUBLE PRECISION NULL,
    eligible BOOLEAN NOT NULL DEFAULT false,
    eligibilityreason TEXT NULL,
    favored BOOLEAN NOT NULL DEFAULT false,
    signalstatus TEXT NOT NULL DEFAULT 'seen',
    skipreason TEXT NULL,
    tradeopened BOOLEAN NOT NULL DEFAULT false,
    featurever TEXT NOT NULL,
    features JSONB NOT NULL DEFAULT '{}'::jsonb,
    created TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (symbol, signaltickid, side)
);

CREATE TABLE IF NOT EXISTS public.unitycandoutcome (
    id BIGSERIAL PRIMARY KEY,
    candidateid BIGINT NOT NULL REFERENCES public.unitycandidate(id) ON DELETE CASCADE,
    timeoutsec INTEGER NOT NULL DEFAULT 900,
    tpprice DOUBLE PRECISION NULL,
    slprice DOUBLE PRECISION NULL,
    firsthit TEXT NOT NULL DEFAULT 'unresolved',
    resolvetickid BIGINT NULL,
    resolvetime TIMESTAMPTZ NULL,
    resolveseconds INTEGER NULL,
    mfe DOUBLE PRECISION NULL,
    mae DOUBLE PRECISION NULL,
    bestfavor DOUBLE PRECISION NULL,
    bestadverse DOUBLE PRECISION NULL,
    pnl DOUBLE PRECISION NULL,
    wouldwin BOOLEAN NULL,
    status TEXT NOT NULL DEFAULT 'unresolved',
    updated TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (candidateid)
);

CREATE TABLE IF NOT EXISTS public.unitycandscenario (
    id BIGSERIAL PRIMARY KEY,
    candidateid BIGINT NOT NULL REFERENCES public.unitycandidate(id) ON DELETE CASCADE,
    code TEXT NOT NULL,
    timeoutsec INTEGER NOT NULL DEFAULT 900,
    tpmult DOUBLE PRECISION NOT NULL,
    slmult DOUBLE PRECISION NOT NULL,
    tpprice DOUBLE PRECISION NULL,
    slprice DOUBLE PRECISION NULL,
    firsthit TEXT NOT NULL DEFAULT 'unresolved',
    resolvetickid BIGINT NULL,
    resolvetime TIMESTAMPTZ NULL,
    resolveseconds INTEGER NULL,
    mfe DOUBLE PRECISION NULL,
    mae DOUBLE PRECISION NULL,
    bestfavor DOUBLE PRECISION NULL,
    bestadverse DOUBLE PRECISION NULL,
    pnl DOUBLE PRECISION NULL,
    wouldwin BOOLEAN NULL,
    status TEXT NOT NULL DEFAULT 'unresolved',
    updated TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (candidateid, code)
);

CREATE INDEX IF NOT EXISTS unitycandtimeidx
ON public.unitycandidate (symbol, time DESC, signaltickid DESC);

CREATE INDEX IF NOT EXISTS unitycandtickidx
ON public.unitycandidate (symbol, signaltickid DESC);

CREATE INDEX IF NOT EXISTS unitycandstateidx
ON public.unitycandidate (symbol, regimeto, signalstatus, eligible, tradeopened);

CREATE INDEX IF NOT EXISTS unitycandtradeidx
ON public.unitycandidate (tradeid)
WHERE tradeid IS NOT NULL;

CREATE INDEX IF NOT EXISTS unitycandoutstatusidx
ON public.unitycandoutcome (status, resolvetime DESC);

CREATE INDEX IF NOT EXISTS unitycandouttickidx
ON public.unitycandoutcome (candidateid, resolvetickid);

CREATE INDEX IF NOT EXISTS unitycandscenstatusidx
ON public.unitycandscenario (candidateid, status, code);

CREATE INDEX IF NOT EXISTS unitycandsceneresidx
ON public.unitycandscenario (resolvetime DESC, code);

CREATE OR REPLACE VIEW public.unitycandpending AS
SELECT
    c.id,
    c.symbol,
    c.signaltickid,
    c.time,
    c.side,
    c.regimeto,
    c.eligible,
    COALESCE(o.status, 'missing') AS outcomestatus
FROM public.unitycandidate c
LEFT JOIN public.unitycandoutcome o
  ON o.candidateid = c.id
WHERE o.id IS NULL
   OR o.status = 'unresolved';

CREATE OR REPLACE VIEW public.unitycandtrain AS
SELECT
    c.id AS candidateid,
    c.symbol,
    c.signaltickid,
    c.time,
    c.side,
    c.regimefrom,
    c.regimeto,
    c.price,
    c.spread,
    c.causalstate,
    c.cleanstate,
    c.score,
    c.reason,
    c.pivottickid,
    c.pivotkind,
    c.pivotprice,
    c.entryprice,
    c.buffer,
    c.risk,
    c.baselinetp,
    c.baselinesl,
    c.eligible,
    c.eligibilityreason,
    c.favored,
    c.signalstatus,
    c.skipreason,
    c.tradeopened,
    c.tradeid,
    c.featurever,
    c.features,
    o.timeoutsec AS baselinetimeoutsec,
    o.tpprice AS baselinetpprice,
    o.slprice AS baselineslprice,
    o.firsthit AS baselinefirsthit,
    o.resolvetickid AS baselineresolvetickid,
    o.resolvetime AS baselineresolvetime,
    o.resolveseconds AS baselineresolveseconds,
    o.mfe AS baselinemfe,
    o.mae AS baselinemae,
    o.bestfavor AS baselinebestfavor,
    o.bestadverse AS baselinebestadverse,
    o.pnl AS baselinepnl,
    o.wouldwin AS baselinewouldwin,
    o.status AS baselinestatus,
    CASE
        WHEN o.status = 'resolved' AND o.firsthit = 'tp' THEN 1
        WHEN o.status = 'resolved' AND o.firsthit IN ('sl', 'regimechange', 'timeout', 'dayend') THEN 0
        ELSE NULL
    END AS targettradeable,
    s.code AS scenariocode,
    s.timeoutsec AS scenariotimeoutsec,
    s.tpmult,
    s.slmult,
    s.tpprice AS scenariotpprice,
    s.slprice AS scenarioslprice,
    s.firsthit AS scenariofirsthit,
    s.resolvetickid AS scenarioresolvetickid,
    s.resolvetime AS scenarioresolvetime,
    s.resolveseconds AS scenarioresolveseconds,
    s.mfe AS scenariomfe,
    s.mae AS scenariomae,
    s.bestfavor AS scenariobestfavor,
    s.bestadverse AS scenariobestadverse,
    s.pnl AS scenariopnl,
    s.wouldwin AS targetscenariowin,
    s.status AS scenariostatus
FROM public.unitycandidate c
LEFT JOIN public.unitycandoutcome o
  ON o.candidateid = c.id
LEFT JOIN public.unitycandscenario s
  ON s.candidateid = c.id;
