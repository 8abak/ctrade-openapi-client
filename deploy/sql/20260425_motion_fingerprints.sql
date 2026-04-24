BEGIN;

CREATE TABLE IF NOT EXISTS public.motionfingerprint (
    id BIGSERIAL PRIMARY KEY,
    signalrule TEXT,
    side TEXT,
    state3 TEXT,
    state10 TEXT,
    state30 TEXT,
    sm3bucket INTEGER,
    eff3bucket INTEGER,
    v3bucket INTEGER,
    a3bucket INTEGER,
    v10bucket INTEGER,
    a10bucket INTEGER,
    total INTEGER,
    targets INTEGER,
    riskfree INTEGER,
    stops INTEGER,
    targetpct DOUBLE PRECISION,
    usefulpct DOUBLE PRECISION,
    stoppct DOUBLE PRECISION,
    avgsectoriskfree DOUBLE PRECISION,
    avgmaxadverse DOUBLE PRECISION,
    avgscore DOUBLE PRECISION,
    lift DOUBLE PRECISION,
    createdat TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS motionfingerprint_signalrule_createdat_idx
    ON public.motionfingerprint (signalrule, createdat DESC);

CREATE INDEX IF NOT EXISTS motionfingerprint_signalrule_lift_desc_idx
    ON public.motionfingerprint (signalrule, lift DESC, usefulpct DESC, total DESC);

COMMIT;
