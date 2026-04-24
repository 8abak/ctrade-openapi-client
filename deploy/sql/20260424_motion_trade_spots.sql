BEGIN;

CREATE TABLE IF NOT EXISTS public.motionpoint (
    id BIGSERIAL PRIMARY KEY,
    tickid BIGINT NOT NULL REFERENCES public.ticks (id) ON DELETE CASCADE,
    "timestamp" TIMESTAMPTZ NOT NULL,
    windowsec INTEGER NOT NULL,
    mid DOUBLE PRECISION NOT NULL,
    bid DOUBLE PRECISION,
    ask DOUBLE PRECISION,
    spread DOUBLE PRECISION,
    pasttickid BIGINT,
    pasttimestamp TIMESTAMPTZ,
    pastmid DOUBLE PRECISION,
    elapsedsec DOUBLE PRECISION,
    pricechange DOUBLE PRECISION,
    velocity DOUBLE PRECISION,
    prevvelocity DOUBLE PRECISION,
    acceleration DOUBLE PRECISION,
    prevacceleration DOUBLE PRECISION,
    jerk DOUBLE PRECISION,
    totalmove DOUBLE PRECISION,
    efficiency DOUBLE PRECISION,
    spreadmultiple DOUBLE PRECISION,
    direction TEXT,
    motionstate TEXT,
    createdat TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS motionpoint_tickid_windowsec_idx
    ON public.motionpoint (tickid, windowsec);

CREATE INDEX IF NOT EXISTS motionpoint_timestamp_idx
    ON public.motionpoint ("timestamp");

CREATE INDEX IF NOT EXISTS motionpoint_tickid_idx
    ON public.motionpoint (tickid);

CREATE INDEX IF NOT EXISTS motionpoint_windowsec_timestamp_desc_idx
    ON public.motionpoint (windowsec, "timestamp" DESC);

CREATE INDEX IF NOT EXISTS motionpoint_motionstate_idx
    ON public.motionpoint (motionstate);

CREATE INDEX IF NOT EXISTS motionpoint_direction_idx
    ON public.motionpoint (direction);

CREATE TABLE IF NOT EXISTS public.motionsignal (
    id BIGSERIAL PRIMARY KEY,
    tickid BIGINT NOT NULL REFERENCES public.ticks (id) ON DELETE CASCADE,
    "timestamp" TIMESTAMPTZ NOT NULL,
    side TEXT NOT NULL,
    mid DOUBLE PRECISION NOT NULL,
    bid DOUBLE PRECISION,
    ask DOUBLE PRECISION,
    spread DOUBLE PRECISION,
    velocity3 DOUBLE PRECISION,
    acceleration3 DOUBLE PRECISION,
    efficiency3 DOUBLE PRECISION,
    spreadmultiple3 DOUBLE PRECISION,
    state3 TEXT,
    velocity10 DOUBLE PRECISION,
    acceleration10 DOUBLE PRECISION,
    efficiency10 DOUBLE PRECISION,
    spreadmultiple10 DOUBLE PRECISION,
    state10 TEXT,
    velocity30 DOUBLE PRECISION,
    acceleration30 DOUBLE PRECISION,
    efficiency30 DOUBLE PRECISION,
    spreadmultiple30 DOUBLE PRECISION,
    state30 TEXT,
    riskfreeprice DOUBLE PRECISION,
    stopprice DOUBLE PRECISION,
    targetprice DOUBLE PRECISION,
    lookaheadsec INTEGER,
    maxfavourable DOUBLE PRECISION,
    maxadverse DOUBLE PRECISION,
    seconds_to_riskfree DOUBLE PRECISION,
    seconds_to_target DOUBLE PRECISION,
    seconds_to_stop DOUBLE PRECISION,
    outcome TEXT,
    score DOUBLE PRECISION,
    signalrule TEXT,
    createdat TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS motionsignal_tickid_side_signalrule_idx
    ON public.motionsignal (tickid, side, signalrule);

CREATE INDEX IF NOT EXISTS motionsignal_timestamp_idx
    ON public.motionsignal ("timestamp");

CREATE INDEX IF NOT EXISTS motionsignal_side_idx
    ON public.motionsignal (side);

CREATE INDEX IF NOT EXISTS motionsignal_outcome_idx
    ON public.motionsignal (outcome);

CREATE INDEX IF NOT EXISTS motionsignal_score_desc_idx
    ON public.motionsignal (score DESC);

CREATE INDEX IF NOT EXISTS motionsignal_signalrule_idx
    ON public.motionsignal (signalrule);

CREATE TABLE IF NOT EXISTS public.motionstate (
    id INTEGER PRIMARY KEY DEFAULT 1,
    lasttickid BIGINT,
    updatedat TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO public.motionstate (id)
VALUES (1)
ON CONFLICT (id) DO NOTHING;

COMMIT;
