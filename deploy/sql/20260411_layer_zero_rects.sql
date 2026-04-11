BEGIN;

CREATE TABLE IF NOT EXISTS public.rects (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    mode TEXT NOT NULL,
    status TEXT NOT NULL,
    state TEXT NOT NULL,
    drawcreatedat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    drawupdatedat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    leftx BIGINT NOT NULL,
    rightx BIGINT NOT NULL,
    firstprice NUMERIC(18, 8) NOT NULL,
    secondprice NUMERIC(18, 8) NOT NULL,
    lowprice NUMERIC(18, 8) NOT NULL,
    highprice NUMERIC(18, 8) NOT NULL,
    height NUMERIC(18, 8) NOT NULL,
    topprice NUMERIC(18, 8) NOT NULL,
    bottomprice NUMERIC(18, 8) NOT NULL,
    lefttime TIMESTAMPTZ,
    righttime TIMESTAMPTZ,
    lefttickid BIGINT,
    righttickid BIGINT,
    entrydir TEXT,
    entryprice NUMERIC(18, 8),
    entrytime TIMESTAMPTZ,
    entrytickid BIGINT,
    stoploss NUMERIC(18, 8),
    takeprofit NUMERIC(18, 8),
    exittime TIMESTAMPTZ,
    exittickid BIGINT,
    exitprice NUMERIC(18, 8),
    exitreason TEXT,
    pnl NUMERIC(18, 8),
    pnlpoints NUMERIC(18, 8),
    drawtoentryms BIGINT,
    entrytoexitms BIGINT,
    smartcloseenabled BOOLEAN NOT NULL DEFAULT FALSE,
    manualclosed BOOLEAN NOT NULL DEFAULT FALSE,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS rects_mode_status_id_idx
    ON public.rects (mode, status, id DESC);

CREATE INDEX IF NOT EXISTS rects_symbol_drawcreatedat_idx
    ON public.rects (symbol, drawcreatedat DESC);

COMMIT;
