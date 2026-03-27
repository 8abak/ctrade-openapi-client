CREATE TABLE IF NOT EXISTS public.otttick (
    id BIGSERIAL PRIMARY KEY,
    tickid BIGINT NOT NULL,
    symbol TEXT NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('ask', 'bid', 'mid')),
    matype TEXT NOT NULL CHECK (matype IN ('SMA', 'EMA', 'WMA', 'TMA', 'VAR', 'WWMA', 'ZLEMA', 'TSF')),
    length INTEGER NOT NULL CHECK (length > 0),
    percent DOUBLE PRECISION NOT NULL CHECK (percent >= 0),
    timestamp TIMESTAMPTZ NOT NULL,
    price DOUBLE PRECISION NOT NULL,
    mavg DOUBLE PRECISION,
    fark DOUBLE PRECISION,
    longstop DOUBLE PRECISION,
    shortstop DOUBLE PRECISION,
    dir INTEGER NOT NULL,
    mt DOUBLE PRECISION,
    ott DOUBLE PRECISION,
    ott2 DOUBLE PRECISION,
    ott3 DOUBLE PRECISION,
    supportbuy BOOLEAN NOT NULL DEFAULT FALSE,
    supportsell BOOLEAN NOT NULL DEFAULT FALSE,
    pricebuy BOOLEAN NOT NULL DEFAULT FALSE,
    pricesell BOOLEAN NOT NULL DEFAULT FALSE,
    colorbuy BOOLEAN NOT NULL DEFAULT FALSE,
    colorsell BOOLEAN NOT NULL DEFAULT FALSE,
    createdat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updatedat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT otttick_identity_uk UNIQUE (tickid, symbol, source, matype, length, percent)
);

CREATE TABLE IF NOT EXISTS public.ottbacktestrun (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('ask', 'bid', 'mid')),
    matype TEXT NOT NULL CHECK (matype IN ('SMA', 'EMA', 'WMA', 'TMA', 'VAR', 'WWMA', 'ZLEMA', 'TSF')),
    length INTEGER NOT NULL CHECK (length > 0),
    percent DOUBLE PRECISION NOT NULL CHECK (percent >= 0),
    signalmode TEXT NOT NULL CHECK (signalmode IN ('support', 'price', 'color')),
    starttickid BIGINT NOT NULL,
    endtickid BIGINT NOT NULL,
    startts TIMESTAMPTZ NOT NULL,
    endts TIMESTAMPTZ NOT NULL,
    tradecount INTEGER NOT NULL DEFAULT 0 CHECK (tradecount >= 0),
    grosspnl DOUBLE PRECISION NOT NULL DEFAULT 0,
    netpnl DOUBLE PRECISION NOT NULL DEFAULT 0,
    createdat TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.ottbacktesttrade (
    id BIGSERIAL PRIMARY KEY,
    runid BIGINT NOT NULL REFERENCES public.ottbacktestrun(id) ON DELETE CASCADE,
    entrytickid BIGINT NOT NULL,
    exittickid BIGINT NOT NULL,
    entryts TIMESTAMPTZ NOT NULL,
    exitts TIMESTAMPTZ NOT NULL,
    direction TEXT NOT NULL CHECK (direction IN ('long', 'short')),
    entryprice DOUBLE PRECISION NOT NULL,
    exitprice DOUBLE PRECISION NOT NULL,
    pnl DOUBLE PRECISION NOT NULL,
    pnlpoints DOUBLE PRECISION NOT NULL,
    barsorticksheld INTEGER NOT NULL CHECK (barsorticksheld >= 0),
    signalentrytype TEXT NOT NULL,
    signalexittype TEXT NOT NULL,
    createdat TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.ottjobstate (
    id BIGSERIAL PRIMARY KEY,
    jobname TEXT NOT NULL,
    symbol TEXT NOT NULL,
    source TEXT NOT NULL CHECK (source IN ('ask', 'bid', 'mid')),
    matype TEXT NOT NULL CHECK (matype IN ('SMA', 'EMA', 'WMA', 'TMA', 'VAR', 'WWMA', 'ZLEMA', 'TSF')),
    length INTEGER NOT NULL CHECK (length > 0),
    percent DOUBLE PRECISION NOT NULL CHECK (percent >= 0),
    lasttickid BIGINT NOT NULL DEFAULT 0,
    lastts TIMESTAMPTZ,
    statejson JSONB NOT NULL DEFAULT '{}'::jsonb,
    createdat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updatedat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT ottjobstate_jobname_uk UNIQUE (jobname)
);

CREATE INDEX IF NOT EXISTS otttick_lookup_idx
    ON public.otttick (symbol, source, matype, length, percent, tickid DESC);

CREATE INDEX IF NOT EXISTS otttick_timestamp_idx
    ON public.otttick (timestamp DESC);

CREATE INDEX IF NOT EXISTS ottbacktestrun_lookup_idx
    ON public.ottbacktestrun (symbol, source, matype, length, percent, signalmode, createdat DESC);

CREATE INDEX IF NOT EXISTS ottbacktestrun_range_idx
    ON public.ottbacktestrun (starttickid, endtickid);

CREATE INDEX IF NOT EXISTS ottbacktesttrade_run_entry_idx
    ON public.ottbacktesttrade (runid, entrytickid, exittickid);

CREATE INDEX IF NOT EXISTS ottjobstate_symbol_idx
    ON public.ottjobstate (symbol, source, matype, length, percent);
