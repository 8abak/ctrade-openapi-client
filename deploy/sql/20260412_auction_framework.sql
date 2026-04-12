BEGIN;

CREATE TABLE IF NOT EXISTS public.auctionsession (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    anchorkind TEXT NOT NULL,
    startts TIMESTAMPTZ NOT NULL,
    endts TIMESTAMPTZ NOT NULL,
    windowseconds INTEGER NOT NULL,
    openprice NUMERIC(18, 8),
    highprice NUMERIC(18, 8),
    lowprice NUMERIC(18, 8),
    closeprice NUMERIC(18, 8),
    pocprice NUMERIC(18, 8),
    vahprice NUMERIC(18, 8),
    valprice NUMERIC(18, 8),
    ibhigh NUMERIC(18, 8),
    iblow NUMERIC(18, 8),
    statekind TEXT,
    opentype TEXT,
    inventorytype TEXT,
    valuedrift NUMERIC(18, 8),
    balancescore NUMERIC(10, 4),
    trendscore NUMERIC(10, 4),
    transitionscore NUMERIC(10, 4),
    updatedts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.auctionbin (
    id BIGSERIAL PRIMARY KEY,
    auctionsessionid BIGINT NOT NULL REFERENCES public.auctionsession(id) ON DELETE CASCADE,
    pricebin NUMERIC(18, 8) NOT NULL,
    tickcount INTEGER NOT NULL DEFAULT 0,
    timems BIGINT NOT NULL DEFAULT 0,
    bidhitcount INTEGER NOT NULL DEFAULT 0,
    askliftcount INTEGER NOT NULL DEFAULT 0,
    spreadsum NUMERIC(18, 8) NOT NULL DEFAULT 0,
    l2bidvol NUMERIC(18, 8) NOT NULL DEFAULT 0,
    l2askvol NUMERIC(18, 8) NOT NULL DEFAULT 0,
    activityscore NUMERIC(18, 8) NOT NULL DEFAULT 0,
    dwellscore NUMERIC(18, 8) NOT NULL DEFAULT 0,
    deltascore NUMERIC(18, 8) NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS public.auctionref (
    id BIGSERIAL PRIMARY KEY,
    auctionsessionid BIGINT NOT NULL REFERENCES public.auctionsession(id) ON DELETE CASCADE,
    refkind TEXT NOT NULL,
    price NUMERIC(18, 8) NOT NULL,
    strength NUMERIC(10, 4) NOT NULL DEFAULT 0,
    validfromts TIMESTAMPTZ,
    validtots TIMESTAMPTZ,
    notesjson JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS public.auctionevent (
    id BIGSERIAL PRIMARY KEY,
    auctionsessionid BIGINT NOT NULL REFERENCES public.auctionsession(id) ON DELETE CASCADE,
    eventts TIMESTAMPTZ NOT NULL,
    eventkind TEXT NOT NULL,
    price1 NUMERIC(18, 8),
    price2 NUMERIC(18, 8),
    direction TEXT,
    strength NUMERIC(10, 4) NOT NULL DEFAULT 0,
    confirmed BOOLEAN NOT NULL DEFAULT FALSE,
    payloadjson JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS public.auctionstate (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    asts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    windowkind TEXT NOT NULL,
    statekind TEXT,
    locationkind TEXT,
    acceptancekind TEXT,
    inventorytype TEXT,
    biaskind TEXT,
    confidence NUMERIC(10, 4),
    invalidationprice NUMERIC(18, 8),
    targetprice1 NUMERIC(18, 8),
    targetprice2 NUMERIC(18, 8),
    summaryjson JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS public.auctionsnap (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    asts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    snapshotjson JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS auctionsession_symbol_anchor_updated_idx
    ON public.auctionsession (symbol, anchorkind, updatedts DESC);

CREATE INDEX IF NOT EXISTS auctionsession_symbol_endts_idx
    ON public.auctionsession (symbol, endts DESC);

CREATE INDEX IF NOT EXISTS auctionbin_session_price_idx
    ON public.auctionbin (auctionsessionid, pricebin);

CREATE INDEX IF NOT EXISTS auctionref_session_kind_idx
    ON public.auctionref (auctionsessionid, refkind);

CREATE INDEX IF NOT EXISTS auctionevent_session_eventts_idx
    ON public.auctionevent (auctionsessionid, eventts DESC);

CREATE INDEX IF NOT EXISTS auctionstate_symbol_window_asts_idx
    ON public.auctionstate (symbol, windowkind, asts DESC);

CREATE INDEX IF NOT EXISTS auctionsnap_symbol_asts_idx
    ON public.auctionsnap (symbol, asts DESC);

COMMIT;
