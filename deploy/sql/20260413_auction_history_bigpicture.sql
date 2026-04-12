BEGIN;

CREATE TABLE IF NOT EXISTS public.auctionhistorysession (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    sessionkind TEXT NOT NULL,
    startts TIMESTAMPTZ NOT NULL,
    endts TIMESTAMPTZ NOT NULL,
    asofts TIMESTAMPTZ NOT NULL,
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
    summaryjson JSONB NOT NULL DEFAULT '{}'::jsonb,
    updatedts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (symbol, sessionkind, startts)
);

CREATE TABLE IF NOT EXISTS public.auctionhistorybin (
    id BIGSERIAL PRIMARY KEY,
    auctionhistorysessionid BIGINT NOT NULL REFERENCES public.auctionhistorysession(id) ON DELETE CASCADE,
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

CREATE TABLE IF NOT EXISTS public.auctionhistoryref (
    id BIGSERIAL PRIMARY KEY,
    auctionhistorysessionid BIGINT NOT NULL REFERENCES public.auctionhistorysession(id) ON DELETE CASCADE,
    refkind TEXT NOT NULL,
    price NUMERIC(18, 8) NOT NULL,
    strength NUMERIC(10, 4) NOT NULL DEFAULT 0,
    validfromts TIMESTAMPTZ,
    validtots TIMESTAMPTZ,
    notesjson JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS public.auctionhistoryevent (
    id BIGSERIAL PRIMARY KEY,
    auctionhistorysessionid BIGINT NOT NULL REFERENCES public.auctionhistorysession(id) ON DELETE CASCADE,
    eventts TIMESTAMPTZ NOT NULL,
    eventkind TEXT NOT NULL,
    price1 NUMERIC(18, 8),
    price2 NUMERIC(18, 8),
    direction TEXT,
    strength NUMERIC(10, 4) NOT NULL DEFAULT 0,
    confirmed BOOLEAN NOT NULL DEFAULT FALSE,
    payloadjson JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS public.auctionhistorystate (
    id BIGSERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    focuskind TEXT NOT NULL,
    snapshotts TIMESTAMPTZ NOT NULL,
    sessionkind TEXT NOT NULL,
    sessionstartts TIMESTAMPTZ,
    sessionendts TIMESTAMPTZ,
    lastprocessedid BIGINT,
    statekind TEXT,
    locationkind TEXT,
    acceptancekind TEXT,
    inventorytype TEXT,
    biaskind TEXT,
    confidence NUMERIC(10, 4),
    invalidationprice NUMERIC(18, 8),
    targetprice1 NUMERIC(18, 8),
    targetprice2 NUMERIC(18, 8),
    summaryjson JSONB NOT NULL DEFAULT '{}'::jsonb,
    UNIQUE (symbol, focuskind, snapshotts)
);

CREATE INDEX IF NOT EXISTS auctionhistorysession_symbol_kind_start_idx
    ON public.auctionhistorysession (symbol, sessionkind, startts DESC);

CREATE INDEX IF NOT EXISTS auctionhistorysession_symbol_range_idx
    ON public.auctionhistorysession (symbol, startts DESC, endts DESC);

CREATE INDEX IF NOT EXISTS auctionhistorybin_session_price_idx
    ON public.auctionhistorybin (auctionhistorysessionid, pricebin);

CREATE INDEX IF NOT EXISTS auctionhistoryref_session_kind_idx
    ON public.auctionhistoryref (auctionhistorysessionid, refkind);

CREATE INDEX IF NOT EXISTS auctionhistoryevent_session_eventts_idx
    ON public.auctionhistoryevent (auctionhistorysessionid, eventts DESC);

CREATE INDEX IF NOT EXISTS auctionhistorystate_symbol_focus_snapshot_idx
    ON public.auctionhistorystate (symbol, focuskind, snapshotts DESC);

COMMIT;
