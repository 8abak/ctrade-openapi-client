BEGIN;

CREATE TABLE IF NOT EXISTS public.mavgconfig (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    method TEXT NOT NULL,
    source TEXT NOT NULL,
    windowseconds INTEGER NOT NULL,
    isenabled BOOLEAN NOT NULL DEFAULT TRUE,
    showonlive BOOLEAN NOT NULL DEFAULT TRUE,
    showonbig BOOLEAN NOT NULL DEFAULT TRUE,
    color TEXT,
    createdat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updatedat TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT mavgconfig_windowseconds_positive CHECK (windowseconds > 0)
);

CREATE UNIQUE INDEX IF NOT EXISTS mavgconfig_name_idx
    ON public.mavgconfig (name);

CREATE INDEX IF NOT EXISTS mavgconfig_enabled_live_idx
    ON public.mavgconfig (isenabled, showonlive, windowseconds ASC, id ASC);

CREATE INDEX IF NOT EXISTS mavgconfig_enabled_big_idx
    ON public.mavgconfig (isenabled, showonbig, windowseconds ASC, id ASC);

CREATE TABLE IF NOT EXISTS public.mavgvalue (
    id BIGSERIAL PRIMARY KEY,
    configid BIGINT NOT NULL REFERENCES public.mavgconfig (id) ON DELETE CASCADE,
    tickid BIGINT NOT NULL,
    ticktime TIMESTAMPTZ NOT NULL,
    value DOUBLE PRECISION NOT NULL,
    createdat TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS mavgvalue_configid_tickid_idx
    ON public.mavgvalue (configid, tickid);

CREATE INDEX IF NOT EXISTS mavgvalue_configid_ticktime_tickid_idx
    ON public.mavgvalue (configid, ticktime ASC, tickid ASC);

CREATE INDEX IF NOT EXISTS mavgvalue_configid_id_idx
    ON public.mavgvalue (configid, id ASC);

CREATE TABLE IF NOT EXISTS public.mavgstate (
    configid BIGINT PRIMARY KEY REFERENCES public.mavgconfig (id) ON DELETE CASCADE,
    symbol TEXT NOT NULL,
    lasttickid BIGINT,
    lastticktime TIMESTAMPTZ,
    lastvalue DOUBLE PRECISION,
    statejson JSONB NOT NULL DEFAULT '{}'::jsonb,
    updatedat TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS mavgstate_symbol_updatedat_idx
    ON public.mavgstate (symbol, updatedat DESC);

INSERT INTO public.mavgconfig (
    name,
    method,
    source,
    windowseconds,
    isenabled,
    showonlive,
    showonbig,
    color
) VALUES
    ('SMA 14m mid', 'SMA', 'mid', 840, TRUE, TRUE, TRUE, '#ffd166'),
    ('SMA 30m mid', 'SMA', 'mid', 1800, TRUE, TRUE, TRUE, '#8ecae6'),
    ('EMA 1h mid', 'EMA', 'mid', 3600, TRUE, TRUE, TRUE, '#ef476f')
ON CONFLICT (name) DO NOTHING;

COMMIT;
