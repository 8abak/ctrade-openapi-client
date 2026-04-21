BEGIN;

ALTER TABLE public.backbonestate
    ADD COLUMN IF NOT EXISTS source TEXT NOT NULL DEFAULT 'adaptivehysteresis';

UPDATE public.backbonestate
SET source = 'adaptivehysteresis'
WHERE source IS NULL OR BTRIM(source) = '';

DROP INDEX IF EXISTS backbonestate_dayid_symbol_idx;

CREATE UNIQUE INDEX IF NOT EXISTS backbonestate_dayid_symbol_source_idx
    ON public.backbonestate (dayid, symbol, source);

CREATE INDEX IF NOT EXISTS backbonestate_symbol_source_updatedat_idx
    ON public.backbonestate (symbol, source, updatedat DESC);

CREATE INDEX IF NOT EXISTS backbonepivots_dayid_source_tickid_idx
    ON public.backbonepivots (dayid, source, tickid ASC, ticktime ASC);

CREATE INDEX IF NOT EXISTS backbonemoves_dayid_source_endtickid_idx
    ON public.backbonemoves (dayid, source, endtickid ASC, endtime ASC);

CREATE INDEX IF NOT EXISTS backbonemoves_dayid_source_starttickid_idx
    ON public.backbonemoves (dayid, source, starttickid ASC, starttime ASC);

COMMIT;
