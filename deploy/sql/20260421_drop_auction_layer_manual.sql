-- MANUAL REVIEW-ONLY CLEANUP
-- Destructive script. Do not run automatically.
-- Intended to remove only obsolete auction-layer objects from the public schema.
-- Do not add this file to deploy/scripts/deploy-datavis.sh or any automated deploy flow.
--
-- Verification query before running:
-- Lists the known auction-only objects targeted by this script that still exist.
-- SELECT
--     n.nspname AS schema_name,
--     c.relname AS object_name,
--     c.relkind AS object_kind
-- FROM pg_class AS c
-- JOIN pg_namespace AS n
--   ON n.oid = c.relnamespace
-- WHERE n.nspname = 'public'
--   AND c.relname IN (
--       'auctionhistorysession_symbol_endts_startts_idx',
--       'auctionbin',
--       'auctionevent',
--       'auctionhistorybin',
--       'auctionhistoryevent',
--       'auctionhistoryref',
--       'auctionhistorysession',
--       'auctionhistorystate',
--       'auctionref',
--       'auctionsession',
--       'auctionsnap',
--       'auctionstate'
--   )
-- ORDER BY c.relkind, c.relname;
--
-- Verification query before/after running:
-- Confirms the protected core tables remain present.
-- SELECT
--     obj,
--     to_regclass(obj) IS NOT NULL AS present
-- FROM unnest(ARRAY[
--     'public.ticks',
--     'public.backbonepivots',
--     'public.backbonemoves',
--     'public.backbonestate',
--     'public.rects'
-- ]) AS t(obj)
-- ORDER BY obj;

BEGIN;

-- No auction-scoped views were found in repo SQL at the time this file was authored.
-- No standalone auction sequences were found either; owned sequences will be removed
-- automatically if they exist and are owned by dropped tables.

DROP INDEX IF EXISTS public.auctionhistorysession_symbol_endts_startts_idx;

DROP TABLE IF EXISTS
    public.auctionhistorybin,
    public.auctionhistoryevent,
    public.auctionhistoryref,
    public.auctionhistorysession,
    public.auctionhistorystate;

DROP TABLE IF EXISTS
    public.auctionbin,
    public.auctionevent,
    public.auctionref,
    public.auctionsession,
    public.auctionsnap,
    public.auctionstate;

COMMIT;

-- Verification query after running:
-- Should return zero rows when the targeted auction-layer objects are gone.
-- SELECT
--     n.nspname AS schema_name,
--     c.relname AS object_name,
--     c.relkind AS object_kind
-- FROM pg_class AS c
-- JOIN pg_namespace AS n
--   ON n.oid = c.relnamespace
-- WHERE n.nspname = 'public'
--   AND c.relname IN (
--       'auctionhistorysession_symbol_endts_startts_idx',
--       'auctionbin',
--       'auctionevent',
--       'auctionhistorybin',
--       'auctionhistoryevent',
--       'auctionhistoryref',
--       'auctionhistorysession',
--       'auctionhistorystate',
--       'auctionref',
--       'auctionsession',
--       'auctionsnap',
--       'auctionstate'
--   )
-- ORDER BY c.relkind, c.relname;
--
-- Verification query after running:
-- Protected core objects should still report present = true where deployed.
-- SELECT
--     obj,
--     to_regclass(obj) IS NOT NULL AS present
-- FROM unnest(ARRAY[
--     'public.ticks',
--     'public.backbonepivots',
--     'public.backbonemoves',
--     'public.backbonestate',
--     'public.rects'
-- ]) AS t(obj)
-- ORDER BY obj;
