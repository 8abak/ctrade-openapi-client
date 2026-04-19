BEGIN;

DROP INDEX IF EXISTS public.auctionhistorysession_symbol_endts_startts_idx;

DROP TABLE IF EXISTS public.auctionhistorybin;
DROP TABLE IF EXISTS public.auctionhistoryref;
DROP TABLE IF EXISTS public.auctionhistoryevent;
DROP TABLE IF EXISTS public.auctionhistorystate;
DROP TABLE IF EXISTS public.auctionhistorysession;

DROP TABLE IF EXISTS public.auctionbin;
DROP TABLE IF EXISTS public.auctionref;
DROP TABLE IF EXISTS public.auctionevent;
DROP TABLE IF EXISTS public.auctionstate;
DROP TABLE IF EXISTS public.auctionsnap;
DROP TABLE IF EXISTS public.auctionsession;

DO $cleanup$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_namespace
        WHERE nspname = 'research'
    ) THEN
        EXECUTE 'DROP VIEW IF EXISTS research.vw_loop_status';
        EXECUTE 'DROP TABLE IF EXISTS research.control_operator_action';
        EXECUTE 'DROP TABLE IF EXISTS research.candidate_library';
        EXECUTE 'DROP TABLE IF EXISTS research.divergence_event';
        EXECUTE 'DROP TABLE IF EXISTS research.engineering_smoketest';
        EXECUTE 'DROP TABLE IF EXISTS research.engineering_patch';
        EXECUTE 'DROP TABLE IF EXISTS research.engineering_action';
        EXECUTE 'DROP TABLE IF EXISTS research.engineering_incident';
        EXECUTE 'DROP TABLE IF EXISTS research.engineering_journal';
        EXECUTE 'DROP TABLE IF EXISTS research.engineering_state';
        EXECUTE 'DROP TABLE IF EXISTS research.candidate_result';
        EXECUTE 'DROP TABLE IF EXISTS research.feature_snapshot';
        EXECUTE 'DROP TABLE IF EXISTS research.entry_label';
        EXECUTE 'DROP TABLE IF EXISTS research.artifact';
        EXECUTE 'DROP TABLE IF EXISTS research.decision';
        EXECUTE 'DROP TABLE IF EXISTS research.runsummary';
        EXECUTE 'DROP TABLE IF EXISTS research.run';
        EXECUTE 'DROP TABLE IF EXISTS research.job';
        EXECUTE 'DROP TABLE IF EXISTS research.journal';
        EXECUTE 'DROP TABLE IF EXISTS research.state';
        EXECUTE 'DROP SCHEMA IF EXISTS research';
    END IF;
END
$cleanup$;

COMMIT;
