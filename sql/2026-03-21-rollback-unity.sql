DROP VIEW IF EXISTS public.unityrecent;
DROP VIEW IF EXISTS public.unityopen;

DROP INDEX IF EXISTS public.unityeventsignalidx;
DROP INDEX IF EXISTS public.unityeventtickidx;
DROP INDEX IF EXISTS public.unitytradeopenuniq;
DROP INDEX IF EXISTS public.unitytradestatusidx;
DROP INDEX IF EXISTS public.unitysignalfavidx;
DROP INDEX IF EXISTS public.unitytickstateidx;
DROP INDEX IF EXISTS public.unityticktimeidx;
DROP INDEX IF EXISTS public.unityswingendidx;
DROP INDEX IF EXISTS public.unityswingstartidx;
DROP INDEX IF EXISTS public.unitypivottickidx;

DROP TABLE IF EXISTS public.unityevent;
DROP TABLE IF EXISTS public.unitytrade;
DROP TABLE IF EXISTS public.unitysignal;
DROP TABLE IF EXISTS public.unitytick;
DROP TABLE IF EXISTS public.unityswing;
DROP TABLE IF EXISTS public.unitypivot;
DROP TABLE IF EXISTS public.unitystate;
