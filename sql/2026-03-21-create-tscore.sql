CREATE TABLE IF NOT EXISTS public.tscore (
    id BIGSERIAL PRIMARY KEY,
    dayid BIGINT NOT NULL,
    tepisodeid BIGINT NOT NULL,
    tconfirmid BIGINT NOT NULL,
    dir TEXT NOT NULL,
    scorename TEXT NOT NULL,
    scorever TEXT NOT NULL,
    structurescore DOUBLE PRECISION NOT NULL,
    contextscore DOUBLE PRECISION NOT NULL,
    truthscore DOUBLE PRECISION NOT NULL,
    penaltyscore DOUBLE PRECISION NOT NULL,
    totalscore DOUBLE PRECISION NOT NULL,
    scoregrade TEXT NOT NULL,
    reason TEXT NOT NULL,
    sourcebuildver TEXT NOT NULL,
    createdts TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS tscore_dayid_idx
ON public.tscore (dayid);

CREATE INDEX IF NOT EXISTS tscore_day_dir_idx
ON public.tscore (dayid, dir);

CREATE INDEX IF NOT EXISTS tscore_score_slice_idx
ON public.tscore (scorename, scorever);

CREATE INDEX IF NOT EXISTS tscore_totalscore_idx
ON public.tscore (totalscore);

CREATE INDEX IF NOT EXISTS tscore_scoregrade_idx
ON public.tscore (scoregrade);

CREATE INDEX IF NOT EXISTS tscore_tconfirmid_idx
ON public.tscore (tconfirmid);

CREATE UNIQUE INDEX IF NOT EXISTS tscore_confirm_score_uniq
ON public.tscore (tconfirmid, scorename, scorever);
