CREATE TABLE IF NOT EXISTS public.trulehit (
    id BIGSERIAL PRIMARY KEY,
    dayid BIGINT NOT NULL,
    tepisodeid BIGINT NOT NULL,
    tconfirmid BIGINT NOT NULL,
    dir TEXT NOT NULL,
    rulename TEXT NOT NULL,
    rulever TEXT NOT NULL,
    ishit BOOLEAN NOT NULL,
    score DOUBLE PRECISION NOT NULL,
    reason TEXT NOT NULL,
    buildver TEXT NOT NULL DEFAULT 'layer4.v1',
    createdts TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS trulehit_dayid_idx
ON public.trulehit (dayid);

CREATE INDEX IF NOT EXISTS trulehit_rule_slice_idx
ON public.trulehit (dayid, dir, rulename, rulever, buildver);

CREATE INDEX IF NOT EXISTS trulehit_tconfirmid_idx
ON public.trulehit (tconfirmid);

CREATE UNIQUE INDEX IF NOT EXISTS trulehit_rule_hit_uniq
ON public.trulehit (tconfirmid, dir, rulename, rulever, buildver);
