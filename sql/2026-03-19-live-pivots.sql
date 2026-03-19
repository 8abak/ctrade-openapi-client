CREATE TABLE IF NOT EXISTS public.pivots (
    id BIGSERIAL PRIMARY KEY,
    dayid BIGINT NOT NULL,
    layer TEXT NOT NULL,
    rev DOUBLE PRECISION NOT NULL,
    tickid BIGINT NOT NULL,
    ts TIMESTAMPTZ NOT NULL,
    px DOUBLE PRECISION NOT NULL,
    ptype CHAR(1) NOT NULL,
    pivotno INTEGER NOT NULL,
    dayrow INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS pivots_day_layer_tick_idx
ON public.pivots (dayid, layer, tickid);

CREATE INDEX IF NOT EXISTS pivots_tickid_idx
ON public.pivots (tickid);

CREATE TABLE IF NOT EXISTS public.pivotcalc_state (
    symbol text NOT NULL,
    dayid bigint NOT NULL,
    layer text NOT NULL,
    rev double precision NOT NULL,
    last_tick_id bigint NOT NULL DEFAULT 0,
    pivotno integer NOT NULL DEFAULT 0,
    anchor_tickid bigint NULL,
    anchor_ts timestamptz NULL,
    anchor_px double precision NULL,
    anchor_dayrow integer NULL,
    cand_dir integer NULL,
    cand_tickid bigint NULL,
    cand_ts timestamptz NULL,
    cand_px double precision NULL,
    cand_dayrow integer NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (symbol, dayid, layer)
);
