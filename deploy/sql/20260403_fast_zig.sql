BEGIN;

CREATE TABLE IF NOT EXISTS public.fast_zig_state (
    symbol text PRIMARY KEY,
    last_processed_tick_id bigint NOT NULL DEFAULT 0,
    last_pivot_id bigint NOT NULL DEFAULT 0,
    updated_at timestamptz NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.fast_zig_pivots (
    version_id bigserial PRIMARY KEY,
    pivot_id bigint NOT NULL,
    symbol text NOT NULL,
    source_tick_id bigint NOT NULL,
    source_timestamp timestamptz NOT NULL,
    direction text NOT NULL CHECK (direction IN ('high', 'low')),
    pivot_price double precision NOT NULL,
    level integer NOT NULL DEFAULT 1,
    visible_from_tick_id bigint NOT NULL,
    visible_to_tick_id bigint NULL,
    created_at timestamptz NOT NULL DEFAULT NOW(),
    updated_at timestamptz NOT NULL DEFAULT NOW()
);

ALTER TABLE public.fast_zig_pivots
    ADD COLUMN IF NOT EXISTS level integer;

ALTER TABLE public.fast_zig_pivots
    ADD COLUMN IF NOT EXISTS updated_at timestamptz;

UPDATE public.fast_zig_pivots
SET level = 1
WHERE level IS NULL;

UPDATE public.fast_zig_pivots
SET updated_at = COALESCE(updated_at, created_at, NOW())
WHERE updated_at IS NULL;

ALTER TABLE public.fast_zig_pivots
    ALTER COLUMN level SET DEFAULT 1;

ALTER TABLE public.fast_zig_pivots
    ALTER COLUMN level SET NOT NULL;

ALTER TABLE public.fast_zig_pivots
    ALTER COLUMN updated_at SET DEFAULT NOW();

ALTER TABLE public.fast_zig_pivots
    ALTER COLUMN updated_at SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fast_zig_pivots_level_check'
    ) THEN
        ALTER TABLE public.fast_zig_pivots
            ADD CONSTRAINT fast_zig_pivots_level_check CHECK (level >= 1);
    END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS fast_zig_state_symbol_idx
    ON public.fast_zig_state (symbol);

CREATE UNIQUE INDEX IF NOT EXISTS fast_zig_pivots_symbol_current_idx
    ON public.fast_zig_pivots (symbol, pivot_id)
    WHERE visible_to_tick_id IS NULL;

CREATE INDEX IF NOT EXISTS fast_zig_pivots_symbol_source_tick_idx
    ON public.fast_zig_pivots (symbol, source_tick_id, pivot_id, version_id);

CREATE INDEX IF NOT EXISTS fast_zig_pivots_symbol_visible_from_idx
    ON public.fast_zig_pivots (symbol, visible_from_tick_id, pivot_id, version_id);

CREATE INDEX IF NOT EXISTS fast_zig_pivots_symbol_current_order_idx
    ON public.fast_zig_pivots (symbol, pivot_id DESC)
    WHERE visible_to_tick_id IS NULL;

CREATE INDEX IF NOT EXISTS fast_zig_pivots_symbol_current_level_order_idx
    ON public.fast_zig_pivots (symbol, level, pivot_id DESC)
    WHERE visible_to_tick_id IS NULL;

COMMIT;
