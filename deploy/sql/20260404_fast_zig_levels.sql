BEGIN;

ALTER TABLE public.fast_zig_pivots
    ADD COLUMN IF NOT EXISTS level integer;

ALTER TABLE public.fast_zig_pivots
    ADD COLUMN IF NOT EXISTS state text;

ALTER TABLE public.fast_zig_pivots
    ADD COLUMN IF NOT EXISTS updated_at timestamptz;

UPDATE public.fast_zig_pivots
SET level = 0
WHERE level IS NULL;

UPDATE public.fast_zig_pivots
SET state = 'final'
WHERE state IS NULL;

UPDATE public.fast_zig_pivots
SET updated_at = COALESCE(updated_at, created_at, NOW())
WHERE updated_at IS NULL;

ALTER TABLE public.fast_zig_pivots
    ALTER COLUMN level SET DEFAULT 0;

ALTER TABLE public.fast_zig_pivots
    ALTER COLUMN level SET NOT NULL;

ALTER TABLE public.fast_zig_pivots
    ALTER COLUMN state SET DEFAULT 'final';

ALTER TABLE public.fast_zig_pivots
    ALTER COLUMN state SET NOT NULL;

ALTER TABLE public.fast_zig_pivots
    ALTER COLUMN updated_at SET DEFAULT NOW();

ALTER TABLE public.fast_zig_pivots
    ALTER COLUMN updated_at SET NOT NULL;

ALTER TABLE public.fast_zig_pivots
    DROP CONSTRAINT IF EXISTS fast_zig_pivots_level_check;

ALTER TABLE public.fast_zig_pivots
    ADD CONSTRAINT fast_zig_pivots_level_check CHECK (level >= 0);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fast_zig_pivots_state_check'
    ) THEN
        ALTER TABLE public.fast_zig_pivots
            ADD CONSTRAINT fast_zig_pivots_state_check CHECK (state IN ('candidate', 'final'));
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS fast_zig_pivots_symbol_current_level_state_idx
    ON public.fast_zig_pivots (symbol, level, state, pivot_id DESC)
    WHERE visible_to_tick_id IS NULL;

COMMIT;
