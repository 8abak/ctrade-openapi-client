# PATH: backend/schema.sql
-- WARNING: Destructive reset. Drops all public tables except `ticks`, then creates the new schema.
-- Run this only if you know what you're doing.

BEGIN;

-- Drop everything except 'ticks'
DO $$
DECLARE
    r record;
BEGIN
    FOR r IN
        SELECT tablename
        FROM pg_tables
        WHERE schemaname = 'public' AND tablename <> 'ticks'
    LOOP
        EXECUTE format('DROP TABLE IF EXISTS public.%I CASCADE;', r.tablename);
    END LOOP;
END$$;

-- Core tables (short names). All tables start with `id BIGSERIAL PRIMARY KEY`.

-- Segment table: gap-delimited segments (>3 minutes)
CREATE TABLE IF NOT EXISTS segm (
    id          BIGSERIAL PRIMARY KEY,
    start_id    BIGINT NOT NULL,
    end_id      BIGINT NOT NULL,
    start_ts    TIMESTAMPTZ NOT NULL,
    end_ts      TIMESTAMPTZ NOT NULL,
    dir         TEXT NOT NULL CHECK (dir IN ('up','dn')),
    span        NUMERIC NOT NULL,                     -- end_mid - start_mid (signed)
    len         INTEGER NOT NULL                      -- number of ticks in segment
);

CREATE INDEX IF NOT EXISTS segm_start_id_idx ON segm (start_id);
CREATE INDEX IF NOT EXISTS segm_end_id_idx   ON segm (end_id);
CREATE INDEX IF NOT EXISTS segm_start_ts_idx ON segm (start_ts);
CREATE INDEX IF NOT EXISTS segm_end_ts_idx   ON segm (end_ts);

-- Small ~$2 moves aligned with segment direction
CREATE TABLE IF NOT EXISTS smal (
    id          BIGSERIAL PRIMARY KEY,
    segm_id     BIGINT NOT NULL REFERENCES segm(id) ON DELETE CASCADE,
    a_id        BIGINT NOT NULL,
    b_id        BIGINT NOT NULL,
    a_ts        TIMESTAMPTZ NOT NULL,
    b_ts        TIMESTAMPTZ NOT NULL,
    dir         TEXT NOT NULL CHECK (dir IN ('up','dn')),
    move        NUMERIC NOT NULL,                     -- absolute dollars
    ticks       INTEGER NOT NULL                      -- count (b_index - a_index + 1)
);

CREATE INDEX IF NOT EXISTS smal_segm_idx ON smal (segm_id);
CREATE INDEX IF NOT EXISTS smal_b_id_idx ON smal (b_id);

-- Predictions within a segment for continuation with big direction
CREATE TABLE IF NOT EXISTS pred (
    id              BIGSERIAL PRIMARY KEY,
    segm_id         BIGINT NOT NULL REFERENCES segm(id) ON DELETE CASCADE,
    at_id           BIGINT NOT NULL,
    at_ts           TIMESTAMPTZ NOT NULL,
    dir             TEXT NOT NULL CHECK (dir IN ('up','dn')),
    goal_usd        NUMERIC NOT NULL DEFAULT 2,
    hit             BOOLEAN NULL,                     -- true/false when resolved, NULL if unresolved at segment end
    resolved_at_id  BIGINT NULL,
    resolved_at_ts  TIMESTAMPTZ NULL
);

CREATE INDEX IF NOT EXISTS pred_segm_idx        ON pred (segm_id);
CREATE INDEX IF NOT EXISTS pred_at_id_idx       ON pred (at_id);
CREATE INDEX IF NOT EXISTS pred_resolved_id_idx ON pred (resolved_at_id);

-- Outcome per segment
CREATE TABLE IF NOT EXISTS outcome (
    id           BIGSERIAL PRIMARY KEY,
    time         TIMESTAMPTZ NOT NULL,               -- first tick ts of segment
    duration     INTEGER NOT NULL,                   -- seconds
    predictions  INTEGER NOT NULL,
    ratio        NUMERIC NOT NULL,                   -- 1 if all right, -1 if all wrong, 0 if equal, else (R-W)/(R+W) rounded 2 decimals
    segm_id      BIGINT NOT NULL REFERENCES segm(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS outcome_time_idx    ON outcome (time DESC);
CREATE INDEX IF NOT EXISTS outcome_segm_idx    ON outcome (segm_id);

-- Key/val state (single row used for last_done_tick_id)
CREATE TABLE IF NOT EXISTS stat (
    id   BIGSERIAL PRIMARY KEY,
    key  TEXT UNIQUE NOT NULL,
    val  TEXT NOT NULL
);

COMMIT;
