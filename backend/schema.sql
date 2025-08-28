# PATH: backend/schema.sql
-- Destructive reset for the ML schema (keeps ticks)
BEGIN;

-- Drop all public tables except 'ticks'
DO $$
DECLARE r record;
BEGIN
  FOR r IN
    SELECT tablename
    FROM pg_tables
    WHERE schemaname = 'public' AND tablename <> 'ticks'
  LOOP
    EXECUTE format('DROP TABLE IF EXISTS %I CASCADE;', r.tablename);
  END LOOP;
END $$;

COMMIT;

-- New compact tables (all have id BIGSERIAL PK)
BEGIN;

CREATE TABLE segm (
  id        BIGSERIAL PRIMARY KEY,
  start_id  BIGINT NOT NULL,
  end_id    BIGINT NOT NULL,
  start_ts  TIMESTAMPTZ NOT NULL,
  end_ts    TIMESTAMPTZ NOT NULL,
  dir       TEXT NOT NULL,        -- overall first->last (may be 'up'/'dn')
  span      NUMERIC NOT NULL,     -- end_mid - start_mid
  len       BIGINT NOT NULL       -- #ticks
);
CREATE INDEX segm_start_id_idx ON segm(start_id);
CREATE INDEX segm_end_id_idx   ON segm(end_id);
CREATE INDEX segm_time_idx     ON segm(start_ts, end_ts);

-- NEW: big intra-segment movements (>= BIG_USD, e.g. $10)
CREATE TABLE bigm (
  id        BIGSERIAL PRIMARY KEY,
  segm_id   BIGINT NOT NULL REFERENCES segm(id) ON DELETE CASCADE,
  a_id      BIGINT NOT NULL,
  b_id      BIGINT NOT NULL,
  a_ts      TIMESTAMPTZ NOT NULL,
  b_ts      TIMESTAMPTZ NOT NULL,
  dir       TEXT NOT NULL,        -- 'up'|'dn'
  move      NUMERIC NOT NULL,     -- absolute dollars
  ticks     BIGINT NOT NULL
);
CREATE INDEX bigm_segm_idx ON bigm(segm_id);
CREATE INDEX bigm_ab_idx   ON bigm(a_id, b_id);

-- Small ~$2 moves aligned with the *current* big move inside a segment
CREATE TABLE smal (
  id        BIGSERIAL PRIMARY KEY,
  segm_id   BIGINT NOT NULL REFERENCES segm(id) ON DELETE CASCADE,
  a_id      BIGINT NOT NULL,
  b_id      BIGINT NOT NULL,
  a_ts      TIMESTAMPTZ NOT NULL,
  b_ts      TIMESTAMPTZ NOT NULL,
  dir       TEXT NOT NULL,
  move      NUMERIC NOT NULL,
  ticks     BIGINT NOT NULL
);
CREATE INDEX smal_segm_idx ON smal(segm_id);
CREATE INDEX smal_ab_idx   ON smal(a_id, b_id);

-- Predictions emitted on qualifying small moves; labeled within same segment
CREATE TABLE pred (
  id              BIGSERIAL PRIMARY KEY,
  segm_id         BIGINT NOT NULL REFERENCES segm(id) ON DELETE CASCADE,
  at_id           BIGINT NOT NULL,
  at_ts           TIMESTAMPTZ NOT NULL,
  dir             TEXT NOT NULL,
  goal_usd        NUMERIC NOT NULL DEFAULT 2,
  hit             BOOLEAN,
  resolved_at_id  BIGINT,
  resolved_at_ts  TIMESTAMPTZ
);
CREATE INDEX pred_segm_idx ON pred(segm_id);
CREATE INDEX pred_at_idx   ON pred(at_id);

-- Segment outcomes (summary)
CREATE TABLE outcome (
  id           BIGSERIAL PRIMARY KEY,
  time         TIMESTAMPTZ NOT NULL,   -- first tick ts
  duration     BIGINT NOT NULL,        -- seconds (end-start)
  predictions  BIGINT NOT NULL,
  ratio        NUMERIC NOT NULL,       -- [-1..1], 2 decimals
  segm_id      BIGINT NOT NULL REFERENCES segm(id) ON DELETE CASCADE
);
CREATE INDEX outcome_time_idx ON outcome(time DESC);
CREATE INDEX outcome_segm_idx ON outcome(segm_id);

-- Simple KV state (resumable)
CREATE TABLE stat (
  id   BIGSERIAL PRIMARY KEY,
  key  TEXT NOT NULL UNIQUE,
  val  BIGINT
);

COMMIT;

-- Helpful indexes for ticks (no-op if they exist)
DO $$
DECLARE has_ts text := NULL;
BEGIN
  SELECT column_name INTO has_ts
  FROM information_schema.columns
  WHERE table_name='ticks' AND column_name IN ('ts','timestamp','time','created_at')
  ORDER BY CASE column_name WHEN 'ts' THEN 1 WHEN 'timestamp' THEN 2 WHEN 'time' THEN 3 ELSE 4 END
  LIMIT 1;

  IF has_ts IS NOT NULL THEN
    EXECUTE format('CREATE INDEX IF NOT EXISTS ticks_%s_idx ON ticks(%I);', has_ts, has_ts);
  END IF;

  IF EXISTS (SELECT 1 FROM information_schema.columns WHERE table_name='ticks' AND column_name='id') THEN
    EXECUTE 'CREATE INDEX IF NOT EXISTS ticks_id_idx ON ticks(id);';
  END IF;
END $$;
