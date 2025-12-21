-- db/legs.sql
-- Early 3-leg (A-B-C-D) detection per segLine.

CREATE TABLE IF NOT EXISTS legs (
  id BIGSERIAL PRIMARY KEY,
  segm_id INT NOT NULL,
  segline_id INT NOT NULL,
  direction SMALLINT NOT NULL, -- +1 up, -1 down
  created_at TIMESTAMPTZ DEFAULT now(),

  -- detection window / params
  early_end_tick_id BIGINT,
  k_neighborhood INT,
  min_move DOUBLE PRECISION,
  break_buffer DOUBLE PRECISION,
  early_max_ticks INT,

  -- core points
  a_tick_id BIGINT NOT NULL,
  b_tick_id BIGINT,
  c_tick_id BIGINT,
  d_tick_id BIGINT,

  -- prices at points (mid + kal)
  a_mid DOUBLE PRECISION,
  a_kal DOUBLE PRECISION,
  b_mid DOUBLE PRECISION,
  b_kal DOUBLE PRECISION,
  c_mid DOUBLE PRECISION,
  c_kal DOUBLE PRECISION,
  d_mid DOUBLE PRECISION,
  d_kal DOUBLE PRECISION,

  -- derived metrics
  ab_ticks INT,
  bc_ticks INT,
  cd_ticks INT,
  ab_move DOUBLE PRECISION,
  bc_move DOUBLE PRECISION,
  cd_move DOUBLE PRECISION,
  bc_retrace_pct DOUBLE PRECISION,

  -- status / quality
  has_b BOOLEAN NOT NULL DEFAULT false,
  has_c BOOLEAN NOT NULL DEFAULT false,
  has_d BOOLEAN NOT NULL DEFAULT false,
  reason TEXT
);

-- Idempotent unique constraint on segline_id (1 row per segLine)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname='legs_segline_uniq'
  ) THEN
    CREATE UNIQUE INDEX legs_segline_uniq ON legs (segline_id);
  END IF;
END$$;

CREATE INDEX IF NOT EXISTS legs_segm_idx ON legs (segm_id);
CREATE INDEX IF NOT EXISTS legs_segline_idx ON legs (segline_id);
CREATE INDEX IF NOT EXISTS legs_segm_segline_idx ON legs (segm_id, segline_id);
