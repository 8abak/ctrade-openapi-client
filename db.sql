-- === SCHEMA: zig tables & pipeline state ===
CREATE TABLE IF NOT EXISTS zigzags (
  id BIGSERIAL PRIMARY KEY,
  day DATE NOT NULL,
  start_tickid BIGINT NOT NULL,
  end_tickid BIGINT NOT NULL,
  start_time TIMESTAMPTZ NOT NULL,
  end_time TIMESTAMPTZ NOT NULL,
  direction VARCHAR(2) NOT NULL CHECK (direction IN ('up','dn')),
  start_price NUMERIC(12,5) NOT NULL,
  end_price NUMERIC(12,5) NOT NULL,
  high_price NUMERIC(12,5) NOT NULL,
  low_price NUMERIC(12,5) NOT NULL,
  threshold NUMERIC(12,5) NOT NULL,
  tick_count INT NOT NULL,
  duration_sec INT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_zigzags_day ON zigzags(day);
CREATE INDEX IF NOT EXISTS idx_zigzags_start_end ON zigzags(start_tickid, end_tickid);

CREATE TABLE IF NOT EXISTS zig_features (
  zig_id BIGINT PRIMARY KEY REFERENCES zigzags(id) ON DELETE CASCADE,
  price_change NUMERIC(12,5) NOT NULL,
  abs_change NUMERIC(12,5) NOT NULL,
  slope_per_sec NUMERIC(12,8) NOT NULL,
  mean_spread NUMERIC(12,6),
  std_mid NUMERIC(12,6),
  realized_vol NUMERIC(12,6),
  mae NUMERIC(12,6),
  mdd NUMERIC(12,6)
);

CREATE TABLE IF NOT EXISTS tick_features (
  id BIGSERIAL PRIMARY KEY,
  zig_id BIGINT REFERENCES zigzags(id) ON DELETE CASCADE,
  tickid BIGINT NOT NULL,
  ts TIMESTAMPTZ NOT NULL,
  pos_ratio NUMERIC(12,6) NOT NULL,
  progress_norm NUMERIC(12,6) NOT NULL,
  ret1 NUMERIC(12,6),
  ret5 NUMERIC(12,6),
  vol20 NUMERIC(12,6),
  drawdown NUMERIC(12,6),
  seconds_since INT NOT NULL,
  target_no_return BOOLEAN NOT NULL,
  pred_proba NUMERIC(6,4),
  pred_is_earliest BOOLEAN
);
CREATE INDEX IF NOT EXISTS idx_tick_features_zig ON tick_features(zig_id);
CREATE INDEX IF NOT EXISTS idx_tick_features_tickid ON tick_features(tickid);

CREATE TABLE IF NOT EXISTS no_return_points (
  zig_id BIGINT PRIMARY KEY REFERENCES zigzags(id) ON DELETE CASCADE,
  tickid BIGINT NOT NULL,
  ts TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS pipeline_state (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL,
  updated_at TIMESTAMPTZ DEFAULT now()
);

-- Helpful indexes on ticks
CREATE INDEX IF NOT EXISTS idx_ticks_day ON ticks((timestamp::date));
CREATE INDEX IF NOT EXISTS idx_ticks_id ON ticks(id);
