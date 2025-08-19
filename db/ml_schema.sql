-- db/ml_schema.sql
-- ML schema for trend continuation pipeline (idempotent)

CREATE TABLE IF NOT EXISTS kalman_states(
  tickid BIGINT PRIMARY KEY,
  timestamp TIMESTAMPTZ NOT NULL,
  price DOUBLE PRECISION NOT NULL,
  level DOUBLE PRECISION NOT NULL,
  slope DOUBLE PRECISION NOT NULL,
  var DOUBLE PRECISION NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_kalman_timestamp ON kalman_states (timestamp);
CREATE INDEX IF NOT EXISTS idx_kalman_tickid ON kalman_states (tickid);

CREATE TABLE IF NOT EXISTS ml_features(
  tickid BIGINT PRIMARY KEY,
  timestamp TIMESTAMPTZ,
  level DOUBLE PRECISION,
  slope DOUBLE PRECISION,
  residual DOUBLE PRECISION,
  vol_ewstd DOUBLE PRECISION,
  vol_ewstd_long DOUBLE PRECISION,
  r50 DOUBLE PRECISION,
  r200 DOUBLE PRECISION,
  r1000 DOUBLE PRECISION,
  rsi DOUBLE PRECISION,
  stoch_k DOUBLE PRECISION,
  stoch_d DOUBLE PRECISION,
  hilbert_amp DOUBLE PRECISION,
  hilbert_phase DOUBLE PRECISION,
  vwap_dist DOUBLE PRECISION,
  r2_lin DOUBLE PRECISION,
  tod_bucket SMALLINT
);
CREATE INDEX IF NOT EXISTS idx_mlf_timestamp ON ml_features (timestamp);
CREATE INDEX IF NOT EXISTS idx_mlf_tickid ON ml_features (tickid);

CREATE TABLE IF NOT EXISTS trend_labels(
  tickid BIGINT PRIMARY KEY,
  direction SMALLINT NOT NULL, -- -1,0,+1
  is_segment_start BOOLEAN NOT NULL DEFAULT FALSE,
  meta JSONB
);
CREATE INDEX IF NOT EXISTS idx_labels_tickid ON trend_labels (tickid);

CREATE TABLE IF NOT EXISTS models(
  model_id TEXT PRIMARY KEY,
  created_at TIMESTAMPTZ DEFAULT NOW(),
  algo TEXT NOT NULL,
  params JSONB,
  calib JSONB,
  notes TEXT
);

CREATE TABLE IF NOT EXISTS predictions(
  tickid BIGINT PRIMARY KEY,
  model_id TEXT NOT NULL,
  p_up DOUBLE PRECISION,
  p_neu DOUBLE PRECISION,
  p_dn DOUBLE PRECISION,
  s_curve JSONB,
  decided_label SMALLINT
);
CREATE INDEX IF NOT EXISTS idx_pred_tickid ON predictions (tickid);

CREATE TABLE IF NOT EXISTS walk_runs(
  run_id TEXT PRIMARY KEY,
  train_start BIGINT,
  train_end BIGINT,
  test_start BIGINT,
  test_end BIGINT,
  model_id TEXT,
  metrics JSONB,
  confirmed BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ DEFAULT NOW()
);
