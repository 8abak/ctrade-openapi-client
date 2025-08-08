-- Peaks: extrema from find_peaks (small=1, big=2)
CREATE TABLE IF NOT EXISTS peaks (
  id BIGSERIAL PRIMARY KEY,
  ts timestamptz NOT NULL,
  tickid BIGINT NOT NULL,
  price DOUBLE PRECISION NOT NULL,
  kind SMALLINT NOT NULL,       -- +1=high, -1=low
  scale SMALLINT NOT NULL,      -- 1=small, 2=big
  prominence DOUBLE PRECISION,
  width DOUBLE PRECISION,
  created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS peaks_ts_idx    ON peaks (ts);
CREATE INDEX IF NOT EXISTS peaks_scale_idx ON peaks (scale, ts);

-- Idempotency (1 per (scale,tickid,kind))
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname='peaks_uniq'
  ) THEN
    CREATE UNIQUE INDEX peaks_uniq ON peaks (scale, tickid, kind);
  END IF;
END$$;

-- Swings: approved moves between alternating peaks
CREATE TABLE IF NOT EXISTS swings (
  id BIGSERIAL PRIMARY KEY,
  scale SMALLINT NOT NULL,           -- 1=small, 2=big
  direction SMALLINT NOT NULL,       -- +1 up, -1 down
  start_ts timestamptz NOT NULL,
  end_ts timestamptz NOT NULL,
  start_tickid BIGINT NOT NULL,
  end_tickid BIGINT NOT NULL,
  start_price DOUBLE PRECISION NOT NULL,
  end_price DOUBLE PRECISION NOT NULL,
  magnitude DOUBLE PRECISION NOT NULL,
  duration_sec INTEGER NOT NULL,
  velocity DOUBLE PRECISION NOT NULL,
  status SMALLINT NOT NULL DEFAULT 1,  -- 1=closed
  created_at timestamptz DEFAULT now()
);

CREATE INDEX IF NOT EXISTS swings_scale_ts_idx ON swings (scale, end_ts);

-- Idempotency (1 per (scale, start_tickid, end_tickid))
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes WHERE schemaname='public' AND indexname='swings_uniq'
  ) THEN
    CREATE UNIQUE INDEX swings_uniq ON swings (scale, start_tickid, end_tickid);
  END IF;
END$$;

-- Progress log per day (optional but handy)
CREATE TABLE IF NOT EXISTS daily_runs (
  run_date date PRIMARY KEY,
  status text NOT NULL,
  small_swings int DEFAULT 0,
  big_swings int DEFAULT 0,
  note text,
  created_at timestamptz DEFAULT now()
);
