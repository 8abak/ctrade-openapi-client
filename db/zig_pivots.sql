-- db/zig_pivots.sql
-- Final zig pivots per segment (local extrema on 21-tick windows).

CREATE TABLE IF NOT EXISTS zig_pivots (
  id BIGSERIAL PRIMARY KEY,
  segm_id BIGINT NOT NULL REFERENCES segms(id) ON DELETE CASCADE,
  tick_id BIGINT NOT NULL REFERENCES ticks(id) ON DELETE CASCADE,
  ts TIMESTAMPTZ NOT NULL,
  price DOUBLE PRECISION NOT NULL,
  direction TEXT NOT NULL CHECK (direction IN ('high', 'low')),
  pivot_index INT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS zig_pivots_segm_idx ON zig_pivots (segm_id);
CREATE INDEX IF NOT EXISTS zig_pivots_segm_order_idx ON zig_pivots (segm_id, pivot_index);
CREATE INDEX IF NOT EXISTS zig_pivots_segm_ts_idx ON zig_pivots (segm_id, ts);
