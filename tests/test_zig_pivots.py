import jobs.buildZigPivots as zig


def _make_ticks(prices):
    ticks = []
    for i, price in enumerate(prices):
        ticks.append(zig.TickRow(id=i, ts=i, price=float(price)))
    return ticks


def test_detect_local_extreme_high_low():
    prices = [10.0] * 21
    prices[10] = 20.0
    assert zig._detect_local_extreme(prices, 10, 10) == "high"

    prices = [10.0] * 21
    prices[10] = 0.0
    assert zig._detect_local_extreme(prices, 10, 10) == "low"


def test_detect_local_extreme_strict_ties():
    prices = [10.0] * 21
    prices[10] = 10.0
    prices[9] = 10.0
    assert zig._detect_local_extreme(prices, 10, 10) is None


def test_compute_zig_pivots_short_segment():
    ticks = _make_ticks([100.0] * 10)
    assert zig.compute_zig_pivots(ticks) == []


def test_compute_zig_pivots_alternation_and_replacement():
    prices = [100.0] * 75
    prices[10] = 90.0   # first low candidate
    prices[30] = 120.0  # first high candidate
    prices[45] = 130.0  # higher high candidate (replace)
    prices[60] = 80.0   # next low candidate

    ticks = _make_ticks(prices)
    pivots = zig.compute_zig_pivots(ticks)

    assert [p["tick_id"] for p in pivots] == [0, 10, 45, 60]
    assert [p["direction"] for p in pivots] == ["high", "low", "high", "low"]
    assert pivots[-1]["tick_id"] == 60


def test_save_zig_pivots_builds_rows(monkeypatch):
    pivots = [
        {"tick_id": 1, "ts": "2024-01-01T00:00:00Z", "price": 100.0, "direction": "high", "pivot_index": 0},
        {"tick_id": 2, "ts": "2024-01-01T00:00:01Z", "price": 90.0, "direction": "low", "pivot_index": 1},
    ]

    called = {}

    def fake_clear(conn, segm_id):
        called["cleared"] = segm_id

    monkeypatch.setattr(zig, "clear_zig_pivots", fake_clear)

    captured = {}

    def fake_execute_values(cur, sql, rows, page_size=5000):
        captured["rows"] = rows
        captured["sql"] = sql
        captured["page_size"] = page_size

    monkeypatch.setattr(zig.psycopg2.extras, "execute_values", fake_execute_values)

    class DummyCursor:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

    monkeypatch.setattr(zig, "dict_cur", lambda conn: DummyCursor())

    n = zig.save_zig_pivots(conn=object(), segm_id=3, pivots=pivots)

    assert n == 2
    assert called["cleared"] == 3
    assert captured["rows"] == [
        (3, 1, "2024-01-01T00:00:00Z", 100.0, "high", 0),
        (3, 2, "2024-01-01T00:00:01Z", 90.0, "low", 1),
    ]
