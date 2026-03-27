from __future__ import annotations

import math
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from datavis.ott import OttConfig, compute_ott_rows


DEFAULT_CONFIG = OttConfig(source="mid", matype="VAR", length=2, percent=1.4)


def sample_ticks() -> List[Dict[str, Any]]:
    base = datetime(2026, 3, 20, 0, 0, tzinfo=timezone.utc)
    mids = [
        3020.10,
        3020.35,
        3019.95,
        3021.20,
        3022.40,
        3021.75,
        3023.10,
        3022.55,
        3024.05,
        3025.20,
        3024.45,
        3023.90,
        3024.85,
        3026.30,
        3025.95,
        3027.10,
    ]
    rows: List[Dict[str, Any]] = []
    for index, mid in enumerate(mids, start=1):
        rows.append(
            {
                "id": index,
                "symbol": "XAUUSD",
                "timestamp": base + timedelta(seconds=index),
                "bid": mid - 0.10,
                "ask": mid + 0.10,
                "mid": mid,
                "spread": 0.20,
                "price": mid,
            }
        )
    return rows


def reference_default_ott(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    src_values = [float(row["mid"]) for row in rows]
    length = DEFAULT_CONFIG.length
    percent = DEFAULT_CONFIG.percent
    valpha = 2.0 / float(length + 1)
    outputs: List[Dict[str, Any]] = []
    var_values: List[float] = []
    long_stops: List[Optional[float]] = []
    short_stops: List[Optional[float]] = []
    dirs: List[int] = []
    ott_values: List[Optional[float]] = []

    for index, src in enumerate(src_values):
        prev_src = src_values[index - 1] if index > 0 else None
        vud1 = (src - prev_src) if prev_src is not None and src > prev_src else 0.0
        vdd1 = (prev_src - src) if prev_src is not None and src < prev_src else 0.0
        vud = sum(
            (src_values[pos] - src_values[pos - 1]) if pos > 0 and src_values[pos] > src_values[pos - 1] else 0.0
            for pos in range(max(1, index - 8), index + 1)
        )
        vdd = sum(
            (src_values[pos - 1] - src_values[pos]) if pos > 0 and src_values[pos] < src_values[pos - 1] else 0.0
            for pos in range(max(1, index - 8), index + 1)
        )
        denom = vud + vdd
        vcmo = 0.0 if abs(denom) <= 1e-12 else (vud - vdd) / denom
        prev_var = var_values[index - 1] if index > 0 else 0.0
        mavg = (valpha * abs(vcmo) * src) + ((1.0 - (valpha * abs(vcmo))) * prev_var)
        var_values.append(mavg)

        fark = mavg * percent * 0.01
        long_stop = mavg - fark
        long_stop_prev = long_stops[index - 1] if index > 0 and long_stops[index - 1] is not None else long_stop
        if mavg > long_stop_prev:
            long_stop = max(long_stop, long_stop_prev)

        short_stop = mavg + fark
        short_stop_prev = short_stops[index - 1] if index > 0 and short_stops[index - 1] is not None else short_stop
        if mavg < short_stop_prev:
            short_stop = min(short_stop, short_stop_prev)

        direction_prev = dirs[index - 1] if index > 0 else 1
        direction = direction_prev
        if direction_prev == -1 and mavg > short_stop_prev:
            direction = 1
        elif direction_prev == 1 and mavg < long_stop_prev:
            direction = -1

        mt = long_stop if direction == 1 else short_stop
        ott = mt * (200.0 + percent) / 200.0 if mavg > mt else mt * (200.0 - percent) / 200.0

        ott2 = ott_values[index - 2] if index >= 2 else None
        ott3 = ott_values[index - 3] if index >= 3 else None
        prev_mavg = var_values[index - 1] if index > 0 else None
        prev_ott2 = ott_values[index - 3] if index >= 3 else None
        prev_ott3 = ott_values[index - 4] if index >= 4 else None
        prev_src_for_cross = src_values[index - 1] if index > 0 else None

        outputs.append(
            {
                "tickid": rows[index]["id"],
                "price": src,
                "mavg": mavg,
                "fark": fark,
                "longstop": long_stop,
                "shortstop": short_stop,
                "dir": direction,
                "mt": mt,
                "ott": ott,
                "ott2": ott2,
                "ott3": ott3,
                "supportbuy": bool(
                    mavg is not None
                    and ott2 is not None
                    and prev_mavg is not None
                    and prev_ott2 is not None
                    and mavg > ott2
                    and prev_mavg <= prev_ott2
                ),
                "supportsell": bool(
                    mavg is not None
                    and ott2 is not None
                    and prev_mavg is not None
                    and prev_ott2 is not None
                    and mavg < ott2
                    and prev_mavg >= prev_ott2
                ),
                "pricebuy": bool(
                    ott2 is not None
                    and prev_src_for_cross is not None
                    and prev_ott2 is not None
                    and src > ott2
                    and prev_src_for_cross <= prev_ott2
                ),
                "pricesell": bool(
                    ott2 is not None
                    and prev_src_for_cross is not None
                    and prev_ott2 is not None
                    and src < ott2
                    and prev_src_for_cross >= prev_ott2
                ),
                "colorbuy": bool(
                    ott2 is not None
                    and ott3 is not None
                    and prev_ott2 is not None
                    and prev_ott3 is not None
                    and ott2 > ott3
                    and prev_ott2 <= prev_ott3
                ),
                "colorsell": bool(
                    ott2 is not None
                    and ott3 is not None
                    and prev_ott2 is not None
                    and prev_ott3 is not None
                    and ott2 < ott3
                    and prev_ott2 >= prev_ott3
                ),
            }
        )

        long_stops.append(long_stop)
        short_stops.append(short_stop)
        dirs.append(direction)
        ott_values.append(ott)

    return outputs


class OttValidationTest(unittest.TestCase):
    def test_default_var_matches_reference(self) -> None:
        rows = sample_ticks()
        actual, _ = compute_ott_rows(rows, DEFAULT_CONFIG)
        expected = reference_default_ott(rows)
        for actual_row, expected_row in zip(actual, expected):
            self.assertEqual(actual_row["tickid"], expected_row["tickid"])
            self.assertTrue(math.isclose(actual_row["price"], expected_row["price"], rel_tol=0.0, abs_tol=1e-9))
            for field in ("mavg", "fark", "longstop", "shortstop", "mt", "ott", "ott2", "ott3"):
                actual_value = actual_row[field]
                expected_value = expected_row[field]
                if expected_value is None:
                    self.assertIsNone(actual_value, field)
                else:
                    self.assertTrue(math.isclose(float(actual_value), float(expected_value), rel_tol=0.0, abs_tol=1e-9), field)
            for field in ("dir", "supportbuy", "supportsell", "pricebuy", "pricesell", "colorbuy", "colorsell"):
                self.assertEqual(actual_row[field], expected_row[field], field)


if __name__ == "__main__":
    unittest.main()
