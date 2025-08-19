# ml/labeler.py
from typing import List, Dict, Any

from .db import upsert_many

def label_trend(features_rows: List[Dict[str, Any]], z_min: float=0.8, kappa_max: float=0.25, K: int=20) -> List[Dict[str, Any]]:
    rows = sorted(features_rows, key=lambda r: r["tickid"])
    out = []
    prev_z = None
    cur_dir = 0
    seg_count = 0

    for r in rows:
        vol = r.get("vol_ewstd") or 0.0
        slope = r.get("slope") or 0.0
        z = slope / (vol + 1e-9)
        kappa = abs(z - (prev_z if prev_z is not None else z))
        prev_z = z

        if (z >= z_min) and (kappa <= kappa_max):
            d = 1
        elif (z <= -z_min) and (kappa <= kappa_max):
            d = -1
        else:
            d = 0

        is_start = False
        if d != 0:
            if d != cur_dir:
                cur_dir = d
                seg_count = 1
                is_start = True
            else:
                seg_count += 1
                is_start = seg_count <= K
        else:
            cur_dir = 0
            seg_count = 0

        out.append({
            "tickid": r["tickid"],
            "direction": int(d),
            "is_segment_start": bool(is_start),
            "meta": None
        })
    return out

def persist_labels(rows: List[Dict[str, Any]]) -> int:
    return upsert_many("trend_labels", rows)
