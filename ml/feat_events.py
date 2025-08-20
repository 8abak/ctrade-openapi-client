# ml/feat_events.py
import numpy as np
import pandas as pd
from typing import Dict, Any, List

def build_event_features(kalman_df: pd.DataFrame,
                         raw_df: pd.DataFrame,
                         start_tickids: List[int]) -> pd.DataFrame:
    """
    kalman_df: ['tickid','level']
    raw_df:    ['tickid','mid'] (if missing, pass kalman_df as raw with 'mid' alias)
    start_tickids: swing start tick ids

    Returns one row per start tick with features and 'tickid' column.
    """
    k = kalman_df.set_index('tickid').sort_index()
    r = raw_df.set_index('tickid').sort_index() if 'mid' in raw_df.columns else k.rename(columns={'level':'mid'})
    df = pd.DataFrame({'tickid': start_tickids}).set_index('tickid')

    # join price levels
    kk = k['level']
    rr = r['mid']

    # helper: shifted diffs on kalman
    for w in [1, 5, 20, 50]:
        df[f'k_slope_{w}'] = kk.diff(w).reindex(df.index)

    for w in [1, 5, 20, 50]:
        df[f'k_acc_{w}'] = kk.diff(w).diff(w).reindex(df.index)

    for w in [50, 200, 1000]:
        df[f'mom_{w}'] = kk.diff(w).reindex(df.index)

    for w in [50, 200, 1000]:
        df[f'vol_{w}'] = (rr.rolling(w).std()).reindex(df.index)

    # last leg size/duration approx: compare to last local extreme (simple backward scan)
    def last_extreme_info(idx):
        vals = kk.loc[:idx]
        if len(vals) < 3: return pd.Series({'last_leg_usd':0.0, 'last_leg_ticks':0})
        # naive: walk back until slope sign changed significantly
        window = vals.tail(1500)  # cap
        diffs = window.diff().fillna(0.0).values
        sign = np.sign(diffs[-1])
        if sign == 0: sign = 1
        acc = 0.0; ticks = 0
        for d in diffs[::-1]:
            if np.sign(d) == sign or d == 0:
                acc += d; ticks += 1
            else:
                break
        return pd.Series({'last_leg_usd': float(abs(acc)), 'last_leg_ticks': int(ticks)})

    ext = df.index.to_series().apply(last_extreme_info)
    df = pd.concat([df, ext], axis=1).fillna(0.0)
    df.reset_index(inplace=True)
    return df
