# ml/labels.py
from dataclasses import dataclass
from typing import List, Iterable, Dict, Optional, Tuple
import numpy as np
import pandas as pd

@dataclass
class SwingStart:
    tickid: int
    price: float  # kalman level at start

def detect_swings_from_kalman(kalman_df: pd.DataFrame,
                              reversal_usd: float = 1.0) -> List[SwingStart]:
    """
    Detect swing starts on Kalman line via dollar reversal threshold.
    kalman_df columns: ['tickid','level'] (level is kalman level/price)
    reversal_usd: min counter-move from last extreme to declare a new swing start.
    """
    x = kalman_df['tickid'].values
    y = kalman_df['level'].values
    if len(x) == 0:
        return []

    swings: List[SwingStart] = []
    last_ext_price = y[0]
    last_ext_ix = 0
    direction = 0  # 0 unknown, +1 up leg, -1 down leg

    for i in range(1, len(y)):
        if direction >= 0:
            # we’re watching for a down reversal relative to last_ext_price
            if y[i] - last_ext_price >= 0:
                # extend up extreme
                last_ext_price = y[i]; last_ext_ix = i; direction = +1
            elif last_ext_price - y[i] >= reversal_usd:
                # down reversal -> new swing starts here
                swings.append(SwingStart(tickid=int(x[i]), price=float(y[i])))
                last_ext_price = y[i]; last_ext_ix = i; direction = -1
        if direction <= 0:
            # we’re watching for an up reversal relative to last_ext_price
            if last_ext_price - y[i] <= 0:
                # extend down extreme
                last_ext_price = y[i]; last_ext_ix = i; direction = -1 if direction != 0 else -1
            elif y[i] - last_ext_price >= reversal_usd:
                # up reversal -> new swing starts here
                swings.append(SwingStart(tickid=int(x[i]), price=float(y[i])))
                last_ext_price = y[i]; last_ext_ix = i; direction = +1
    # Ensure first bar is a swing start (optional)
    if not swings or swings[0].tickid != int(x[0]):
        swings.insert(0, SwingStart(tickid=int(x[0]), price=float(y[0])))
    return swings

def resolve_outcome(price_series: pd.Series,
                    start_tick: int,
                    start_price: float,
                    threshold_usd: int,
                    max_ticks: int = 15000) -> Tuple[str, int, float, Optional[int]]:
    """
    Resolve first-touch outcome from a start. price_series indexed by tickid, values=kalman price.
    Returns (outcome, time_to_outcome, price_at_resolve, tickid_resolve)
    """
    up_target = start_price + threshold_usd
    dn_target = start_price - threshold_usd

    # restrict to future window
    future = price_series.loc[start_tick:]
    if len(future) == 0:
        return ('nt', 0, float(start_price), None)

    # step forward up to max_ticks
    end_idx = min(max_ticks, len(future))
    view = future.iloc[:end_idx]

    # Check first-touch: compute where crosses the targets
    touch_up = view[view >= up_target]
    touch_dn = view[view <= dn_target]

    if not touch_up.empty and not touch_dn.empty:
        # first one by index order (tick)
        t_up = touch_up.index[0]
        t_dn = touch_dn.index[0]
        if t_up < t_dn:
            return ('up', int(t_up - start_tick), float(view.loc[t_up]), int(t_up))
        else:
            return ('dn', int(t_dn - start_tick), float(view.loc[t_dn]), int(t_dn))
    elif not touch_up.empty:
        t_up = touch_up.index[0]
        return ('up', int(t_up - start_tick), float(view.loc[t_up]), int(t_up))
    elif not touch_dn.empty:
        t_dn = touch_dn.index[0]
        return ('dn', int(t_dn - start_tick), float(view.loc[t_dn]), int(t_dn))
    else:
        return ('nt', int(view.index[-1] - start_tick), float(view.iloc[-1]), None)
