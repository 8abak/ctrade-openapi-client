from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Sequence, Tuple


SCENARIO_GRID: Tuple[Dict[str, float], ...] = (
    {"code": "tp075sl100", "tpmult": 0.75, "slmult": 1.00},
    {"code": "tp100sl100", "tpmult": 1.00, "slmult": 1.00},
    {"code": "tp125sl100", "tpmult": 1.25, "slmult": 1.00},
    {"code": "tp150sl100", "tpmult": 1.50, "slmult": 1.00},
    {"code": "tp100sl075", "tpmult": 1.00, "slmult": 0.75},
    {"code": "tp100sl125", "tpmult": 1.00, "slmult": 1.25},
)


def _ensure_dt(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(str(value))


def _mark_for_side(side: str, row: Dict[str, Any]) -> Optional[float]:
    if side == "long":
        value = row.get("bid")
    else:
        value = row.get("ask")
    if value is None:
        value = row.get("mid")
    if value is None:
        return None
    return float(value)


def _favor_adverse(side: str, entryprice: float, mark: float) -> Tuple[float, float]:
    if side == "long":
        favor = mark - entryprice
        adverse = entryprice - mark
    else:
        favor = entryprice - mark
        adverse = mark - entryprice
    return max(0.0, favor), max(0.0, adverse)


def _build_geometry(
    *,
    side: str,
    entryprice: Optional[float],
    risk: Optional[float],
    tpmult: float,
    slmult: float,
) -> Optional[Dict[str, float]]:
    if entryprice is None or risk is None:
        return None
    entry = float(entryprice)
    base_risk = float(risk)
    if base_risk <= 0.0:
        return None
    if side == "long":
        tpprice = entry + base_risk * float(tpmult)
        slprice = entry - base_risk * float(slmult)
    else:
        tpprice = entry - base_risk * float(tpmult)
        slprice = entry + base_risk * float(slmult)
    return {
        "entryprice": entry,
        "risk": base_risk,
        "tpprice": float(tpprice),
        "slprice": float(slprice),
    }


def _make_result(
    *,
    tpprice: Optional[float],
    slprice: Optional[float],
    status: str,
    firsthit: str,
    resolvetickid: Optional[int] = None,
    resolvetime: Optional[datetime] = None,
    resolveseconds: Optional[int] = None,
    bestfavor: Optional[float] = None,
    bestadverse: Optional[float] = None,
    pnl: Optional[float] = None,
    wouldwin: Optional[bool] = None,
) -> Dict[str, Any]:
    mfe = bestfavor
    mae = bestadverse
    return {
        "tpprice": float(tpprice) if tpprice is not None else None,
        "slprice": float(slprice) if slprice is not None else None,
        "firsthit": firsthit,
        "resolvetickid": int(resolvetickid) if resolvetickid is not None else None,
        "resolvetime": resolvetime,
        "resolveseconds": int(resolveseconds) if resolveseconds is not None else None,
        "mfe": float(mfe) if mfe is not None else None,
        "mae": float(mae) if mae is not None else None,
        "bestfavor": float(bestfavor) if bestfavor is not None else None,
        "bestadverse": float(bestadverse) if bestadverse is not None else None,
        "pnl": float(pnl) if pnl is not None else None,
        "wouldwin": wouldwin,
        "status": status,
    }


def resolve_geometry(
    *,
    signaltickid: int,
    side: str,
    entrytime: datetime,
    geometry: Optional[Dict[str, float]],
    future_rows: Sequence[Dict[str, Any]],
    timeoutsec: int,
    dayendtickid: Optional[int],
    regimechangestate: str,
    allow_resolution: bool,
) -> Dict[str, Any]:
    tpprice = geometry["tpprice"] if geometry else None
    slprice = geometry["slprice"] if geometry else None
    if not allow_resolution or geometry is None:
        return _make_result(
            tpprice=tpprice,
            slprice=slprice,
            status="ineligible",
            firsthit="unresolved",
        )

    if dayendtickid is not None and int(dayendtickid) <= 0:
        dayendtickid = None

    entryprice = float(geometry["entryprice"])
    timeout_at = entrytime + timedelta(seconds=max(1, int(timeoutsec)))
    if dayendtickid is not None and int(dayendtickid) <= int(signaltickid):
        return _make_result(
            tpprice=tpprice,
            slprice=slprice,
            status="resolved",
            firsthit="dayend",
            resolvetickid=int(dayendtickid),
            resolvetime=entrytime,
            resolveseconds=0,
            bestfavor=0.0,
            bestadverse=0.0,
            pnl=0.0,
            wouldwin=False,
        )

    bestfavor = 0.0
    bestadverse = 0.0
    for raw_row in future_rows:
        tickid = int(raw_row["id"])
        ticktime = _ensure_dt(raw_row["timestamp"])
        mark = _mark_for_side(side, raw_row)
        if mark is None:
            continue
        favor, adverse = _favor_adverse(side, entryprice, mark)
        bestfavor = max(bestfavor, favor)
        bestadverse = max(bestadverse, adverse)

        hit = None
        exitprice = None
        resolvetime = ticktime
        if side == "long":
            if mark >= float(tpprice):
                hit = "tp"
                exitprice = float(tpprice)
            elif mark <= float(slprice):
                hit = "sl"
                exitprice = float(slprice)
        else:
            if mark <= float(tpprice):
                hit = "tp"
                exitprice = float(tpprice)
            elif mark >= float(slprice):
                hit = "sl"
                exitprice = float(slprice)

        if hit is None and str(raw_row.get("causalstate") or "") == regimechangestate:
            hit = "regimechange"
            exitprice = float(mark)
        if hit is None and ticktime >= timeout_at:
            hit = "timeout"
            exitprice = float(mark)
            resolvetime = timeout_at
        if hit is None and dayendtickid is not None and tickid >= int(dayendtickid):
            hit = "dayend"
            exitprice = float(mark)

        if hit is None or exitprice is None:
            continue

        pnl = exitprice - entryprice if side == "long" else entryprice - exitprice
        return _make_result(
            tpprice=tpprice,
            slprice=slprice,
            status="resolved",
            firsthit=hit,
            resolvetickid=tickid,
            resolvetime=resolvetime,
            resolveseconds=max(0, int((resolvetime - entrytime).total_seconds())),
            bestfavor=bestfavor,
            bestadverse=bestadverse,
            pnl=pnl,
            wouldwin=(hit == "tp"),
        )

    return _make_result(
        tpprice=tpprice,
        slprice=slprice,
        status="unresolved",
        firsthit="unresolved",
        bestfavor=bestfavor,
        bestadverse=bestadverse,
    )


def evaluate_candidate(
    candidate: Dict[str, Any],
    future_rows: Sequence[Dict[str, Any]],
    *,
    timeoutsec: int,
    dayendtickid: Optional[int],
) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
    side = str(candidate["side"])
    entrytime = _ensure_dt(candidate["time"])
    entryprice = float(candidate["entryprice"]) if candidate.get("entryprice") is not None else None
    risk = float(candidate["risk"]) if candidate.get("risk") is not None else None
    regimechangestate = "red" if side == "long" else "green"

    baseline_geometry = None
    if candidate.get("baselinetp") is not None and candidate.get("baselinesl") is not None and entryprice is not None and risk is not None:
        baseline_geometry = {
            "entryprice": entryprice,
            "risk": risk,
            "tpprice": float(candidate["baselinetp"]),
            "slprice": float(candidate["baselinesl"]),
        }

    baseline = resolve_geometry(
        signaltickid=int(candidate["signaltickid"]),
        side=side,
        entrytime=entrytime,
        geometry=baseline_geometry,
        future_rows=future_rows,
        timeoutsec=timeoutsec,
        dayendtickid=dayendtickid,
        regimechangestate=regimechangestate,
        allow_resolution=bool(candidate.get("eligible")),
    )

    scenarios: List[Dict[str, Any]] = []
    can_scenario = entryprice is not None and risk is not None and risk > 0.0
    for spec in SCENARIO_GRID:
        geometry = _build_geometry(
            side=side,
            entryprice=entryprice,
            risk=risk,
            tpmult=float(spec["tpmult"]),
            slmult=float(spec["slmult"]),
        )
        result = resolve_geometry(
            signaltickid=int(candidate["signaltickid"]),
            side=side,
            entrytime=entrytime,
            geometry=geometry,
            future_rows=future_rows,
            timeoutsec=timeoutsec,
            dayendtickid=dayendtickid,
            regimechangestate=regimechangestate,
            allow_resolution=can_scenario,
        )
        result.update(
            {
                "code": str(spec["code"]),
                "tpmult": float(spec["tpmult"]),
                "slmult": float(spec["slmult"]),
            }
        )
        scenarios.append(result)

    return baseline, scenarios
