from __future__ import annotations

import threading
from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

import psycopg2
import psycopg2.extras


class RectServiceError(RuntimeError):
    def __init__(self, message: str, *, code: str = "RECT_ERROR", status_code: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.status_code = status_code


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except Exception:
        return None


def _as_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except Exception:
        return None


def _as_iso(value: Any) -> Optional[str]:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


def _copy_metadata(value: Any) -> Dict[str, Any]:
    return dict(value or {})


def _ms_between(start: Optional[datetime], end: Optional[datetime]) -> Optional[int]:
    if not isinstance(start, datetime) or not isinstance(end, datetime):
        return None
    return max(0, int((end - start).total_seconds() * 1000.0))


class RectPaperService:
    def __init__(self, *, db_factory: Callable[..., Any], symbol: str) -> None:
        self._db_factory = db_factory
        self._symbol = symbol
        self._lock = threading.RLock()
        self._current: Dict[str, Dict[str, Any]] = {}

    def start(self) -> None:
        self._restore_open_rects()

    def stop(self) -> None:
        return

    def current_rect(self, mode: str) -> Optional[Dict[str, Any]]:
        normalized_mode = self._normalize_mode(mode)
        with self._lock:
            rect = self._current.get(normalized_mode)
            return self._serialize_rect(rect) if rect else None

    def create_rect(
        self,
        *,
        mode: str,
        leftx: int,
        rightx: int,
        firstprice: float,
        secondprice: float,
        smartcloseenabled: bool,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        normalized_mode = self._normalize_mode(mode)
        with self._lock:
            existing = self._current.get(normalized_mode)
            if existing and not self._is_terminal(existing):
                raise RectServiceError("Clear the current rectangle before creating a new one.", code="RECT_ALREADY_ACTIVE", status_code=409)
            normalized = self._normalize_rect_input(leftx=leftx, rightx=rightx, firstprice=firstprice, secondprice=secondprice)
            tick_meta = self._fetch_tick_meta([normalized["leftx"], normalized["rightx"]])
            created_at = _utc_now()
            row = {
                "symbol": self._symbol,
                "mode": normalized_mode,
                "status": "armed",
                "state": "armededitable",
                "drawcreatedat": created_at,
                "drawupdatedat": created_at,
                "leftx": normalized["leftx"],
                "rightx": normalized["rightx"],
                "firstprice": normalized["firstprice"],
                "secondprice": normalized["secondprice"],
                "lowprice": normalized["lowprice"],
                "highprice": normalized["highprice"],
                "height": normalized["height"],
                "topprice": normalized["highprice"],
                "bottomprice": normalized["lowprice"],
                "lefttickid": normalized["leftx"],
                "righttickid": normalized["rightx"],
                "lefttime": tick_meta[normalized["leftx"]]["timestamp"],
                "righttime": tick_meta[normalized["rightx"]]["timestamp"],
                "entrydir": None,
                "entryprice": None,
                "entrytime": None,
                "entrytickid": None,
                "stoploss": None,
                "takeprofit": None,
                "exittime": None,
                "exittickid": None,
                "exitprice": None,
                "exitreason": None,
                "pnl": None,
                "pnlpoints": None,
                "drawtoentryms": None,
                "entrytoexitms": None,
                "smartcloseenabled": bool(smartcloseenabled),
                "manualclosed": False,
                "metadata": _copy_metadata(metadata),
                "lasttickid": normalized["rightx"],
                "lasttime": tick_meta[normalized["rightx"]]["timestamp"],
            }
            stored = self._insert_rect_row(row)
            state = self._hydrate_row(stored)
            self._current[normalized_mode] = state
            return self._serialize_rect(state)

    def update_rect(
        self,
        *,
        rect_id: int,
        mode: str,
        leftx: int,
        rightx: int,
        firstprice: float,
        secondprice: float,
        smartcloseenabled: bool,
    ) -> Dict[str, Any]:
        normalized_mode = self._normalize_mode(mode)
        with self._lock:
            current = self._require_current_rect(rect_id=rect_id, mode=normalized_mode)
            if current["state"] != "armededitable":
                raise RectServiceError("The rectangle is locked after breakout.", code="RECT_NOT_EDITABLE", status_code=409)
            normalized = self._normalize_rect_input(leftx=leftx, rightx=rightx, firstprice=firstprice, secondprice=secondprice)
            tick_meta = self._fetch_tick_meta([normalized["leftx"], normalized["rightx"]])
            current.update({
                "drawupdatedat": _utc_now(),
                "leftx": normalized["leftx"],
                "rightx": normalized["rightx"],
                "firstprice": normalized["firstprice"],
                "secondprice": normalized["secondprice"],
                "lowprice": normalized["lowprice"],
                "highprice": normalized["highprice"],
                "height": normalized["height"],
                "topprice": normalized["highprice"],
                "bottomprice": normalized["lowprice"],
                "lefttickid": normalized["leftx"],
                "righttickid": normalized["rightx"],
                "lefttime": tick_meta[normalized["leftx"]]["timestamp"],
                "righttime": tick_meta[normalized["rightx"]]["timestamp"],
                "smartcloseenabled": bool(smartcloseenabled),
                "lasttickid": max(int(current.get("lasttickid") or 0), normalized["rightx"]),
            })
            stored = self._update_edit_row(current)
            state = self._hydrate_row(stored)
            self._current[normalized_mode] = state
            return self._serialize_rect(state)

    def set_smart_close(self, *, rect_id: int, mode: str, enabled: bool) -> Dict[str, Any]:
        normalized_mode = self._normalize_mode(mode)
        with self._lock:
            current = self._require_current_rect(rect_id=rect_id, mode=normalized_mode)
            current["smartcloseenabled"] = bool(enabled)
            current["drawupdatedat"] = _utc_now()
            stored = self._update_smart_close_row(current)
            state = self._hydrate_row(stored)
            self._current[normalized_mode] = state
            return self._serialize_rect(state)

    def clear_rect(self, *, rect_id: int, mode: str) -> Optional[Dict[str, Any]]:
        normalized_mode = self._normalize_mode(mode)
        with self._lock:
            current = self._require_current_rect(rect_id=rect_id, mode=normalized_mode)
            if current["state"] in {"triggeredlong", "triggeredshort"}:
                raise RectServiceError("Manual Close is required while the paper trade is active.", code="RECT_TRADE_ACTIVE", status_code=409)
            if current["state"] == "armededitable":
                current["status"] = "cancelled"
                current["state"] = "cancelled"
                current["drawupdatedat"] = _utc_now()
                self._update_cancel_row(current)
            self._current.pop(normalized_mode, None)
            return None

    def manual_close(self, *, rect_id: int, mode: str) -> Dict[str, Any]:
        normalized_mode = self._normalize_mode(mode)
        with self._lock:
            current = self._require_current_rect(rect_id=rect_id, mode=normalized_mode)
            if current["state"] not in {"triggeredlong", "triggeredshort"}:
                raise RectServiceError("No active paper trade is available to close.", code="RECT_NOT_TRIGGERED", status_code=409)
            last_tick = current.get("lasttick")
            if not last_tick:
                raise RectServiceError("Manual close requires at least one streamed tick after entry.", code="RECT_NO_LIVE_TICK", status_code=409)
            return self._close_rect_locked(current, last_tick=last_tick, exitreason="manual", manualclosed=True)

    def process_tick(self, mode: str, tick: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        normalized_mode = self._normalize_mode(mode)
        with self._lock:
            current = self._current.get(normalized_mode)
            if not current:
                return None
            tick_id = _as_int(tick.get("id"))
            if tick_id is None:
                return self._serialize_rect(current)
            normalized_tick = self._normalize_tick(tick)
            if tick_id <= int(current.get("lasttickid") or 0):
                current["lasttick"] = normalized_tick
                return self._serialize_rect(current)
            current["lasttickid"] = tick_id
            current["lasttime"] = normalized_tick["timestamp"]
            current["lasttick"] = normalized_tick

            if current["state"] == "armededitable":
                if tick_id <= int(current["rightx"]):
                    return self._serialize_rect(current)
                if float(normalized_tick["ask"]) > float(current["highprice"]):
                    return self._trigger_rect_locked(current, tick=normalized_tick, direction="long")
                if float(normalized_tick["bid"]) < float(current["lowprice"]):
                    return self._trigger_rect_locked(current, tick=normalized_tick, direction="short")
                return self._serialize_rect(current)

            if current["state"] == "triggeredlong":
                smart_reason = self._smart_close_reason(current=current, tick=normalized_tick)
                if smart_reason:
                    return self._close_rect_locked(current, last_tick=normalized_tick, exitreason=smart_reason, manualclosed=False)
                if float(normalized_tick["bid"]) <= float(current["stoploss"]):
                    return self._close_rect_locked(current, last_tick=normalized_tick, exitreason="stoploss", manualclosed=False)
                if float(normalized_tick["bid"]) >= float(current["takeprofit"]):
                    return self._close_rect_locked(current, last_tick=normalized_tick, exitreason="takeprofit", manualclosed=False)
                return self._serialize_rect(current)

            if current["state"] == "triggeredshort":
                smart_reason = self._smart_close_reason(current=current, tick=normalized_tick)
                if smart_reason:
                    return self._close_rect_locked(current, last_tick=normalized_tick, exitreason=smart_reason, manualclosed=False)
                if float(normalized_tick["ask"]) >= float(current["stoploss"]):
                    return self._close_rect_locked(current, last_tick=normalized_tick, exitreason="stoploss", manualclosed=False)
                if float(normalized_tick["ask"]) <= float(current["takeprofit"]):
                    return self._close_rect_locked(current, last_tick=normalized_tick, exitreason="takeprofit", manualclosed=False)
                return self._serialize_rect(current)

            return self._serialize_rect(current)

    def _restore_open_rects(self) -> None:
        query = """
            SELECT DISTINCT ON (mode) *
            FROM public.rects
            WHERE symbol = %s AND status IN ('armed', 'triggered')
            ORDER BY mode, id DESC
        """
        try:
            with self._db_factory(readonly=True) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(query, (self._symbol,))
                    rows = [dict(row) for row in cur.fetchall()]
        except psycopg2.Error:
            return
        with self._lock:
            for row in rows:
                state = self._hydrate_row(row)
                self._current[state["mode"]] = state

    def _normalize_mode(self, mode: str) -> str:
        normalized = str(mode or "live").strip().lower()
        if normalized not in {"live", "review"}:
            raise RectServiceError("mode must be live or review", code="RECT_INVALID_MODE", status_code=400)
        return normalized

    def _normalize_rect_input(self, *, leftx: int, rightx: int, firstprice: float, secondprice: float) -> Dict[str, Any]:
        resolved_leftx = _as_int(leftx)
        resolved_rightx = _as_int(rightx)
        resolved_firstprice = _as_float(firstprice)
        resolved_secondprice = _as_float(secondprice)
        if resolved_leftx is None or resolved_rightx is None:
            raise RectServiceError("Rectangle x coordinates are required.", code="RECT_INVALID_X", status_code=400)
        if resolved_firstprice is None or resolved_secondprice is None:
            raise RectServiceError("Rectangle prices are required.", code="RECT_INVALID_PRICE", status_code=400)
        if resolved_rightx <= resolved_leftx:
            raise RectServiceError("The second point must be to the right of the first point.", code="RECT_RIGHT_EDGE_REQUIRED", status_code=400)
        lowprice = min(resolved_firstprice, resolved_secondprice)
        highprice = max(resolved_firstprice, resolved_secondprice)
        height = highprice - lowprice
        if height <= 0:
            raise RectServiceError("Rectangle height must be greater than zero.", code="RECT_ZERO_HEIGHT", status_code=400)
        return {
            "leftx": resolved_leftx,
            "rightx": resolved_rightx,
            "firstprice": float(resolved_firstprice),
            "secondprice": float(resolved_secondprice),
            "lowprice": float(lowprice),
            "highprice": float(highprice),
            "height": float(height),
        }

    def _fetch_tick_meta(self, tick_ids: List[int]) -> Dict[int, Dict[str, Any]]:
        deduped = sorted({int(item) for item in tick_ids if item is not None})
        if not deduped:
            raise RectServiceError("Tick coordinates are required.", code="RECT_TICK_REQUIRED", status_code=400)
        with self._db_factory(readonly=True) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, timestamp
                    FROM public.ticks
                    WHERE symbol = %s AND id = ANY(%s)
                    ORDER BY id ASC
                    """,
                    (self._symbol, deduped),
                )
                rows = {int(row["id"]): dict(row) for row in cur.fetchall()}
        missing = [item for item in deduped if item not in rows]
        if missing:
            raise RectServiceError("One or more rectangle ticks could not be resolved.", code="RECT_TICK_NOT_FOUND", status_code=404)
        return rows

    def _insert_rect_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        with self._db_factory(readonly=False, autocommit=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    INSERT INTO public.rects (
                        symbol, mode, status, state, drawcreatedat, drawupdatedat,
                        leftx, rightx, firstprice, secondprice, lowprice, highprice, height,
                        topprice, bottomprice, lefttime, righttime, lefttickid, righttickid,
                        entrydir, entryprice, entrytime, entrytickid, stoploss, takeprofit,
                        exittime, exittickid, exitprice, exitreason, pnl, pnlpoints,
                        drawtoentryms, entrytoexitms, smartcloseenabled, manualclosed, metadata
                    ) VALUES (
                        %(symbol)s, %(mode)s, %(status)s, %(state)s, %(drawcreatedat)s, %(drawupdatedat)s,
                        %(leftx)s, %(rightx)s, %(firstprice)s, %(secondprice)s, %(lowprice)s, %(highprice)s, %(height)s,
                        %(topprice)s, %(bottomprice)s, %(lefttime)s, %(righttime)s, %(lefttickid)s, %(righttickid)s,
                        %(entrydir)s, %(entryprice)s, %(entrytime)s, %(entrytickid)s, %(stoploss)s, %(takeprofit)s,
                        %(exittime)s, %(exittickid)s, %(exitprice)s, %(exitreason)s, %(pnl)s, %(pnlpoints)s,
                        %(drawtoentryms)s, %(entrytoexitms)s, %(smartcloseenabled)s, %(manualclosed)s, %(metadata)s
                    )
                    RETURNING *
                    """,
                    row,
                )
                stored = dict(cur.fetchone() or {})
            conn.commit()
        return stored

    def _update_row(self, query: str, row: Dict[str, Any]) -> Dict[str, Any]:
        with self._db_factory(readonly=False, autocommit=False) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, row)
                stored = dict(cur.fetchone() or {})
            conn.commit()
        return stored

    def _update_edit_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return self._update_row(
            """
            UPDATE public.rects
            SET
                drawupdatedat = %(drawupdatedat)s,
                status = %(status)s,
                state = %(state)s,
                leftx = %(leftx)s,
                rightx = %(rightx)s,
                firstprice = %(firstprice)s,
                secondprice = %(secondprice)s,
                lowprice = %(lowprice)s,
                highprice = %(highprice)s,
                height = %(height)s,
                topprice = %(topprice)s,
                bottomprice = %(bottomprice)s,
                lefttime = %(lefttime)s,
                righttime = %(righttime)s,
                lefttickid = %(lefttickid)s,
                righttickid = %(righttickid)s,
                smartcloseenabled = %(smartcloseenabled)s
            WHERE id = %(id)s
            RETURNING *
            """,
            row,
        )

    def _update_smart_close_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return self._update_row(
            """
            UPDATE public.rects
            SET
                drawupdatedat = %(drawupdatedat)s,
                smartcloseenabled = %(smartcloseenabled)s
            WHERE id = %(id)s
            RETURNING *
            """,
            row,
        )

    def _update_trigger_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return self._update_row(
            """
            UPDATE public.rects
            SET
                drawupdatedat = %(drawupdatedat)s,
                status = %(status)s,
                state = %(state)s,
                entrydir = %(entrydir)s,
                entryprice = %(entryprice)s,
                entrytime = %(entrytime)s,
                entrytickid = %(entrytickid)s,
                stoploss = %(stoploss)s,
                takeprofit = %(takeprofit)s,
                drawtoentryms = %(drawtoentryms)s
            WHERE id = %(id)s
            RETURNING *
            """,
            row,
        )

    def _update_close_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return self._update_row(
            """
            UPDATE public.rects
            SET
                drawupdatedat = %(drawupdatedat)s,
                status = %(status)s,
                state = %(state)s,
                exittime = %(exittime)s,
                exittickid = %(exittickid)s,
                exitprice = %(exitprice)s,
                exitreason = %(exitreason)s,
                pnl = %(pnl)s,
                pnlpoints = %(pnlpoints)s,
                entrytoexitms = %(entrytoexitms)s,
                manualclosed = %(manualclosed)s
            WHERE id = %(id)s
            RETURNING *
            """,
            row,
        )

    def _update_cancel_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return self._update_row(
            """
            UPDATE public.rects
            SET
                drawupdatedat = %(drawupdatedat)s,
                status = %(status)s,
                state = %(state)s
            WHERE id = %(id)s
            RETURNING *
            """,
            row,
        )

    def _trigger_rect_locked(self, current: Dict[str, Any], *, tick: Dict[str, Any], direction: str) -> Dict[str, Any]:
        height = float(current["height"])
        if direction == "long":
            entryprice = float(tick["ask"])
            stoploss = float(current["lowprice"]) - (0.05 * height)
            takeprofit = float(current["highprice"]) + height
            state = "triggeredlong"
        else:
            entryprice = float(tick["bid"])
            stoploss = float(current["highprice"]) + (0.05 * height)
            takeprofit = float(current["lowprice"]) - height
            state = "triggeredshort"
        current.update({
            "status": "triggered",
            "state": state,
            "entrydir": direction,
            "entryprice": entryprice,
            "entrytime": tick["timestamp"],
            "entrytickid": tick["id"],
            "stoploss": float(stoploss),
            "takeprofit": float(takeprofit),
            "drawtoentryms": _ms_between(current.get("drawcreatedat"), tick["timestamp"]),
            "drawupdatedat": _utc_now(),
        })
        stored = self._update_trigger_row(current)
        state_row = self._hydrate_row(stored)
        state_row["lasttickid"] = current["lasttickid"]
        state_row["lasttime"] = current["lasttime"]
        state_row["lasttick"] = deepcopy(current.get("lasttick"))
        self._current[current["mode"]] = state_row
        return self._serialize_rect(state_row)

    def _close_rect_locked(self, current: Dict[str, Any], *, last_tick: Dict[str, Any], exitreason: str, manualclosed: bool) -> Dict[str, Any]:
        if current.get("entrydir") == "long":
            exitprice = float(last_tick["bid"])
            pnlpoints = float(exitprice) - float(current["entryprice"])
        else:
            exitprice = float(last_tick["ask"])
            pnlpoints = float(current["entryprice"]) - float(exitprice)
        current.update({
            "status": "closed",
            "state": "closed",
            "exittime": last_tick["timestamp"],
            "exittickid": last_tick["id"],
            "exitprice": float(exitprice),
            "exitreason": str(exitreason),
            "pnl": float(pnlpoints),
            "pnlpoints": float(pnlpoints),
            "entrytoexitms": _ms_between(current.get("entrytime"), last_tick["timestamp"]),
            "manualclosed": bool(manualclosed),
            "drawupdatedat": _utc_now(),
        })
        stored = self._update_close_row(current)
        state_row = self._hydrate_row(stored)
        state_row["lasttickid"] = current["lasttickid"]
        state_row["lasttime"] = current["lasttime"]
        state_row["lasttick"] = deepcopy(current.get("lasttick"))
        self._current[current["mode"]] = state_row
        return self._serialize_rect(state_row)

    def _smart_close_reason(self, *, current: Dict[str, Any], tick: Dict[str, Any]) -> Optional[str]:
        _ = tick
        if not current.get("smartcloseenabled"):
            return None
        return None

    def _require_current_rect(self, *, rect_id: int, mode: str) -> Dict[str, Any]:
        current = self._current.get(mode)
        if not current or int(current.get("id") or 0) != int(rect_id):
            raise RectServiceError("The requested rectangle is not active in memory.", code="RECT_NOT_ACTIVE", status_code=404)
        return current

    def _is_terminal(self, rect: Dict[str, Any]) -> bool:
        return str(rect.get("state") or "").lower() in {"closed", "cancelled"}

    def _normalize_tick(self, tick: Dict[str, Any]) -> Dict[str, Any]:
        timestamp = tick.get("timestamp")
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp)
        if not isinstance(timestamp, datetime):
            raise RectServiceError("Tick timestamp is required.", code="RECT_TICK_TIMESTAMP", status_code=400)
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        bid = _as_float(tick.get("bid"))
        ask = _as_float(tick.get("ask"))
        tick_id = _as_int(tick.get("id"))
        if tick_id is None or bid is None or ask is None:
            raise RectServiceError("Tick id/bid/ask are required.", code="RECT_TICK_INVALID", status_code=400)
        return {"id": tick_id, "timestamp": timestamp, "bid": float(bid), "ask": float(ask)}

    def _hydrate_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        state = {
            "id": _as_int(row.get("id")),
            "symbol": row.get("symbol") or self._symbol,
            "mode": self._normalize_mode(row.get("mode") or "live"),
            "status": str(row.get("status") or "armed").lower(),
            "state": str(row.get("state") or "armededitable").lower(),
            "drawcreatedat": row.get("drawcreatedat"),
            "drawupdatedat": row.get("drawupdatedat"),
            "leftx": _as_int(row.get("leftx")),
            "rightx": _as_int(row.get("rightx")),
            "firstprice": float(row.get("firstprice")),
            "secondprice": float(row.get("secondprice")),
            "lowprice": float(row.get("lowprice")),
            "highprice": float(row.get("highprice")),
            "height": float(row.get("height")),
            "topprice": float(row.get("topprice")),
            "bottomprice": float(row.get("bottomprice")),
            "lefttime": row.get("lefttime"),
            "righttime": row.get("righttime"),
            "lefttickid": _as_int(row.get("lefttickid")),
            "righttickid": _as_int(row.get("righttickid")),
            "entrydir": row.get("entrydir"),
            "entryprice": _as_float(row.get("entryprice")),
            "entrytime": row.get("entrytime"),
            "entrytickid": _as_int(row.get("entrytickid")),
            "stoploss": _as_float(row.get("stoploss")),
            "takeprofit": _as_float(row.get("takeprofit")),
            "exittime": row.get("exittime"),
            "exittickid": _as_int(row.get("exittickid")),
            "exitprice": _as_float(row.get("exitprice")),
            "exitreason": row.get("exitreason"),
            "pnl": _as_float(row.get("pnl")),
            "pnlpoints": _as_float(row.get("pnlpoints")),
            "drawtoentryms": _as_int(row.get("drawtoentryms")),
            "entrytoexitms": _as_int(row.get("entrytoexitms")),
            "smartcloseenabled": bool(row.get("smartcloseenabled")),
            "manualclosed": bool(row.get("manualclosed")),
            "metadata": _copy_metadata(row.get("metadata")),
            "lasttickid": _as_int(row.get("exittickid")) or _as_int(row.get("entrytickid")) or _as_int(row.get("righttickid")),
            "lasttime": row.get("exittime") or row.get("entrytime") or row.get("righttime"),
            "lasttick": None,
        }
        if state["status"] == "triggered" and state["state"] not in {"triggeredlong", "triggeredshort"}:
            state["state"] = "triggeredlong" if state.get("entrydir") == "long" else "triggeredshort"
        return state

    def _serialize_rect(self, rect: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not rect:
            return None
        payload = {
            "id": rect.get("id"),
            "symbol": rect.get("symbol"),
            "mode": rect.get("mode"),
            "status": rect.get("status"),
            "state": rect.get("state"),
            "drawcreatedat": _as_iso(rect.get("drawcreatedat")),
            "drawupdatedat": _as_iso(rect.get("drawupdatedat")),
            "leftx": rect.get("leftx"),
            "rightx": rect.get("rightx"),
            "firstprice": rect.get("firstprice"),
            "secondprice": rect.get("secondprice"),
            "lowprice": rect.get("lowprice"),
            "highprice": rect.get("highprice"),
            "height": rect.get("height"),
            "topprice": rect.get("topprice"),
            "bottomprice": rect.get("bottomprice"),
            "lefttime": _as_iso(rect.get("lefttime")),
            "righttime": _as_iso(rect.get("righttime")),
            "lefttickid": rect.get("lefttickid"),
            "righttickid": rect.get("righttickid"),
            "entrydir": rect.get("entrydir"),
            "entryprice": rect.get("entryprice"),
            "entrytime": _as_iso(rect.get("entrytime")),
            "entrytickid": rect.get("entrytickid"),
            "stoploss": rect.get("stoploss"),
            "takeprofit": rect.get("takeprofit"),
            "exittime": _as_iso(rect.get("exittime")),
            "exittickid": rect.get("exittickid"),
            "exitprice": rect.get("exitprice"),
            "exitreason": rect.get("exitreason"),
            "pnl": rect.get("pnl"),
            "pnlpoints": rect.get("pnlpoints"),
            "drawtoentryms": rect.get("drawtoentryms"),
            "entrytoexitms": rect.get("entrytoexitms"),
            "smartcloseenabled": bool(rect.get("smartcloseenabled")),
            "manualclosed": bool(rect.get("manualclosed")),
            "metadata": deepcopy(rect.get("metadata") or {}),
            "editable": rect.get("state") == "armededitable",
            "tradeactive": rect.get("state") in {"triggeredlong", "triggeredshort"},
            "closed": rect.get("state") == "closed",
            "orientation": "ascending" if float(rect.get("firstprice") or 0.0) <= float(rect.get("secondprice") or 0.0) else "descending",
            "lasttickid": rect.get("lasttickid"),
            "lasttime": _as_iso(rect.get("lasttime")),
        }
        current_pnl = self._current_pnl_points(rect)
        payload["currentpnl"] = current_pnl
        payload["currentpnlpoints"] = current_pnl
        return payload

    def _current_pnl_points(self, rect: Dict[str, Any]) -> Optional[float]:
        if rect.get("state") == "closed":
            return rect.get("pnlpoints")
        if rect.get("state") not in {"triggeredlong", "triggeredshort"}:
            return None
        last_tick = rect.get("lasttick")
        if not last_tick:
            return None
        entryprice = _as_float(rect.get("entryprice"))
        if entryprice is None:
            return None
        if rect.get("entrydir") == "long":
            return float(last_tick["bid"]) - float(entryprice)
        return float(entryprice) - float(last_tick["ask"])
