# PATH: backend/journal.py
import os
from datetime import datetime, timezone
from typing import Optional, Dict, Any

# Where nginx serves /src from (your setup)
# Put journal files under: /home/ec2-user/cTrade/src/journal/
DEFAULT_JOURNAL_DIR = "/home/ec2-user/cTrade/src/journal"
JOURNAL_DIR = os.getenv("JOURNAL_DIR", DEFAULT_JOURNAL_DIR)


def _ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def _today_filename_utc() -> str:
    # journal is per-day; UTC keeps it predictable
    d = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    return f"{d}.txt"


def append_line(
    line: str,
    filename: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Append a single line to today's journal file (or a provided filename).
    Returns info including the file path and URL path under /src/...
    """
    _ensure_dir(JOURNAL_DIR)

    fname = filename or _today_filename_utc()
    full_path = os.path.join(JOURNAL_DIR, fname)

    # Normalize line: single line, newline-terminated
    clean = (line or "").replace("\r", " ").replace("\n", " ").strip()
    if not clean:
        clean = "(empty)"

    with open(full_path, "a", encoding="utf-8") as f:
        f.write(clean + "\n")

    # nginx: https://datavis.au/src/journal/<fname>
    url_path = f"/src/journal/{fname}"

    return {"ok": True, "path": full_path, "url_path": url_path, "filename": fname}


def format_event(
    event: str,
    segm_id: Optional[int] = None,
    segline_id: Optional[int] = None,
    details: Optional[str] = None,
    extra: Optional[Dict[str, Any]] = None,
) -> str:
    ts = datetime.now(timezone.utc).isoformat()
    parts = [ts, f"event={event}"]
    if segm_id is not None:
        parts.append(f"segm={int(segm_id)}")
    if segline_id is not None:
        parts.append(f"segline={int(segline_id)}")
    if details:
        parts.append(f"details={details}")
    if extra and isinstance(extra, dict):
        for k, v in extra.items():
            if v is None:
                continue
            parts.append(f"{k}={v}")
    return " ".join(parts)
