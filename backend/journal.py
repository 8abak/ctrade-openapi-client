# PATH: backend/jobs/journal.py

from datetime import datetime
from pathlib import Path

JOURNAL_PATH = Path(__file__).resolve().parents[2] / "frontend/src/journal/break.log"


def write_journal(msg: str):
    JOURNAL_PATH.parent.mkdir(parents=True, exist_ok=True)
    ts = datetime.utcnow().isoformat()
    with open(JOURNAL_PATH, "a") as f:
        f.write(f"[{ts}] {msg}\n")
