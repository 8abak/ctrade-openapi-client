"""Read-only snapshot of paper operations and historical feed availability."""
import hashlib
import json
import sqlite3
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from datavis.db import db_connection


def main():
    root = Path('/home/ec2-user/acd-paper-state')
    db = sqlite3.connect(f'file:{root}/paper.sqlite?mode=ro', uri=True)
    state = json.loads(db.execute('SELECT data FROM state WHERE id=1').fetchone()[0])
    events = [dict(id=r[0], timestamp=r[1], kind=r[2], data=json.loads(r[3])) for r in db.execute('SELECT * FROM events ORDER BY id')]
    db.close()
    source = Path('/home/ec2-user/acd-paper-release')
    paths = ['datavis/acd_continuation.py','datavis/acd_paper_service.py','datavis/hlrtacd/acd.py','datavis/hlrtacd/models.py']
    with db_connection(readonly=True) as conn:
        with conn.cursor() as cur:
            cur.execute('SELECT MIN(timestamp),MAX(timestamp) FROM public.ticks WHERE symbol=%s',('XAUUSD',))
            bounds = [x.isoformat() if x else None for x in cur.fetchone()]
    services = ['acd-paper.service','acd-paper-watchdog.timer','datavis.service','tickcollector.service']
    status = {s:subprocess.run(['systemctl','show',s,'-p','ActiveState','-p','SubState','-p','NRestarts'],capture_output=True,text=True).stdout for s in services}
    unit_command = subprocess.run(['systemctl','show','acd-paper.service','-p','ExecStart','-p','WorkingDirectory','-p','ActiveEnterTimestamp'],capture_output=True,text=True).stdout
    errors = subprocess.run(['journalctl','-u','acd-paper.service','-u','acd-paper-watchdog.service','--since','24 hours ago','-p','warning','--no-pager','-n','80'],capture_output=True,text=True).stdout
    print(json.dumps(dict(capturedAt=datetime.now(timezone.utc).isoformat(), state=state, events=events, health=json.loads((root/'health.json').read_text()),config=json.loads((root/'config.json').read_text()),services=status,unitCommand=unit_command,sourceSHA256={p:hashlib.sha256((source/p).read_bytes()).hexdigest() for p in paths},feedBounds=bounds,warningErrorLogTail=errors),indent=2))


if __name__ == '__main__':
    main()
