from __future__ import annotations

import argparse
import os
import time

from datavis.backbone import BackboneLiveRuntime, db_connection


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run the live backbone incremental worker.")
    parser.add_argument("--symbol", default=os.getenv("DATAVIS_SYMBOL", "XAUUSD"), help="Symbol to process.")
    parser.add_argument("--batch-size", type=int, default=400, help="Tick batch size per poll.")
    parser.add_argument("--poll-seconds", type=float, default=0.20, help="Sleep time between empty polls.")
    return parser


def main() -> int:
    args = build_parser().parse_args()
    runtime = BackboneLiveRuntime(symbol=str(args.symbol).strip().upper(), batch_size=max(1, int(args.batch_size)))
    poll_seconds = max(0.05, float(args.poll_seconds))

    with db_connection(readonly=False, autocommit=False) as conn:
        runtime.bootstrap(conn)
        conn.commit()

    while True:
        with db_connection(readonly=False, autocommit=False) as conn:
            result = runtime.process_once(conn)
            conn.commit()
        if int(result.get("tickcount") or 0) <= 0:
            time.sleep(poll_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
