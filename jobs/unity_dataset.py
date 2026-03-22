from __future__ import annotations

import argparse
import csv
import json
import sys
from pathlib import Path
from typing import Any, Dict, List

import psycopg2
import psycopg2.extras

from backend.db import DATABASE_URL


def db_connect():
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = True
    return conn


def fetch_dataset(
    conn,
    *,
    symbol: str,
    only_resolved: bool,
    scenario: str | None,
    limit: int | None,
) -> List[Dict[str, Any]]:
    where = ["symbol = %s"]
    params: List[Any] = [symbol]
    if only_resolved:
        where.append("baselinestatus = 'resolved'")
        where.append("scenariostatus = 'resolved'")
    if scenario:
        where.append("scenariocode = %s")
        params.append(scenario)

    sql = f"""
    SELECT *
    FROM public.unitycandtrain
    WHERE {' AND '.join(where)}
    ORDER BY signaltickid ASC, scenariocode ASC NULLS LAST
    """
    if limit is not None and int(limit) > 0:
        sql += " LIMIT %s"
        params.append(int(limit))

    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, tuple(params))
        return cur.fetchall()


def write_csv(rows: List[Dict[str, Any]], path: Path):
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            cooked = {}
            for key, value in row.items():
                if isinstance(value, (dict, list)):
                    cooked[key] = json.dumps(value, separators=(",", ":"))
                else:
                    cooked[key] = value
            writer.writerow(cooked)


def write_jsonl(rows: List[Dict[str, Any]], path: Path):
    lines = [json.dumps(row, default=str, separators=(",", ":")) for row in rows]
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Export the flattened UNITY candidate training dataset.")
    p.add_argument("--symbol", default="XAUUSD")
    p.add_argument("--scenario", default=None, help="Optional scenario code filter such as tp100sl100.")
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--only-resolved", action="store_true")
    p.add_argument("--format", choices=["csv", "jsonl"], default="csv")
    p.add_argument("--output", required=True)
    return p.parse_args()


def main():
    args = parse_args()
    conn = db_connect()
    try:
        rows = fetch_dataset(
            conn,
            symbol=args.symbol,
            only_resolved=bool(args.only_resolved),
            scenario=args.scenario,
            limit=args.limit,
        )
    finally:
        conn.close()

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if args.format == "csv":
        write_csv(rows, output_path)
    else:
        write_jsonl(rows, output_path)

    print(
        json.dumps(
            {
                "ok": True,
                "rows": len(rows),
                "symbol": args.symbol,
                "scenario": args.scenario,
                "only_resolved": bool(args.only_resolved),
                "format": args.format,
                "output": str(output_path),
            },
            separators=(",", ":"),
        ),
        file=sys.stdout,
        flush=True,
    )


if __name__ == "__main__":
    main()
