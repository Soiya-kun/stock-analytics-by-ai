"""Export only daily chart inputs from local PostgreSQL, without mutating the DB."""
from __future__ import annotations
import argparse
from datetime import date, timedelta
import gzip
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import psycopg
from psycopg.rows import dict_row

from daily_stock_cloud import open_state, make_payload
from stock_review_page import write_html, validate_html


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--date", default="latest")
    args = parser.parse_args()
    folder = args.output_dir
    folder.mkdir(parents=True, exist_ok=True)
    path = folder / "prices.sqlite"
    if path.exists():
        raise ValueError("Use a new output directory for each seed export")
    numeric = lambda key: f"nullif(nullif(replace(payload->>'{key}',',',''),'-'),'')::double precision"
    with psycopg.connect(os.environ["DATABASE_URL"], row_factory=dict_row) as pg:
        pg.execute("set transaction isolation level repeatable read read only")
        latest = pg.execute("select max(record_date) as day from raw.kabuplus_records where dataset_key='japan-all-stock-prices/daily'").fetchone()["day"]
        day = latest if args.date == "latest" else date.fromisoformat(args.date)
        if not day or day > latest:
            raise ValueError("Requested seed date is not present in local PostgreSQL")
        conn = open_state(path)
        try:
            start = day - timedelta(days=500)
            sql = "select security_code as sc, record_date::text as day," + ",".join(
                numeric(key) + " as " + alias for key, alias in [
                    ("始値", "open"), ("高値", "high"), ("安値", "low"), ("株価", "close"),
                    ("出来高", "volume"), ("前日終値", "previous_close"), ("前日比", "change"),
                    ("前日比（％）", "change_pct"), ("売買代金（千円）", "turnover"),
                ]
            ) + ",payload->>'名称' as name,payload->>'市場' as market,payload->>'業種' as industry from raw.kabuplus_records where dataset_key='japan-all-stock-prices/daily' and record_date between %s and %s and security_code is not null order by record_date,security_code"
            count = 0
            with pg.cursor(name="daily_seed_prices") as cur, conn:
                cur.itersize = 5000
                cur.execute(sql, (start, day))
                for row in cur:
                    conn.execute("insert into prices values (?,?,?,?,?,?,?,?,?,?,?)", tuple(row[k] for k in (
                        "sc", "day", "open", "high", "low", "close", "volume", "previous_close", "change", "change_pct", "turnover",
                    )))
                    conn.execute("insert or replace into stocks values (?,?,?,?)", tuple(row[k] for k in ("sc", "name", "market", "industry")))
                    count += 1
            actions = pg.execute("""
                select e.sc,e.action_date,e.price_multiplier,
                    case when v.sc is not null then 'verified' else 'inferred' end as source
                from analytics.effective_price_actions e
                left join analytics.verified_price_actions v using(sc,action_date)
                where e.action_date>%s
            """, (start,)).fetchall()
            with conn:
                conn.executemany("insert into actions values (?,?,?,?)", [
                    (a["sc"], a["action_date"].isoformat(), float(a["price_multiplier"]), a["source"]) for a in actions
                ])
                conn.execute("insert into meta values ('latest_date',?)", (day.isoformat(),))
                conn.execute("insert into meta values ('schema_version','1')")
            payload = make_payload(conn, day)
            write_html(Path("/workspace/site/turnover-chart-viewer/template.html"), folder / "index.html", payload)
            summary = validate_html(folder / "index.html", day)
            summary.update(rows=count, actions=len(actions), first_date=conn.execute("select min(day) from prices").fetchone()[0])
            if conn.execute("pragma integrity_check").fetchone()[0] != "ok":
                raise ValueError("Seed SQLite integrity check failed")
            conn.execute("vacuum")
        finally:
            conn.close()
    with path.open("rb") as source, gzip.open(folder / "prices.sqlite.gz", "wb", compresslevel=6) as out:
        import shutil
        shutil.copyfileobj(source, out)
    summary.update(state_sha256=hashlib.sha256((folder / "prices.sqlite.gz").read_bytes()).hexdigest(), compressed_bytes=(folder / "prices.sqlite.gz").stat().st_size)
    (folder / "seed.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
