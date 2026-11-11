from __future__ import annotations

import argparse
import json
import os
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Sequence

import psycopg
from psycopg.rows import dict_row
from stock_review_page import HISTORY_DAYS, as_number, build_payload, write_html


ROOT_DIR = Path(__file__).resolve().parents[1]
DEFAULT_TEMPLATE = ROOT_DIR / "site" / "turnover-chart-viewer" / "template.html"
DEFAULT_OUTPUT_DIR = ROOT_DIR / "outputs" / "turnover-chart-viewer"
JST = timezone(timedelta(hours=9))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a self-contained high-turnover stock chart viewer."
    )
    parser.add_argument(
        "--dsn",
        default=os.environ.get("DATABASE_URL"),
        help="PostgreSQL DSN. Defaults to DATABASE_URL.",
    )
    parser.add_argument(
        "--date",
        dest="target_date",
        default="latest",
        help="Trading date (YYYY-MM-DD) or latest. Default: latest.",
    )
    parser.add_argument(
        "--min-turnover-million-yen",
        type=Decimal,
        default=Decimal("3000"),
        help="Minimum trading value in million yen. Default: 3000 (30 oku yen).",
    )
    parser.add_argument(
        "--lookback-bars",
        type=int,
        default=250,
        help="Initially visible daily sessions. Default: 250 (about one year).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=300,
        help="Maximum charts, ranked by turnover. Use 0 for no limit. Default: 300.",
    )
    parser.add_argument("--output", type=Path)
    parser.add_argument("--template", type=Path, default=DEFAULT_TEMPLATE)
    return parser.parse_args()


def resolve_target_date(conn: psycopg.Connection[Any], value: str) -> date:
    if value != "latest":
        return date.fromisoformat(value)
    with conn.cursor() as cur:
        cur.execute(
            """
            select max(record_date) as latest_record_date
            from raw.kabuplus_records
            where dataset_key = 'japan-all-stock-prices/daily'
            """
        )
        latest = cur.fetchone()["latest_record_date"]
    if latest is None:
        raise ValueError("No KABU+ daily stock price data is available.")
    return latest


def fetch_candidates(
    conn: psycopg.Connection[Any],
    *,
    target_date: date,
    min_turnover_thousand_yen: Decimal,
    limit: int,
) -> tuple[list[dict[str, Any]], int]:
    limit_sql = "limit %s" if limit > 0 else ""
    params: list[Any] = [target_date, min_turnover_thousand_yen]
    if limit > 0:
        params.append(limit)
    sql = f"""
        with candidates as (
            select
                security_code as sc,
                util.null_if_blank_or_dash(payload ->> '名称') as name,
                util.null_if_blank_or_dash(payload ->> '市場') as market,
                util.null_if_blank_or_dash(payload ->> '業種') as industry,
                util.to_numeric_or_null(payload ->> '株価') as close_price,
                util.to_numeric_or_null(payload ->> '前日比（％）') as day_change_pct,
                util.to_numeric_or_null(payload ->> '売買代金（千円）') as turnover_thousand_yen
            from raw.kabuplus_records
            where dataset_key = 'japan-all-stock-prices/daily'
              and record_date = %s
        ),
        matched as (
            select *, count(*) over () as matched_count
            from candidates
            where turnover_thousand_yen >= %s
              and sc is not null
              and close_price is not null
        )
        select *
        from matched
        order by turnover_thousand_yen desc nulls last, sc
        {limit_sql}
    """
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = [dict(row) for row in cur.fetchall()]
    matched_count = int(rows[0]["matched_count"]) if rows else 0
    for row in rows:
        row.pop("matched_count", None)
    return rows, matched_count


def fetch_price_history(
    conn: psycopg.Connection[Any],
    *,
    stock_codes: Sequence[str],
    target_date: date,
    lookback_bars: int,
) -> list[dict[str, Any]]:
    if not stock_codes:
        return []
    # Retain warm-up history for the SMAs and weekly view; crop only in the UI.
    start_date = target_date - timedelta(days=HISTORY_DAYS)
    sql = """
        select
            security_code as sc,
            coalesce(util.to_date_compact_or_null(payload ->> '日付'), record_date, file_date) as trade_date,
            util.to_numeric_or_null(payload ->> '始値') as raw_open_price,
            util.to_numeric_or_null(payload ->> '高値') as raw_high_price,
            util.to_numeric_or_null(payload ->> '安値') as raw_low_price,
            util.to_numeric_or_null(payload ->> '株価') as raw_close_price,
            util.to_numeric_or_null(payload ->> '出来高') as raw_volume
        from raw.kabuplus_records
        where dataset_key = 'japan-all-stock-prices/daily'
          and security_code = any(%s)
          and record_date between %s and %s
        order by security_code, record_date
    """
    with conn.cursor() as cur:
        cur.execute(sql, (list(stock_codes), start_date, target_date))
        raw_rows = [dict(row) for row in cur.fetchall()]
        cur.execute(
            """
            select sc, action_date, price_multiplier
            from analytics.effective_price_actions
            where sc = any(%s)
              and action_date <= %s
            order by sc, action_date
            """,
            (list(stock_codes), target_date),
        )
        actions = [dict(row) for row in cur.fetchall()]

    actions_by_code: dict[str, list[dict[str, Any]]] = {}
    for action in actions:
        actions_by_code.setdefault(action["sc"], []).append(action)

    adjusted_by_code: dict[str, list[dict[str, Any]]] = {}
    for row in raw_rows:
        if any(row[key] is None for key in (
            "trade_date", "raw_open_price", "raw_high_price", "raw_low_price", "raw_close_price"
        )):
            continue
        factor = Decimal("1")
        for action in actions_by_code.get(row["sc"], []):
            if action["action_date"] > row["trade_date"]:
                factor *= action["price_multiplier"]
        adjusted_by_code.setdefault(row["sc"], []).append(
            {
                "sc": row["sc"],
                "trade_date": row["trade_date"],
                "open_price": row["raw_open_price"] * factor,
                "high_price": row["raw_high_price"] * factor,
                "low_price": row["raw_low_price"] * factor,
                "close_price": row["raw_close_price"] * factor,
                "volume": (
                    row["raw_volume"] / factor if row["raw_volume"] is not None else None
                ),
            }
        )

    return [
        row
        for sc in stock_codes
        for row in adjusted_by_code.get(sc, [])
    ]


def main() -> int:
    args = parse_args()
    if not args.dsn:
        raise ValueError("Pass --dsn or set DATABASE_URL.")
    if args.min_turnover_million_yen < 0:
        raise ValueError("--min-turnover-million-yen must be non-negative.")
    if args.lookback_bars < 20 or args.lookback_bars > 500:
        raise ValueError("--lookback-bars must be between 20 and 500.")
    if args.limit < 0:
        raise ValueError("--limit must be zero or greater.")

    with psycopg.connect(args.dsn, row_factory=dict_row) as conn:
        target_date = resolve_target_date(conn, args.target_date)
        candidates, matched_count = fetch_candidates(
            conn,
            target_date=target_date,
            min_turnover_thousand_yen=args.min_turnover_million_yen * 1000,
            limit=args.limit,
        )
        if not candidates:
            raise ValueError(
                f"No stocks matched {target_date.isoformat()} at the requested threshold."
            )
        price_rows = fetch_price_history(
            conn,
            stock_codes=[row["sc"] for row in candidates],
            target_date=target_date,
            lookback_bars=args.lookback_bars,
        )

    payload = build_payload(
        candidates=candidates,
        price_rows=price_rows,
        target_date=target_date,
        min_turnover_million_yen=args.min_turnover_million_yen,
        lookback_bars=args.lookback_bars,
        matched_count=matched_count,
    )
    output_path = args.output or (
        DEFAULT_OUTPUT_DIR
        / f"{target_date.isoformat()}-turnover-{args.min_turnover_million_yen:g}m.html"
    )
    write_html(args.template, output_path, payload)
    print(
        json.dumps(
            {
                "output": str(output_path.resolve()),
                "target_date": target_date.isoformat(),
                "threshold_million_yen": float(args.min_turnover_million_yen),
                "matched": matched_count,
                "displayed": len(candidates),
                "lookback_bars": args.lookback_bars,
            },
            ensure_ascii=False,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
