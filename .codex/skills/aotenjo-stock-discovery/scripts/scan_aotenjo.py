from __future__ import annotations

import argparse
import csv
import json
import os
from datetime import date, datetime
from pathlib import Path
from typing import Any

try:
    import psycopg
    from psycopg.rows import dict_row
except ModuleNotFoundError:  # pragma: no cover - --help should work locally
    psycopg = None
    dict_row = None


BASIS_COLUMNS = {
    "high": "high_price",
    "close": "close_price",
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scan Japanese stocks that just entered adjusted all-time-high aotenjo territory."
    )
    parser.add_argument("--dsn", default=None, help="PostgreSQL DSN. Defaults to DATABASE_URL.")
    parser.add_argument(
        "--as-of-date",
        type=date.fromisoformat,
        default=None,
        help="Use the latest trading day on or before YYYY-MM-DD.",
    )
    parser.add_argument(
        "--latest",
        action="store_true",
        help="Scan the latest imported trading day. This is the default when --as-of-date is omitted.",
    )
    parser.add_argument(
        "--breakout-basis",
        choices=sorted(BASIS_COLUMNS),
        default="high",
        help="Use high for intraday aotenjo or close for close-confirmed aotenjo.",
    )
    parser.add_argument("--breakout-buffer-pct", type=float, default=0.0)
    parser.add_argument("--min-history-bars", type=int, default=500)
    parser.add_argument("--volume-lookback-bars", type=int, default=20)
    parser.add_argument("--min-volume-ratio", type=float, default=1.0)
    parser.add_argument("--min-turnover-thousand-yen", type=float, default=0.0)
    parser.add_argument("--limit", type=int, default=50)
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs") / "aotenjo",
        help="Directory for CSV and JSON outputs.",
    )
    return parser.parse_args()


def require_psycopg() -> None:
    if psycopg is None or dict_row is None:
        raise ModuleNotFoundError("psycopg is required. Run this through docker compose.")


def get_dsn(args: argparse.Namespace) -> str:
    dsn = args.dsn or os.environ.get("DATABASE_URL")
    if not dsn:
        raise RuntimeError("DATABASE_URL is not set. Pass --dsn or run through docker compose.")
    return dsn


def decimal_default(value: Any) -> Any:
    if hasattr(value, "isoformat"):
        return value.isoformat()
    if hasattr(value, "__float__"):
        return float(value)
    return str(value)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=decimal_default) + "\n",
        encoding="utf-8",
    )


def run_scan(args: argparse.Namespace) -> int:
    require_psycopg()
    if args.volume_lookback_bars < 1:
        raise ValueError("--volume-lookback-bars must be >= 1")
    if args.limit < 1:
        raise ValueError("--limit must be >= 1")

    dsn = get_dsn(args)
    basis_column = BASIS_COLUMNS[args.breakout_basis]
    params: dict[str, Any] = {
        "breakout_buffer_pct": args.breakout_buffer_pct,
        "min_history_bars": args.min_history_bars,
        "volume_lookback_bars": args.volume_lookback_bars,
        "min_volume_ratio": args.min_volume_ratio,
        "min_turnover_thousand_yen": args.min_turnover_thousand_yen,
        "limit": args.limit,
        "breakout_basis": args.breakout_basis,
    }

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        target_day = conn.execute(
            "select max(trade_date) as trade_date from analytics.stock_prices_adjusted_daily"
            + (" where trade_date <= %(as_of_date)s" if args.as_of_date is not None else ""),
            {"as_of_date": args.as_of_date} if args.as_of_date is not None else None,
        ).fetchone()["trade_date"]

        if target_day is None:
            raise RuntimeError("No trading day found for the requested scan.")

        params["target_day"] = target_day
        query = f"""
with target_rows as (
    select
        sc,
        name,
        market,
        industry,
        trade_date,
        high_price,
        close_price,
        {basis_column} as basis_price,
        volume,
        turnover_thousand_yen,
        day_change_pct
    from analytics.stock_prices_adjusted_daily
    where trade_date = %(target_day)s
      and high_price is not null
      and close_price is not null
      and {basis_column} is not null
),
history_ranked as (
    select
        sc,
        trade_date,
        {basis_column} as basis_price,
        volume,
        row_number() over (
            partition by sc
            order by trade_date desc
        ) as reverse_seq
    from analytics.stock_prices_adjusted_daily
    where trade_date < %(target_day)s
      and {basis_column} is not null
),
history_summary as (
    select
        sc,
        count(*) as history_bars,
        max(basis_price) as prior_all_time_high,
        avg(volume) filter (where reverse_seq <= %(volume_lookback_bars)s) as avg_volume
    from history_ranked
    group by sc
),
history_high_dates as (
    select
        h.sc,
        max(h.trade_date) as prior_all_time_high_date
    from history_ranked h
    join history_summary s
      on s.sc = h.sc
     and s.prior_all_time_high = h.basis_price
    group by h.sc
),
candidates as (
    select
        t.*,
        s.history_bars,
        s.prior_all_time_high,
        d.prior_all_time_high_date,
        s.avg_volume,
        (t.basis_price / nullif(s.prior_all_time_high, 0) - 1)::numeric(20, 10) as breakout_margin_pct,
        (t.volume / nullif(s.avg_volume, 0))::numeric(20, 10) as volume_ratio
    from target_rows t
    join history_summary s on s.sc = t.sc
    left join history_high_dates d on d.sc = t.sc
    where s.history_bars >= %(min_history_bars)s
      and s.prior_all_time_high is not null
      and s.prior_all_time_high > 0
      and t.basis_price > s.prior_all_time_high * (1 + %(breakout_buffer_pct)s)
      and coalesce(t.volume / nullif(s.avg_volume, 0), 0) >= %(min_volume_ratio)s
      and coalesce(t.turnover_thousand_yen, 0) >= %(min_turnover_thousand_yen)s
)
select
    c.sc,
    c.name,
    c.market,
    c.industry,
    c.trade_date,
    %(breakout_basis)s as breakout_basis,
    c.basis_price,
    c.prior_all_time_high,
    c.prior_all_time_high_date,
    c.breakout_margin_pct,
    c.close_price,
    c.high_price,
    c.day_change_pct,
    c.volume,
    c.avg_volume,
    c.volume_ratio,
    c.turnover_thousand_yen,
    c.history_bars
from candidates c
order by c.volume_ratio desc nulls last,
         c.breakout_margin_pct desc,
         c.turnover_thousand_yen desc nulls last
limit %(limit)s
"""
        rows = [dict(row) for row in conn.execute(query, params).fetchall()]

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    stem = f"aotenjo_{target_day}_{args.breakout_basis}_{timestamp}"
    csv_path = args.output_dir / f"{stem}.csv"
    summary_path = args.output_dir / f"{stem}_summary.json"
    write_csv(csv_path, rows)
    write_json(
        summary_path,
        {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "target_trade_date": target_day,
            "parameters": {
                "as_of_date": args.as_of_date,
                "breakout_basis": args.breakout_basis,
                "breakout_buffer_pct": args.breakout_buffer_pct,
                "min_history_bars": args.min_history_bars,
                "volume_lookback_bars": args.volume_lookback_bars,
                "min_volume_ratio": args.min_volume_ratio,
                "min_turnover_thousand_yen": args.min_turnover_thousand_yen,
                "limit": args.limit,
            },
            "candidate_count_returned": len(rows),
            "files": {
                "candidates_csv": str(csv_path),
                "summary_json": str(summary_path),
            },
        },
    )
    print(f"target_trade_date={target_day} candidates={len(rows)}")
    print(f"candidates_csv={csv_path}")
    print(f"summary_json={summary_path}")
    return 0


def main() -> int:
    args = parse_args()
    return run_scan(args)


if __name__ == "__main__":
    raise SystemExit(main())
