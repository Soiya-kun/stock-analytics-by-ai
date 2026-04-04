from __future__ import annotations

import argparse
import csv
import json
import logging
import math
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import median
from typing import Iterable, Sequence

from entry_breakout_research import handle_command as handle_research_command
from entry_breakout_research import register_subcommands as register_research_subcommands

try:
    import psycopg
    from psycopg.rows import dict_row
except ModuleNotFoundError:  # pragma: no cover - local help can work without runtime deps
    psycopg = None
    dict_row = None

try:
    import yaml
except ModuleNotFoundError:  # pragma: no cover - local help can work without runtime deps
    yaml = None


DEFAULT_FORWARD_BARS = (20, 60, 120)


@dataclass(frozen=True)
class RangeBreakoutParams:
    lookback_years: int
    max_range_width_pct: float
    breakout_buffer_pct: float
    min_volume_ratio: float
    volume_lookback_bars: int
    cooldown_bars: int
    breakout_basis: str
    range_high_basis: str
    range_low_basis: str

    @property
    def lookback_bars(self) -> int:
        return self.lookback_years * 245

    @property
    def tag(self) -> str:
        return (
            f"lb{self.lookback_years}y"
            f"_rw{int(self.max_range_width_pct * 100)}"
            f"_bb{int(self.breakout_buffer_pct * 10000)}"
            f"_vr{int(self.min_volume_ratio * 100)}"
        )


@dataclass(frozen=True)
class BreakoutLabelStudyParams:
    candidate_start_date: date
    candidate_end_date: date
    range_lookback_bars: int
    max_range_width_pct: float
    breakout_buffer_pct: float
    min_volume_ratio: float
    volume_lookback_bars: int
    cooldown_bars: int
    trend_confirm_bars: int
    trend_eval_bars: int
    failure_drawdown_bars: int
    trend_min_return_pct: float
    trend_min_confirm_return_pct: float
    failure_drawdown_pct: float
    breakout_basis: str
    range_high_basis: str
    range_low_basis: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze multi-year range breakout candidates from PostgreSQL."
    )
    parser.add_argument(
        "--dsn",
        default=None,
        help="PostgreSQL DSN. Defaults to DATABASE_URL.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    grid = subparsers.add_parser(
        "grid-search",
        help="Run a parameter grid in parallel and summarize historical breakout performance.",
    )
    add_common_analysis_args(grid)
    grid.add_argument(
        "--lookback-years-grid",
        default="2,3,4,5",
        help="Comma-separated list of lookback years.",
    )
    grid.add_argument(
        "--range-width-pcts-grid",
        default="0.30,0.50,0.80",
        help="Comma-separated list of max range width percentages.",
    )
    grid.add_argument(
        "--breakout-buffer-pcts-grid",
        default="0.00,0.01,0.02",
        help="Comma-separated list of breakout buffer percentages.",
    )
    grid.add_argument(
        "--min-volume-ratios-grid",
        default="1.00,1.50",
        help="Comma-separated list of minimum breakout-day volume ratios.",
    )
    grid.add_argument(
        "--processes",
        type=int,
        default=4,
        help="Number of worker processes.",
    )
    grid.add_argument(
        "--primary-horizon",
        type=int,
        default=60,
        help="Forward bar horizon used for sorting the summary.",
    )
    grid.add_argument(
        "--max-combinations",
        type=int,
        default=None,
        help="Optional hard cap on parameter combinations after expansion.",
    )

    scan = subparsers.add_parser(
        "scan",
        help="Scan the latest market day for current breakout candidates.",
    )
    add_common_analysis_args(scan)
    add_single_parameter_args(scan)
    scan.add_argument(
        "--as-of-date",
        type=parse_date,
        default=None,
        help="Limit scan to the latest trading day on or before YYYY-MM-DD.",
    )
    scan.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Maximum number of rows to write.",
    )

    study = subparsers.add_parser(
        "label-study",
        help="Label breakout cases as trend / non_trend / neutral using forward price-only outcomes.",
    )
    add_label_study_args(study)
    register_research_subcommands(subparsers)

    return parser.parse_args()


def add_common_analysis_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--start-date",
        type=parse_date,
        default=date(2018, 1, 1),
        help="Evaluate signals on or after YYYY-MM-DD.",
    )
    parser.add_argument(
        "--end-date",
        type=parse_date,
        default=None,
        help="Evaluate signals using history on or before YYYY-MM-DD.",
    )
    parser.add_argument(
        "--forward-bars",
        default="20,60,120",
        help="Comma-separated forward bar horizons.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs") / "range-breakout",
        help="Directory for CSV and JSON outputs.",
    )
    parser.add_argument(
        "--volume-lookback-bars",
        type=int,
        default=20,
        help="Lookback bars for average breakout-day volume.",
    )
    parser.add_argument(
        "--cooldown-bars",
        type=int,
        default=60,
        help="Skip repeated signals for the same code inside this many bars.",
    )
    parser.add_argument(
        "--breakout-basis",
        choices=["close", "high"],
        default="close",
        help="Price series used to judge the breakout day.",
    )
    parser.add_argument(
        "--range-high-basis",
        choices=["close", "high"],
        default="close",
        help="Price series used for the upper edge of the range.",
    )
    parser.add_argument(
        "--range-low-basis",
        choices=["close", "low"],
        default="close",
        help="Price series used for the lower edge of the range.",
    )


def add_single_parameter_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--lookback-years",
        type=int,
        default=3,
        help="Breakout lookback in years.",
    )
    parser.add_argument(
        "--max-range-width-pct",
        type=float,
        default=0.50,
        help="Maximum allowed width of the historical range.",
    )
    parser.add_argument(
        "--breakout-buffer-pct",
        type=float,
        default=0.01,
        help="Required margin above the historical range high.",
    )
    parser.add_argument(
        "--min-volume-ratio",
        type=float,
        default=1.50,
        help="Minimum breakout-day volume relative to recent average.",
    )


def add_label_study_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--candidate-start-date",
        type=parse_date,
        default=date(2018, 7, 1),
        help="Earliest breakout date to study.",
    )
    parser.add_argument(
        "--candidate-end-date",
        type=parse_date,
        default=date(2020, 12, 30),
        help="Latest breakout date to study.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("research") / "range-breakout-2018-study",
        help="Directory for durable study artifacts.",
    )
    parser.add_argument(
        "--range-lookback-bars",
        type=int,
        default=120,
        help="Lookback bars used to define the pre-breakout range.",
    )
    parser.add_argument(
        "--max-range-width-pct",
        type=float,
        default=0.35,
        help="Maximum allowed width of the pre-breakout range.",
    )
    parser.add_argument(
        "--breakout-buffer-pct",
        type=float,
        default=0.02,
        help="Required breakout margin above the range high.",
    )
    parser.add_argument(
        "--min-volume-ratio",
        type=float,
        default=1.20,
        help="Minimum breakout-day volume relative to recent average volume.",
    )
    parser.add_argument(
        "--volume-lookback-bars",
        type=int,
        default=20,
        help="Lookback bars for average breakout-day volume.",
    )
    parser.add_argument(
        "--cooldown-bars",
        type=int,
        default=60,
        help="Suppress repeated breakout cases for the same code inside this many bars.",
    )
    parser.add_argument(
        "--trend-confirm-bars",
        type=int,
        default=120,
        help="Bars after breakout used to confirm sustained upside.",
    )
    parser.add_argument(
        "--trend-eval-bars",
        type=int,
        default=240,
        help="Bars after breakout used to judge long-term upside.",
    )
    parser.add_argument(
        "--failure-drawdown-bars",
        type=int,
        default=60,
        help="Bars after breakout used to judge early failure.",
    )
    parser.add_argument(
        "--trend-min-return-pct",
        type=float,
        default=0.40,
        help="Minimum future max return needed to call the case a long trend.",
    )
    parser.add_argument(
        "--trend-min-confirm-return-pct",
        type=float,
        default=0.20,
        help="Minimum return at trend-confirm-bars needed to call the case a long trend.",
    )
    parser.add_argument(
        "--failure-drawdown-pct",
        type=float,
        default=-0.10,
        help="Early drawdown threshold that marks a failed breakout.",
    )
    parser.add_argument(
        "--breakout-basis",
        choices=["close", "high"],
        default="close",
        help="Price series used to judge the breakout day.",
    )
    parser.add_argument(
        "--range-high-basis",
        choices=["close", "high"],
        default="close",
        help="Price series used for the upper edge of the range.",
    )
    parser.add_argument(
        "--range-low-basis",
        choices=["close", "low"],
        default="close",
        help="Price series used for the lower edge of the range.",
    )


def parse_date(value: str) -> date:
    return date.fromisoformat(value)


def parse_csv_ints(raw: str) -> list[int]:
    return [int(part.strip()) for part in raw.split(",") if part.strip()]


def parse_csv_floats(raw: str) -> list[float]:
    return [float(part.strip()) for part in raw.split(",") if part.strip()]


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def require_dsn(explicit_dsn: str | None) -> str:
    dsn = explicit_dsn or os.environ.get("DATABASE_URL")
    if not dsn:
        raise ValueError("Pass --dsn or set DATABASE_URL.")
    return dsn


def require_psycopg() -> None:
    if psycopg is None or dict_row is None:
        raise ModuleNotFoundError(
            "psycopg is required. Run this script through Docker Compose or install requirements.txt."
        )


def require_yaml() -> None:
    if yaml is None:
        raise ModuleNotFoundError(
            "PyYAML is required. Run this script through Docker Compose or install requirements.txt."
        )


def sql_date_literal(value: date | None) -> str | None:
    return value.isoformat() if value else None


def build_signal_query(
    params: RangeBreakoutParams,
    forward_bars: Sequence[int],
    start_date: date,
    end_date: date | None,
    latest_only: bool,
    latest_limit: int | None = None,
) -> str:
    if not forward_bars:
        raise ValueError("At least one forward bar horizon is required.")

    range_high_col = "high_price" if params.range_high_basis == "high" else "close_price"
    range_low_col = "low_price" if params.range_low_basis == "low" else "close_price"
    breakout_col = "high_price" if params.breakout_basis == "high" else "close_price"

    lead_columns = ",\n        ".join(
        f"lead(close_price, {bars}) over (partition by sc order by trade_date) as close_fwd_{bars}"
        for bars in forward_bars
    )
    select_forward_columns = ",\n    ".join(
        f"close_fwd_{bars}" for bars in forward_bars
    )

    base_filters = [
        "close_price is not null",
        "high_price is not null",
        "low_price is not null",
        "volume is not null",
    ]
    if end_date is not None:
        base_filters.append(f"trade_date <= date '{sql_date_literal(end_date)}'")

    signal_filters = [
        f"lookback_obs >= {params.lookback_bars}",
        "range_high is not null",
        "range_low is not null",
        "range_low > 0",
        f"range_width_pct <= {params.max_range_width_pct}",
        f"breakout_price >= range_high * (1 + {params.breakout_buffer_pct})",
        f"coalesce(volume_ratio, 0) >= {params.min_volume_ratio}",
        f"trade_date >= date '{sql_date_literal(start_date)}'",
    ]

    latest_market_day_cte = ""
    latest_filter = ""
    if latest_only:
        latest_market_day_cte = """
, latest_market_day as (
    select max(trade_date) as trade_date
    from analytics.stock_prices_adjusted_daily
"""
        latest_filters = []
        if end_date is not None:
            latest_filters.append(f"trade_date <= date '{sql_date_literal(end_date)}'")
        latest_market_day_cte += (
            ("    where " + " and ".join(latest_filters) + "\n") if latest_filters else ""
        )
        latest_market_day_cte += ")"
        latest_filter = "and trade_date = (select trade_date from latest_market_day)"

    limit_clause = f"\nlimit {latest_limit}" if latest_limit is not None else ""

    return f"""
with base as (
    select
        sc,
        name,
        market,
        industry,
        trade_date,
        close_price,
        high_price,
        low_price,
        volume,
        row_number() over (partition by sc order by trade_date) as trade_seq,
        count(*) over lookback_window as lookback_obs,
        max({range_high_col}) over lookback_window as range_high,
        min({range_low_col}) over lookback_window as range_low,
        avg(volume) over volume_window as avg_volume,
        {lead_columns}
    from analytics.stock_prices_adjusted_daily
    where {" and ".join(base_filters)}
    window
        lookback_window as (
            partition by sc
            order by trade_date
            rows between {params.lookback_bars} preceding and 1 preceding
        ),
        volume_window as (
            partition by sc
            order by trade_date
            rows between {params.volume_lookback_bars} preceding and 1 preceding
        )
),
signals as (
    select
        sc,
        name,
        market,
        industry,
        trade_date,
        trade_seq,
        lookback_obs,
        close_price,
        high_price,
        low_price,
        volume,
        range_high,
        range_low,
        avg_volume,
        {breakout_col} as breakout_price,
        (range_high - range_low) / nullif(range_low, 0) as range_width_pct,
        ({breakout_col} / nullif(range_high, 0)) - 1 as breakout_pct,
        volume / nullif(avg_volume, 0) as volume_ratio,
        {select_forward_columns}
    from base
)
{latest_market_day_cte}
select
    sc,
    name,
    market,
    industry,
    trade_date,
    trade_seq,
    lookback_obs,
    close_price,
    high_price,
    low_price,
    volume,
    range_high,
    range_low,
    avg_volume,
    breakout_price,
    range_width_pct,
    breakout_pct,
    volume_ratio,
    {select_forward_columns}
from signals
where {" and ".join(signal_filters)}
    {latest_filter}
order by breakout_pct desc, volume_ratio desc, sc, trade_date{limit_clause}
"""


def build_label_study_params(args: argparse.Namespace) -> BreakoutLabelStudyParams:
    return BreakoutLabelStudyParams(
        candidate_start_date=args.candidate_start_date,
        candidate_end_date=args.candidate_end_date,
        range_lookback_bars=args.range_lookback_bars,
        max_range_width_pct=args.max_range_width_pct,
        breakout_buffer_pct=args.breakout_buffer_pct,
        min_volume_ratio=args.min_volume_ratio,
        volume_lookback_bars=args.volume_lookback_bars,
        cooldown_bars=args.cooldown_bars,
        trend_confirm_bars=args.trend_confirm_bars,
        trend_eval_bars=args.trend_eval_bars,
        failure_drawdown_bars=args.failure_drawdown_bars,
        trend_min_return_pct=args.trend_min_return_pct,
        trend_min_confirm_return_pct=args.trend_min_confirm_return_pct,
        failure_drawdown_pct=args.failure_drawdown_pct,
        breakout_basis=args.breakout_basis,
        range_high_basis=args.range_high_basis,
        range_low_basis=args.range_low_basis,
    )


def build_label_study_query(
    params: BreakoutLabelStudyParams,
    source_relation: str = "analytics.stock_prices_adjusted_daily",
) -> str:
    base_start_date = params.candidate_start_date - timedelta(days=params.range_lookback_bars * 2)
    base_end_date = params.candidate_end_date + timedelta(days=params.trend_eval_bars * 2)
    range_high_col = "high_price" if params.range_high_basis == "high" else "close_price"
    range_low_col = "low_price" if params.range_low_basis == "low" else "close_price"
    breakout_col = "high_price" if params.breakout_basis == "high" else "close_price"

    future_bars = sorted(
        {
            20,
            60,
            params.trend_confirm_bars,
            params.trend_eval_bars,
        }
    )
    lead_columns = ",\n        ".join(
        f"lead(close_price, {bars}) over (partition by sc order by trade_date) as close_fwd_{bars}"
        for bars in future_bars
    )
    select_forward_columns = ",\n    ".join(
        f"close_fwd_{bars}" for bars in future_bars
    )

    return f"""
with base as (
    select
        sc,
        name,
        market,
        industry,
        trade_date,
        close_price,
        high_price,
        low_price,
        volume,
        row_number() over (partition by sc order by trade_date) as trade_seq,
        count(*) over range_window as lookback_obs,
        max({range_high_col}) over range_window as range_high,
        min({range_low_col}) over range_window as range_low,
        avg(volume) over volume_window as avg_volume,
        {lead_columns},
        max(close_price) over future_eval_window as future_max_close_{params.trend_eval_bars},
        min(close_price) over future_failure_window as future_min_close_{params.failure_drawdown_bars}
    from {source_relation}
    where close_price is not null
      and high_price is not null
      and low_price is not null
      and volume is not null
      and trade_date >= date '{base_start_date.isoformat()}'
      and trade_date <= date '{base_end_date.isoformat()}'
    window
        range_window as (
            partition by sc
            order by trade_date
            rows between {params.range_lookback_bars} preceding and 1 preceding
        ),
        volume_window as (
            partition by sc
            order by trade_date
            rows between {params.volume_lookback_bars} preceding and 1 preceding
        ),
        future_eval_window as (
            partition by sc
            order by trade_date
            rows between 1 following and {params.trend_eval_bars} following
        ),
        future_failure_window as (
            partition by sc
            order by trade_date
            rows between 1 following and {params.failure_drawdown_bars} following
        )
),
candidates as (
    select
        sc,
        name,
        market,
        industry,
        trade_date,
        trade_seq,
        lookback_obs,
        close_price,
        high_price,
        low_price,
        volume,
        range_high,
        range_low,
        avg_volume,
        {breakout_col} as breakout_price,
        (range_high - range_low) / nullif(range_low, 0) as range_width_pct,
        ({breakout_col} / nullif(range_high, 0)) - 1 as breakout_pct,
        volume / nullif(avg_volume, 0) as volume_ratio,
        {select_forward_columns},
        future_max_close_{params.trend_eval_bars},
        future_min_close_{params.failure_drawdown_bars}
    from base
)
select
    *
from candidates
where lookback_obs >= {params.range_lookback_bars}
  and trade_date >= date '{params.candidate_start_date.isoformat()}'
  and trade_date <= date '{params.candidate_end_date.isoformat()}'
  and range_high is not null
  and range_low is not null
  and range_low > 0
  and range_width_pct <= {params.max_range_width_pct}
  and breakout_price >= range_high * (1 + {params.breakout_buffer_pct})
  and coalesce(volume_ratio, 0) >= {params.min_volume_ratio}
order by sc, trade_date
"""


def fetch_label_study_rows(
    dsn: str,
    params: BreakoutLabelStudyParams,
) -> list[dict]:
    require_psycopg()
    base_start_date = params.candidate_start_date - timedelta(days=params.range_lookback_bars * 2)
    base_end_date = params.candidate_end_date + timedelta(days=params.trend_eval_bars * 2)
    query = build_label_study_query(params, source_relation="temp_price_daily")

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        conn.execute("set timezone to 'Asia/Tokyo'")
        with conn.cursor() as cur:
            logging.info(
                "Materializing temp_price_daily for %s to %s",
                base_start_date,
                base_end_date,
            )
            cur.execute("drop table if exists temp_price_daily")
            cur.execute(
                f"""
                create temporary table temp_price_daily as
                select
                    sc,
                    name,
                    market,
                    industry,
                    trade_date,
                    close_price,
                    high_price,
                    low_price,
                    volume
                from analytics.stock_prices_adjusted_daily
                where trade_date >= date '{base_start_date.isoformat()}'
                  and trade_date <= date '{base_end_date.isoformat()}'
                """
            )
            cur.execute(
                """
                create index temp_price_daily_sc_trade_date_idx
                    on temp_price_daily (sc, trade_date)
                """
            )
            cur.execute("analyze temp_price_daily")
            logging.info("Running label-study query")
            cur.execute(query)
            return list(cur.fetchall())


def classify_breakout_case(
    row: dict,
    params: BreakoutLabelStudyParams,
) -> dict[str, object]:
    breakout_price = to_float(row["breakout_price"])
    confirm_close = to_float(row[f"close_fwd_{params.trend_confirm_bars}"])
    eval_close = to_float(row[f"close_fwd_{params.trend_eval_bars}"])
    future_max_close = to_float(row[f"future_max_close_{params.trend_eval_bars}"])
    future_min_close = to_float(row[f"future_min_close_{params.failure_drawdown_bars}"])

    enriched = dict(row)
    if breakout_price is None or breakout_price == 0:
        enriched["label"] = "incomplete"
        enriched["label_reason"] = "breakout_price_missing"
        return enriched

    return_20d = compute_return_from_row(row, breakout_price, 20)
    return_60d = compute_return_from_row(row, breakout_price, 60)
    confirm_return = compute_return_from_value(confirm_close, breakout_price)
    eval_return = compute_return_from_value(eval_close, breakout_price)
    future_max_return = compute_return_from_value(future_max_close, breakout_price)
    future_min_return = compute_return_from_value(future_min_close, breakout_price)

    enriched["return_20d"] = return_20d
    enriched["return_60d"] = return_60d
    enriched[f"return_{params.trend_confirm_bars}d"] = confirm_return
    enriched[f"return_{params.trend_eval_bars}d"] = eval_return
    enriched[f"future_max_return_{params.trend_eval_bars}d"] = future_max_return
    enriched[f"future_min_return_{params.failure_drawdown_bars}d"] = future_min_return

    if (
        future_max_return is None
        or confirm_return is None
        or future_min_return is None
    ):
        enriched["label"] = "incomplete"
        enriched["label_reason"] = "insufficient_future_bars"
        return enriched

    if (
        future_max_return >= params.trend_min_return_pct
        and confirm_return >= params.trend_min_confirm_return_pct
        and future_min_return > params.failure_drawdown_pct
    ):
        enriched["label"] = "trend"
        enriched["label_reason"] = "future_max_and_confirm_return_cleared_thresholds"
        return enriched

    if future_min_return <= params.failure_drawdown_pct:
        enriched["label"] = "non_trend"
        enriched["label_reason"] = "early_drawdown_breached_failure_threshold"
        return enriched

    if future_max_return < params.trend_min_return_pct / 2:
        enriched["label"] = "non_trend"
        enriched["label_reason"] = "future_max_return_never_reached_half_trend_threshold"
        return enriched

    if confirm_return <= 0:
        enriched["label"] = "non_trend"
        enriched["label_reason"] = "confirm_horizon_return_non_positive"
        return enriched

    enriched["label"] = "neutral"
    enriched["label_reason"] = "advanced_but_not_enough_for_long_trend"
    return enriched


def compute_return_from_row(row: dict, breakout_price: float, bars: int) -> float | None:
    return compute_return_from_value(to_float(row.get(f"close_fwd_{bars}")), breakout_price)


def compute_return_from_value(value: float | None, base: float) -> float | None:
    if value is None or base == 0:
        return None
    return (value / base) - 1


def summarize_label_group(
    rows: Sequence[dict[str, object]],
    params: BreakoutLabelStudyParams,
) -> dict[str, object]:
    return {
        "count": len(rows),
        "unique_symbols": len({row["sc"] for row in rows}),
        "avg_breakout_pct": average_or_none(
            [to_float(row["breakout_pct"]) for row in rows if row.get("breakout_pct") is not None]
        ),
        "avg_range_width_pct": average_or_none(
            [to_float(row["range_width_pct"]) for row in rows if row.get("range_width_pct") is not None]
        ),
        "avg_volume_ratio": average_or_none(
            [to_float(row["volume_ratio"]) for row in rows if row.get("volume_ratio") is not None]
        ),
        "avg_return_20d": average_or_none(
            [to_float(row.get("return_20d")) for row in rows if row.get("return_20d") is not None]
        ),
        "avg_return_60d": average_or_none(
            [to_float(row.get("return_60d")) for row in rows if row.get("return_60d") is not None]
        ),
        f"avg_return_{params.trend_confirm_bars}d": average_or_none(
            [
                to_float(row.get(f"return_{params.trend_confirm_bars}d"))
                for row in rows
                if row.get(f"return_{params.trend_confirm_bars}d") is not None
            ]
        ),
        f"avg_return_{params.trend_eval_bars}d": average_or_none(
            [
                to_float(row.get(f"return_{params.trend_eval_bars}d"))
                for row in rows
                if row.get(f"return_{params.trend_eval_bars}d") is not None
            ]
        ),
        f"avg_future_max_return_{params.trend_eval_bars}d": average_or_none(
            [
                to_float(row.get(f"future_max_return_{params.trend_eval_bars}d"))
                for row in rows
                if row.get(f"future_max_return_{params.trend_eval_bars}d") is not None
            ]
        ),
        f"avg_future_min_return_{params.failure_drawdown_bars}d": average_or_none(
            [
                to_float(row.get(f"future_min_return_{params.failure_drawdown_bars}d"))
                for row in rows
                if row.get(f"future_min_return_{params.failure_drawdown_bars}d") is not None
            ]
        ),
    }


def build_label_study_summary(
    study_id: str,
    params: BreakoutLabelStudyParams,
    labeled_rows: Sequence[dict[str, object]],
) -> dict[str, object]:
    label_order = ("trend", "non_trend", "neutral", "incomplete")
    rows_by_label = {
        label: [row for row in labeled_rows if row["label"] == label]
        for label in label_order
    }

    complete_count = sum(
        len(rows_by_label[label]) for label in ("trend", "non_trend", "neutral")
    )
    trend_rate = (
        len(rows_by_label["trend"]) / complete_count if complete_count else None
    )

    return {
        "study_id": study_id,
        "generated_at": datetime.now().isoformat(),
        "dataset_key": "japan-all-stock-prices/daily",
        "candidate_window": {
            "start_date": params.candidate_start_date,
            "end_date": params.candidate_end_date,
        },
        "breakout_rule": {
            "range_lookback_bars": params.range_lookback_bars,
            "max_range_width_pct": params.max_range_width_pct,
            "breakout_buffer_pct": params.breakout_buffer_pct,
            "min_volume_ratio": params.min_volume_ratio,
            "volume_lookback_bars": params.volume_lookback_bars,
            "cooldown_bars": params.cooldown_bars,
            "breakout_basis": params.breakout_basis,
            "range_high_basis": params.range_high_basis,
            "range_low_basis": params.range_low_basis,
        },
        "label_rule": {
            "trend_confirm_bars": params.trend_confirm_bars,
            "trend_eval_bars": params.trend_eval_bars,
            "failure_drawdown_bars": params.failure_drawdown_bars,
            "trend_min_return_pct": params.trend_min_return_pct,
            "trend_min_confirm_return_pct": params.trend_min_confirm_return_pct,
            "failure_drawdown_pct": params.failure_drawdown_pct,
        },
        "counts": {
            "candidate_count": len(labeled_rows),
            "trend": len(rows_by_label["trend"]),
            "non_trend": len(rows_by_label["non_trend"]),
            "neutral": len(rows_by_label["neutral"]),
            "incomplete": len(rows_by_label["incomplete"]),
            "trend_rate_excluding_incomplete": trend_rate,
        },
        "label_stats": {
            label: summarize_label_group(rows_by_label[label], params)
            for label in label_order
        },
    }


def write_yaml(path: Path, payload: dict[str, object]) -> None:
    require_yaml()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        yaml.safe_dump(payload, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )


def build_label_study_report(
    study_id: str,
    params: BreakoutLabelStudyParams,
    summary: dict[str, object],
    labeled_rows: Sequence[dict[str, object]],
) -> str:
    rows_by_label = {
        label: [row for row in labeled_rows if row["label"] == label]
        for label in ("trend", "non_trend", "neutral", "incomplete")
    }
    chronological_trend = sorted(rows_by_label["trend"], key=lambda row: row["trade_date"])[:15]
    chronological_non_trend = sorted(rows_by_label["non_trend"], key=lambda row: row["trade_date"])[:15]

    lines = [
        f"# {study_id}",
        "",
        "## Method",
        "",
        "- Detect breakout candidates from a numeric price-only range definition.",
        "- Label each case as `trend`, `non_trend`, `neutral`, or `incomplete` using forward returns and early drawdown only.",
        "- Suppress repeated cases for the same code inside the cooldown window.",
        "",
        "## Parameters",
        "",
        f"- Candidate window: {params.candidate_start_date} to {params.candidate_end_date}",
        f"- Range lookback bars: {params.range_lookback_bars}",
        f"- Max range width pct: {params.max_range_width_pct}",
        f"- Breakout buffer pct: {params.breakout_buffer_pct}",
        f"- Min volume ratio: {params.min_volume_ratio}",
        f"- Trend confirm bars: {params.trend_confirm_bars}",
        f"- Trend eval bars: {params.trend_eval_bars}",
        f"- Failure drawdown bars: {params.failure_drawdown_bars}",
        f"- Trend min return pct: {params.trend_min_return_pct}",
        f"- Trend min confirm return pct: {params.trend_min_confirm_return_pct}",
        f"- Failure drawdown pct: {params.failure_drawdown_pct}",
        "",
        "## Label Counts",
        "",
        f"- Candidate count: {summary['counts']['candidate_count']}",
        f"- Trend: {summary['counts']['trend']}",
        f"- Non-trend: {summary['counts']['non_trend']}",
        f"- Neutral: {summary['counts']['neutral']}",
        f"- Incomplete: {summary['counts']['incomplete']}",
        f"- Trend rate excluding incomplete: {summary['counts']['trend_rate_excluding_incomplete']}",
        "",
        "## Chronological Trend Cases",
        "",
        "| breakout_date | sc | name | breakout_pct | return_120d | future_max_return | reason |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    lines.extend(
        format_report_row(row, params) for row in chronological_trend
    )
    lines.extend(
        [
            "",
            "## Chronological Non-Trend Cases",
            "",
            "| breakout_date | sc | name | breakout_pct | return_120d | future_min_return | reason |",
            "| --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    lines.extend(
        format_report_row(row, params, failure_view=True) for row in chronological_non_trend
    )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- This study is price-only. No fundamentals or news are used.",
            "- `neutral` means the breakout advanced, but not enough to satisfy the long-trend rule.",
            "- `incomplete` means there were not enough future bars to apply the label rule.",
        ]
    )
    return "\n".join(lines) + "\n"


def format_report_row(
    row: dict[str, object],
    params: BreakoutLabelStudyParams,
    failure_view: bool = False,
) -> str:
    confirm_key = f"return_{params.trend_confirm_bars}d"
    future_key = (
        f"future_min_return_{params.failure_drawdown_bars}d"
        if failure_view
        else f"future_max_return_{params.trend_eval_bars}d"
    )
    return (
        f"| {row['trade_date']} | {row['sc']} | {row['name']} | "
        f"{format_pct(row.get('breakout_pct'))} | {format_pct(row.get(confirm_key))} | "
        f"{format_pct(row.get(future_key))} | {row['label_reason']} |"
    )


def format_pct(value: object) -> str:
    numeric = to_float(value)
    if numeric is None:
        return ""
    return f"{numeric * 100:.2f}%"


def fetch_signal_rows(
    dsn: str,
    params: RangeBreakoutParams,
    forward_bars: Sequence[int],
    start_date: date,
    end_date: date | None,
    latest_only: bool,
    latest_limit: int | None = None,
) -> list[dict]:
    require_psycopg()
    query = build_signal_query(
        params=params,
        forward_bars=forward_bars,
        start_date=start_date,
        end_date=end_date,
        latest_only=latest_only,
        latest_limit=latest_limit,
    )

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        conn.execute("set timezone to 'Asia/Tokyo'")
        with conn.cursor() as cur:
            cur.execute(query)
            return list(cur.fetchall())


def dedupe_signals(rows: Iterable[dict], cooldown_bars: int) -> list[dict]:
    deduped: list[dict] = []
    last_trade_seq_by_sc: dict[str, int] = {}

    for row in sorted(rows, key=lambda item: (item["sc"], item["trade_date"], item["trade_seq"])):
        last_seq = last_trade_seq_by_sc.get(row["sc"])
        if last_seq is not None and row["trade_seq"] - last_seq <= cooldown_bars:
            continue
        deduped.append(row)
        last_trade_seq_by_sc[row["sc"]] = row["trade_seq"]

    return deduped


def to_float(value: object) -> float | None:
    if value is None:
        return None
    return float(value)


def compute_forward_metrics(rows: Sequence[dict], horizon: int) -> dict[str, float | int | None]:
    values: list[float] = []
    column_name = f"close_fwd_{horizon}"

    for row in rows:
        current_price = to_float(row["close_price"])
        future_price = to_float(row[column_name])
        if current_price is None or future_price is None or current_price == 0:
            continue
        values.append((future_price / current_price) - 1)

    if not values:
        return {
            f"return_{horizon}d_avg": None,
            f"return_{horizon}d_median": None,
            f"return_{horizon}d_win_ratio": None,
            f"return_{horizon}d_samples": 0,
        }

    wins = sum(1 for value in values if value > 0)
    return {
        f"return_{horizon}d_avg": sum(values) / len(values),
        f"return_{horizon}d_median": median(values),
        f"return_{horizon}d_win_ratio": wins / len(values),
        f"return_{horizon}d_samples": len(values),
    }


def summarize_rows(
    params: RangeBreakoutParams,
    rows: Sequence[dict],
    forward_bars: Sequence[int],
) -> dict[str, object]:
    breakout_values = [to_float(row["breakout_pct"]) for row in rows if row["breakout_pct"] is not None]
    range_width_values = [to_float(row["range_width_pct"]) for row in rows if row["range_width_pct"] is not None]
    volume_ratio_values = [to_float(row["volume_ratio"]) for row in rows if row["volume_ratio"] is not None]

    summary: dict[str, object] = {
        "lookback_years": params.lookback_years,
        "lookback_bars": params.lookback_bars,
        "max_range_width_pct": params.max_range_width_pct,
        "breakout_buffer_pct": params.breakout_buffer_pct,
        "min_volume_ratio": params.min_volume_ratio,
        "volume_lookback_bars": params.volume_lookback_bars,
        "cooldown_bars": params.cooldown_bars,
        "breakout_basis": params.breakout_basis,
        "range_high_basis": params.range_high_basis,
        "range_low_basis": params.range_low_basis,
        "signal_count": len(rows),
        "unique_symbols": len({row["sc"] for row in rows}),
        "latest_signal_date": max((row["trade_date"] for row in rows), default=None),
        "avg_breakout_pct": average_or_none(breakout_values),
        "avg_range_width_pct": average_or_none(range_width_values),
        "avg_volume_ratio": average_or_none(volume_ratio_values),
    }

    for bars in forward_bars:
        summary.update(compute_forward_metrics(rows, bars))

    return summary


def average_or_none(values: Sequence[float | None]) -> float | None:
    filtered = [value for value in values if value is not None]
    if not filtered:
        return None
    return sum(filtered) / len(filtered)


def worker_run_grid(payload: dict[str, object]) -> dict[str, object]:
    dsn = str(payload["dsn"])
    params = RangeBreakoutParams(**payload["params"])
    forward_bars = list(payload["forward_bars"])
    start_date = parse_date(str(payload["start_date"]))
    end_date = parse_date(str(payload["end_date"])) if payload["end_date"] else None

    rows = fetch_signal_rows(
        dsn=dsn,
        params=params,
        forward_bars=forward_bars,
        start_date=start_date,
        end_date=end_date,
        latest_only=False,
    )
    deduped_rows = dedupe_signals(rows, params.cooldown_bars)
    return summarize_rows(params, deduped_rows, forward_bars)


def expand_grid(args: argparse.Namespace) -> list[RangeBreakoutParams]:
    params_list = [
        RangeBreakoutParams(
            lookback_years=lookback_years,
            max_range_width_pct=max_range_width_pct,
            breakout_buffer_pct=breakout_buffer_pct,
            min_volume_ratio=min_volume_ratio,
            volume_lookback_bars=args.volume_lookback_bars,
            cooldown_bars=args.cooldown_bars,
            breakout_basis=args.breakout_basis,
            range_high_basis=args.range_high_basis,
            range_low_basis=args.range_low_basis,
        )
        for lookback_years in parse_csv_ints(args.lookback_years_grid)
        for max_range_width_pct in parse_csv_floats(args.range_width_pcts_grid)
        for breakout_buffer_pct in parse_csv_floats(args.breakout_buffer_pcts_grid)
        for min_volume_ratio in parse_csv_floats(args.min_volume_ratios_grid)
    ]
    if args.max_combinations is not None:
        params_list = params_list[: args.max_combinations]
    return params_list


def write_csv(path: Path, rows: Sequence[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return

    fieldnames = list(rows[0].keys())
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({key: normalize_for_csv(value) for key, value in row.items()})


def normalize_for_csv(value: object) -> object:
    if isinstance(value, float):
        return f"{value:.8f}"
    return value


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )


def run_grid_search(args: argparse.Namespace, dsn: str) -> int:
    forward_bars = parse_csv_ints(args.forward_bars)
    params_grid = expand_grid(args)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    logging.info(
        "Starting grid search with %s combinations and %s worker processes",
        len(params_grid),
        args.processes,
    )

    results: list[dict[str, object]] = []
    payloads = [
        {
            "dsn": dsn,
            "params": asdict(params),
            "forward_bars": forward_bars,
            "start_date": args.start_date.isoformat(),
            "end_date": args.end_date.isoformat() if args.end_date else None,
        }
        for params in params_grid
    ]

    with ProcessPoolExecutor(max_workers=args.processes) as executor:
        future_to_params = {
            executor.submit(worker_run_grid, payload): payload["params"] for payload in payloads
        }
        for future in as_completed(future_to_params):
            params_payload = future_to_params[future]
            result = future.result()
            results.append(result)
            logging.info(
                "Finished params lookback=%sy range=%.2f breakout=%.2f volume=%.2f -> signals=%s",
                params_payload["lookback_years"],
                params_payload["max_range_width_pct"],
                params_payload["breakout_buffer_pct"],
                params_payload["min_volume_ratio"],
                result["signal_count"],
            )

    primary_horizon = args.primary_horizon
    primary_column = f"return_{primary_horizon}d_avg"
    results.sort(
        key=lambda row: (
            row.get(primary_column) is not None,
            row.get(primary_column) if row.get(primary_column) is not None else -math.inf,
            row["signal_count"],
        ),
        reverse=True,
    )

    summary_path = output_dir / f"grid_summary_{timestamp}.csv"
    write_csv(summary_path, results)

    best_result = results[0] if results else {}
    meta_payload = {
        "command": "grid-search",
        "generated_at": datetime.now().isoformat(),
        "forward_bars": forward_bars,
        "start_date": args.start_date,
        "end_date": args.end_date,
        "processes": args.processes,
        "results_file": str(summary_path),
        "best_result": best_result,
    }
    write_json(output_dir / f"grid_summary_{timestamp}.json", meta_payload)

    logging.info("Wrote grid summary to %s", summary_path)
    if best_result:
        logging.info("Best combination: %s", best_result)
    return 0


def build_single_params(args: argparse.Namespace) -> RangeBreakoutParams:
    return RangeBreakoutParams(
        lookback_years=args.lookback_years,
        max_range_width_pct=args.max_range_width_pct,
        breakout_buffer_pct=args.breakout_buffer_pct,
        min_volume_ratio=args.min_volume_ratio,
        volume_lookback_bars=args.volume_lookback_bars,
        cooldown_bars=args.cooldown_bars,
        breakout_basis=args.breakout_basis,
        range_high_basis=args.range_high_basis,
        range_low_basis=args.range_low_basis,
    )


def run_scan(args: argparse.Namespace, dsn: str) -> int:
    forward_bars = parse_csv_ints(args.forward_bars)
    params = build_single_params(args)
    rows = fetch_signal_rows(
        dsn=dsn,
        params=params,
        forward_bars=forward_bars,
        start_date=args.start_date,
        end_date=args.as_of_date or args.end_date,
        latest_only=True,
        latest_limit=args.limit,
    )
    deduped_rows = dedupe_signals(rows, params.cooldown_bars)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    summary = summarize_rows(params, deduped_rows, forward_bars)
    signals_path = output_dir / f"latest_scan_{timestamp}.csv"
    meta_path = output_dir / f"latest_scan_{timestamp}.json"
    write_csv(signals_path, deduped_rows)
    write_json(
        meta_path,
        {
            "command": "scan",
            "generated_at": datetime.now().isoformat(),
            "params": asdict(params),
            "as_of_date": args.as_of_date or args.end_date,
            "summary": summary,
            "signals_file": str(signals_path),
        },
    )

    logging.info("Wrote latest scan results to %s", signals_path)
    logging.info("Scan summary: %s", summary)
    return 0


def run_label_study(args: argparse.Namespace, dsn: str) -> int:
    params = build_label_study_params(args)
    raw_rows = fetch_label_study_rows(dsn, params)
    deduped_rows = dedupe_signals(raw_rows, params.cooldown_bars)
    labeled_rows = [classify_breakout_case(row, params) for row in deduped_rows]

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    study_id = f"range_breakout_2018_study_{timestamp}"
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    summary = build_label_study_summary(study_id, params, labeled_rows)
    summary["files"] = {
        "all_cases_csv": str(output_dir / f"{study_id}_cases_all.csv"),
        "trend_cases_csv": str(output_dir / f"{study_id}_cases_trend.csv"),
        "non_trend_cases_csv": str(output_dir / f"{study_id}_cases_non_trend.csv"),
        "neutral_cases_csv": str(output_dir / f"{study_id}_cases_neutral.csv"),
        "incomplete_cases_csv": str(output_dir / f"{study_id}_cases_incomplete.csv"),
        "summary_yaml": str(output_dir / f"{study_id}_summary.yaml"),
        "report_md": str(output_dir / f"{study_id}_report.md"),
    }

    write_csv(Path(summary["files"]["all_cases_csv"]), labeled_rows)
    for label in ("trend", "non_trend", "neutral", "incomplete"):
        write_csv(
            Path(summary["files"][f"{label}_cases_csv"]),
            [row for row in labeled_rows if row["label"] == label],
        )
    write_yaml(Path(summary["files"]["summary_yaml"]), summary)
    Path(summary["files"]["report_md"]).write_text(
        build_label_study_report(study_id, params, summary, labeled_rows),
        encoding="utf-8",
    )

    logging.info("Wrote label study summary to %s", summary["files"]["summary_yaml"])
    logging.info("Label counts: %s", summary["counts"])
    return 0


def main() -> int:
    args = parse_args()
    configure_logging(args.log_level)
    dsn = require_dsn(args.dsn)

    if args.command == "grid-search":
        return run_grid_search(args, dsn)
    if args.command == "scan":
        return run_scan(args, dsn)
    if args.command == "label-study":
        return run_label_study(args, dsn)

    handled = handle_research_command(args, dsn)
    if handled is not None:
        return handled

    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
