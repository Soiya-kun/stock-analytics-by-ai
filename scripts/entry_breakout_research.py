from __future__ import annotations

import argparse
import csv
import hashlib
import json
import logging
import math
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Sequence

try:
    import psycopg
    from psycopg.rows import dict_row
except ModuleNotFoundError:  # pragma: no cover
    psycopg = None
    dict_row = None

try:
    import yaml
except ModuleNotFoundError:  # pragma: no cover
    yaml = None


INFERENCE_OUTPUT_DIR = Path("research") / "inferred-price-actions"
ENTRY_OUTPUT_DIR = Path("research") / "entry-breakout-6m"
SOURCE_RELATION = "analytics.stock_prices_adjusted_daily"
INFERENCE_LOWER_RATIO = 0.09
INFERENCE_UPPER_SPLIT_RATIO = 0.55
INFERENCE_LOWER_REVERSE_RATIO = 1.80
INFERENCE_UPPER_REVERSE_RATIO = 10.50
FEATURE_COLUMNS = (
    "breakout_margin_pct",
    "day_return_pct",
    "gap_pct",
    "body_ratio",
    "upper_wick_ratio",
    "lower_wick_ratio",
    "volume_ratio",
    "bullish_count_10",
    "bullish_count_20",
    "bullish_count_60",
    "up_day_count_10",
    "up_day_count_20",
    "up_day_count_60",
    "high_volume_bullish_count_20",
    "high_volume_bullish_count_60",
    "long_upper_wick_count_20",
    "long_upper_wick_count_60",
    "long_lower_wick_count_20",
    "long_lower_wick_count_60",
    "prior_return_20d_pct",
    "prior_return_60d_pct",
    "ma_gap_20_pct",
    "ma_gap_60_pct",
    "ma_slope_20_pct",
    "ma_slope_60_pct",
    "range_width_pct",
    "range_high_touch_count_120",
    "higher_high_count_20",
    "higher_low_count_20",
    "atr_20_pct",
    "atr_20_to_range_ratio",
)


@dataclass(frozen=True)
class PriceActionInferenceParams:
    min_factor: int
    max_factor: int
    median_error_threshold: float
    max_error_threshold: float
    output_dir: Path


@dataclass(frozen=True)
class EntryStudyParams:
    output_dir: Path
    train_start_date: date
    train_end_date: date
    validation_start_date: date
    validation_end_date: date
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
    source_relation: str = SOURCE_RELATION

    @property
    def candidate_start_date(self) -> date:
        return self.train_start_date

    @property
    def candidate_end_date(self) -> date:
        return self.validation_end_date


def register_subcommands(subparsers: argparse._SubParsersAction[argparse.ArgumentParser]) -> None:
    infer_parser = subparsers.add_parser(
        "infer-price-actions",
        help="Infer split and reverse-split events from one-day OHLC integer jumps.",
    )
    infer_parser.add_argument("--output-dir", type=Path, default=INFERENCE_OUTPUT_DIR)
    infer_parser.add_argument("--min-factor", type=int, default=2)
    infer_parser.add_argument("--max-factor", type=int, default=10)
    infer_parser.add_argument("--median-error-threshold", type=float, default=0.06)
    infer_parser.add_argument("--max-error-threshold", type=float, default=0.12)

    prepare_parser = subparsers.add_parser(
        "prepare-adjusted-prices",
        help="Validate the adjusted-price layer and write an audit summary.",
    )
    prepare_parser.add_argument("--output-dir", type=Path, default=INFERENCE_OUTPUT_DIR)

    dataset_parser = subparsers.add_parser(
        "build-entry-dataset",
        help="Build the durable 6-month breakout entry dataset from adjusted prices.",
    )
    add_entry_study_args(dataset_parser)

    mine_parser = subparsers.add_parser(
        "mine-entry-hypotheses",
        help="Mine interpretable threshold rules from the train split.",
    )
    mine_parser.add_argument("--run-id", default=None)
    mine_parser.add_argument("--output-dir", type=Path, default=ENTRY_OUTPUT_DIR)
    mine_parser.add_argument("--processes", type=int, default=4)
    mine_parser.add_argument("--min-cases", type=int, default=20)
    mine_parser.add_argument("--top-features", type=int, default=8)
    mine_parser.add_argument("--top-rules-per-feature", type=int, default=3)

    eval_parser = subparsers.add_parser(
        "evaluate-entry-hypotheses",
        help="Evaluate mined entry hypotheses on the validation split.",
    )
    eval_parser.add_argument("--run-id", default=None)
    eval_parser.add_argument("--output-dir", type=Path, default=ENTRY_OUTPUT_DIR)


def add_entry_study_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--output-dir", type=Path, default=ENTRY_OUTPUT_DIR)
    parser.add_argument("--train-start-date", type=date.fromisoformat, default=date(2018, 1, 1))
    parser.add_argument("--train-end-date", type=date.fromisoformat, default=date(2020, 12, 31))
    parser.add_argument("--validation-start-date", type=date.fromisoformat, default=date(2021, 1, 1))
    parser.add_argument("--validation-end-date", type=date.fromisoformat, default=date(2022, 12, 30))
    parser.add_argument("--range-lookback-bars", type=int, default=120)
    parser.add_argument("--max-range-width-pct", type=float, default=0.35)
    parser.add_argument("--breakout-buffer-pct", type=float, default=0.02)
    parser.add_argument("--min-volume-ratio", type=float, default=1.20)
    parser.add_argument("--volume-lookback-bars", type=int, default=20)
    parser.add_argument("--cooldown-bars", type=int, default=60)
    parser.add_argument("--trend-confirm-bars", type=int, default=120)
    parser.add_argument("--trend-eval-bars", type=int, default=240)
    parser.add_argument("--failure-drawdown-bars", type=int, default=60)
    parser.add_argument("--trend-min-return-pct", type=float, default=0.40)
    parser.add_argument("--trend-min-confirm-return-pct", type=float, default=0.20)
    parser.add_argument("--failure-drawdown-pct", type=float, default=-0.10)
    parser.add_argument("--breakout-basis", choices=["close", "high"], default="close")
    parser.add_argument("--range-high-basis", choices=["close", "high"], default="close")
    parser.add_argument("--range-low-basis", choices=["close", "low"], default="close")


def handle_command(args: argparse.Namespace, dsn: str) -> int | None:
    if args.command == "infer-price-actions":
        return run_infer_price_actions(args, dsn)
    if args.command == "prepare-adjusted-prices":
        return run_prepare_adjusted_prices(args, dsn)
    if args.command == "build-entry-dataset":
        return run_build_entry_dataset(args, dsn)
    if args.command == "mine-entry-hypotheses":
        return run_mine_entry_hypotheses(args, dsn)
    if args.command == "evaluate-entry-hypotheses":
        return run_evaluate_entry_hypotheses(args, dsn)
    return None


def require_psycopg() -> None:
    if psycopg is None or dict_row is None:
        raise ModuleNotFoundError("psycopg is required. Run this through docker compose.")


def require_yaml() -> None:
    if yaml is None:
        raise ModuleNotFoundError("PyYAML is required. Run this through docker compose.")


def to_float(value: object) -> float | None:
    if value is None:
        return None
    return float(value)


def to_int(value: object) -> int | None:
    if value is None:
        return None
    return int(value)


def iso_timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def average_or_none(values: Iterable[float | None]) -> float | None:
    filtered = [value for value in values if value is not None]
    if not filtered:
        return None
    return sum(filtered) / len(filtered)


def format_pct(value: object) -> str:
    numeric = to_float(value)
    if numeric is None:
        return ""
    return f"{numeric * 100:.2f}%"


def normalize_for_csv(value: object) -> object:
    if isinstance(value, float):
        return f"{value:.10f}"
    return value


def json_default(value: object) -> object:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return str(value)


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


def write_yaml(path: Path, payload: dict[str, object]) -> None:
    require_yaml()
    path.parent.mkdir(parents=True, exist_ok=True)
    serializable_payload = json.loads(
        json.dumps(payload, ensure_ascii=False, default=json_default)
    )
    path.write_text(
        yaml.safe_dump(serializable_payload, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )


def build_inference_params(args: argparse.Namespace) -> PriceActionInferenceParams:
    return PriceActionInferenceParams(
        min_factor=args.min_factor,
        max_factor=args.max_factor,
        median_error_threshold=args.median_error_threshold,
        max_error_threshold=args.max_error_threshold,
        output_dir=args.output_dir,
    )


def build_entry_params(args: argparse.Namespace) -> EntryStudyParams:
    return EntryStudyParams(
        output_dir=args.output_dir,
        train_start_date=args.train_start_date,
        train_end_date=args.train_end_date,
        validation_start_date=args.validation_start_date,
        validation_end_date=args.validation_end_date,
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


def fetch_inference_source_rows(dsn: str) -> list[dict[str, object]]:
    require_psycopg()
    query = f"""
select
    sc,
    name,
    market,
    industry,
    trade_date,
    null::date as prev_trade_date,
    open_price,
    high_price,
    low_price,
    close_price,
    previous_close_price as prev_close_price
from analytics.stock_prices_daily
where previous_close_price is not null
  and open_price is not null
  and high_price is not null
  and low_price is not null
  and close_price is not null
  and (
        open_price / nullif(previous_close_price, 0) between {INFERENCE_LOWER_RATIO} and {INFERENCE_UPPER_SPLIT_RATIO}
     or close_price / nullif(previous_close_price, 0) between {INFERENCE_LOWER_RATIO} and {INFERENCE_UPPER_SPLIT_RATIO}
     or open_price / nullif(previous_close_price, 0) between {INFERENCE_LOWER_REVERSE_RATIO} and {INFERENCE_UPPER_REVERSE_RATIO}
     or close_price / nullif(previous_close_price, 0) between {INFERENCE_LOWER_REVERSE_RATIO} and {INFERENCE_UPPER_REVERSE_RATIO}
  )
"""
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        conn.execute("set timezone to 'Asia/Tokyo'")
        with conn.cursor() as cur:
            cur.execute(query)
            return list(cur.fetchall())


def detect_inferred_price_action(
    row: dict[str, object],
    params: PriceActionInferenceParams,
) -> dict[str, object] | None:
    prev_close = to_float(row["prev_close_price"])
    if prev_close is None or prev_close <= 0:
        return None

    ohlc_pairs = [
        ("open", to_float(row["open_price"])),
        ("high", to_float(row["high_price"])),
        ("low", to_float(row["low_price"])),
        ("close", to_float(row["close_price"])),
    ]
    if any(value is None or value <= 0 for _, value in ohlc_pairs):
        return None

    best_candidate: dict[str, object] | None = None
    for integer_factor in range(params.min_factor, params.max_factor + 1):
        for action_type, target_ratio in (
            ("split", 1.0 / integer_factor),
            ("reverse_split", float(integer_factor)),
        ):
            rel_errors = []
            ratios = {}
            error_by_field = {}
            for label, value in ohlc_pairs:
                ratio = value / prev_close
                rel_error = abs((ratio / target_ratio) - 1)
                ratios[label] = ratio
                error_by_field[label] = rel_error
                rel_errors.append(rel_error)

            rel_errors_sorted = sorted(rel_errors)
            median_error = (
                (rel_errors_sorted[1] + rel_errors_sorted[2]) / 2
                if len(rel_errors_sorted) == 4
                else rel_errors_sorted[len(rel_errors_sorted) // 2]
            )
            max_error = max(rel_errors)
            if median_error > params.median_error_threshold:
                continue
            if max_error > params.max_error_threshold:
                continue

            candidate = {
                "sc": row["sc"],
                "action_date": row["trade_date"],
                "action_type": action_type,
                "integer_factor": integer_factor,
                "price_multiplier": (1 / integer_factor) if action_type == "split" else integer_factor,
                "detection_method": "ohlc_integer_jump",
                "evidence_json": {
                    "name": row["name"],
                    "market": row["market"],
                    "industry": row["industry"],
                    "prev_trade_date": row["prev_trade_date"],
                    "prev_close_price": prev_close,
                    "ohlc": {label: value for label, value in ohlc_pairs},
                    "ohlc_ratios_to_prev_close": ratios,
                    "target_ratio": target_ratio,
                    "median_relative_error": median_error,
                    "max_relative_error": max_error,
                    "relative_errors": error_by_field,
                    "thresholds": {
                        "median_error_threshold": params.median_error_threshold,
                        "max_error_threshold": params.max_error_threshold,
                    },
                },
            }
            score = (median_error, max_error, integer_factor)
            if best_candidate is None or score < best_candidate["_score"]:
                best_candidate = {**candidate, "_score": score}

    if best_candidate is None:
        return None

    best_candidate.pop("_score", None)
    return best_candidate


def persist_inferred_price_actions(dsn: str, events: Sequence[dict[str, object]]) -> None:
    require_psycopg()
    insert_sql = """
insert into analytics.inferred_price_actions (
    sc,
    action_date,
    action_type,
    integer_factor,
    price_multiplier,
    detection_method,
    evidence_json
)
values (%s, %s, %s, %s, %s, %s, %s::jsonb)
"""
    with psycopg.connect(dsn) as conn:
        conn.execute("set timezone to 'Asia/Tokyo'")
        with conn.cursor() as cur:
            cur.execute(
                "delete from analytics.inferred_price_actions where detection_method = 'ohlc_integer_jump'"
            )
            if events:
                cur.executemany(
                    insert_sql,
                    [
                        (
                            event["sc"],
                            event["action_date"],
                            event["action_type"],
                            event["integer_factor"],
                            event["price_multiplier"],
                            event["detection_method"],
                            json.dumps(event["evidence_json"], ensure_ascii=False),
                        )
                        for event in events
                    ],
                )
        conn.commit()


def fetch_official_corporate_actions(dsn: str) -> list[dict[str, object]]:
    require_psycopg()
    query = """
select
    sc,
    action_date,
    split_ratio
from analytics.corporate_actions_monthly
where action_date is not null
  and split_ratio is not null
"""
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        conn.execute("set timezone to 'Asia/Tokyo'")
        with conn.cursor() as cur:
            cur.execute(query)
            return list(cur.fetchall())


def build_inference_artifacts(
    params: PriceActionInferenceParams,
    events: Sequence[dict[str, object]],
    official_rows: Sequence[dict[str, object]],
) -> tuple[dict[str, object], str]:
    official_by_key = {(row["sc"], row["action_date"]): row for row in official_rows}
    event_keys = {(event["sc"], event["action_date"]) for event in events}
    exact_matches = sum(1 for event in events if (event["sc"], event["action_date"]) in official_by_key)
    inferred_only = [
        event
        for event in events
        if (event["sc"], event["action_date"]) not in official_by_key
    ]
    official_only = [
        row
        for key, row in official_by_key.items()
        if key not in event_keys
    ]
    summary = {
        "type": "inferred_price_actions",
        "version": 1,
        "generated_at": datetime.now().isoformat(),
        "detection_method": "ohlc_integer_jump",
        "parameters": {
            "min_factor": params.min_factor,
            "max_factor": params.max_factor,
            "median_error_threshold": params.median_error_threshold,
            "max_error_threshold": params.max_error_threshold,
        },
        "counts": {
            "event_count": len(events),
            "split": sum(1 for event in events if event["action_type"] == "split"),
            "reverse_split": sum(1 for event in events if event["action_type"] == "reverse_split"),
            "matched_official_same_date": exact_matches,
            "inferred_only_same_date": len(inferred_only),
            "official_only_same_date": len(official_only),
        },
        "samples": {
            "inferred_only": [
                {
                    "sc": event["sc"],
                    "action_date": event["action_date"],
                    "action_type": event["action_type"],
                    "integer_factor": event["integer_factor"],
                }
                for event in inferred_only[:20]
            ],
            "official_only": official_only[:20],
        },
    }
    lines = [
        "# Inferred Price Action Audit",
        "",
        "## Method",
        "",
        "- Raw prices remain untouched in `analytics.stock_prices_daily`.",
        "- One-day integer jump inference uses only `open/high/low/close` against the previous close.",
        "- Inferred events are stored in `analytics.inferred_price_actions` and applied later in `analytics.stock_prices_adjusted_daily`.",
        "",
        "## Counts",
        "",
        f"- Event count: {summary['counts']['event_count']}",
        f"- Split: {summary['counts']['split']}",
        f"- Reverse split: {summary['counts']['reverse_split']}",
        f"- Matched official same date: {summary['counts']['matched_official_same_date']}",
        f"- Inferred only same date: {summary['counts']['inferred_only_same_date']}",
        f"- Official only same date: {summary['counts']['official_only_same_date']}",
        "",
        "## Inferred-Only Samples",
        "",
        "| action_date | sc | action_type | integer_factor |",
        "| --- | --- | --- | --- |",
    ]
    lines.extend(
        f"| {event['action_date']} | {event['sc']} | {event['action_type']} | {event['integer_factor']} |"
        for event in inferred_only[:20]
    )
    lines.extend(
        [
            "",
            "## Official-Only Samples",
            "",
            "| action_date | sc | split_ratio |",
            "| --- | --- | --- |",
        ]
    )
    lines.extend(
        f"| {row['action_date']} | {row['sc']} | {row['split_ratio']} |"
        for row in official_only[:20]
    )
    return summary, "\n".join(lines) + "\n"


def run_infer_price_actions(args: argparse.Namespace, dsn: str) -> int:
    params = build_inference_params(args)
    source_rows = fetch_inference_source_rows(dsn)
    events = []
    for row in source_rows:
        detected = detect_inferred_price_action(row, params)
        if detected is not None:
            events.append(detected)

    persist_inferred_price_actions(dsn, events)
    official_rows = fetch_official_corporate_actions(dsn)
    summary, report = build_inference_artifacts(params, events, official_rows)

    run_id = f"price_action_inference_{iso_timestamp()}"
    output_dir = params.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = output_dir / f"{run_id}_summary.yaml"
    report_path = output_dir / f"{run_id}_split_audit.md"
    events_path = output_dir / f"{run_id}_events.csv"
    write_yaml(summary_path, summary)
    report_path.write_text(report, encoding="utf-8")
    write_csv(
        events_path,
        [
            {
                "sc": event["sc"],
                "action_date": event["action_date"],
                "action_type": event["action_type"],
                "integer_factor": event["integer_factor"],
                "price_multiplier": event["price_multiplier"],
                "median_relative_error": event["evidence_json"]["median_relative_error"],
                "max_relative_error": event["evidence_json"]["max_relative_error"],
            }
            for event in events
        ],
    )
    logging.info("Detected %s inferred price actions", len(events))
    logging.info("Wrote inferred price-action summary to %s", summary_path)
    return 0


def fetch_adjusted_price_summary(dsn: str) -> dict[str, object]:
    require_psycopg()
    query = """
with import_summary as (
    select
        completed_rows as raw_rows,
        first_file_date as first_trade_date,
        last_file_date as last_trade_date
    from analytics.import_status
    where dataset_key = 'japan-all-stock-prices/daily'
),
event_counts as (
    select
        count(*) as event_count,
        count(*) filter (where action_type = 'split') as split_count,
        count(*) filter (where action_type = 'reverse_split') as reverse_split_count,
        count(distinct sc) as adjusted_symbols
    from analytics.inferred_price_actions
)
select
    import_summary.raw_rows,
    import_summary.raw_rows as adjusted_rows,
    null::bigint as adjusted_event_rows,
    event_counts.adjusted_symbols,
    import_summary.first_trade_date,
    import_summary.last_trade_date,
    event_counts.event_count,
    event_counts.split_count,
    event_counts.reverse_split_count
from import_summary
cross join event_counts
"""
    sample_query = """
with prev_rows as (
    select
        ipa.sc,
        ipa.action_date,
        ipa.action_type,
        ipa.integer_factor,
        (
            select max(p.trade_date)
            from analytics.stock_prices_adjusted_daily p
            where p.sc = ipa.sc
              and p.trade_date < ipa.action_date
        ) as prev_trade_date
    from analytics.inferred_price_actions ipa
    order by ipa.action_date desc, ipa.sc
    limit 20
)
select
    p.sc,
    p.action_date,
    p.action_type,
    p.integer_factor,
    p.prev_trade_date,
    prev.close_price as prev_raw_close,
    curr.close_price as action_day_raw_close,
    (
        prev.close_price * coalesce(
            (
                select exp(sum(ln(ipa2.price_multiplier::double precision)))::numeric(20, 10)
                from analytics.inferred_price_actions ipa2
                where ipa2.sc = p.sc
                  and ipa2.action_date > p.prev_trade_date
            ),
            1::numeric(20, 10)
        )
    )::numeric(20, 8) as prev_adjusted_close,
    (
        curr.close_price * coalesce(
            (
                select exp(sum(ln(ipa3.price_multiplier::double precision)))::numeric(20, 10)
                from analytics.inferred_price_actions ipa3
                where ipa3.sc = p.sc
                  and ipa3.action_date > p.action_date
            ),
            1::numeric(20, 10)
        )
    )::numeric(20, 8) as action_day_adjusted_close
from prev_rows p
left join analytics.stock_prices_daily prev
    on prev.sc = p.sc and prev.trade_date = p.prev_trade_date
left join analytics.stock_prices_daily curr
    on curr.sc = p.sc and curr.trade_date = p.action_date
order by p.action_date desc, p.sc
"""
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        conn.execute("set timezone to 'Asia/Tokyo'")
        with conn.cursor() as cur:
            cur.execute(query)
            summary_row = dict(cur.fetchone())
            cur.execute(sample_query)
            summary_row["samples"] = list(cur.fetchall())
            return summary_row


def run_prepare_adjusted_prices(args: argparse.Namespace, dsn: str) -> int:
    summary = fetch_adjusted_price_summary(dsn)
    run_id = f"adjusted_prices_{iso_timestamp()}"
    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = output_dir / f"{run_id}_summary.yaml"
    report_path = output_dir / f"{run_id}_audit.md"
    write_yaml(
        summary_path,
        {
            "type": "adjusted_price_audit",
            "version": 1,
            "generated_at": datetime.now().isoformat(),
            "summary": summary,
        },
    )
    lines = [
        "# Adjusted Price Audit",
        "",
        "## Counts",
        "",
        f"- Raw rows: {summary['raw_rows']}",
        f"- Adjusted rows: {summary['adjusted_rows']}",
        f"- Rows with non-1 adjustment factor: {summary['adjusted_event_rows']}",
        f"- Symbols with inferred actions: {summary['adjusted_symbols']}",
        f"- Inferred events: {summary['event_count']}",
        f"- Split events: {summary['split_count']}",
        f"- Reverse split events: {summary['reverse_split_count']}",
        f"- First trade date: {summary['first_trade_date']}",
        f"- Last trade date: {summary['last_trade_date']}",
        "",
        "## Continuity Samples",
        "",
        "| action_date | sc | type | factor | prev_raw_close | action_day_raw_close | prev_adjusted_close | action_day_adjusted_close |",
        "| --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    lines.extend(
        f"| {row['action_date']} | {row['sc']} | {row['action_type']} | {row['integer_factor']} | "
        f"{row['prev_raw_close']} | {row['action_day_raw_close']} | "
        f"{row['prev_adjusted_close']} | {row['action_day_adjusted_close']} |"
        for row in summary["samples"]
    )
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logging.info("Wrote adjusted-price audit to %s", summary_path)
    return 0


def materialize_entry_temp_tables(
    conn: psycopg.Connection[Any],
    params: EntryStudyParams,
) -> None:
    range_high_expr = "high_price" if params.range_high_basis == "high" else "close_price"
    range_low_expr = "low_price" if params.range_low_basis == "low" else "close_price"
    breakout_expr = "high_price" if params.breakout_basis == "high" else "close_price"
    base_start_date = params.candidate_start_date - timedelta(days=params.range_lookback_bars * 4)
    base_end_date = params.candidate_end_date + timedelta(days=params.trend_eval_bars * 4)

    with conn.cursor() as cur:
        cur.execute("drop table if exists temp_adjusted_price_daily")
        cur.execute(
            f"""
            create temporary table temp_adjusted_price_daily as
            select
                sc,
                name,
                market,
                industry,
                trade_date,
                raw_close_price,
                adjustment_factor,
                open_price,
                high_price,
                low_price,
                close_price,
                volume
            from {params.source_relation}
            where trade_date >= date '{base_start_date.isoformat()}'
              and trade_date <= date '{base_end_date.isoformat()}'
              and open_price is not null
              and high_price is not null
              and low_price is not null
              and close_price is not null
              and volume is not null
            """
        )
        cur.execute(
            "create index temp_adjusted_price_daily_sc_trade_date_idx on temp_adjusted_price_daily (sc, trade_date)"
        )
        cur.execute("analyze temp_adjusted_price_daily")
        cur.execute("drop table if exists temp_adjusted_metrics")
        cur.execute(
            f"""
            create temporary table temp_adjusted_metrics as
            with base as (
                select
                    sc,
                    name,
                    market,
                    industry,
                    trade_date,
                    raw_close_price,
                    adjustment_factor,
                    open_price,
                    high_price,
                    low_price,
                    close_price,
                    volume,
                    row_number() over (partition by sc order by trade_date) as trade_seq,
                    lag(close_price) over (partition by sc order by trade_date) as prev_close,
                    lag(high_price) over (partition by sc order by trade_date) as prev_high,
                    lag(low_price) over (partition by sc order by trade_date) as prev_low,
                    lag(close_price, 20) over (partition by sc order by trade_date) as close_lag_20,
                    lag(close_price, 60) over (partition by sc order by trade_date) as close_lag_60,
                    avg(volume) over volume_hist_window as avg_volume_20,
                    avg(close_price) over ma20_window as ma20,
                    avg(close_price) over ma60_window as ma60,
                    count(*) over range_window as lookback_obs,
                    max({range_high_expr}) over range_window as range_high,
                    min({range_low_expr}) over range_window as range_low,
                    lead(close_price, 20) over (partition by sc order by trade_date) as close_fwd_20,
                    lead(close_price, 60) over (partition by sc order by trade_date) as close_fwd_60,
                    lead(close_price, 120) over (partition by sc order by trade_date) as close_fwd_120,
                    lead(close_price, 240) over (partition by sc order by trade_date) as close_fwd_240,
                    max(close_price) over future_eval_window as future_max_close_240,
                    min(close_price) over future_failure_window as future_min_close_60,
                    {range_high_expr} as range_high_basis_price,
                    {breakout_expr} as breakout_basis_price
                from temp_adjusted_price_daily
                window
                    volume_hist_window as (
                        partition by sc
                        order by trade_date
                        rows between {params.volume_lookback_bars} preceding and 1 preceding
                    ),
                    ma20_window as (
                        partition by sc
                        order by trade_date
                        rows between 19 preceding and current row
                    ),
                    ma60_window as (
                        partition by sc
                        order by trade_date
                        rows between 59 preceding and current row
                    ),
                    range_window as (
                        partition by sc
                        order by trade_date
                        rows between {params.range_lookback_bars} preceding and 1 preceding
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
            enriched as (
                select
                    *,
                    lag(ma20, 20) over (partition by sc order by trade_date) as ma20_lag_20,
                    lag(ma60, 20) over (partition by sc order by trade_date) as ma60_lag_20,
                    avg(
                        greatest(
                            high_price - low_price,
                            abs(high_price - prev_close),
                            abs(low_price - prev_close)
                        )
                    ) over (
                        partition by sc
                        order by trade_date
                        rows between 19 preceding and current row
                    ) as atr_20,
                    case when close_price > open_price then 1 else 0 end as bullish_flag,
                    case when prev_close is not null and close_price > prev_close then 1 else 0 end as up_day_flag,
                    case when prev_high is not null and high_price > prev_high then 1 else 0 end as higher_high_flag,
                    case when prev_low is not null and low_price > prev_low then 1 else 0 end as higher_low_flag,
                    abs(close_price - open_price) / nullif(high_price - low_price, 0) as body_ratio,
                    (high_price - greatest(open_price, close_price)) / nullif(high_price - low_price, 0) as upper_wick_ratio,
                    (least(open_price, close_price) - low_price) / nullif(high_price - low_price, 0) as lower_wick_ratio
                from base
            )
            select
                *,
                case when close_price > open_price and coalesce(volume / nullif(avg_volume_20, 0), 0) >= 1.50 then 1 else 0 end as high_volume_bullish_flag,
                case when coalesce(upper_wick_ratio, 0) >= 0.50 then 1 else 0 end as long_upper_wick_flag,
                case when coalesce(lower_wick_ratio, 0) >= 0.50 then 1 else 0 end as long_lower_wick_flag,
                coalesce(volume / nullif(avg_volume_20, 0), 0) as volume_ratio,
                (close_price / nullif(prev_close, 0)) - 1 as day_return_pct,
                (open_price / nullif(prev_close, 0)) - 1 as gap_pct,
                (breakout_basis_price / nullif(range_high, 0)) - 1 as breakout_margin_pct,
                (range_high - range_low) / nullif(range_low, 0) as range_width_pct,
                (close_price / nullif(close_lag_20, 0)) - 1 as prior_return_20d_pct,
                (close_price / nullif(close_lag_60, 0)) - 1 as prior_return_60d_pct,
                (close_price / nullif(ma20, 0)) - 1 as ma_gap_20_pct,
                (close_price / nullif(ma60, 0)) - 1 as ma_gap_60_pct,
                (ma20 / nullif(ma20_lag_20, 0)) - 1 as ma_slope_20_pct,
                (ma60 / nullif(ma60_lag_20, 0)) - 1 as ma_slope_60_pct,
                (atr_20 / nullif(close_price, 0)) as atr_20_pct,
                (atr_20 / nullif(range_high - range_low, 0)) as atr_20_to_range_ratio,
                (close_fwd_20 / nullif(close_price, 0)) - 1 as return_20d_pct,
                (close_fwd_60 / nullif(close_price, 0)) - 1 as return_60d_pct,
                (close_fwd_120 / nullif(close_price, 0)) - 1 as return_120d_pct,
                (close_fwd_240 / nullif(close_price, 0)) - 1 as return_240d_pct,
                (future_max_close_240 / nullif(close_price, 0)) - 1 as future_max_return_240d_pct,
                (future_min_close_60 / nullif(close_price, 0)) - 1 as future_min_return_60d_pct
            from enriched
            """
        )
        cur.execute(
            "create index temp_adjusted_metrics_sc_trade_seq_idx on temp_adjusted_metrics (sc, trade_seq)"
        )
        cur.execute(
            "create index temp_adjusted_metrics_trade_date_idx on temp_adjusted_metrics (trade_date)"
        )
        cur.execute("analyze temp_adjusted_metrics")


def fetch_entry_candidate_rows(dsn: str, params: EntryStudyParams) -> list[dict[str, object]]:
    require_psycopg()
    query = f"""
with candidates as (
    select
        m.sc,
        m.name,
        m.market,
        m.industry,
        m.trade_date,
        m.trade_seq,
        m.lookback_obs,
        m.raw_close_price,
        m.adjustment_factor,
        m.close_price as adjusted_close_price,
        m.close_price as entry_price,
        m.range_high,
        m.range_low,
        m.range_width_pct,
        m.breakout_margin_pct,
        m.day_return_pct,
        m.gap_pct,
        m.body_ratio,
        m.upper_wick_ratio,
        m.lower_wick_ratio,
        m.volume_ratio,
        (m.close_price > m.open_price) as is_bullish,
        m.prior_return_20d_pct,
        m.prior_return_60d_pct,
        m.ma_gap_20_pct,
        m.ma_gap_60_pct,
        m.ma_slope_20_pct,
        m.ma_slope_60_pct,
        m.atr_20_pct,
        m.atr_20_to_range_ratio,
        m.return_20d_pct,
        m.return_60d_pct,
        m.return_120d_pct,
        m.return_240d_pct,
        m.future_max_return_240d_pct,
        m.future_min_return_60d_pct,
        case
            when m.trade_date >= date '{params.train_start_date.isoformat()}'
             and m.trade_date <= date '{params.train_end_date.isoformat()}'
                then 'train'
            when m.trade_date >= date '{params.validation_start_date.isoformat()}'
             and m.trade_date <= date '{params.validation_end_date.isoformat()}'
                then 'validation'
            else 'other'
        end as dataset_split
    from temp_adjusted_metrics m
    where m.trade_date >= date '{params.candidate_start_date.isoformat()}'
      and m.trade_date <= date '{params.candidate_end_date.isoformat()}'
      and m.lookback_obs >= {params.range_lookback_bars}
      and m.range_high is not null
      and m.range_low is not null
      and m.range_low > 0
      and m.range_width_pct <= {params.max_range_width_pct}
      and m.breakout_basis_price >= m.range_high * (1 + {params.breakout_buffer_pct})
      and coalesce(m.volume_ratio, 0) >= {params.min_volume_ratio}
)
select
    c.*,
    hist.bullish_count_10,
    hist.bullish_count_20,
    hist.bullish_count_60,
    hist.up_day_count_10,
    hist.up_day_count_20,
    hist.up_day_count_60,
    hist.high_volume_bullish_count_20,
    hist.high_volume_bullish_count_60,
    hist.long_upper_wick_count_20,
    hist.long_upper_wick_count_60,
    hist.long_lower_wick_count_20,
    hist.long_lower_wick_count_60,
    hist.range_high_touch_count_120,
    hist.higher_high_count_20,
    hist.higher_low_count_20
from candidates c
left join lateral (
    select
        sum(case when m2.trade_seq >= c.trade_seq - 10 then m2.bullish_flag else 0 end) as bullish_count_10,
        sum(case when m2.trade_seq >= c.trade_seq - 20 then m2.bullish_flag else 0 end) as bullish_count_20,
        sum(case when m2.trade_seq >= c.trade_seq - 60 then m2.bullish_flag else 0 end) as bullish_count_60,
        sum(case when m2.trade_seq >= c.trade_seq - 10 then m2.up_day_flag else 0 end) as up_day_count_10,
        sum(case when m2.trade_seq >= c.trade_seq - 20 then m2.up_day_flag else 0 end) as up_day_count_20,
        sum(case when m2.trade_seq >= c.trade_seq - 60 then m2.up_day_flag else 0 end) as up_day_count_60,
        sum(case when m2.trade_seq >= c.trade_seq - 20 then m2.high_volume_bullish_flag else 0 end) as high_volume_bullish_count_20,
        sum(case when m2.trade_seq >= c.trade_seq - 60 then m2.high_volume_bullish_flag else 0 end) as high_volume_bullish_count_60,
        sum(case when m2.trade_seq >= c.trade_seq - 20 then m2.long_upper_wick_flag else 0 end) as long_upper_wick_count_20,
        sum(case when m2.trade_seq >= c.trade_seq - 60 then m2.long_upper_wick_flag else 0 end) as long_upper_wick_count_60,
        sum(case when m2.trade_seq >= c.trade_seq - 20 then m2.long_lower_wick_flag else 0 end) as long_lower_wick_count_20,
        sum(case when m2.trade_seq >= c.trade_seq - 60 then m2.long_lower_wick_flag else 0 end) as long_lower_wick_count_60,
        sum(case when m2.range_high_basis_price >= c.range_high * 0.99 then 1 else 0 end) as range_high_touch_count_120,
        sum(case when m2.trade_seq >= c.trade_seq - 20 then m2.higher_high_flag else 0 end) as higher_high_count_20,
        sum(case when m2.trade_seq >= c.trade_seq - 20 then m2.higher_low_flag else 0 end) as higher_low_count_20
    from temp_adjusted_metrics m2
    where m2.sc = c.sc
      and m2.trade_seq between c.trade_seq - {params.range_lookback_bars} and c.trade_seq - 1
) hist on true
order by c.sc, c.trade_date
"""
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        conn.execute("set timezone to 'Asia/Tokyo'")
        materialize_entry_temp_tables(conn, params)
        with conn.cursor() as cur:
            cur.execute(query)
            return list(cur.fetchall())


def dedupe_cases(rows: Sequence[dict[str, object]], cooldown_bars: int) -> list[dict[str, object]]:
    deduped: list[dict[str, object]] = []
    last_trade_seq_by_sc: dict[str, int] = {}
    for row in rows:
        last_trade_seq = last_trade_seq_by_sc.get(str(row["sc"]))
        current_trade_seq = to_int(row["trade_seq"])
        if current_trade_seq is None:
            continue
        if last_trade_seq is not None and current_trade_seq - last_trade_seq <= cooldown_bars:
            continue
        deduped.append(dict(row))
        last_trade_seq_by_sc[str(row["sc"])] = current_trade_seq
    return deduped


def classify_entry_case(row: dict[str, object], params: EntryStudyParams) -> dict[str, object]:
    enriched = dict(row)
    future_max_return = to_float(row["future_max_return_240d_pct"])
    confirm_return = to_float(row["return_120d_pct"])
    future_min_return = to_float(row["future_min_return_60d_pct"])

    if future_max_return is None or confirm_return is None or future_min_return is None:
        enriched["label"] = "incomplete"
        enriched["label_reason"] = "insufficient_future_bars"
    elif (
        future_max_return >= params.trend_min_return_pct
        and confirm_return >= params.trend_min_confirm_return_pct
        and future_min_return > params.failure_drawdown_pct
    ):
        enriched["label"] = "trend"
        enriched["label_reason"] = "future_max_and_confirm_return_cleared_thresholds"
    elif future_min_return <= params.failure_drawdown_pct:
        enriched["label"] = "non_trend"
        enriched["label_reason"] = "early_drawdown_breached_failure_threshold"
    elif future_max_return < params.trend_min_return_pct / 2:
        enriched["label"] = "non_trend"
        enriched["label_reason"] = "future_max_return_never_reached_half_trend_threshold"
    elif confirm_return <= 0:
        enriched["label"] = "non_trend"
        enriched["label_reason"] = "confirm_horizon_return_non_positive"
    else:
        enriched["label"] = "neutral"
        enriched["label_reason"] = "advanced_but_not_enough_for_long_trend"

    enriched["case_id"] = f"{row['sc']}_{row['trade_date']}"
    return enriched


def summarize_cases(rows: Sequence[dict[str, object]]) -> dict[str, object]:
    by_split: dict[str, dict[str, int | float | None]] = {}
    for split in ("train", "validation", "other"):
        split_rows = [row for row in rows if row["dataset_split"] == split]
        label_counts = {
            label: sum(1 for row in split_rows if row["label"] == label)
            for label in ("trend", "non_trend", "neutral", "incomplete")
        }
        complete_count = label_counts["trend"] + label_counts["non_trend"] + label_counts["neutral"]
        by_split[split] = {
            "candidate_count": len(split_rows),
            **label_counts,
            "trend_rate_excluding_incomplete": (
                label_counts["trend"] / complete_count if complete_count else None
            ),
        }

    return {
        "candidate_count": len(rows),
        "unique_symbols": len({row["sc"] for row in rows}),
        "split_counts": by_split,
        "avg_breakout_margin_pct": average_or_none(to_float(row["breakout_margin_pct"]) for row in rows),
        "avg_volume_ratio": average_or_none(to_float(row["volume_ratio"]) for row in rows),
    }


def persist_entry_run(
    dsn: str,
    run_id: str,
    params: EntryStudyParams,
    manifest_path: Path,
    summary_path: Path,
) -> None:
    require_psycopg()
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into research.entry_study_runs (
                    run_id,
                    command_name,
                    source_relation,
                    train_start_date,
                    train_end_date,
                    validation_start_date,
                    validation_end_date,
                    parameters_json,
                    manifest_path,
                    summary_path
                )
                values (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s)
                on conflict (run_id) do update set
                    command_name = excluded.command_name,
                    source_relation = excluded.source_relation,
                    train_start_date = excluded.train_start_date,
                    train_end_date = excluded.train_end_date,
                    validation_start_date = excluded.validation_start_date,
                    validation_end_date = excluded.validation_end_date,
                    parameters_json = excluded.parameters_json,
                    manifest_path = excluded.manifest_path,
                    summary_path = excluded.summary_path
                """,
                (
                    run_id,
                    "build-entry-dataset",
                    params.source_relation,
                    params.train_start_date,
                    params.train_end_date,
                    params.validation_start_date,
                    params.validation_end_date,
                    json.dumps(asdict(params), ensure_ascii=False, default=json_default),
                    str(manifest_path),
                    str(summary_path),
                ),
            )
        conn.commit()


def persist_entry_cases(dsn: str, run_id: str, rows: Sequence[dict[str, object]]) -> None:
    require_psycopg()
    columns = [
        "case_id",
        "sc",
        "name",
        "market",
        "industry",
        "trade_date",
        "trade_seq",
        "dataset_split",
        "label",
        "label_reason",
        "lookback_obs",
        "entry_price",
        "raw_close_price",
        "adjusted_close_price",
        "adjustment_factor",
        "range_high",
        "range_low",
        "range_width_pct",
        "breakout_margin_pct",
        "day_return_pct",
        "gap_pct",
        "body_ratio",
        "upper_wick_ratio",
        "lower_wick_ratio",
        "volume_ratio",
        "is_bullish",
        "bullish_count_10",
        "bullish_count_20",
        "bullish_count_60",
        "up_day_count_10",
        "up_day_count_20",
        "up_day_count_60",
        "high_volume_bullish_count_20",
        "high_volume_bullish_count_60",
        "long_upper_wick_count_20",
        "long_upper_wick_count_60",
        "long_lower_wick_count_20",
        "long_lower_wick_count_60",
        "prior_return_20d_pct",
        "prior_return_60d_pct",
        "ma_gap_20_pct",
        "ma_gap_60_pct",
        "ma_slope_20_pct",
        "ma_slope_60_pct",
        "range_high_touch_count_120",
        "higher_high_count_20",
        "higher_low_count_20",
        "atr_20_pct",
        "atr_20_to_range_ratio",
        "return_20d_pct",
        "return_60d_pct",
        "return_120d_pct",
        "return_240d_pct",
        "future_max_return_240d_pct",
        "future_min_return_60d_pct",
    ]
    insert_sql = f"""
insert into research.entry_cases (
    run_id,
    {", ".join(columns)}
)
values (
    %s,
    {", ".join(["%s"] * len(columns))}
)
"""
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute("delete from research.entry_cases where run_id = %s", (run_id,))
            cur.executemany(
                insert_sql,
                [(run_id, *[row.get(column) for column in columns]) for row in rows],
            )
        conn.commit()


def build_dataset_manifest(
    run_id: str,
    params: EntryStudyParams,
    summary: dict[str, object],
    files: dict[str, str],
) -> dict[str, object]:
    return {
        "type": "entry_breakout_6m_study",
        "version": 1,
        "run_id": run_id,
        "generated_at": datetime.now().isoformat(),
        "source_relation": params.source_relation,
        "dataset_window": {
            "train_start_date": params.train_start_date,
            "train_end_date": params.train_end_date,
            "validation_start_date": params.validation_start_date,
            "validation_end_date": params.validation_end_date,
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
        "counts": summary,
        "files": files,
    }


def build_dataset_report(
    run_id: str,
    summary: dict[str, object],
    rows: Sequence[dict[str, object]],
) -> str:
    trend_rows = [row for row in rows if row["label"] == "trend"][:15]
    non_trend_rows = [row for row in rows if row["label"] == "non_trend"][:15]
    lines = [
        f"# {run_id}",
        "",
        "## Method",
        "",
        "- Use `analytics.stock_prices_adjusted_daily` so inferred split events are applied at read time.",
        "- Detect 6-month breakout candidates from the breakout-day close versus the prior 120-bar range.",
        "- Extract interpretable price and volume features at the breakout point.",
        "- Label each case from future adjusted prices only.",
        "",
        "## Counts",
        "",
        f"- Candidate count: {summary['candidate_count']}",
        f"- Unique symbols: {summary['unique_symbols']}",
        f"- Average breakout margin: {format_pct(summary['avg_breakout_margin_pct'])}",
        f"- Average volume ratio: {summary['avg_volume_ratio']}",
        "",
        "## Split Counts",
        "",
        "| split | candidates | trend | non_trend | neutral | incomplete | trend_rate_excluding_incomplete |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for split, counts in summary["split_counts"].items():
        lines.append(
            f"| {split} | {counts['candidate_count']} | {counts['trend']} | {counts['non_trend']} | "
            f"{counts['neutral']} | {counts['incomplete']} | {format_pct(counts['trend_rate_excluding_incomplete'])} |"
        )
    lines.extend(
        [
            "",
            "## Trend Samples",
            "",
            "| breakout_date | sc | name | breakout_margin | volume_ratio | return_120d | future_max_return_240d |",
            "| --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    lines.extend(
        f"| {row['trade_date']} | {row['sc']} | {row['name']} | {format_pct(row['breakout_margin_pct'])} | "
        f"{row['volume_ratio']:.2f} | {format_pct(row['return_120d_pct'])} | {format_pct(row['future_max_return_240d_pct'])} |"
        for row in trend_rows
    )
    lines.extend(
        [
            "",
            "## Non-Trend Samples",
            "",
            "| breakout_date | sc | name | breakout_margin | volume_ratio | return_120d | future_min_return_60d |",
            "| --- | --- | --- | --- | --- | --- | --- |",
        ]
    )
    lines.extend(
        f"| {row['trade_date']} | {row['sc']} | {row['name']} | {format_pct(row['breakout_margin_pct'])} | "
        f"{row['volume_ratio']:.2f} | {format_pct(row['return_120d_pct'])} | {format_pct(row['future_min_return_60d_pct'])} |"
        for row in non_trend_rows
    )
    return "\n".join(lines) + "\n"


def run_build_entry_dataset(args: argparse.Namespace, dsn: str) -> int:
    params = build_entry_params(args)
    run_id = f"entry_breakout_6m_{iso_timestamp()}"
    raw_rows = fetch_entry_candidate_rows(dsn, params)
    deduped_rows = dedupe_cases(raw_rows, params.cooldown_bars)
    labeled_rows = [classify_entry_case(row, params) for row in deduped_rows]
    summary = summarize_cases(labeled_rows)

    output_dir = params.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    files = {
        "cases_all_csv": str(output_dir / f"{run_id}_cases_all.csv"),
        "cases_train_csv": str(output_dir / f"{run_id}_cases_train.csv"),
        "cases_validation_csv": str(output_dir / f"{run_id}_cases_validation.csv"),
        "manifest_yaml": str(output_dir / f"{run_id}_manifest.yaml"),
        "summary_md": str(output_dir / f"{run_id}_summary.md"),
    }
    write_csv(Path(files["cases_all_csv"]), labeled_rows)
    write_csv(Path(files["cases_train_csv"]), [row for row in labeled_rows if row["dataset_split"] == "train"])
    write_csv(
        Path(files["cases_validation_csv"]),
        [row for row in labeled_rows if row["dataset_split"] == "validation"],
    )
    manifest = build_dataset_manifest(run_id, params, summary, files)
    write_yaml(Path(files["manifest_yaml"]), manifest)
    Path(files["summary_md"]).write_text(
        build_dataset_report(run_id, summary, labeled_rows),
        encoding="utf-8",
    )
    persist_entry_run(dsn, run_id, params, Path(files["manifest_yaml"]), Path(files["summary_md"]))
    persist_entry_cases(dsn, run_id, labeled_rows)
    logging.info("Wrote entry dataset manifest to %s", files["manifest_yaml"])
    logging.info("Dataset counts: %s", summary)
    return 0


def fetch_latest_run_id(dsn: str) -> str:
    require_psycopg()
    query = """
select run_id
from research.entry_study_runs
where command_name = 'build-entry-dataset'
order by created_at desc
limit 1
"""
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            row = cur.fetchone()
            if row is None:
                raise ValueError("No build-entry-dataset run exists yet.")
            return str(row["run_id"])


def fetch_cases_for_rule_mining(
    dsn: str,
    run_id: str,
    dataset_split: str,
) -> list[dict[str, object]]:
    require_psycopg()
    query = """
select *
from research.entry_cases
where run_id = %s
  and dataset_split = %s
  and label in ('trend', 'non_trend')
order by trade_date, sc
"""
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (run_id, dataset_split))
            return [dict(row) for row in cur.fetchall()]


def condition_matches(row: dict[str, object], feature: str, operator: str, threshold: float) -> bool:
    value = to_float(row.get(feature))
    if value is None:
        return False
    if operator == ">=":
        return value >= threshold
    if operator == "<=":
        return value <= threshold
    raise ValueError(f"Unsupported operator: {operator}")


def evaluate_conditions(
    rows: Sequence[dict[str, object]],
    conditions: Sequence[dict[str, object]],
) -> dict[str, object]:
    selected = [
        row
        for row in rows
        if all(
            condition_matches(row, str(cond["feature"]), str(cond["operator"]), float(cond["threshold"]))
            for cond in conditions
        )
    ]
    trend_total = sum(1 for row in rows if row["label"] == "trend")
    trend_selected = sum(1 for row in selected if row["label"] == "trend")
    non_trend_selected = sum(1 for row in selected if row["label"] == "non_trend")
    base_trend_rate = trend_total / len(rows) if rows else None
    precision = trend_selected / len(selected) if selected else None
    recall = trend_selected / trend_total if trend_total else None
    coverage = len(selected) / len(rows) if rows else None
    uplift = (
        precision / base_trend_rate
        if precision is not None and base_trend_rate not in (None, 0)
        else None
    )
    return {
        "selected_count": len(selected),
        "trend_selected": trend_selected,
        "non_trend_selected": non_trend_selected,
        "precision": precision,
        "recall": recall,
        "coverage": coverage,
        "base_trend_rate": base_trend_rate,
        "trend_rate_uplift": uplift,
    }


def quantile_thresholds(values: Sequence[float]) -> list[float]:
    if not values:
        return []
    sorted_values = sorted(values)
    thresholds = []
    for quantile in (0.15, 0.25, 0.35, 0.50, 0.65, 0.75, 0.85):
        index = min(len(sorted_values) - 1, max(0, round((len(sorted_values) - 1) * quantile)))
        thresholds.append(sorted_values[index])
    return sorted({round(value, 8) for value in thresholds})


def rule_name_from_conditions(conditions: Sequence[dict[str, object]]) -> str:
    return " and ".join(
        f"{cond['feature']} {cond['operator']} {float(cond['threshold']):.4f}"
        for cond in conditions
    )


def build_hypothesis_id(rule: dict[str, object]) -> str:
    raw = json.dumps(rule, sort_keys=True, ensure_ascii=False, default=json_default)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:12]


def worker_mine_feature_rules(payload: dict[str, object]) -> list[dict[str, object]]:
    feature = str(payload["feature"])
    rows = list(payload["rows"])
    min_cases = int(payload["min_cases"])
    top_rules_per_feature = int(payload["top_rules_per_feature"])
    values = sorted(
        {
            to_float(row.get(feature))
            for row in rows
            if row.get(feature) is not None and not math.isnan(float(row[feature]))
        }
    )
    numeric_values = [value for value in values if value is not None]
    thresholds = quantile_thresholds(numeric_values)
    candidates: list[dict[str, object]] = []
    for operator in (">=", "<="):
        for threshold in thresholds:
            conditions = [{"feature": feature, "operator": operator, "threshold": threshold}]
            metrics = evaluate_conditions(rows, conditions)
            if metrics["selected_count"] < min_cases:
                continue
            if metrics["precision"] is None or metrics["base_trend_rate"] is None:
                continue
            if metrics["precision"] <= metrics["base_trend_rate"]:
                continue
            candidate = {
                "hypothesis_id": "",
                "rule_name": rule_name_from_conditions(conditions),
                "conditions": conditions,
                "train_metrics": metrics,
            }
            candidate["hypothesis_id"] = build_hypothesis_id(candidate)
            candidates.append(candidate)

    candidates.sort(
        key=lambda item: (
            item["train_metrics"]["precision"],
            item["train_metrics"]["trend_rate_uplift"],
            item["train_metrics"]["coverage"],
        ),
        reverse=True,
    )
    return candidates[:top_rules_per_feature]


def dedupe_rules(rules: Sequence[dict[str, object]]) -> list[dict[str, object]]:
    seen: set[str] = set()
    deduped: list[dict[str, object]] = []
    for rule in rules:
        if rule["hypothesis_id"] in seen:
            continue
        seen.add(rule["hypothesis_id"])
        deduped.append(rule)
    return deduped


def mine_hypotheses(
    rows: Sequence[dict[str, object]],
    processes: int,
    min_cases: int,
    top_features: int,
    top_rules_per_feature: int,
) -> list[dict[str, object]]:
    payloads = [
        {
            "feature": feature,
            "rows": rows,
            "min_cases": min_cases,
            "top_rules_per_feature": top_rules_per_feature,
        }
        for feature in FEATURE_COLUMNS
    ]
    univariate_rules: list[dict[str, object]] = []
    with ProcessPoolExecutor(max_workers=processes) as executor:
        future_to_feature = {
            executor.submit(worker_mine_feature_rules, payload): payload["feature"]
            for payload in payloads
        }
        for future in as_completed(future_to_feature):
            univariate_rules.extend(future.result())

    univariate_rules = dedupe_rules(
        sorted(
            univariate_rules,
            key=lambda item: (
                item["train_metrics"]["precision"],
                item["train_metrics"]["trend_rate_uplift"],
                item["train_metrics"]["coverage"],
            ),
            reverse=True,
        )
    )

    top_feature_rules: list[dict[str, object]] = []
    feature_names: list[str] = []
    for rule in univariate_rules:
        feature_name = str(rule["conditions"][0]["feature"])
        if feature_name not in feature_names:
            feature_names.append(feature_name)
        if len(feature_names) > top_features:
            break
        top_feature_rules.append(rule)

    pair_rules: list[dict[str, object]] = []
    for index, left_rule in enumerate(top_feature_rules):
        for right_rule in top_feature_rules[index + 1 :]:
            left_feature = str(left_rule["conditions"][0]["feature"])
            right_feature = str(right_rule["conditions"][0]["feature"])
            if left_feature == right_feature:
                continue
            conditions = [*left_rule["conditions"], *right_rule["conditions"]]
            metrics = evaluate_conditions(rows, conditions)
            if metrics["selected_count"] < min_cases:
                continue
            if metrics["precision"] is None or metrics["base_trend_rate"] is None:
                continue
            if metrics["precision"] <= metrics["base_trend_rate"]:
                continue
            candidate = {
                "hypothesis_id": "",
                "rule_name": rule_name_from_conditions(conditions),
                "conditions": conditions,
                "train_metrics": metrics,
            }
            candidate["hypothesis_id"] = build_hypothesis_id(candidate)
            pair_rules.append(candidate)

    pair_rules = dedupe_rules(
        sorted(
            pair_rules,
            key=lambda item: (
                item["train_metrics"]["precision"],
                item["train_metrics"]["trend_rate_uplift"],
                item["train_metrics"]["coverage"],
            ),
            reverse=True,
        )
    )
    return dedupe_rules([*univariate_rules[:20], *pair_rules[:20]])


def persist_hypotheses(
    dsn: str,
    run_id: str,
    stage: str,
    hypotheses: Sequence[dict[str, object]],
    metrics_key: str,
) -> None:
    require_psycopg()
    insert_sql = """
insert into research.entry_hypotheses (
    run_id,
    hypothesis_id,
    stage,
    rule_name,
    rule_json,
    metrics_json
)
values (%s, %s, %s, %s, %s::jsonb, %s::jsonb)
"""
    with psycopg.connect(dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "delete from research.entry_hypotheses where run_id = %s and stage = %s",
                (run_id, stage),
            )
            cur.executemany(
                insert_sql,
                [
                    (
                        run_id,
                        hypothesis["hypothesis_id"],
                        stage,
                        hypothesis["rule_name"],
                        json.dumps(
                            {
                                "rule_name": hypothesis["rule_name"],
                                "conditions": hypothesis["conditions"],
                            },
                            ensure_ascii=False,
                        ),
                        json.dumps(hypothesis[metrics_key], ensure_ascii=False, default=json_default),
                    )
                    for hypothesis in hypotheses
                ],
            )
        conn.commit()


def build_hypothesis_payload(
    run_id: str,
    stage: str,
    hypotheses: Sequence[dict[str, object]],
) -> dict[str, object]:
    return {
        "type": "entry_breakout_hypotheses",
        "version": 1,
        "run_id": run_id,
        "stage": stage,
        "generated_at": datetime.now().isoformat(),
        "hypotheses": list(hypotheses),
    }


def run_mine_entry_hypotheses(args: argparse.Namespace, dsn: str) -> int:
    run_id = args.run_id or fetch_latest_run_id(dsn)
    train_rows = fetch_cases_for_rule_mining(dsn, run_id, "train")
    if not train_rows:
        raise ValueError(f"No train cases found for run_id={run_id}")

    hypotheses = mine_hypotheses(
        rows=train_rows,
        processes=args.processes,
        min_cases=args.min_cases,
        top_features=args.top_features,
        top_rules_per_feature=args.top_rules_per_feature,
    )
    persist_hypotheses(dsn, run_id, "train", hypotheses, "train_metrics")

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    hypotheses_path = output_dir / f"{run_id}_hypotheses.yaml"
    report_path = output_dir / f"{run_id}_hypotheses.md"
    write_yaml(hypotheses_path, build_hypothesis_payload(run_id, "train", hypotheses))

    lines = [
        f"# {run_id} Train Hypotheses",
        "",
        "| hypothesis_id | rule_name | precision | recall | coverage | uplift | selected_count |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for hypothesis in hypotheses:
        metrics = hypothesis["train_metrics"]
        uplift = metrics["trend_rate_uplift"]
        uplift_display = f"{uplift:.2f}" if uplift is not None else ""
        lines.append(
            f"| {hypothesis['hypothesis_id']} | {hypothesis['rule_name']} | "
            f"{format_pct(metrics['precision'])} | {format_pct(metrics['recall'])} | "
            f"{format_pct(metrics['coverage'])} | {uplift_display} | {metrics['selected_count']} |"
        )
    report_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logging.info("Wrote hypotheses to %s", hypotheses_path)
    return 0


def fetch_train_hypotheses(dsn: str, run_id: str) -> list[dict[str, object]]:
    require_psycopg()
    query = """
select
    hypothesis_id,
    rule_name,
    rule_json,
    metrics_json
from research.entry_hypotheses
where run_id = %s
  and stage = 'train'
order by created_at, hypothesis_id
"""
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            cur.execute(query, (run_id,))
            rows = []
            for row in cur.fetchall():
                rule_json = row["rule_json"]
                metrics_json = row["metrics_json"]
                if isinstance(rule_json, str):
                    rule_json = json.loads(rule_json)
                if isinstance(metrics_json, str):
                    metrics_json = json.loads(metrics_json)
                rows.append(
                    {
                        "hypothesis_id": row["hypothesis_id"],
                        "rule_name": row["rule_name"],
                        "conditions": rule_json["conditions"],
                        "train_metrics": metrics_json,
                    }
                )
            return rows


def run_evaluate_entry_hypotheses(args: argparse.Namespace, dsn: str) -> int:
    run_id = args.run_id or fetch_latest_run_id(dsn)
    validation_rows = fetch_cases_for_rule_mining(dsn, run_id, "validation")
    hypotheses = fetch_train_hypotheses(dsn, run_id)
    if not validation_rows:
        raise ValueError(f"No validation cases found for run_id={run_id}")
    if not hypotheses:
        raise ValueError(f"No train hypotheses found for run_id={run_id}")

    evaluated = []
    for hypothesis in hypotheses:
        validation_metrics = evaluate_conditions(validation_rows, hypothesis["conditions"])
        evaluated.append({**hypothesis, "validation_metrics": validation_metrics})
    persist_hypotheses(dsn, run_id, "validation", evaluated, "validation_metrics")

    output_dir = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    evaluation_yaml = output_dir / f"{run_id}_hypotheses_validation.yaml"
    evaluation_md = output_dir / f"{run_id}_evaluation.md"
    write_yaml(evaluation_yaml, build_hypothesis_payload(run_id, "validation", evaluated))

    lines = [
        f"# {run_id} Validation Evaluation",
        "",
        "| hypothesis_id | rule_name | train_precision | validation_precision | validation_coverage | validation_uplift | selected_count |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for hypothesis in evaluated:
        train_metrics = hypothesis["train_metrics"]
        validation_metrics = hypothesis["validation_metrics"]
        uplift = validation_metrics["trend_rate_uplift"]
        uplift_display = f"{uplift:.2f}" if uplift is not None else ""
        lines.append(
            f"| {hypothesis['hypothesis_id']} | {hypothesis['rule_name']} | "
            f"{format_pct(train_metrics['precision'])} | {format_pct(validation_metrics['precision'])} | "
            f"{format_pct(validation_metrics['coverage'])} | {uplift_display} | "
            f"{validation_metrics['selected_count']} |"
        )
    evaluation_md.write_text("\n".join(lines) + "\n", encoding="utf-8")
    logging.info("Wrote validation evaluation to %s", evaluation_yaml)
    return 0
