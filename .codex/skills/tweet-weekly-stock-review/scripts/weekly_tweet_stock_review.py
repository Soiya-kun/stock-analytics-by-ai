from __future__ import annotations

import argparse
import json
import os
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any
from zoneinfo import ZoneInfo

import psycopg
from psycopg.rows import dict_row


JST = ZoneInfo("Asia/Tokyo")


@dataclass(frozen=True)
class DateRange:
    start: date
    end: date


def parse_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"Invalid date: {value}") from exc


def previous_week(as_of: date) -> DateRange:
    current_monday = as_of - timedelta(days=as_of.weekday())
    start = current_monday - timedelta(days=7)
    end = current_monday - timedelta(days=1)
    return DateRange(start=start, end=end)


def trailing_week(as_of: date) -> DateRange:
    return DateRange(start=as_of - timedelta(days=6), end=as_of)


def decimal_to_float(value: Any) -> Any:
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, list):
        return [decimal_to_float(item) for item in value]
    if isinstance(value, dict):
        return {key: decimal_to_float(item) for key, item in value.items()}
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def pct(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{float(value) * 100:.1f}%"


def ratio(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.2f}x"


def rate(numerator: int | None, denominator: int | None) -> float | None:
    if not denominator:
        return None
    return numerator / denominator * 100


def fmt_rate(numerator: int | None, denominator: int | None) -> str:
    value = rate(numerator, denominator)
    if value is None:
        return "n/a"
    return f"{value:.1f}% ({numerator}/{denominator})"


def connect() -> psycopg.Connection:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        raise SystemExit("DATABASE_URL is not set. Run via docker compose analysis service or set it explicitly.")
    return psycopg.connect(dsn, row_factory=dict_row)


def fetch_one(cur: psycopg.Cursor, sql: str, params: dict[str, Any]) -> dict[str, Any]:
    cur.execute(sql, params)
    row = cur.fetchone()
    return dict(row) if row else {}


def fetch_all(cur: psycopg.Cursor, sql: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    cur.execute(sql, params)
    return [dict(row) for row in cur.fetchall()]


SCOPED_CTE = """
with scoped as (
    select distinct on (post_id, sc)
        *
    from research.tweet_stock_mentions
    where post_date_jst between %(start_date)s and %(end_date)s
    order by post_id, sc, created_at desc
)
"""


def build_review(conn: psycopg.Connection, dr: DateRange) -> dict[str, Any]:
    params = {"start_date": dr.start, "end_date": dr.end}
    with conn.cursor() as cur:
        source_posts = fetch_one(
            cur,
            """
            select
                count(*) as source_post_count,
                count(distinct target_username) as source_user_count
            from analytics.monitored_x_posts
            where post_date_jst between %(start_date)s and %(end_date)s
            """,
            params,
        )
        coverage = fetch_one(
            cur,
            SCOPED_CTE
            + """
            select
                count(*) as mention_rows,
                count(distinct post_id) as posts_with_mentions,
                count(distinct sc) as unique_stocks,
                count(distinct target_username) as monitored_users,
                count(distinct run_id) as contributing_runs,
                min(post_date_jst) as first_mention_date,
                max(post_date_jst) as last_mention_date
            from scoped
            """,
            params,
        )
        outcomes = fetch_one(
            cur,
            SCOPED_CTE
            + """
            select
                count(*) filter (where event_day_return_pct is not null) as event_return_count,
                avg(event_day_return_pct) as avg_event_day_return_pct,
                percentile_cont(0.5) within group (order by event_day_return_pct)
                    filter (where event_day_return_pct is not null) as median_event_day_return_pct,
                count(*) filter (where max_close_return_5d_pct is not null) as return_5d_count,
                count(*) filter (where max_close_return_5d_pct > 0) as positive_5d_count,
                count(*) filter (where max_close_return_5d_pct >= 0.05) as strong_5d_count,
                avg(max_close_return_5d_pct) as avg_max_close_return_5d_pct,
                percentile_cont(0.5) within group (order by max_close_return_5d_pct)
                    filter (where max_close_return_5d_pct is not null) as median_max_close_return_5d_pct,
                count(*) filter (where max_close_return_20d_pct is not null) as return_20d_count,
                count(*) filter (where max_close_return_20d_pct > 0) as positive_20d_count,
                count(*) filter (where max_close_return_20d_pct >= 0.10) as strong_20d_count,
                avg(max_close_return_20d_pct) as avg_max_close_return_20d_pct,
                percentile_cont(0.5) within group (order by max_close_return_20d_pct)
                    filter (where max_close_return_20d_pct is not null) as median_max_close_return_20d_pct,
                avg(volume_ratio_20d) as avg_volume_ratio_20d,
                count(*) filter (where price_jump_flag) as price_jump_flag_count,
                count(*) filter (where volume_spike_flag) as volume_spike_flag_count
            from scoped
            """,
            params,
        )
        top_stocks = fetch_all(
            cur,
            SCOPED_CTE
            + """
            select
                sc,
                max(company_name) as company_name,
                count(*) as mention_rows,
                count(distinct post_id) as post_count,
                count(distinct target_username) as distinct_users,
                array_agg(distinct target_username order by target_username) as mentioned_by,
                avg(event_day_return_pct) as avg_event_day_return_pct,
                avg(max_close_return_5d_pct) as avg_max_close_return_5d_pct,
                max(max_close_return_5d_pct) as best_max_close_return_5d_pct,
                avg(max_close_return_20d_pct) as avg_max_close_return_20d_pct,
                max(max_close_return_20d_pct) as best_max_close_return_20d_pct,
                avg(volume_ratio_20d) as avg_volume_ratio_20d,
                bool_or(price_jump_flag) as any_price_jump_flag,
                bool_or(volume_spike_flag) as any_volume_spike_flag,
                (array_agg(analysis_summary order by post_created_at))[1] as sample_summary
            from scoped
            group by sc
            order by distinct_users desc, mention_rows desc,
                     best_max_close_return_5d_pct desc nulls last,
                     best_max_close_return_20d_pct desc nulls last,
                     sc
            limit 15
            """,
            params,
        )
        cross_user = [row for row in top_stocks if row["distinct_users"] >= 2]
        best_mentions = fetch_all(
            cur,
            SCOPED_CTE
            + """
            select
                sc,
                company_name,
                target_username,
                post_date_jst,
                tweet_url,
                event_day_return_pct,
                max_close_return_5d_pct,
                max_close_return_20d_pct,
                volume_ratio_20d,
                analysis_summary
            from scoped
            order by max_close_return_5d_pct desc nulls last,
                     max_close_return_20d_pct desc nulls last
            limit 10
            """,
            params,
        )
        worst_mentions = fetch_all(
            cur,
            SCOPED_CTE
            + """
            select
                sc,
                company_name,
                target_username,
                post_date_jst,
                tweet_url,
                event_day_return_pct,
                max_close_return_5d_pct,
                max_close_return_20d_pct,
                volume_ratio_20d,
                analysis_summary
            from scoped
            where max_close_return_5d_pct is not null
            order by max_close_return_5d_pct asc,
                     max_close_return_20d_pct asc nulls last
            limit 10
            """,
            params,
        )
        by_user = fetch_all(
            cur,
            SCOPED_CTE
            + """
            select
                target_username,
                count(*) as mention_rows,
                count(distinct post_id) as post_count,
                count(distinct sc) as unique_stocks,
                count(*) filter (where max_close_return_5d_pct is not null) as return_5d_count,
                count(*) filter (where max_close_return_5d_pct > 0) as positive_5d_count,
                avg(max_close_return_5d_pct) as avg_max_close_return_5d_pct,
                count(*) filter (where max_close_return_20d_pct is not null) as return_20d_count,
                count(*) filter (where max_close_return_20d_pct > 0) as positive_20d_count,
                avg(max_close_return_20d_pct) as avg_max_close_return_20d_pct
            from scoped
            group by target_username
            order by mention_rows desc, avg_max_close_return_5d_pct desc nulls last, target_username
            limit 20
            """,
            params,
        )
        flag_cohorts = fetch_all(
            cur,
            SCOPED_CTE
            + """
            select
                price_jump_flag,
                volume_spike_flag,
                count(*) as mention_rows,
                count(*) filter (where max_close_return_5d_pct is not null) as return_5d_count,
                count(*) filter (where max_close_return_5d_pct > 0) as positive_5d_count,
                avg(max_close_return_5d_pct) as avg_max_close_return_5d_pct,
                avg(max_close_return_20d_pct) as avg_max_close_return_20d_pct,
                avg(volume_ratio_20d) as avg_volume_ratio_20d
            from scoped
            group by price_jump_flag, volume_spike_flag
            order by price_jump_flag desc, volume_spike_flag desc
            """,
            params,
        )

    return decimal_to_float(
        {
            "range": {"start_date": dr.start, "end_date": dr.end},
            "source_posts": source_posts,
            "coverage": coverage,
            "outcomes": outcomes,
            "cross_user": cross_user,
            "top_stocks": top_stocks,
            "best_mentions": best_mentions,
            "worst_mentions": worst_mentions,
            "by_user": by_user,
            "flag_cohorts": flag_cohorts,
        }
    )


def markdown_table(headers: list[str], rows: Iterable[list[Any]]) -> str:
    def clean(cell: Any) -> str:
        return str(cell).replace("\n", " ").replace("|", "\\|")

    lines = ["| " + " | ".join(headers) + " |", "| " + " | ".join(["---"] * len(headers)) + " |"]
    for row in rows:
        lines.append("| " + " | ".join(clean(cell) for cell in row) + " |")
    return "\n".join(lines)


def render_markdown(review: dict[str, Any]) -> str:
    dr = review["range"]
    source = review["source_posts"]
    coverage = review["coverage"]
    outcomes = review["outcomes"]
    mention_rows = int(coverage.get("mention_rows") or 0)

    lines = [
        f"# Tweet Weekly Stock Review: {dr['start_date']} to {dr['end_date']}",
        "",
        "## Coverage",
        "",
        f"- Source monitored posts: {source.get('source_post_count', 0)}",
        f"- Posts with stock mentions: {coverage.get('posts_with_mentions', 0)}",
        f"- Mention rows: {mention_rows}",
        f"- Unique stocks: {coverage.get('unique_stocks', 0)}",
        f"- Monitored users with mentions: {coverage.get('monitored_users', 0)}",
        f"- Contributing persisted runs: {coverage.get('contributing_runs', 0)}",
    ]

    if mention_rows == 0:
        lines.extend(
            [
                "",
                "No persisted tweet-stock mention rows were found for this range.",
                "Run tweet-stock-analysis for the week first, then rerun this review.",
            ]
        )
        return "\n".join(lines)

    lines.extend(
        [
            "",
            "## Outcome Stats",
            "",
            f"- Event-day average / median: {pct(outcomes.get('avg_event_day_return_pct'))} / {pct(outcomes.get('median_event_day_return_pct'))}",
            f"- 5d positive hit rate: {fmt_rate(outcomes.get('positive_5d_count'), outcomes.get('return_5d_count'))}",
            f"- 5d >= 5% rate: {fmt_rate(outcomes.get('strong_5d_count'), outcomes.get('return_5d_count'))}",
            f"- 5d average / median max close return: {pct(outcomes.get('avg_max_close_return_5d_pct'))} / {pct(outcomes.get('median_max_close_return_5d_pct'))}",
            f"- 20d positive hit rate: {fmt_rate(outcomes.get('positive_20d_count'), outcomes.get('return_20d_count'))}",
            f"- 20d >= 10% rate: {fmt_rate(outcomes.get('strong_20d_count'), outcomes.get('return_20d_count'))}",
            f"- 20d average / median max close return: {pct(outcomes.get('avg_max_close_return_20d_pct'))} / {pct(outcomes.get('median_max_close_return_20d_pct'))}",
            f"- Average volume ratio: {ratio(outcomes.get('avg_volume_ratio_20d'))}",
            f"- Price-jump flags: {outcomes.get('price_jump_flag_count', 0)}",
            f"- Volume-spike flags: {outcomes.get('volume_spike_flag_count', 0)}",
        ]
    )

    cross_user = review["cross_user"]
    if cross_user:
        lines.extend(["", "## Cross-User Confirmation", ""])
        lines.append(
            markdown_table(
                ["sc", "company", "users", "mentions", "best 5d", "best 20d", "summary"],
                [
                    [
                        row["sc"],
                        row["company_name"],
                        ", ".join(row["mentioned_by"]),
                        row["mention_rows"],
                        pct(row.get("best_max_close_return_5d_pct")),
                        pct(row.get("best_max_close_return_20d_pct")),
                        row.get("sample_summary") or "",
                    ]
                    for row in cross_user[:10]
                ],
            )
        )

    lines.extend(["", "## Top Stocks", ""])
    lines.append(
        markdown_table(
            ["sc", "company", "users", "mentions", "avg 5d", "best 5d", "avg vol", "flags"],
            [
                [
                    row["sc"],
                    row["company_name"],
                    row["distinct_users"],
                    row["mention_rows"],
                    pct(row.get("avg_max_close_return_5d_pct")),
                    pct(row.get("best_max_close_return_5d_pct")),
                    ratio(row.get("avg_volume_ratio_20d")),
                    f"price={row.get('any_price_jump_flag')}, volume={row.get('any_volume_spike_flag')}",
                ]
                for row in review["top_stocks"][:10]
            ],
        )
    )

    lines.extend(["", "## Best Mentions", ""])
    lines.append(
        markdown_table(
            ["date", "user", "sc", "company", "5d", "20d", "volume", "url"],
            [
                [
                    row["post_date_jst"],
                    row["target_username"],
                    row["sc"],
                    row["company_name"],
                    pct(row.get("max_close_return_5d_pct")),
                    pct(row.get("max_close_return_20d_pct")),
                    ratio(row.get("volume_ratio_20d")),
                    row["tweet_url"],
                ]
                for row in review["best_mentions"][:5]
            ],
        )
    )

    lines.extend(["", "## Weakest Mentions", ""])
    lines.append(
        markdown_table(
            ["date", "user", "sc", "company", "5d", "20d", "volume", "url"],
            [
                [
                    row["post_date_jst"],
                    row["target_username"],
                    row["sc"],
                    row["company_name"],
                    pct(row.get("max_close_return_5d_pct")),
                    pct(row.get("max_close_return_20d_pct")),
                    ratio(row.get("volume_ratio_20d")),
                    row["tweet_url"],
                ]
                for row in review["worst_mentions"][:5]
            ],
        )
    )

    lines.extend(["", "## By User", ""])
    lines.append(
        markdown_table(
            ["user", "mentions", "stocks", "5d hit", "avg 5d", "20d hit", "avg 20d"],
            [
                [
                    row["target_username"],
                    row["mention_rows"],
                    row["unique_stocks"],
                    fmt_rate(row.get("positive_5d_count"), row.get("return_5d_count")),
                    pct(row.get("avg_max_close_return_5d_pct")),
                    fmt_rate(row.get("positive_20d_count"), row.get("return_20d_count")),
                    pct(row.get("avg_max_close_return_20d_pct")),
                ]
                for row in review["by_user"][:10]
            ],
        )
    )

    return "\n".join(lines)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Review persisted tweet-stock analysis rows for a weekly date range.")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--last-week", action="store_true", help="Use the previous JST Monday-Sunday calendar week.")
    group.add_argument(
        "--trailing-week",
        action="store_true",
        help="Use the exact trailing seven JST calendar dates including the execution date. This is the default.",
    )
    group.add_argument("--start-date", type=parse_date, help="Start date in YYYY-MM-DD.")
    parser.add_argument("--end-date", type=parse_date, help="End date in YYYY-MM-DD. Required with --start-date.")
    parser.add_argument("--as-of", type=parse_date, help="Reference date for --last-week or --trailing-week. Defaults to today in JST.")
    parser.add_argument("--format", choices=("markdown", "json"), default="markdown")
    return parser.parse_args()


def resolve_range(args: argparse.Namespace) -> DateRange:
    as_of = args.as_of or datetime.now(JST).date()
    if args.last_week:
        return previous_week(as_of)
    if args.trailing_week or args.start_date is None:
        return trailing_week(as_of)
    if not args.end_date:
        raise SystemExit("--end-date is required with --start-date.")
    if args.end_date < args.start_date:
        raise SystemExit("--end-date must be on or after --start-date.")
    return DateRange(start=args.start_date, end=args.end_date)


def main() -> int:
    args = parse_args()
    dr = resolve_range(args)
    with connect() as conn:
        review = build_review(conn, dr)
    if args.format == "json":
        print(json.dumps(review, ensure_ascii=False, indent=2))
    else:
        print(render_markdown(review))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
