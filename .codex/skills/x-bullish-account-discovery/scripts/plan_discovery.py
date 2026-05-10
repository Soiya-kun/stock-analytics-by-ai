from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone


JST = timezone(timedelta(hours=9), "JST")
UTC = timezone.utc


@dataclass(frozen=True)
class CostModel:
    count_request_price: float = 0.005
    full_archive_count_request_price: float = 0.010
    post_read_price: float = 0.005
    user_read_price: float = 0.010


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Plan cost-aware X bullish-account discovery queries."
    )
    parser.add_argument("--target-date", required=True, type=date.fromisoformat)
    parser.add_argument("--stock-code", required=True)
    parser.add_argument("--company-name", required=True)
    parser.add_argument("--alias", action="append", default=[], dest="aliases")
    parser.add_argument("--lookback-days", type=int, default=35)
    parser.add_argument("--max-posts", type=int, default=200)
    parser.add_argument("--estimated-authors", type=int, default=50)
    parser.add_argument("--counts-mode", choices=["recent", "all"], default="all")
    return parser.parse_args()


def quote(term: str) -> str:
    escaped = term.replace('"', '\\"')
    return f'"{escaped}"'


def build_queries(stock_code: str, company_name: str, aliases: list[str]) -> list[str]:
    names = [company_name, *aliases]
    terms: list[str] = [
        f"{stock_code} lang:ja -is:retweet",
        f"{stock_code} 株 lang:ja -is:retweet",
        f"{stock_code} (買い OR 上がる OR 強い OR 初動 OR 本命) lang:ja -is:retweet",
    ]
    for name in names:
        q = quote(name)
        terms.extend(
            [
                f"{q} lang:ja -is:retweet",
                f"{q} 株 lang:ja -is:retweet",
                f"{q} (買い OR 上がる OR 強い OR 初動 OR 本命) lang:ja -is:retweet",
                f"{q} (決算期待 OR 上方修正 OR 材料 OR 増配 OR ブレイク) lang:ja -is:retweet",
            ]
        )
    deduped: list[str] = []
    seen: set[str] = set()
    for term in terms:
        if term not in seen:
            deduped.append(term)
            seen.add(term)
    return deduped


def format_dt(value: datetime) -> str:
    return value.astimezone(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def main() -> int:
    args = parse_args()
    start_date = args.target_date - timedelta(days=args.lookback_days)
    start_jst = datetime.combine(start_date, time.min, tzinfo=JST)
    end_jst = datetime.combine(args.target_date, time.max, tzinfo=JST)
    queries = build_queries(args.stock_code, args.company_name, args.aliases)

    cost = CostModel()
    count_price = (
        cost.count_request_price
        if args.counts_mode == "recent"
        else cost.full_archive_count_request_price
    )
    estimated_cost = (
        len(queries) * count_price
        + args.max_posts * cost.post_read_price
        + min(args.max_posts, args.estimated_authors) * cost.user_read_price
    )

    print("X bullish account discovery plan")
    print(f"target_date_jst: {args.target_date.isoformat()}")
    print(f"start_date_jst:  {start_date.isoformat()}")
    print(f"start_time_utc:  {format_dt(start_jst)}")
    print(f"end_time_utc:    {format_dt(end_jst)}")
    print(f"query_count:     {len(queries)}")
    print(f"counts_mode:     {args.counts_mode}")
    print(f"max_posts:       {args.max_posts}")
    print(f"est_authors:     {min(args.max_posts, args.estimated_authors)}")
    print(f"est_cost_usd:    {estimated_cost:.3f}")
    print()
    print("queries:")
    for index, query in enumerate(queries, start=1):
        print(f"{index:02d}. {query}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
