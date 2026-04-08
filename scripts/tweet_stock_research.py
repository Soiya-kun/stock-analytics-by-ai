from __future__ import annotations

import argparse
import csv
import json
import logging
from dataclasses import asdict, dataclass
from datetime import date, datetime, time as dt_time, timedelta
from pathlib import Path
from typing import Any, Sequence

try:
    import psycopg
    from psycopg.rows import dict_row
    from psycopg.types.json import Jsonb
except ModuleNotFoundError:  # pragma: no cover
    psycopg = None
    dict_row = None
    Jsonb = None

try:
    import yaml
except ModuleNotFoundError:  # pragma: no cover
    yaml = None


TWEET_ANALYSIS_OUTPUT_DIR = Path("research") / "tweet-stock-analysis"
SOURCE_RELATION = "analytics.monitored_x_posts"
COMPANY_RELATION = "analytics.listed_companies_latest"
DEFAULT_VOLUME_LOOKBACK_DAYS = 20
DEFAULT_FORWARD_RETURN_DAYS = (5, 20)
MARKET_CLOSE_CUTOFF_JST = dt_time(15, 30)


@dataclass(frozen=True)
class TweetAnalysisParams:
    output_dir: Path
    start_date: date
    end_date: date
    target_username: str | None
    limit: int | None
    volume_lookback_days: int
    forward_return_days: tuple[int, ...]


def register_subcommands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    prepare_parser = subparsers.add_parser(
        "prepare-tweet-analysis",
        help="Export monitored tweets and a listed-company snapshot for manual LLM annotation.",
    )
    prepare_parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    prepare_parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    prepare_parser.add_argument("--target-username", default=None)
    prepare_parser.add_argument("--limit", type=int, default=None)
    prepare_parser.add_argument("--output-dir", type=Path, default=TWEET_ANALYSIS_OUTPUT_DIR)
    prepare_parser.add_argument(
        "--volume-lookback-days",
        type=int,
        default=DEFAULT_VOLUME_LOOKBACK_DAYS,
    )
    prepare_parser.add_argument(
        "--forward-return-days",
        default="5,20",
        help="Comma-separated forward day horizons saved into the template.",
    )

    enrich_parser = subparsers.add_parser(
        "enrich-tweet-analysis",
        help="Add stock price and volume context to an annotated tweet-analysis YAML/JSON file.",
    )
    enrich_parser.add_argument("--input-file", type=Path, required=True)
    enrich_parser.add_argument("--output-file", type=Path, default=None)

    persist_parser = subparsers.add_parser(
        "persist-tweet-analysis",
        help="Persist a fully annotated tweet-analysis YAML/JSON file into research tables.",
    )
    persist_parser.add_argument("--input-file", type=Path, required=True)


def handle_command(args: argparse.Namespace, dsn: str) -> int | None:
    if args.command == "prepare-tweet-analysis":
        return run_prepare_tweet_analysis(args, dsn)
    if args.command == "enrich-tweet-analysis":
        return run_enrich_tweet_analysis(args, dsn)
    if args.command == "persist-tweet-analysis":
        return run_persist_tweet_analysis(args, dsn)
    return None


def require_psycopg() -> None:
    if psycopg is None or dict_row is None or Jsonb is None:
        raise ModuleNotFoundError("psycopg is required. Run this through docker compose.")


def require_yaml() -> None:
    if yaml is None:
        raise ModuleNotFoundError("PyYAML is required. Run this through docker compose.")


def iso_timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def json_default(value: object) -> object:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    return str(value)


def normalize_for_csv(value: object) -> object:
    if isinstance(value, float):
        return f"{value:.10f}"
    return value


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


def write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, default=json_default),
        encoding="utf-8",
    )


def write_yaml(path: Path, payload: dict[str, object]) -> None:
    require_yaml()
    path.parent.mkdir(parents=True, exist_ok=True)
    serializable = json.loads(json.dumps(payload, ensure_ascii=False, default=json_default))
    path.write_text(
        yaml.safe_dump(serializable, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )


def load_analysis_file(path: Path) -> dict[str, Any]:
    suffix = path.suffix.lower()
    if suffix == ".json":
        return json.loads(path.read_text(encoding="utf-8"))
    if suffix in {".yaml", ".yml"}:
        require_yaml()
        payload = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError(f"Expected mapping at top-level in {path}")
        return payload
    raise ValueError(f"Unsupported analysis file extension: {path.suffix}")


def parse_forward_return_days(raw: str) -> tuple[int, ...]:
    values = tuple(sorted({int(part.strip()) for part in raw.split(",") if part.strip()}))
    if not values:
        raise ValueError("--forward-return-days must contain at least one positive integer.")
    if any(value <= 0 for value in values):
        raise ValueError("--forward-return-days must contain positive integers.")
    return values


def build_params(args: argparse.Namespace) -> TweetAnalysisParams:
    return TweetAnalysisParams(
        output_dir=args.output_dir,
        start_date=args.start_date,
        end_date=args.end_date,
        target_username=args.target_username,
        limit=args.limit,
        volume_lookback_days=args.volume_lookback_days,
        forward_return_days=parse_forward_return_days(args.forward_return_days),
    )


def slugify_target(target_username: str | None) -> str:
    if not target_username:
        return "all-targets"
    return target_username.strip().lstrip("@").lower().replace("/", "-")


def build_run_id(params: TweetAnalysisParams) -> str:
    return (
        f"tweet_stock_{params.start_date:%Y%m%d}_{params.end_date:%Y%m%d}_"
        f"{slugify_target(params.target_username)}_{iso_timestamp()}"
    )


def fetch_tweets(
    dsn: str,
    params: TweetAnalysisParams,
) -> list[dict[str, Any]]:
    require_psycopg()
    query = """
select
    target_username,
    author_user_id,
    author_username,
    author_name,
    post_id,
    tweet_url,
    created_at,
    created_at_jst,
    post_date_jst,
    text,
    like_count,
    reply_count,
    quote_count,
    repost_count,
    bookmark_count,
    impression_count
from analytics.monitored_x_posts
where post_date_jst between %s and %s
  and (%s::text is null or target_username = %s::text)
order by created_at, post_id
"""
    if params.limit is not None:
        query += "\nlimit %s"

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        conn.execute("set timezone to 'Asia/Tokyo'")
        with conn.cursor() as cur:
            query_args: list[object] = [
                params.start_date,
                params.end_date,
                params.target_username,
                params.target_username,
            ]
            if params.limit is not None:
                query_args.append(params.limit)
            cur.execute(query, query_args)
            return list(cur.fetchall())


def fetch_company_snapshot(dsn: str) -> list[dict[str, Any]]:
    require_psycopg()
    query = """
select
    sc,
    name,
    market,
    industry,
    trade_date,
    close_price,
    volume,
    market_cap_million_yen
from analytics.listed_companies_latest
order by sc
"""
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        conn.execute("set timezone to 'Asia/Tokyo'")
        with conn.cursor() as cur:
            cur.execute(query)
            return list(cur.fetchall())


def build_template_tweet(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "post_id": row["post_id"],
        "target_username": row["target_username"],
        "author_user_id": row["author_user_id"],
        "author_username": row["author_username"],
        "author_name": row.get("author_name"),
        "created_at": row["created_at"],
        "created_at_jst": row["created_at_jst"],
        "post_date_jst": row["post_date_jst"],
        "tweet_url": row["tweet_url"],
        "text": row["text"],
        "metrics": {
            "like_count": row.get("like_count"),
            "reply_count": row.get("reply_count"),
            "quote_count": row.get("quote_count"),
            "repost_count": row.get("repost_count"),
            "bookmark_count": row.get("bookmark_count"),
            "impression_count": row.get("impression_count"),
        },
        "mentions": [],
    }


def build_prepare_manifest(
    run_id: str,
    params: TweetAnalysisParams,
    tweets: Sequence[dict[str, Any]],
    companies: Sequence[dict[str, Any]],
    files: dict[str, str],
) -> dict[str, Any]:
    return {
        "type": "tweet_stock_analysis_prepare",
        "version": 1,
        "run_id": run_id,
        "generated_at": datetime.now().isoformat(),
        "source_relation": SOURCE_RELATION,
        "company_relation": COMPANY_RELATION,
        "date_range": {
            "start_date": params.start_date,
            "end_date": params.end_date,
        },
        "target_username": params.target_username,
        "parameters": {
            "volume_lookback_days": params.volume_lookback_days,
            "forward_return_days": list(params.forward_return_days),
            "limit": params.limit,
        },
        "counts": {
            "tweet_count": len(tweets),
            "company_snapshot_count": len(companies),
        },
        "files": files,
    }


def run_prepare_tweet_analysis(args: argparse.Namespace, dsn: str) -> int:
    params = build_params(args)
    run_id = build_run_id(params)
    run_dir = params.output_dir / run_id
    tweets = fetch_tweets(dsn, params)
    companies = fetch_company_snapshot(dsn)

    template_payload = {
        "type": "tweet_stock_analysis",
        "version": 1,
        "run": {
            "run_id": run_id,
            "source_relation": SOURCE_RELATION,
            "company_relation": COMPANY_RELATION,
            "start_date": params.start_date,
            "end_date": params.end_date,
            "target_username": params.target_username,
            "generated_at": datetime.now().isoformat(),
            "parameters": {
                "volume_lookback_days": params.volume_lookback_days,
                "forward_return_days": list(params.forward_return_days),
                "limit": params.limit,
            },
        },
        "tweets": [build_template_tweet(row) for row in tweets],
        "notes": "",
    }

    tweets_csv_rows = [
        {
            "post_id": row["post_id"],
            "target_username": row["target_username"],
            "author_username": row["author_username"],
            "created_at_jst": row["created_at_jst"],
            "post_date_jst": row["post_date_jst"],
            "tweet_url": row["tweet_url"],
            "text": row["text"],
        }
        for row in tweets
    ]
    company_csv_rows = [
        {
            "sc": row["sc"],
            "name": row["name"],
            "market": row["market"],
            "industry": row["industry"],
            "trade_date": row["trade_date"],
            "close_price": row["close_price"],
            "volume": row["volume"],
            "market_cap_million_yen": row["market_cap_million_yen"],
        }
        for row in companies
    ]

    files = {
        "tweets_csv": str(run_dir / "tweets.csv"),
        "company_snapshot_csv": str(run_dir / "company_snapshot.csv"),
        "analysis_template_yaml": str(run_dir / "analysis_template.yaml"),
        "analysis_template_json": str(run_dir / "analysis_template.json"),
        "manifest_yaml": str(run_dir / "manifest.yaml"),
    }
    manifest = build_prepare_manifest(run_id, params, tweets, companies, files)

    write_csv(run_dir / "tweets.csv", tweets_csv_rows)
    write_csv(run_dir / "company_snapshot.csv", company_csv_rows)
    write_yaml(run_dir / "analysis_template.yaml", template_payload)
    write_json(run_dir / "analysis_template.json", template_payload)
    write_yaml(run_dir / "manifest.yaml", manifest)

    logging.info(
        "Prepared tweet analysis run %s with %s tweets under %s",
        run_id,
        len(tweets),
        run_dir,
    )
    return 0


def parse_analysis_datetime(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value
    if not isinstance(value, str):
        raise ValueError(f"Expected datetime string, got: {value!r}")
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed
    return parsed.astimezone().replace(tzinfo=None)


def normalize_match_confidence(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("match_confidence must be one of high / medium / low.")
    normalized = value.strip().lower()
    if normalized not in {"high", "medium", "low"}:
        raise ValueError("match_confidence must be one of high / medium / low.")
    return normalized


def build_mention_id(post_id: str, sc: str, index: int) -> str:
    return f"{post_id}_{sc}_{index + 1}"


def determine_search_date(tweet_created_at_jst: datetime) -> date:
    if tweet_created_at_jst.time() >= MARKET_CLOSE_CUTOFF_JST:
        return tweet_created_at_jst.date() + timedelta(days=1)
    return tweet_created_at_jst.date()


def classify_tweet_session(
    tweet_created_at_jst: datetime,
    event_trade_date: date | None,
) -> str:
    if event_trade_date is None:
        return "unknown"
    if event_trade_date > tweet_created_at_jst.date():
        if tweet_created_at_jst.time() >= MARKET_CLOSE_CUTOFF_JST:
            return "after_market"
        return "non_trading_day"
    if tweet_created_at_jst.time() < dt_time(9, 0):
        return "pre_market"
    if tweet_created_at_jst.time() < MARKET_CLOSE_CUTOFF_JST:
        return "intraday"
    return "after_market"


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    return float(value)


def fetch_company_row(
    conn: psycopg.Connection[Any],
    sc: str,
) -> dict[str, Any] | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select sc, name, market, industry, trade_date
            from analytics.listed_companies_latest
            where sc = %s
            """,
            (sc,),
        )
        return cur.fetchone()


def fetch_price_rows_after(
    conn: psycopg.Connection[Any],
    sc: str,
    search_date: date,
    limit: int,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                trade_date,
                open_price,
                high_price,
                low_price,
                close_price,
                volume,
                name,
                market,
                industry
            from analytics.stock_prices_adjusted_daily
            where sc = %s
              and trade_date >= %s
            order by trade_date
            limit %s
            """,
            (sc, search_date, limit),
        )
        return list(cur.fetchall())


def fetch_price_rows_before(
    conn: psycopg.Connection[Any],
    sc: str,
    before_date: date,
    limit: int,
) -> list[dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                trade_date,
                close_price,
                volume
            from analytics.stock_prices_adjusted_daily
            where sc = %s
              and trade_date < %s
            order by trade_date desc
            limit %s
            """,
            (sc, before_date, limit),
        )
        return list(cur.fetchall())


def build_market_context(
    conn: psycopg.Connection[Any],
    sc: str,
    tweet_created_at_jst: datetime,
    *,
    volume_lookback_days: int,
    forward_return_days: Sequence[int],
) -> dict[str, Any]:
    company_row = fetch_company_row(conn, sc)
    search_date = determine_search_date(tweet_created_at_jst)
    forward_limit = max(max(forward_return_days), 1) + 1
    forward_rows = fetch_price_rows_after(conn, sc, search_date, forward_limit)

    if not forward_rows:
        return {
            "tweet_session": classify_tweet_session(tweet_created_at_jst, None),
            "company_snapshot_name": company_row["name"] if company_row else None,
            "market": company_row["market"] if company_row else None,
            "industry": company_row["industry"] if company_row else None,
            "event_trade_date": None,
            "previous_trade_date": None,
            "next_trade_date": None,
            "previous_close_price": None,
            "event_open_price": None,
            "event_high_price": None,
            "event_low_price": None,
            "event_close_price": None,
            "next_close_price": None,
            "event_volume": None,
            "avg_volume_20d": None,
            "volume_ratio_20d": None,
            "event_day_return_pct": None,
            "intraday_peak_return_pct": None,
            "max_close_return_5d_pct": None,
            "max_close_return_20d_pct": None,
        }

    event_row = forward_rows[0]
    history_rows = fetch_price_rows_before(
        conn,
        sc,
        event_row["trade_date"],
        max(volume_lookback_days, 1),
    )
    previous_row = history_rows[0] if history_rows else None
    next_row = forward_rows[1] if len(forward_rows) > 1 else None

    average_volume = None
    volumes = [to_float(row["volume"]) for row in history_rows if row.get("volume") is not None]
    if volumes:
        average_volume = sum(volumes) / len(volumes)

    previous_close = to_float(previous_row["close_price"]) if previous_row else None
    event_close = to_float(event_row["close_price"])
    event_high = to_float(event_row["high_price"])
    event_volume = to_float(event_row["volume"])
    volume_ratio = (
        event_volume / average_volume
        if event_volume is not None and average_volume not in {None, 0}
        else None
    )

    def forward_max_return(days: int) -> float | None:
        if previous_close in {None, 0}:
            return None
        subset = [to_float(row["close_price"]) for row in forward_rows[:days] if row.get("close_price") is not None]
        if not subset:
            return None
        return max((price / previous_close) - 1 for price in subset if price is not None)

    return {
        "tweet_session": classify_tweet_session(tweet_created_at_jst, event_row["trade_date"]),
        "company_snapshot_name": (
            company_row["name"] if company_row else event_row.get("name")
        ),
        "market": company_row["market"] if company_row else event_row.get("market"),
        "industry": company_row["industry"] if company_row else event_row.get("industry"),
        "event_trade_date": event_row["trade_date"],
        "previous_trade_date": previous_row["trade_date"] if previous_row else None,
        "next_trade_date": next_row["trade_date"] if next_row else None,
        "previous_close_price": previous_close,
        "event_open_price": to_float(event_row["open_price"]),
        "event_high_price": event_high,
        "event_low_price": to_float(event_row["low_price"]),
        "event_close_price": event_close,
        "next_close_price": to_float(next_row["close_price"]) if next_row else None,
        "event_volume": event_volume,
        "avg_volume_20d": average_volume,
        "volume_ratio_20d": volume_ratio,
        "event_day_return_pct": (
            (event_close / previous_close) - 1
            if event_close is not None and previous_close not in {None, 0}
            else None
        ),
        "intraday_peak_return_pct": (
            (event_high / previous_close) - 1
            if event_high is not None and previous_close not in {None, 0}
            else None
        ),
        "max_close_return_5d_pct": forward_max_return(5),
        "max_close_return_20d_pct": forward_max_return(20),
    }


def iter_mentions(payload: dict[str, Any]) -> list[tuple[dict[str, Any], dict[str, Any], int]]:
    rows: list[tuple[dict[str, Any], dict[str, Any], int]] = []
    tweets = payload.get("tweets")
    if not isinstance(tweets, list):
        raise ValueError("Analysis payload must contain a tweets list.")
    for tweet in tweets:
        if not isinstance(tweet, dict):
            raise ValueError("Each tweet entry must be a mapping.")
        mentions = tweet.get("mentions", [])
        if not isinstance(mentions, list):
            raise ValueError("tweet.mentions must be a list.")
        for index, mention in enumerate(mentions):
            if not isinstance(mention, dict):
                raise ValueError("Each mention entry must be a mapping.")
            rows.append((tweet, mention, index))
    return rows


def run_enrich_tweet_analysis(args: argparse.Namespace, dsn: str) -> int:
    require_psycopg()
    payload = load_analysis_file(args.input_file)
    run = payload.get("run")
    if not isinstance(run, dict):
        raise ValueError("Analysis payload must contain run metadata.")

    parameters = run.get("parameters", {})
    if not isinstance(parameters, dict):
        raise ValueError("run.parameters must be a mapping.")

    volume_lookback_days = int(
        parameters.get("volume_lookback_days", DEFAULT_VOLUME_LOOKBACK_DAYS)
    )
    forward_return_days = tuple(
        int(value)
        for value in parameters.get("forward_return_days", DEFAULT_FORWARD_RETURN_DAYS)
    )

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        conn.execute("set timezone to 'Asia/Tokyo'")
        for tweet, mention, index in iter_mentions(payload):
            sc = str(mention.get("sc", "")).strip()
            if not sc:
                raise ValueError(f"Tweet {tweet.get('post_id')} has a mention without sc.")
            company_name = str(mention.get("company_name", "")).strip()
            if not company_name:
                raise ValueError(
                    f"Tweet {tweet.get('post_id')} / {sc} is missing company_name."
                )
            mention["match_confidence"] = normalize_match_confidence(
                mention.get("match_confidence")
            )
            if not str(mention.get("extraction_rationale", "")).strip():
                raise ValueError(
                    f"Tweet {tweet.get('post_id')} / {sc} is missing extraction_rationale."
                )
            mention["mention_id"] = mention.get("mention_id") or build_mention_id(
                str(tweet["post_id"]),
                sc,
                index,
            )
            tweet_created_at_jst = parse_analysis_datetime(tweet["created_at_jst"])
            mention["market_context"] = build_market_context(
                conn,
                sc,
                tweet_created_at_jst,
                volume_lookback_days=volume_lookback_days,
                forward_return_days=forward_return_days,
            )

    output_file = args.output_file or args.input_file.with_name("enriched_analysis.yaml")
    write_yaml(output_file, payload)
    logging.info("Wrote enriched tweet analysis to %s", output_file)
    return 0


def summarize_mentions(
    run_id: str,
    payload: dict[str, Any],
    rows: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    tweets = payload.get("tweets", [])
    tweet_count = len(tweets) if isinstance(tweets, list) else 0
    return {
        "run_id": run_id,
        "tweet_count": tweet_count,
        "mention_count": len(rows),
        "unique_symbols": len({row["sc"] for row in rows}),
        "volume_spike_count": sum(1 for row in rows if row["volume_spike_flag"]),
        "price_jump_count": sum(1 for row in rows if row["price_jump_flag"]),
        "top_symbols": [
            {
                "sc": sc,
                "count": sum(1 for row in rows if row["sc"] == sc),
            }
            for sc in sorted({row["sc"] for row in rows})
        ][:20],
    }


def build_summary_markdown(summary: dict[str, Any], rows: Sequence[dict[str, Any]]) -> str:
    lines = [
        f"# {summary['run_id']}",
        "",
        "## Summary",
        "",
        f"- tweet_count: {summary['tweet_count']}",
        f"- mention_count: {summary['mention_count']}",
        f"- unique_symbols: {summary['unique_symbols']}",
        f"- volume_spike_count: {summary['volume_spike_count']}",
        f"- price_jump_count: {summary['price_jump_count']}",
        "",
        "## Mentions",
        "",
    ]
    for row in rows[:50]:
        lines.append(
            f"- `{row['sc']}` {row['company_name']} | volume_spike={row['volume_spike_flag']} | "
            f"price_jump={row['price_jump_flag']} | {row['tweet_url']}"
        )
    return "\n".join(lines) + "\n"


def persist_tweet_analysis_run(
    conn: psycopg.Connection[Any],
    *,
    run_id: str,
    payload: dict[str, Any],
    input_file: Path,
    summary_path: Path,
) -> None:
    run = payload["run"]
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into research.tweet_analysis_runs (
                run_id,
                command_name,
                source_relation,
                company_relation,
                start_date,
                end_date,
                target_username,
                parameters_json,
                manifest_path,
                summary_path,
                notes
            )
            values (
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s
            )
            on conflict (run_id) do update
            set command_name = excluded.command_name,
                source_relation = excluded.source_relation,
                company_relation = excluded.company_relation,
                start_date = excluded.start_date,
                end_date = excluded.end_date,
                target_username = excluded.target_username,
                parameters_json = excluded.parameters_json,
                manifest_path = excluded.manifest_path,
                summary_path = excluded.summary_path,
                notes = excluded.notes
            """,
            (
                run_id,
                "persist-tweet-analysis",
                run.get("source_relation", SOURCE_RELATION),
                run.get("company_relation", COMPANY_RELATION),
                run["start_date"],
                run["end_date"],
                run.get("target_username"),
                Jsonb(run),
                str(input_file),
                str(summary_path),
                payload.get("notes"),
            ),
        )


def persist_tweet_mentions(
    conn: psycopg.Connection[Any],
    *,
    run_id: str,
    rows: Sequence[dict[str, Any]],
) -> None:
    columns = [
        "mention_id",
        "post_id",
        "target_username",
        "author_user_id",
        "author_username",
        "post_created_at",
        "post_date_jst",
        "tweet_url",
        "tweet_text",
        "sc",
        "company_name",
        "market",
        "industry",
        "match_confidence",
        "extraction_rationale",
        "tweet_session",
        "event_trade_date",
        "previous_trade_date",
        "next_trade_date",
        "previous_close_price",
        "event_open_price",
        "event_high_price",
        "event_low_price",
        "event_close_price",
        "next_close_price",
        "event_volume",
        "avg_volume_20d",
        "volume_ratio_20d",
        "event_day_return_pct",
        "intraday_peak_return_pct",
        "max_close_return_5d_pct",
        "max_close_return_20d_pct",
        "volume_spike_flag",
        "volume_spike_reason",
        "price_jump_flag",
        "price_jump_reason",
        "analysis_summary",
        "analysis_json",
    ]
    with conn.cursor() as cur:
        cur.execute("delete from research.tweet_stock_mentions where run_id = %s", (run_id,))
        if not rows:
            return
        insert_sql = f"""
insert into research.tweet_stock_mentions (
    run_id,
    {", ".join(columns)}
)
values (
    %s,
    {", ".join(["%s"] * len(columns))}
)
"""
        cur.executemany(
            insert_sql,
            [
                (
                    run_id,
                    *[
                        Jsonb(row[column]) if column == "analysis_json" else row[column]
                        for column in columns
                    ],
                )
                for row in rows
            ],
        )


def build_persist_rows(payload: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for tweet, mention, index in iter_mentions(payload):
        mention_id = mention.get("mention_id") or build_mention_id(
            str(tweet["post_id"]),
            str(mention["sc"]),
            index,
        )
        market_context = mention.get("market_context")
        if not isinstance(market_context, dict):
            raise ValueError(
                f"Tweet {tweet.get('post_id')} / {mention.get('sc')} is missing market_context."
            )
        for field_name in (
            "volume_spike_flag",
            "volume_spike_reason",
            "price_jump_flag",
            "price_jump_reason",
            "analysis_summary",
        ):
            value = mention.get(field_name)
            if field_name.endswith("_flag"):
                if not isinstance(value, bool):
                    raise ValueError(
                        f"Tweet {tweet.get('post_id')} / {mention.get('sc')} requires boolean {field_name}."
                    )
            elif not str(value or "").strip():
                raise ValueError(
                    f"Tweet {tweet.get('post_id')} / {mention.get('sc')} requires {field_name}."
                )

        rows.append(
            {
                "mention_id": mention_id,
                "post_id": str(tweet["post_id"]),
                "target_username": tweet["target_username"],
                "author_user_id": tweet["author_user_id"],
                "author_username": tweet["author_username"],
                "post_created_at": tweet["created_at"],
                "post_date_jst": tweet["post_date_jst"],
                "tweet_url": tweet["tweet_url"],
                "tweet_text": tweet["text"],
                "sc": str(mention["sc"]),
                "company_name": str(mention["company_name"]),
                "market": market_context.get("market"),
                "industry": market_context.get("industry"),
                "match_confidence": normalize_match_confidence(mention["match_confidence"]),
                "extraction_rationale": str(mention["extraction_rationale"]),
                "tweet_session": market_context.get("tweet_session"),
                "event_trade_date": market_context.get("event_trade_date"),
                "previous_trade_date": market_context.get("previous_trade_date"),
                "next_trade_date": market_context.get("next_trade_date"),
                "previous_close_price": market_context.get("previous_close_price"),
                "event_open_price": market_context.get("event_open_price"),
                "event_high_price": market_context.get("event_high_price"),
                "event_low_price": market_context.get("event_low_price"),
                "event_close_price": market_context.get("event_close_price"),
                "next_close_price": market_context.get("next_close_price"),
                "event_volume": market_context.get("event_volume"),
                "avg_volume_20d": market_context.get("avg_volume_20d"),
                "volume_ratio_20d": market_context.get("volume_ratio_20d"),
                "event_day_return_pct": market_context.get("event_day_return_pct"),
                "intraday_peak_return_pct": market_context.get("intraday_peak_return_pct"),
                "max_close_return_5d_pct": market_context.get("max_close_return_5d_pct"),
                "max_close_return_20d_pct": market_context.get("max_close_return_20d_pct"),
                "volume_spike_flag": mention["volume_spike_flag"],
                "volume_spike_reason": str(mention["volume_spike_reason"]),
                "price_jump_flag": mention["price_jump_flag"],
                "price_jump_reason": str(mention["price_jump_reason"]),
                "analysis_summary": str(mention["analysis_summary"]),
                "analysis_json": {
                    "tweet": {
                        "post_id": tweet["post_id"],
                        "tweet_url": tweet["tweet_url"],
                        "created_at_jst": tweet["created_at_jst"],
                        "text": tweet["text"],
                    },
                    "mention": mention,
                },
            }
        )
    return rows


def run_persist_tweet_analysis(args: argparse.Namespace, dsn: str) -> int:
    require_psycopg()
    payload = load_analysis_file(args.input_file)
    run = payload.get("run")
    if not isinstance(run, dict):
        raise ValueError("Analysis payload must contain run metadata.")
    run_id = str(run.get("run_id", "")).strip()
    if not run_id:
        raise ValueError("run.run_id is required.")

    rows = build_persist_rows(payload)
    summary = summarize_mentions(run_id, payload, rows)
    output_dir = args.input_file.resolve().parent
    summary_yaml_path = output_dir / "persist_summary.yaml"
    summary_md_path = output_dir / "persist_summary.md"

    write_yaml(summary_yaml_path, summary)
    summary_md_path.write_text(
        build_summary_markdown(summary, rows),
        encoding="utf-8",
    )

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        conn.execute("set timezone to 'Asia/Tokyo'")
        with conn.transaction():
            persist_tweet_analysis_run(
                conn,
                run_id=run_id,
                payload=payload,
                input_file=args.input_file.resolve(),
                summary_path=summary_md_path.resolve(),
            )
            persist_tweet_mentions(conn, run_id=run_id, rows=rows)

    logging.info(
        "Persisted tweet analysis run %s with %s mentions.",
        run_id,
        len(rows),
    )
    return 0
