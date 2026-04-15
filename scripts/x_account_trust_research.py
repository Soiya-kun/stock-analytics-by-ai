from __future__ import annotations

import argparse
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from statistics import median
from typing import Any, Sequence

try:
    import psycopg
    from psycopg.rows import dict_row
    from psycopg.types.json import Jsonb
except ModuleNotFoundError:  # pragma: no cover
    psycopg = None
    dict_row = None
    Jsonb = None

from tweet_stock_research import (
    COMPANY_RELATION,
    DEFAULT_FORWARD_RETURN_DAYS,
    DEFAULT_VOLUME_LOOKBACK_DAYS,
    SOURCE_RELATION,
    build_market_context,
    fetch_company_snapshot,
    load_analysis_file,
    normalize_match_confidence,
    parse_analysis_datetime,
    parse_forward_return_days,
    require_psycopg,
    write_csv,
    write_json,
    write_yaml,
)


X_SIGNAL_ANALYSIS_OUTPUT_DIR = Path("research") / "x-signal-analysis"
X_ACCOUNT_TRUST_OUTPUT_DIR = Path("research") / "account-trust"
SIGNAL_ANALYSIS_VERSION = "x-signal-v1"
ACCOUNT_TRUST_VERSION = "x-account-trust-v1"
ACCOUNT_ROLE_CHOICES = ("benchmark", "candidate", "all")
SIGNAL_LABEL_CHOICES = ("bullish", "non_bullish", "irrelevant")
CONFIDENCE_CHOICES = ("high", "medium", "low")


@dataclass(frozen=True)
class XSignalAnalysisParams:
    output_dir: Path
    start_date: date
    end_date: date
    target_username: str | None
    account_role: str
    batch_size: int
    volume_lookback_days: int
    forward_return_days: tuple[int, ...]
    analysis_version: str


@dataclass(frozen=True)
class XAccountTrustParams:
    output_dir: Path
    start_date: date
    end_date: date
    candidate_username: str | None
    cluster_window_days: int
    unique_success_horizon_days: int
    unique_success_return_pct: float
    overlap_weight: float
    early_weight: float
    unique_weight: float
    insufficient_min_clusters: int
    insufficient_min_unique_picks: int
    trusted_score_threshold: float
    watch_score_threshold: float
    analysis_version: str


def register_subcommands(
    subparsers: argparse._SubParsersAction[argparse.ArgumentParser],
) -> None:
    prepare_parser = subparsers.add_parser(
        "prepare-x-signal-analysis",
        help="Export unanalyzed monitored X posts for manual stock-signal annotation.",
    )
    prepare_parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    prepare_parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    prepare_parser.add_argument("--target-username", default=None)
    prepare_parser.add_argument(
        "--account-role",
        default="all",
        choices=ACCOUNT_ROLE_CHOICES,
    )
    prepare_parser.add_argument("--batch-size", type=int, default=100)
    prepare_parser.add_argument(
        "--output-dir",
        type=Path,
        default=X_SIGNAL_ANALYSIS_OUTPUT_DIR,
    )
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
    prepare_parser.add_argument(
        "--analysis-version",
        default=SIGNAL_ANALYSIS_VERSION,
    )

    enrich_parser = subparsers.add_parser(
        "enrich-x-signal-analysis",
        help="Add market context to a manually annotated x-signal analysis file.",
    )
    enrich_parser.add_argument("--input-file", type=Path, required=True)
    enrich_parser.add_argument("--output-file", type=Path, default=None)

    persist_parser = subparsers.add_parser(
        "persist-x-signal-analysis",
        help="Persist a reviewed x-signal analysis file into canonical signal tables.",
    )
    persist_parser.add_argument("--input-file", type=Path, required=True)

    evaluate_parser = subparsers.add_parser(
        "evaluate-x-account-trust",
        help="Score candidate X accounts against benchmark bullish stock-signal clusters.",
    )
    evaluate_parser.add_argument("--start-date", type=date.fromisoformat, required=True)
    evaluate_parser.add_argument("--end-date", type=date.fromisoformat, required=True)
    evaluate_parser.add_argument("--candidate-username", default=None)
    evaluate_parser.add_argument(
        "--output-dir",
        type=Path,
        default=X_ACCOUNT_TRUST_OUTPUT_DIR,
    )
    evaluate_parser.add_argument("--cluster-window-days", type=int, default=30)
    evaluate_parser.add_argument(
        "--unique-success-horizon-days",
        type=int,
        default=20,
    )
    evaluate_parser.add_argument(
        "--unique-success-return-pct",
        type=float,
        default=0.10,
    )
    evaluate_parser.add_argument("--overlap-weight", type=float, default=0.35)
    evaluate_parser.add_argument("--early-weight", type=float, default=0.35)
    evaluate_parser.add_argument("--unique-weight", type=float, default=0.30)
    evaluate_parser.add_argument(
        "--insufficient-min-clusters",
        type=int,
        default=15,
    )
    evaluate_parser.add_argument(
        "--insufficient-min-unique-picks",
        type=int,
        default=5,
    )
    evaluate_parser.add_argument(
        "--trusted-score-threshold",
        type=float,
        default=0.60,
    )
    evaluate_parser.add_argument(
        "--watch-score-threshold",
        type=float,
        default=0.35,
    )
    evaluate_parser.add_argument(
        "--analysis-version",
        default=ACCOUNT_TRUST_VERSION,
    )


def handle_command(args: argparse.Namespace, dsn: str) -> int | None:
    if args.command == "prepare-x-signal-analysis":
        return run_prepare_x_signal_analysis(args, dsn)
    if args.command == "enrich-x-signal-analysis":
        return run_enrich_x_signal_analysis(args, dsn)
    if args.command == "persist-x-signal-analysis":
        return run_persist_x_signal_analysis(args, dsn)
    if args.command == "evaluate-x-account-trust":
        return run_evaluate_x_account_trust(args, dsn)
    return None


def iso_timestamp() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def slugify_username(username: str | None) -> str:
    if not username:
        return "all-targets"
    return username.strip().lstrip("@").lower().replace("/", "-")


def build_signal_run_id(params: XSignalAnalysisParams) -> str:
    return (
        f"x_signal_{params.start_date:%Y%m%d}_{params.end_date:%Y%m%d}_"
        f"{params.account_role}_{slugify_username(params.target_username)}_{iso_timestamp()}"
    )


def build_trust_run_id(params: XAccountTrustParams) -> str:
    return (
        f"x_account_trust_{params.start_date:%Y%m%d}_{params.end_date:%Y%m%d}_"
        f"{slugify_username(params.candidate_username)}_{iso_timestamp()}"
    )


def build_signal_params(args: argparse.Namespace) -> XSignalAnalysisParams:
    if args.batch_size <= 0:
        raise ValueError("--batch-size must be positive.")
    return XSignalAnalysisParams(
        output_dir=args.output_dir,
        start_date=args.start_date,
        end_date=args.end_date,
        target_username=args.target_username,
        account_role=args.account_role,
        batch_size=args.batch_size,
        volume_lookback_days=args.volume_lookback_days,
        forward_return_days=parse_forward_return_days(args.forward_return_days),
        analysis_version=str(args.analysis_version).strip() or SIGNAL_ANALYSIS_VERSION,
    )


def build_trust_params(args: argparse.Namespace) -> XAccountTrustParams:
    weight_sum = args.overlap_weight + args.early_weight + args.unique_weight
    if weight_sum <= 0:
        raise ValueError("Score weights must sum to a positive number.")
    return XAccountTrustParams(
        output_dir=args.output_dir,
        start_date=args.start_date,
        end_date=args.end_date,
        candidate_username=args.candidate_username,
        cluster_window_days=args.cluster_window_days,
        unique_success_horizon_days=args.unique_success_horizon_days,
        unique_success_return_pct=args.unique_success_return_pct,
        overlap_weight=args.overlap_weight / weight_sum,
        early_weight=args.early_weight / weight_sum,
        unique_weight=args.unique_weight / weight_sum,
        insufficient_min_clusters=args.insufficient_min_clusters,
        insufficient_min_unique_picks=args.insufficient_min_unique_picks,
        trusted_score_threshold=args.trusted_score_threshold,
        watch_score_threshold=args.watch_score_threshold,
        analysis_version=str(args.analysis_version).strip() or ACCOUNT_TRUST_VERSION,
    )


def normalize_signal_label(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError(
            "signal_label must be one of bullish / non_bullish / irrelevant."
        )
    normalized = value.strip().lower()
    if normalized not in SIGNAL_LABEL_CHOICES:
        raise ValueError(
            "signal_label must be one of bullish / non_bullish / irrelevant."
        )
    return normalized


def normalize_confidence(value: Any, *, field_name: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field_name} must be one of high / medium / low.")
    normalized = value.strip().lower()
    if normalized not in CONFIDENCE_CHOICES:
        raise ValueError(f"{field_name} must be one of high / medium / low.")
    return normalized


def normalize_account_role(value: Any, *, allow_all: bool) -> str:
    if not isinstance(value, str):
        raise ValueError("account_role must be benchmark, candidate, or all.")
    normalized = value.strip().lower()
    allowed = ACCOUNT_ROLE_CHOICES if allow_all else ACCOUNT_ROLE_CHOICES[:2]
    if normalized not in allowed:
        raise ValueError("account_role must be benchmark, candidate, or all.")
    return normalized


def fetch_posts_for_signal_analysis(
    dsn: str,
    params: XSignalAnalysisParams,
) -> list[dict[str, Any]]:
    require_psycopg()
    query = """
select
    target_username,
    account_role,
    benchmark_weight,
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
from analytics.monitored_x_posts p
where post_date_jst between %s and %s
  and (%s::text is null or target_username = %s::text)
  and (%s::text = 'all' or account_role = %s::text)
  and not exists (
      select 1
      from research.x_signal_analysis_post_reviews review
      where review.post_id = p.post_id
  )
order by created_at, post_id
limit %s
"""
    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        conn.execute("set timezone to 'Asia/Tokyo'")
        with conn.cursor() as cur:
            cur.execute(
                query,
                (
                    params.start_date,
                    params.end_date,
                    params.target_username,
                    params.target_username,
                    params.account_role,
                    params.account_role,
                    params.batch_size,
                ),
            )
            return list(cur.fetchall())


def build_signal_template_post(row: dict[str, Any]) -> dict[str, Any]:
    return {
        "post_id": row["post_id"],
        "target_username": row["target_username"],
        "account_role": row["account_role"],
        "benchmark_weight": float(row["benchmark_weight"])
        if row.get("benchmark_weight") is not None
        else None,
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
        "signals": [],
        "review_notes": "",
    }


def build_signal_prepare_manifest(
    run_id: str,
    params: XSignalAnalysisParams,
    posts: Sequence[dict[str, Any]],
    companies: Sequence[dict[str, Any]],
    files: dict[str, str],
) -> dict[str, Any]:
    posts_by_role: dict[str, int] = {}
    for row in posts:
        posts_by_role[row["account_role"]] = posts_by_role.get(row["account_role"], 0) + 1
    return {
        "type": "x_signal_analysis_prepare",
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
        "account_role": params.account_role,
        "parameters": {
            "batch_size": params.batch_size,
            "volume_lookback_days": params.volume_lookback_days,
            "forward_return_days": list(params.forward_return_days),
            "analysis_version": params.analysis_version,
        },
        "counts": {
            "post_count": len(posts),
            "company_snapshot_count": len(companies),
            "posts_by_role": posts_by_role,
        },
        "files": files,
    }


def persist_signal_analysis_run(
    conn: psycopg.Connection[Any],
    *,
    run_id: str,
    command_name: str,
    source_relation: str,
    start_date: date,
    end_date: date,
    account_role: str,
    target_username: str | None,
    batch_size: int | None,
    parameters_json: dict[str, Any],
    manifest_path: Path | None,
    summary_path: Path | None,
    notes: str | None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into research.x_signal_analysis_runs (
                run_id,
                command_name,
                source_relation,
                start_date,
                end_date,
                account_role,
                target_username,
                batch_size,
                parameters_json,
                manifest_path,
                summary_path,
                notes
            )
            values (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            on conflict (run_id) do update
            set command_name = excluded.command_name,
                source_relation = excluded.source_relation,
                start_date = excluded.start_date,
                end_date = excluded.end_date,
                account_role = excluded.account_role,
                target_username = excluded.target_username,
                batch_size = excluded.batch_size,
                parameters_json = excluded.parameters_json,
                manifest_path = excluded.manifest_path,
                summary_path = excluded.summary_path,
                notes = excluded.notes
            """,
            (
                run_id,
                command_name,
                source_relation,
                start_date,
                end_date,
                account_role,
                target_username,
                batch_size,
                Jsonb(parameters_json),
                str(manifest_path) if manifest_path else None,
                str(summary_path) if summary_path else None,
                notes,
            ),
        )


def run_prepare_x_signal_analysis(args: argparse.Namespace, dsn: str) -> int:
    params = build_signal_params(args)
    run_id = build_signal_run_id(params)
    run_dir = params.output_dir / run_id
    posts = fetch_posts_for_signal_analysis(dsn, params)
    companies = fetch_company_snapshot(dsn)

    template_payload = {
        "type": "x_signal_analysis",
        "version": 1,
        "run": {
            "run_id": run_id,
            "source_relation": SOURCE_RELATION,
            "company_relation": COMPANY_RELATION,
            "start_date": params.start_date,
            "end_date": params.end_date,
            "target_username": params.target_username,
            "account_role": params.account_role,
            "generated_at": datetime.now().isoformat(),
            "parameters": {
                "batch_size": params.batch_size,
                "volume_lookback_days": params.volume_lookback_days,
                "forward_return_days": list(params.forward_return_days),
                "analysis_version": params.analysis_version,
            },
        },
        "posts": [build_signal_template_post(row) for row in posts],
        "notes": "",
    }

    posts_csv_rows = [
        {
            "post_id": row["post_id"],
            "target_username": row["target_username"],
            "account_role": row["account_role"],
            "benchmark_weight": row["benchmark_weight"],
            "author_username": row["author_username"],
            "created_at_jst": row["created_at_jst"],
            "post_date_jst": row["post_date_jst"],
            "tweet_url": row["tweet_url"],
            "text": row["text"],
        }
        for row in posts
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
        "posts_csv": str(run_dir / "posts.csv"),
        "company_snapshot_csv": str(run_dir / "company_snapshot.csv"),
        "analysis_template_yaml": str(run_dir / "analysis_template.yaml"),
        "analysis_template_json": str(run_dir / "analysis_template.json"),
        "manifest_yaml": str(run_dir / "manifest.yaml"),
    }
    manifest = build_signal_prepare_manifest(run_id, params, posts, companies, files)

    write_csv(run_dir / "posts.csv", posts_csv_rows)
    write_csv(run_dir / "company_snapshot.csv", company_csv_rows)
    write_yaml(run_dir / "analysis_template.yaml", template_payload)
    write_json(run_dir / "analysis_template.json", template_payload)
    write_yaml(run_dir / "manifest.yaml", manifest)

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        conn.execute("set timezone to 'Asia/Tokyo'")
        with conn.transaction():
            persist_signal_analysis_run(
                conn,
                run_id=run_id,
                command_name="prepare-x-signal-analysis",
                source_relation=SOURCE_RELATION,
                start_date=params.start_date,
                end_date=params.end_date,
                account_role=params.account_role,
                target_username=params.target_username,
                batch_size=params.batch_size,
                parameters_json=template_payload["run"]["parameters"],
                manifest_path=(run_dir / "manifest.yaml").resolve(),
                summary_path=None,
                notes=template_payload.get("notes"),
            )

    logging.info(
        "Prepared x-signal analysis run %s with %s posts under %s",
        run_id,
        len(posts),
        run_dir,
    )
    return 0


def iter_signal_posts(payload: dict[str, Any]) -> list[dict[str, Any]]:
    posts = payload.get("posts")
    if not isinstance(posts, list):
        raise ValueError("Analysis payload must contain a posts list.")
    for post in posts:
        if not isinstance(post, dict):
            raise ValueError("Each post entry must be a mapping.")
    return posts


def run_enrich_x_signal_analysis(args: argparse.Namespace, dsn: str) -> int:
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
        for post in iter_signal_posts(payload):
            signals = post.get("signals", [])
            if not isinstance(signals, list):
                raise ValueError("post.signals must be a list.")
            tweet_created_at_jst = parse_analysis_datetime(post["created_at_jst"])
            for signal in signals:
                if not isinstance(signal, dict):
                    raise ValueError("Each signal entry must be a mapping.")
                sc = str(signal.get("sc", "")).strip()
                if not sc:
                    raise ValueError(f"Post {post.get('post_id')} has a signal without sc.")
                company_name = str(signal.get("company_name", "")).strip()
                if not company_name:
                    raise ValueError(
                        f"Post {post.get('post_id')} / {sc} is missing company_name."
                    )
                signal["match_confidence"] = normalize_match_confidence(
                    signal.get("match_confidence")
                )
                signal["signal_label"] = normalize_signal_label(signal.get("signal_label"))
                signal["signal_confidence"] = normalize_confidence(
                    signal.get("signal_confidence"),
                    field_name="signal_confidence",
                )
                if not str(signal.get("extraction_rationale", "")).strip():
                    raise ValueError(
                        f"Post {post.get('post_id')} / {sc} is missing extraction_rationale."
                    )
                if not str(signal.get("signal_rationale", "")).strip():
                    raise ValueError(
                        f"Post {post.get('post_id')} / {sc} is missing signal_rationale."
                    )
                signal["market_context"] = build_market_context(
                    conn,
                    sc,
                    tweet_created_at_jst,
                    volume_lookback_days=volume_lookback_days,
                    forward_return_days=forward_return_days,
                )

    output_file = args.output_file or args.input_file.with_name("enriched_analysis.yaml")
    write_yaml(output_file, payload)
    logging.info("Wrote enriched x-signal analysis to %s", output_file)
    return 0


def build_signal_rows(
    payload: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    run = payload["run"]
    parameters = run.get("parameters", {})
    analysis_version = str(
        parameters.get("analysis_version", SIGNAL_ANALYSIS_VERSION)
    ).strip() or SIGNAL_ANALYSIS_VERSION

    review_rows: list[dict[str, Any]] = []
    signal_rows: list[dict[str, Any]] = []
    bullish_count = 0

    for post in iter_signal_posts(payload):
        account_role = normalize_account_role(post.get("account_role"), allow_all=False)
        post_id = str(post["post_id"])
        signals = post.get("signals", [])
        if not isinstance(signals, list):
            raise ValueError("post.signals must be a list.")
        review_rows.append(
            {
                "post_id": post_id,
                "target_username": post["target_username"],
                "account_role": account_role,
                "source_run_id": run["run_id"],
                "review_status": "reviewed",
                "analysis_version": analysis_version,
                "review_json": {
                    "signals_count": len(signals),
                    "review_notes": post.get("review_notes"),
                },
            }
        )
        for signal in signals:
            if not isinstance(signal, dict):
                raise ValueError("Each signal entry must be a mapping.")
            market_context = signal.get("market_context")
            if not isinstance(market_context, dict):
                raise ValueError(
                    f"Post {post_id} / {signal.get('sc')} is missing market_context."
                )
            row = {
                "post_id": post_id,
                "author_username": post["author_username"],
                "target_username": post["target_username"],
                "account_role": account_role,
                "post_created_at": post["created_at"],
                "tweet_url": post["tweet_url"],
                "tweet_text": post["text"],
                "sc": str(signal["sc"]).strip(),
                "company_name": str(signal["company_name"]).strip(),
                "match_confidence": normalize_match_confidence(signal["match_confidence"]),
                "extraction_rationale": str(signal["extraction_rationale"]).strip(),
                "signal_label": normalize_signal_label(signal.get("signal_label")),
                "signal_confidence": normalize_confidence(
                    signal.get("signal_confidence"),
                    field_name="signal_confidence",
                ),
                "signal_rationale": str(signal["signal_rationale"]).strip(),
                "tweet_session": market_context.get("tweet_session"),
                "event_trade_date": market_context.get("event_trade_date"),
                "previous_close_price": market_context.get("previous_close_price"),
                "event_close_price": market_context.get("event_close_price"),
                "volume_ratio_20d": market_context.get("volume_ratio_20d"),
                "max_close_return_5d_pct": market_context.get("max_close_return_5d_pct"),
                "max_close_return_20d_pct": market_context.get("max_close_return_20d_pct"),
                "analysis_version": analysis_version,
                "source_run_id": run["run_id"],
                "analysis_json": {
                    "post": {
                        "post_id": post_id,
                        "target_username": post["target_username"],
                        "account_role": account_role,
                        "tweet_url": post["tweet_url"],
                        "created_at_jst": post["created_at_jst"],
                        "text": post["text"],
                    },
                    "signal": signal,
                },
            }
            if row["signal_label"] == "bullish":
                bullish_count += 1
            signal_rows.append(row)

    summary = {
        "reviewed_post_count": len(review_rows),
        "signal_count": len(signal_rows),
        "bullish_signal_count": bullish_count,
        "unique_symbols": len({row["sc"] for row in signal_rows}),
        "empty_review_count": sum(
            1
            for review_row in review_rows
            if review_row["review_json"].get("signals_count", 0) == 0
        ),
    }
    return review_rows, signal_rows, summary


def persist_signal_reviews(
    conn: psycopg.Connection[Any],
    *,
    review_rows: Sequence[dict[str, Any]],
) -> None:
    if not review_rows:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into research.x_signal_analysis_post_reviews (
                post_id,
                target_username,
                account_role,
                source_run_id,
                review_status,
                analysis_version,
                review_json,
                reviewed_at,
                updated_at
            )
            values (%s, %s, %s, %s, %s, %s, %s, now(), now())
            on conflict (post_id) do update
            set target_username = excluded.target_username,
                account_role = excluded.account_role,
                source_run_id = excluded.source_run_id,
                review_status = excluded.review_status,
                analysis_version = excluded.analysis_version,
                review_json = excluded.review_json,
                reviewed_at = now(),
                updated_at = now()
            """,
            [
                (
                    row["post_id"],
                    row["target_username"],
                    row["account_role"],
                    row["source_run_id"],
                    row["review_status"],
                    row["analysis_version"],
                    Jsonb(row["review_json"]),
                )
                for row in review_rows
            ],
        )


def persist_signal_rows(
    conn: psycopg.Connection[Any],
    *,
    reviewed_post_ids: Sequence[str],
    signal_rows: Sequence[dict[str, Any]],
) -> None:
    if not reviewed_post_ids:
        return
    with conn.cursor() as cur:
        cur.execute(
            "delete from research.x_post_stock_signals where post_id = any(%s)",
            (list(reviewed_post_ids),),
        )
        if not signal_rows:
            return
        cur.executemany(
            """
            insert into research.x_post_stock_signals (
                post_id,
                sc,
                author_username,
                target_username,
                account_role,
                post_created_at,
                tweet_url,
                tweet_text,
                company_name,
                match_confidence,
                extraction_rationale,
                signal_label,
                signal_confidence,
                signal_rationale,
                tweet_session,
                event_trade_date,
                previous_close_price,
                event_close_price,
                volume_ratio_20d,
                max_close_return_5d_pct,
                max_close_return_20d_pct,
                analysis_version,
                source_run_id,
                analysis_json,
                created_at,
                updated_at
            )
            values (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s, now(), now()
            )
            """,
            [
                (
                    row["post_id"],
                    row["sc"],
                    row["author_username"],
                    row["target_username"],
                    row["account_role"],
                    row["post_created_at"],
                    row["tweet_url"],
                    row["tweet_text"],
                    row["company_name"],
                    row["match_confidence"],
                    row["extraction_rationale"],
                    row["signal_label"],
                    row["signal_confidence"],
                    row["signal_rationale"],
                    row["tweet_session"],
                    row["event_trade_date"],
                    row["previous_close_price"],
                    row["event_close_price"],
                    row["volume_ratio_20d"],
                    row["max_close_return_5d_pct"],
                    row["max_close_return_20d_pct"],
                    row["analysis_version"],
                    row["source_run_id"],
                    Jsonb(row["analysis_json"]),
                )
                for row in signal_rows
            ],
        )


def build_signal_persist_summary_markdown(
    run_id: str,
    summary: dict[str, Any],
    signal_rows: Sequence[dict[str, Any]],
) -> str:
    lines = [
        f"# {run_id}",
        "",
        "## Summary",
        "",
        f"- reviewed_post_count: {summary['reviewed_post_count']}",
        f"- signal_count: {summary['signal_count']}",
        f"- bullish_signal_count: {summary['bullish_signal_count']}",
        f"- unique_symbols: {summary['unique_symbols']}",
        f"- empty_review_count: {summary['empty_review_count']}",
        "",
        "## Sample Signals",
        "",
    ]
    for row in signal_rows[:50]:
        lines.append(
            f"- `{row['target_username']}` `{row['sc']}` {row['company_name']} | "
            f"signal={row['signal_label']} | confidence={row['signal_confidence']} | {row['tweet_url']}"
        )
    return "\n".join(lines) + "\n"


def run_persist_x_signal_analysis(args: argparse.Namespace, dsn: str) -> int:
    require_psycopg()
    payload = load_analysis_file(args.input_file)
    run = payload.get("run")
    if not isinstance(run, dict):
        raise ValueError("Analysis payload must contain run metadata.")
    run_id = str(run.get("run_id", "")).strip()
    if not run_id:
        raise ValueError("run.run_id is required.")

    review_rows, signal_rows, summary = build_signal_rows(payload)
    output_dir = args.input_file.resolve().parent
    summary_yaml_path = output_dir / "persist_summary.yaml"
    summary_md_path = output_dir / "persist_summary.md"
    signal_rows_path = output_dir / "persisted_signals.csv"

    write_yaml(summary_yaml_path, {"run_id": run_id, **summary})
    summary_md_path.write_text(
        build_signal_persist_summary_markdown(run_id, summary, signal_rows),
        encoding="utf-8",
    )
    write_csv(signal_rows_path, signal_rows)

    parameters = run.get("parameters", {})
    if not isinstance(parameters, dict):
        raise ValueError("run.parameters must be a mapping.")

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        conn.execute("set timezone to 'Asia/Tokyo'")
        with conn.transaction():
            persist_signal_analysis_run(
                conn,
                run_id=run_id,
                command_name="persist-x-signal-analysis",
                source_relation=str(run.get("source_relation", SOURCE_RELATION)),
                start_date=date.fromisoformat(str(run["start_date"])),
                end_date=date.fromisoformat(str(run["end_date"])),
                account_role=normalize_account_role(
                    run.get("account_role", "all"),
                    allow_all=True,
                ),
                target_username=run.get("target_username"),
                batch_size=int(parameters["batch_size"])
                if parameters.get("batch_size") is not None
                else None,
                parameters_json=parameters,
                manifest_path=args.input_file.resolve(),
                summary_path=summary_md_path.resolve(),
                notes=payload.get("notes"),
            )
            persist_signal_rows(
                conn,
                reviewed_post_ids=[row["post_id"] for row in review_rows],
                signal_rows=signal_rows,
            )
            persist_signal_reviews(conn, review_rows=review_rows)

    logging.info(
        "Persisted x-signal analysis run %s with %s reviewed posts and %s signals.",
        run_id,
        len(review_rows),
        len(signal_rows),
    )
    return 0


def fetch_candidate_usernames(
    conn: psycopg.Connection[Any],
    *,
    candidate_username: str | None,
) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            select target_username
            from ingest.x_monitored_accounts
            where is_active
              and account_role = 'candidate'
              and (%s::text is null or target_username = %s::text)
            order by target_username
            """,
            (candidate_username, candidate_username),
        )
        return [row["target_username"] for row in cur.fetchall()]


def fetch_bullish_signals(
    conn: psycopg.Connection[Any],
    *,
    start_date: date,
    end_date: date,
    candidate_username: str | None,
) -> list[dict[str, Any]]:
    query = """
select
    target_username,
    author_username,
    account_role,
    benchmark_weight,
    post_id,
    post_created_at,
    sc,
    company_name,
    tweet_url,
    event_trade_date,
    max_close_return_20d_pct
from analytics.x_bullish_stock_signals
where (post_created_at at time zone 'Asia/Tokyo')::date between %s and %s
  and (%s::text is null or target_username = %s::text or account_role = 'benchmark')
order by sc, post_created_at, post_id
"""
    with conn.cursor() as cur:
        cur.execute(query, (start_date, end_date, candidate_username, candidate_username))
        return list(cur.fetchall())


def finalize_cluster(
    *,
    sc: str,
    cluster_index: int,
    rows: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    ordered_rows = sorted(rows, key=lambda row: (row["post_created_at"], row["post_id"]))
    cluster_start_at = ordered_rows[0]["post_created_at"]
    cluster_end_at = ordered_rows[-1]["post_created_at"]
    return {
        "cluster_id": f"{sc}_{cluster_index + 1}_{cluster_start_at:%Y%m%d%H%M%S}",
        "sc": sc,
        "company_name": ordered_rows[0]["company_name"],
        "cluster_start_at": cluster_start_at,
        "cluster_end_at": cluster_end_at,
        "signals": ordered_rows,
    }


def cluster_bullish_signals(
    signals: Sequence[dict[str, Any]],
    *,
    cluster_window_days: int,
) -> list[dict[str, Any]]:
    clusters: list[dict[str, Any]] = []
    signals_by_sc: dict[str, list[dict[str, Any]]] = {}
    for row in signals:
        signals_by_sc.setdefault(row["sc"], []).append(row)

    for sc in sorted(signals_by_sc):
        sc_rows = sorted(
            signals_by_sc[sc],
            key=lambda row: (row["post_created_at"], row["post_id"]),
        )
        cluster_index = 0
        current_rows: list[dict[str, Any]] = []
        cluster_start_at: datetime | None = None
        for row in sc_rows:
            post_created_at = row["post_created_at"]
            if cluster_start_at is None:
                cluster_start_at = post_created_at
                current_rows = [row]
                continue
            if post_created_at <= cluster_start_at + timedelta(days=cluster_window_days):
                current_rows.append(row)
                continue
            clusters.append(
                finalize_cluster(
                    sc=sc,
                    cluster_index=cluster_index,
                    rows=current_rows,
                )
            )
            cluster_index += 1
            cluster_start_at = post_created_at
            current_rows = [row]
        if current_rows:
            clusters.append(
                finalize_cluster(
                    sc=sc,
                    cluster_index=cluster_index,
                    rows=current_rows,
                )
            )
    return clusters


def build_candidate_cluster_rows(
    clusters: Sequence[dict[str, Any]],
    *,
    candidate_usernames: Sequence[str],
    params: XAccountTrustParams,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for cluster in clusters:
        benchmark_signals = [
            signal for signal in cluster["signals"] if signal["account_role"] == "benchmark"
        ]
        benchmark_first = benchmark_signals[0] if benchmark_signals else None
        benchmark_usernames = sorted(
            {signal["target_username"] for signal in benchmark_signals}
        )
        for candidate_username in candidate_usernames:
            candidate_signals = [
                signal
                for signal in cluster["signals"]
                if signal["target_username"] == candidate_username
                and signal["account_role"] == "candidate"
            ]
            if not candidate_signals:
                continue
            candidate_first = candidate_signals[0]
            lead_hours = (
                (benchmark_first["post_created_at"] - candidate_first["post_created_at"]).total_seconds()
                / 3600.0
                if benchmark_first is not None
                else None
            )
            cluster_success_return_pct = candidate_first.get("max_close_return_20d_pct")
            cluster_success = (
                cluster_success_return_pct is not None
                and float(cluster_success_return_pct) >= params.unique_success_return_pct
            )
            rows.append(
                {
                    "candidate_username": candidate_username,
                    "cluster_id": cluster["cluster_id"],
                    "sc": cluster["sc"],
                    "company_name": cluster["company_name"],
                    "cluster_start_at": cluster["cluster_start_at"],
                    "cluster_end_at": cluster["cluster_end_at"],
                    "candidate_first_post_id": candidate_first["post_id"],
                    "candidate_first_post_at": candidate_first["post_created_at"],
                    "candidate_signal_count": len(candidate_signals),
                    "benchmark_overlap": bool(benchmark_signals),
                    "benchmark_first_post_at": benchmark_first["post_created_at"]
                    if benchmark_first is not None
                    else None,
                    "candidate_beat_benchmark": lead_hours > 0 if lead_hours is not None else None,
                    "lead_hours": lead_hours,
                    "unique_pick": not benchmark_signals,
                    "cluster_success": cluster_success,
                    "cluster_success_return_pct": cluster_success_return_pct,
                    "benchmark_signal_count": len(benchmark_signals),
                    "benchmark_user_count": len(benchmark_usernames),
                    "candidate_post_ids": [signal["post_id"] for signal in candidate_signals],
                    "benchmark_usernames": benchmark_usernames,
                    "details_json": {
                        "candidate_tweet_urls": [
                            signal["tweet_url"] for signal in candidate_signals
                        ],
                        "benchmark_tweet_urls": [
                            signal["tweet_url"] for signal in benchmark_signals
                        ],
                        "cluster_window_days": params.cluster_window_days,
                        "unique_success_horizon_days": params.unique_success_horizon_days,
                        "unique_success_return_pct": params.unique_success_return_pct,
                    },
                }
            )
    return rows


def rate_or_none(numerator: int, denominator: int) -> float | None:
    if denominator <= 0:
        return None
    return numerator / denominator


def determine_verdict(
    *,
    insufficient_data_flag: bool,
    trust_score: float,
    params: XAccountTrustParams,
) -> str:
    if insufficient_data_flag:
        return "insufficient_data"
    if trust_score >= params.trusted_score_threshold:
        return "trusted_candidate"
    if trust_score >= params.watch_score_threshold:
        return "watch"
    return "low_confidence"


def build_trust_score_rows(
    cluster_rows: Sequence[dict[str, Any]],
    *,
    candidate_usernames: Sequence[str],
    params: XAccountTrustParams,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for candidate_username in candidate_usernames:
        candidate_clusters = [
            row for row in cluster_rows if row["candidate_username"] == candidate_username
        ]
        overlap_clusters = [row for row in candidate_clusters if row["benchmark_overlap"]]
        early_clusters = [
            row for row in overlap_clusters if row["candidate_beat_benchmark"] is True
        ]
        unique_clusters = [row for row in candidate_clusters if row["unique_pick"]]
        successful_unique_clusters = [
            row for row in unique_clusters if row["cluster_success"] is True
        ]
        lead_hours_values = [
            float(row["lead_hours"])
            for row in overlap_clusters
            if row.get("lead_hours") is not None
        ]
        benchmark_overlap_rate = rate_or_none(
            len(overlap_clusters),
            len(candidate_clusters),
        )
        early_overlap_rate = rate_or_none(
            len(early_clusters),
            len(overlap_clusters),
        )
        unique_pick_success_rate = rate_or_none(
            len(successful_unique_clusters),
            len(unique_clusters),
        )
        trust_score = (
            params.overlap_weight * (benchmark_overlap_rate or 0.0)
            + params.early_weight * (early_overlap_rate or 0.0)
            + params.unique_weight * (unique_pick_success_rate or 0.0)
        )
        insufficient_data_flag = (
            len(candidate_clusters) < params.insufficient_min_clusters
            or len(unique_clusters) < params.insufficient_min_unique_picks
        )
        verdict = determine_verdict(
            insufficient_data_flag=insufficient_data_flag,
            trust_score=trust_score,
            params=params,
        )
        rows.append(
            {
                "candidate_username": candidate_username,
                "benchmark_overlap_rate": benchmark_overlap_rate,
                "early_overlap_rate": early_overlap_rate,
                "unique_pick_success_rate": unique_pick_success_rate,
                "median_lead_hours": median(lead_hours_values)
                if lead_hours_values
                else None,
                "bullish_cluster_count": len(candidate_clusters),
                "overlap_cluster_count": len(overlap_clusters),
                "early_overlap_count": len(early_clusters),
                "unique_pick_count": len(unique_clusters),
                "successful_unique_pick_count": len(successful_unique_clusters),
                "insufficient_data_flag": insufficient_data_flag,
                "trust_score": trust_score,
                "verdict": verdict,
                "summary_json": {
                    "candidate_username": candidate_username,
                    "benchmark_overlap_rate": benchmark_overlap_rate,
                    "early_overlap_rate": early_overlap_rate,
                    "unique_pick_success_rate": unique_pick_success_rate,
                    "median_lead_hours": median(lead_hours_values)
                    if lead_hours_values
                    else None,
                    "bullish_cluster_count": len(candidate_clusters),
                    "overlap_cluster_count": len(overlap_clusters),
                    "early_overlap_count": len(early_clusters),
                    "unique_pick_count": len(unique_clusters),
                    "successful_unique_pick_count": len(successful_unique_clusters),
                    "insufficient_data_flag": insufficient_data_flag,
                    "trust_score": trust_score,
                    "verdict": verdict,
                },
            }
        )
    return rows


def persist_trust_run(
    conn: psycopg.Connection[Any],
    *,
    run_id: str,
    params: XAccountTrustParams,
    manifest_path: Path,
    summary_path: Path,
    notes: str | None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into research.x_account_trust_runs (
                run_id,
                command_name,
                source_relation,
                start_date,
                end_date,
                candidate_username,
                parameters_json,
                manifest_path,
                summary_path,
                notes
            )
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            on conflict (run_id) do update
            set command_name = excluded.command_name,
                source_relation = excluded.source_relation,
                start_date = excluded.start_date,
                end_date = excluded.end_date,
                candidate_username = excluded.candidate_username,
                parameters_json = excluded.parameters_json,
                manifest_path = excluded.manifest_path,
                summary_path = excluded.summary_path,
                notes = excluded.notes
            """,
            (
                run_id,
                "evaluate-x-account-trust",
                "analytics.x_bullish_stock_signals",
                params.start_date,
                params.end_date,
                params.candidate_username,
                Jsonb(
                    {
                        "cluster_window_days": params.cluster_window_days,
                        "unique_success_horizon_days": params.unique_success_horizon_days,
                        "unique_success_return_pct": params.unique_success_return_pct,
                        "overlap_weight": params.overlap_weight,
                        "early_weight": params.early_weight,
                        "unique_weight": params.unique_weight,
                        "insufficient_min_clusters": params.insufficient_min_clusters,
                        "insufficient_min_unique_picks": params.insufficient_min_unique_picks,
                        "trusted_score_threshold": params.trusted_score_threshold,
                        "watch_score_threshold": params.watch_score_threshold,
                        "analysis_version": params.analysis_version,
                    }
                ),
                str(manifest_path),
                str(summary_path),
                notes,
            ),
        )


def persist_trust_clusters(
    conn: psycopg.Connection[Any],
    *,
    run_id: str,
    cluster_rows: Sequence[dict[str, Any]],
) -> None:
    if not cluster_rows:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into research.x_account_trust_clusters (
                run_id,
                candidate_username,
                cluster_id,
                sc,
                company_name,
                cluster_start_at,
                cluster_end_at,
                candidate_first_post_id,
                candidate_first_post_at,
                candidate_signal_count,
                benchmark_overlap,
                benchmark_first_post_at,
                candidate_beat_benchmark,
                lead_hours,
                unique_pick,
                cluster_success,
                cluster_success_return_pct,
                benchmark_signal_count,
                benchmark_user_count,
                candidate_post_ids,
                benchmark_usernames,
                details_json,
                created_at
            )
            values (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s, %s, now()
            )
            """,
            [
                (
                    run_id,
                    row["candidate_username"],
                    row["cluster_id"],
                    row["sc"],
                    row["company_name"],
                    row["cluster_start_at"],
                    row["cluster_end_at"],
                    row["candidate_first_post_id"],
                    row["candidate_first_post_at"],
                    row["candidate_signal_count"],
                    row["benchmark_overlap"],
                    row["benchmark_first_post_at"],
                    row["candidate_beat_benchmark"],
                    row["lead_hours"],
                    row["unique_pick"],
                    row["cluster_success"],
                    row["cluster_success_return_pct"],
                    row["benchmark_signal_count"],
                    row["benchmark_user_count"],
                    Jsonb(row["candidate_post_ids"]),
                    Jsonb(row["benchmark_usernames"]),
                    Jsonb(row["details_json"]),
                )
                for row in cluster_rows
            ],
        )


def persist_trust_scores(
    conn: psycopg.Connection[Any],
    *,
    run_id: str,
    score_rows: Sequence[dict[str, Any]],
) -> None:
    if not score_rows:
        return
    with conn.cursor() as cur:
        cur.executemany(
            """
            insert into research.x_account_trust_scores (
                run_id,
                candidate_username,
                benchmark_overlap_rate,
                early_overlap_rate,
                unique_pick_success_rate,
                median_lead_hours,
                bullish_cluster_count,
                overlap_cluster_count,
                early_overlap_count,
                unique_pick_count,
                successful_unique_pick_count,
                insufficient_data_flag,
                trust_score,
                verdict,
                summary_json,
                created_at
            )
            values (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, now()
            )
            """,
            [
                (
                    run_id,
                    row["candidate_username"],
                    row["benchmark_overlap_rate"],
                    row["early_overlap_rate"],
                    row["unique_pick_success_rate"],
                    row["median_lead_hours"],
                    row["bullish_cluster_count"],
                    row["overlap_cluster_count"],
                    row["early_overlap_count"],
                    row["unique_pick_count"],
                    row["successful_unique_pick_count"],
                    row["insufficient_data_flag"],
                    row["trust_score"],
                    row["verdict"],
                    Jsonb(row["summary_json"]),
                )
                for row in score_rows
            ],
        )


def format_rate(value: Any) -> str:
    if value is None:
        return ""
    return f"{float(value) * 100:.2f}%"


def format_number(value: Any) -> str:
    if value is None:
        return ""
    return f"{float(value):.2f}"


def format_cluster_bullets(rows: Sequence[dict[str, Any]]) -> list[str]:
    if not rows:
        return ["- none"]
    bullets: list[str] = []
    for row in rows[:25]:
        lead_hours = (
            f", lead_hours={float(row['lead_hours']):.2f}"
            if row.get("lead_hours") is not None
            else ""
        )
        return_pct = (
            f", max_close_return_20d={format_rate(row.get('cluster_success_return_pct'))}"
            if row.get("cluster_success_return_pct") is not None
            else ""
        )
        bullets.append(
            f"- `{row['sc']}` {row['company_name']} | "
            f"cluster_start={row['cluster_start_at'].isoformat()} | "
            f"benchmark_overlap={row['benchmark_overlap']} | "
            f"candidate_first={row['candidate_first_post_at'].isoformat()}{lead_hours}{return_pct}"
        )
    return bullets


def build_trust_report(
    run_id: str,
    score_rows: Sequence[dict[str, Any]],
    cluster_rows: Sequence[dict[str, Any]],
) -> str:
    lines = [
        f"# {run_id}",
        "",
        "## Candidate Scores",
        "",
        "| candidate | verdict | trust_score | overlap_rate | early_rate | unique_success_rate | bullish_clusters | unique_picks | median_lead_hours |",
        "| --- | --- | --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in sorted(score_rows, key=lambda item: item["candidate_username"]):
        lines.append(
            "| {candidate} | {verdict} | {score} | {overlap} | {early} | {unique} | {clusters} | {unique_count} | {lead} |".format(
                candidate=row["candidate_username"],
                verdict=row["verdict"],
                score=format_rate(row["trust_score"]),
                overlap=format_rate(row["benchmark_overlap_rate"]),
                early=format_rate(row["early_overlap_rate"]),
                unique=format_rate(row["unique_pick_success_rate"]),
                clusters=row["bullish_cluster_count"],
                unique_count=row["unique_pick_count"],
                lead=format_number(row["median_lead_hours"]),
            )
        )

    for score_row in sorted(score_rows, key=lambda item: item["candidate_username"]):
        candidate_username = score_row["candidate_username"]
        candidate_clusters = [
            row for row in cluster_rows if row["candidate_username"] == candidate_username
        ]
        overlap_clusters = [row for row in candidate_clusters if row["benchmark_overlap"]]
        early_clusters = [
            row for row in overlap_clusters if row["candidate_beat_benchmark"] is True
        ]
        unique_winners = [
            row for row in candidate_clusters if row["unique_pick"] and row["cluster_success"] is True
        ]
        unique_failures = [
            row for row in candidate_clusters if row["unique_pick"] and row["cluster_success"] is not True
        ]
        lines.extend(
            [
                "",
                f"## {candidate_username}",
                "",
                "### Overlap With Benchmark",
                "",
            ]
        )
        lines.extend(format_cluster_bullets(overlap_clusters))
        lines.extend(["", "### Earlier Than Benchmark", ""])
        lines.extend(format_cluster_bullets(early_clusters))
        lines.extend(["", "### Candidate-Only Winners", ""])
        lines.extend(format_cluster_bullets(unique_winners))
        lines.extend(["", "### Failed Unique Picks", ""])
        lines.extend(format_cluster_bullets(unique_failures))
    return "\n".join(lines) + "\n"


def build_trust_manifest(
    run_id: str,
    params: XAccountTrustParams,
    candidate_usernames: Sequence[str],
    score_rows: Sequence[dict[str, Any]],
    cluster_rows: Sequence[dict[str, Any]],
    files: dict[str, str],
) -> dict[str, Any]:
    return {
        "type": "x_account_trust_evaluation",
        "version": 1,
        "run_id": run_id,
        "generated_at": datetime.now().isoformat(),
        "source_relation": "analytics.x_bullish_stock_signals",
        "date_range": {
            "start_date": params.start_date,
            "end_date": params.end_date,
        },
        "candidate_username": params.candidate_username,
        "candidate_usernames": list(candidate_usernames),
        "parameters": {
            "cluster_window_days": params.cluster_window_days,
            "unique_success_horizon_days": params.unique_success_horizon_days,
            "unique_success_return_pct": params.unique_success_return_pct,
            "overlap_weight": params.overlap_weight,
            "early_weight": params.early_weight,
            "unique_weight": params.unique_weight,
            "insufficient_min_clusters": params.insufficient_min_clusters,
            "insufficient_min_unique_picks": params.insufficient_min_unique_picks,
            "trusted_score_threshold": params.trusted_score_threshold,
            "watch_score_threshold": params.watch_score_threshold,
            "analysis_version": params.analysis_version,
        },
        "counts": {
            "candidate_count": len(candidate_usernames),
            "score_count": len(score_rows),
            "cluster_count": len(cluster_rows),
        },
        "files": files,
    }


def run_evaluate_x_account_trust(args: argparse.Namespace, dsn: str) -> int:
    require_psycopg()
    params = build_trust_params(args)
    run_id = build_trust_run_id(params)
    output_dir = params.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)

    with psycopg.connect(dsn, row_factory=dict_row) as conn:
        conn.execute("set timezone to 'Asia/Tokyo'")
        candidate_usernames = fetch_candidate_usernames(
            conn,
            candidate_username=params.candidate_username,
        )
        if not candidate_usernames:
            raise ValueError("No active candidate accounts matched the requested filter.")
        signals = fetch_bullish_signals(
            conn,
            start_date=params.start_date,
            end_date=params.end_date,
            candidate_username=params.candidate_username,
        )
        clusters = cluster_bullish_signals(
            signals,
            cluster_window_days=params.cluster_window_days,
        )
        cluster_rows = build_candidate_cluster_rows(
            clusters,
            candidate_usernames=candidate_usernames,
            params=params,
        )
        score_rows = build_trust_score_rows(
            cluster_rows,
            candidate_usernames=candidate_usernames,
            params=params,
        )

        files = {
            "scores_csv": str(output_dir / f"{run_id}_scores.csv"),
            "clusters_csv": str(output_dir / f"{run_id}_clusters.csv"),
            "manifest_yaml": str(output_dir / f"{run_id}_manifest.yaml"),
            "report_md": str(output_dir / f"{run_id}_report.md"),
        }
        manifest = build_trust_manifest(
            run_id,
            params,
            candidate_usernames,
            score_rows,
            cluster_rows,
            files,
        )
        report_text = build_trust_report(run_id, score_rows, cluster_rows)

        write_csv(Path(files["scores_csv"]), score_rows)
        write_csv(Path(files["clusters_csv"]), cluster_rows)
        write_yaml(Path(files["manifest_yaml"]), manifest)
        Path(files["report_md"]).write_text(report_text, encoding="utf-8")

        with conn.transaction():
            persist_trust_run(
                conn,
                run_id=run_id,
                params=params,
                manifest_path=Path(files["manifest_yaml"]).resolve(),
                summary_path=Path(files["report_md"]).resolve(),
                notes=None,
            )
            persist_trust_clusters(conn, run_id=run_id, cluster_rows=cluster_rows)
            persist_trust_scores(conn, run_id=run_id, score_rows=score_rows)

    logging.info(
        "Evaluated x-account trust run %s with %s candidates and %s clusters.",
        run_id,
        len(candidate_usernames),
        len(cluster_rows),
    )
    return 0
