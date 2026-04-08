from __future__ import annotations

import argparse
import logging
import math
import os
import time
from dataclasses import dataclass
from datetime import date, datetime, time as dt_time, timedelta, timezone
from typing import Any
from urllib.parse import quote

try:
    import psycopg
    from psycopg.rows import dict_row
    from psycopg.types.json import Jsonb
except ModuleNotFoundError:  # pragma: no cover - local help can work without runtime deps
    psycopg = None
    dict_row = None
    Jsonb = None

try:
    import requests
except ModuleNotFoundError:  # pragma: no cover - local help can work without runtime deps
    requests = None

try:
    from requests_oauthlib import OAuth1Session
except ModuleNotFoundError:  # pragma: no cover - local help can work without runtime deps
    OAuth1Session = None


JST = timezone(timedelta(hours=9))
UTC = timezone.utc
DEFAULT_API_BASE_URL = "https://api.x.com/2"
DEFAULT_TIMEOUT_SECONDS = 30
DEFAULT_INTERVAL_SECONDS = 3600
DEFAULT_FRESHNESS_MINUTES = 60
MAX_RATE_LIMIT_RETRIES = 2


@dataclass(frozen=True)
class MonitoredAccount:
    target_username: str
    target_user_id: str | None
    is_active: bool


@dataclass(frozen=True)
class TimelineState:
    target_user_id: str
    since_id: str | None
    last_seen_post_id: str | None
    last_seen_created_at: datetime | None


@dataclass(frozen=True)
class TimelineSyncStatus:
    target_user_id: str
    last_polled_at: datetime | None
    last_success_at: datetime | None
    last_seen_created_at: datetime | None


@dataclass(frozen=True)
class PollTargetResult:
    target_username: str
    fetched_count: int
    inserted_count: int
    updated_count: int


class XApiError(RuntimeError):
    def __init__(
        self,
        status_code: int,
        message: str,
        headers: dict[str, str] | None = None,
        payload: Any | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.headers = headers or {}
        self.payload = payload


class XCollectorClient:
    def __init__(
        self,
        api_key: str,
        api_key_secret: str,
        access_token: str,
        access_token_secret: str,
        bearer_token: str | None,
        api_base_url: str = DEFAULT_API_BASE_URL,
        timeout_seconds: int = DEFAULT_TIMEOUT_SECONDS,
    ) -> None:
        ensure_dependency("requests", requests)
        ensure_dependency("requests-oauthlib", OAuth1Session)
        self.api_base_url = api_base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.user_session = OAuth1Session(
            client_key=api_key,
            client_secret=api_key_secret,
            resource_owner_key=access_token,
            resource_owner_secret=access_token_secret,
        )
        self.user_session.headers.update(
            {"User-Agent": "stock-analytics-xcollector/1.0"}
        )
        self.bearer_session = None
        if bearer_token:
            self.bearer_session = requests.Session()
            self.bearer_session.headers.update(
                {
                    "Authorization": f"Bearer {bearer_token}",
                    "User-Agent": "stock-analytics-xcollector/1.0",
                }
            )

    def close(self) -> None:
        self.user_session.close()
        if self.bearer_session is not None:
            self.bearer_session.close()

    def get_user_by_username(self, username: str) -> dict[str, Any]:
        payload = self._request_user_context(
            f"/users/by/username/{quote(username)}",
            params={"user.fields": "id,username,name,protected,verified"},
        )
        return payload["data"]

    def check_timeline_access(self, user_id: str) -> None:
        self._request_user_context(
            f"/users/{user_id}/tweets",
            params={
                "max_results": 5,
                "exclude": "retweets,replies",
                "tweet.fields": "created_at",
            },
        )

    def fetch_user_posts(
        self,
        user_id: str,
        *,
        since_id: str | None,
        start_time: datetime | None,
        end_time: datetime | None = None,
    ) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]]]:
        params: dict[str, Any] = {
            "exclude": "retweets,replies",
            "expansions": "author_id",
            "max_results": 100,
            "tweet.fields": ",".join(
                [
                    "attachments",
                    "author_id",
                    "conversation_id",
                    "created_at",
                    "entities",
                    "lang",
                    "public_metrics",
                    "referenced_tweets",
                ]
            ),
            "user.fields": "id,username,name,protected,verified",
        }
        if since_id:
            params["since_id"] = since_id
        elif start_time is not None:
            params["start_time"] = format_api_timestamp(start_time)
        if end_time is not None:
            params["end_time"] = format_api_timestamp(end_time)

        posts: list[dict[str, Any]] = []
        users_by_id: dict[str, dict[str, Any]] = {}
        pagination_token: str | None = None

        while True:
            request_params = dict(params)
            if pagination_token:
                request_params["pagination_token"] = pagination_token
            payload = self._request_user_context(
                f"/users/{user_id}/tweets",
                params=request_params,
            )
            for user in payload.get("includes", {}).get("users", []):
                users_by_id[user["id"]] = user
            posts.extend(payload.get("data", []))
            pagination_token = payload.get("meta", {}).get("next_token")
            if not pagination_token:
                break

        return posts, users_by_id

    def fetch_usage(self) -> dict[str, Any] | None:
        if self.bearer_session is None:
            return None
        return self._request(self.bearer_session, "/usage/tweets", params=None)

    def _request_user_context(
        self,
        path: str,
        params: dict[str, Any] | None,
    ) -> dict[str, Any]:
        return self._request(self.user_session, path, params=params)

    def _request(
        self,
        session: Any,
        path: str,
        *,
        params: dict[str, Any] | None,
    ) -> dict[str, Any]:
        url = f"{self.api_base_url}{path}"
        for attempt in range(MAX_RATE_LIMIT_RETRIES + 1):
            response = session.get(url, params=params, timeout=self.timeout_seconds)
            if response.status_code == 429 and attempt < MAX_RATE_LIMIT_RETRIES:
                wait_seconds = rate_limit_wait_seconds(response.headers)
                logging.warning(
                    "Rate limited on %s, sleeping %.1f seconds before retry.",
                    path,
                    wait_seconds,
                )
                time.sleep(wait_seconds)
                continue
            if response.ok:
                if not response.content:
                    return {}
                return response.json()

            try:
                payload = response.json()
            except ValueError:
                payload = response.text
            raise XApiError(
                status_code=response.status_code,
                message=render_api_error_message(response.status_code, payload),
                headers=dict(response.headers),
                payload=payload,
            )

        raise RuntimeError(f"Unexpected retry loop exit for {path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect monitored X user posts into PostgreSQL."
    )
    parser.add_argument(
        "--dsn",
        default=None,
        help="PostgreSQL DSN. Defaults to DATABASE_URL.",
    )
    parser.add_argument(
        "--api-base-url",
        default=DEFAULT_API_BASE_URL,
        help="Base URL for the X API.",
    )
    parser.add_argument(
        "--timeout-seconds",
        type=int,
        default=DEFAULT_TIMEOUT_SECONDS,
        help="HTTP timeout for X API requests.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity.",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser(
        "sync-targets",
        help="Resolve configured target usernames and verify timeline access.",
    )
    poll_once = subparsers.add_parser(
        "poll-once",
        help="Fetch new posts once for all resolved monitored accounts.",
    )
    poll_once.add_argument(
        "--target-username",
        default=None,
        help="Optional monitored username filter.",
    )
    daemon = subparsers.add_parser(
        "daemon",
        help="Run one poll immediately, then continue on a fixed interval.",
    )
    daemon.add_argument(
        "--interval-seconds",
        type=int,
        default=None,
        help="Polling interval in seconds. Defaults to X_COLLECT_INTERVAL_SECONDS or 3600.",
    )
    subparsers.add_parser(
        "usage",
        help="Fetch usage information with the optional bearer token and store it.",
    )
    ensure_current = subparsers.add_parser(
        "ensure-current",
        help="Check whether recent polling is fresh enough, and only fetch incrementally when it is stale.",
    )
    ensure_current.add_argument(
        "--target-username",
        default=None,
        help="Optional monitored username filter.",
    )
    ensure_current.add_argument(
        "--freshness-minutes",
        type=int,
        default=DEFAULT_FRESHNESS_MINUTES,
        help="Treat the account as current when the last successful poll is within this many minutes.",
    )
    backfill = subparsers.add_parser(
        "backfill",
        help="Fetch a historical date range without changing the incremental since_id checkpoint.",
    )
    backfill.add_argument(
        "--days",
        type=int,
        default=30,
        help="Number of JST calendar days to backfill, ending on --end-date or today.",
    )
    backfill.add_argument(
        "--end-date",
        type=date.fromisoformat,
        default=None,
        help="Backfill through this JST date in YYYY-MM-DD format. Defaults to today.",
    )
    backfill.add_argument(
        "--target-username",
        default=None,
        help="Optional monitored username filter.",
    )
    return parser.parse_args()


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def ensure_dependency(name: str, module: Any) -> None:
    if module is None:
        raise RuntimeError(
            f"{name} is required for this command. Install dependencies from requirements.txt first."
        )


def get_dsn(explicit_dsn: str | None) -> str:
    dsn = explicit_dsn or os.getenv("DATABASE_URL")
    if not dsn:
        raise RuntimeError("PostgreSQL DSN is required via --dsn or DATABASE_URL.")
    return dsn


def build_client(args: argparse.Namespace) -> XCollectorClient:
    return XCollectorClient(
        api_key=require_env("X_API_KEY"),
        api_key_secret=require_env("X_API_KEY_SECRET"),
        access_token=require_env("X_ACCESS_TOKEN"),
        access_token_secret=require_env("X_ACCESS_TOKEN_SECRET"),
        bearer_token=os.getenv("X_BEARER_TOKEN"),
        api_base_url=args.api_base_url,
        timeout_seconds=args.timeout_seconds,
    )


def require_env(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"{name} is required.")
    return value


def connect_db(dsn: str) -> psycopg.Connection[Any]:
    ensure_dependency("psycopg", psycopg)
    return psycopg.connect(dsn, row_factory=dict_row)


def normalize_username(username: str) -> str:
    return username.strip().lstrip("@").lower()


def render_api_error_message(status_code: int, payload: Any) -> str:
    if isinstance(payload, dict):
        title = payload.get("title")
        detail = payload.get("detail")
        errors = payload.get("errors")
        parts = [part for part in [title, detail] if part]
        if isinstance(errors, list):
            for item in errors:
                if not isinstance(item, dict):
                    continue
                error_detail = item.get("detail") or item.get("message")
                if error_detail:
                    parts.append(str(error_detail))
        if parts:
            return f"HTTP {status_code}: {' | '.join(parts)}"
    if isinstance(payload, str) and payload.strip():
        return f"HTTP {status_code}: {payload.strip()}"
    return f"HTTP {status_code}"


def rate_limit_wait_seconds(headers: dict[str, str]) -> float:
    reset_value = headers.get("x-rate-limit-reset")
    if not reset_value:
        return 60.0
    try:
        reset_timestamp = float(reset_value)
    except ValueError:
        return 60.0
    return max(1.0, reset_timestamp - time.time() + 1.0)


def parse_api_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def format_api_timestamp(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def current_jst_midnight_utc(now: datetime | None = None) -> datetime:
    reference = now.astimezone(JST) if now else datetime.now(JST)
    midnight_jst = datetime.combine(reference.date(), dt_time.min, tzinfo=JST)
    return midnight_jst.astimezone(UTC)


def current_jst_date(now: datetime | None = None) -> date:
    reference = now.astimezone(JST) if now else datetime.now(JST)
    return reference.date()


def jst_day_bounds_utc(start_date: date, end_date: date) -> tuple[datetime, datetime]:
    start_dt_jst = datetime.combine(start_date, dt_time.min, tzinfo=JST)
    end_dt_jst = datetime.combine(end_date + timedelta(days=1), dt_time.min, tzinfo=JST)
    return start_dt_jst.astimezone(UTC), end_dt_jst.astimezone(UTC)


def post_id_sort_key(post_id: str | None) -> tuple[int, str]:
    if post_id is None:
        return (-1, "")
    try:
        return (int(post_id), post_id)
    except ValueError:
        return (0, post_id)


def fetch_monitored_accounts(
    conn: psycopg.Connection[Any],
    *,
    resolved_only: bool,
) -> list[MonitoredAccount]:
    clauses = ["is_active"]
    if resolved_only:
        clauses.append("target_user_id is not null")

    query = f"""
        select target_username, target_user_id, is_active
        from ingest.x_monitored_accounts
        where {' and '.join(clauses)}
        order by target_username
    """
    with conn.cursor() as cur:
        cur.execute(query)
        rows = cur.fetchall()
    return [
        MonitoredAccount(
            target_username=row["target_username"],
            target_user_id=row["target_user_id"],
            is_active=row["is_active"],
        )
        for row in rows
    ]


def filter_accounts_by_username(
    accounts: list[MonitoredAccount],
    target_username: str | None,
) -> list[MonitoredAccount]:
    if not target_username:
        return accounts
    normalized = normalize_username(target_username)
    return [
        account
        for account in accounts
        if normalize_username(account.target_username) == normalized
    ]


def fetch_timeline_state(
    conn: psycopg.Connection[Any],
    target_user_id: str,
) -> TimelineState | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                target_user_id,
                since_id,
                last_seen_post_id,
                last_seen_created_at
            from ingest.x_timeline_state
            where target_user_id = %s
            """,
            (target_user_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return TimelineState(
        target_user_id=row["target_user_id"],
        since_id=row["since_id"],
        last_seen_post_id=row["last_seen_post_id"],
        last_seen_created_at=row["last_seen_created_at"],
    )


def fetch_timeline_sync_status(
    conn: psycopg.Connection[Any],
    target_user_id: str,
) -> TimelineSyncStatus | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            select
                target_user_id,
                last_polled_at,
                last_success_at,
                last_seen_created_at
            from ingest.x_timeline_state
            where target_user_id = %s
            """,
            (target_user_id,),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return TimelineSyncStatus(
        target_user_id=row["target_user_id"],
        last_polled_at=row["last_polled_at"],
        last_success_at=row["last_success_at"],
        last_seen_created_at=row["last_seen_created_at"],
    )


def ensure_timeline_state(
    conn: psycopg.Connection[Any],
    target_user_id: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into ingest.x_timeline_state (
                target_user_id,
                created_at,
                updated_at
            )
            values (%s, now(), now())
            on conflict (target_user_id) do nothing
            """,
            (target_user_id,),
        )


def upsert_user(
    conn: psycopg.Connection[Any],
    user_payload: dict[str, Any],
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into raw.x_users (
                user_id,
                username,
                name,
                protected,
                verified,
                payload,
                updated_at
            )
            values (%s, %s, %s, %s, %s, %s, now())
            on conflict (user_id) do update
            set username = excluded.username,
                name = excluded.name,
                protected = excluded.protected,
                verified = excluded.verified,
                payload = excluded.payload,
                updated_at = now()
            """,
            (
                user_payload["id"],
                user_payload["username"],
                user_payload.get("name"),
                user_payload.get("protected"),
                user_payload.get("verified"),
                Jsonb(user_payload),
            ),
        )


def upsert_posts(
    conn: psycopg.Connection[Any],
    target_username: str,
    posts: list[dict[str, Any]],
    users_by_id: dict[str, dict[str, Any]],
) -> tuple[int, int]:
    if not posts:
        return 0, 0

    post_ids = [post["id"] for post in posts]
    with conn.cursor() as cur:
        cur.execute(
            "select post_id from raw.x_posts where post_id = any(%s)",
            (post_ids,),
        )
        existing_ids = {row["post_id"] for row in cur.fetchall()}

    inserted_count = 0
    updated_count = 0
    seen_at = datetime.now(UTC)

    for user_payload in users_by_id.values():
        upsert_user(conn, user_payload)

    with conn.cursor() as cur:
        for post in posts:
            author_id = post.get("author_id")
            author_payload = users_by_id.get(author_id or "")
            author_username = (
                author_payload["username"]
                if author_payload is not None
                else target_username
            )
            if post["id"] in existing_ids:
                updated_count += 1
            else:
                inserted_count += 1

            cur.execute(
                """
                insert into raw.x_posts (
                    post_id,
                    author_user_id,
                    author_username,
                    created_at,
                    text,
                    conversation_id,
                    lang,
                    public_metrics_json,
                    referenced_posts_json,
                    entities_json,
                    attachments_json,
                    payload,
                    first_seen_at,
                    last_seen_at
                )
                values (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s
                )
                on conflict (post_id) do update
                set author_user_id = excluded.author_user_id,
                    author_username = excluded.author_username,
                    created_at = excluded.created_at,
                    text = excluded.text,
                    conversation_id = excluded.conversation_id,
                    lang = excluded.lang,
                    public_metrics_json = excluded.public_metrics_json,
                    referenced_posts_json = excluded.referenced_posts_json,
                    entities_json = excluded.entities_json,
                    attachments_json = excluded.attachments_json,
                    payload = excluded.payload,
                    last_seen_at = excluded.last_seen_at
                """,
                (
                    post["id"],
                    author_id,
                    author_username,
                    parse_api_datetime(post.get("created_at")),
                    post.get("text", ""),
                    post.get("conversation_id"),
                    post.get("lang"),
                    Jsonb(post.get("public_metrics") or {}),
                    Jsonb(post["referenced_tweets"])
                    if post.get("referenced_tweets") is not None
                    else None,
                    Jsonb(post["entities"]) if post.get("entities") is not None else None,
                    Jsonb(post["attachments"])
                    if post.get("attachments") is not None
                    else None,
                    Jsonb(post),
                    seen_at,
                    seen_at,
                ),
            )

    return inserted_count, updated_count


def mark_monitored_account_synced(
    conn: psycopg.Connection[Any],
    *,
    target_username: str,
    target_user_id: str,
    access_error: str | None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            update ingest.x_monitored_accounts
            set target_user_id = %s,
                last_resolved_at = now(),
                last_access_check_at = now(),
                last_error = %s,
                updated_at = now()
            where target_username = %s
            """,
            (target_user_id, access_error, target_username),
        )


def clear_monitored_account_resolution(
    conn: psycopg.Connection[Any],
    *,
    target_username: str,
    last_error: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            with removed as (
                select target_user_id
                from ingest.x_monitored_accounts
                where target_username = %s
            )
            delete from ingest.x_timeline_state
            where target_user_id in (
                select target_user_id
                from removed
                where target_user_id is not null
            )
            """,
            (target_username,),
        )
        cur.execute(
            """
            update ingest.x_monitored_accounts
            set target_user_id = null,
                last_resolved_at = null,
                last_access_check_at = now(),
                last_error = %s,
                updated_at = now()
            where target_username = %s
            """,
            (last_error, target_username),
        )


def update_monitored_account_access(
    conn: psycopg.Connection[Any],
    *,
    target_username: str,
    last_error: str | None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            update ingest.x_monitored_accounts
            set last_access_check_at = now(),
                last_error = %s,
                updated_at = now()
            where target_username = %s
            """,
            (last_error, target_username),
        )


def mark_timeline_failure(
    conn: psycopg.Connection[Any],
    *,
    target_user_id: str,
    status_code: int | None,
    error_message: str,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into ingest.x_timeline_state (
                target_user_id,
                last_polled_at,
                consecutive_failures,
                last_http_status,
                last_error,
                created_at,
                updated_at
            )
            values (
                %s,
                now(),
                1,
                %s,
                %s,
                now(),
                now()
            )
            on conflict (target_user_id) do update
            set last_polled_at = now(),
                consecutive_failures = ingest.x_timeline_state.consecutive_failures + 1,
                last_http_status = excluded.last_http_status,
                last_error = excluded.last_error,
                updated_at = now()
            """,
            (target_user_id, status_code, error_message),
        )


def mark_timeline_success(
    conn: psycopg.Connection[Any],
    *,
    target_user_id: str,
    since_id: str | None,
    last_seen_post_id: str | None,
    last_seen_created_at: datetime | None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into ingest.x_timeline_state (
                target_user_id,
                since_id,
                last_polled_at,
                last_success_at,
                last_seen_post_id,
                last_seen_created_at,
                consecutive_failures,
                last_http_status,
                last_error,
                created_at,
                updated_at
            )
            values (
                %s, %s, now(), now(), %s, %s, 0, 200, null, now(), now()
            )
            on conflict (target_user_id) do update
            set since_id = excluded.since_id,
                last_polled_at = excluded.last_polled_at,
                last_success_at = excluded.last_success_at,
                last_seen_post_id = excluded.last_seen_post_id,
                last_seen_created_at = excluded.last_seen_created_at,
                consecutive_failures = 0,
                last_http_status = 200,
                last_error = null,
                updated_at = now()
            """,
            (
                target_user_id,
                since_id,
                last_seen_post_id,
                last_seen_created_at,
            ),
        )


def start_poll_run(
    conn: psycopg.Connection[Any],
    *,
    run_mode: str,
    target_count: int,
) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            insert into ingest.x_poll_runs (
                run_mode,
                status,
                target_count,
                started_at
            )
            values (%s, 'running', %s, now())
            returning run_id
            """,
            (run_mode, target_count),
        )
        row = cur.fetchone()
    return int(row["run_id"])


def finish_poll_run(
    conn: psycopg.Connection[Any],
    *,
    run_id: int,
    status: str,
    success_count: int,
    failure_count: int,
    fetched_post_count: int,
    inserted_post_count: int,
    updated_post_count: int,
    last_error: str | None,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            update ingest.x_poll_runs
            set status = %s,
                success_count = %s,
                failure_count = %s,
                fetched_post_count = %s,
                inserted_post_count = %s,
                updated_post_count = %s,
                last_error = %s,
                finished_at = now()
            where run_id = %s
            """,
            (
                status,
                success_count,
                failure_count,
                fetched_post_count,
                inserted_post_count,
                updated_post_count,
                last_error,
                run_id,
            ),
        )


def sync_targets(
    conn: psycopg.Connection[Any],
    client: XCollectorClient,
) -> int:
    targets = fetch_monitored_accounts(conn, resolved_only=False)
    if not targets:
        logging.info(
            "No monitored X accounts are configured in ingest.x_monitored_accounts."
        )
        return 0

    synced_count = 0
    for account in targets:
        normalized_username = normalize_username(account.target_username)
        logging.info("Resolving @%s", normalized_username)
        try:
            user_payload = client.get_user_by_username(normalized_username)
        except XApiError as exc:
            logging.warning("Failed to resolve @%s: %s", normalized_username, exc)
            with conn.transaction():
                if exc.status_code == 404:
                    clear_monitored_account_resolution(
                        conn,
                        target_username=account.target_username,
                        last_error=str(exc),
                    )
                else:
                    update_monitored_account_access(
                        conn,
                        target_username=account.target_username,
                        last_error=str(exc),
                    )
            continue

        access_error: str | None = None
        try:
            client.check_timeline_access(user_payload["id"])
        except XApiError as exc:
            access_error = str(exc)
            logging.warning(
                "Timeline access check for @%s returned %s",
                normalized_username,
                access_error,
            )

        with conn.transaction():
            upsert_user(conn, user_payload)
            mark_monitored_account_synced(
                conn,
                target_username=account.target_username,
                target_user_id=user_payload["id"],
                access_error=access_error,
            )
            ensure_timeline_state(conn, user_payload["id"])
        synced_count += 1

    logging.info("Synced %s monitored account(s).", synced_count)
    return synced_count


def poll_target(
    conn: psycopg.Connection[Any],
    client: XCollectorClient,
    account: MonitoredAccount,
) -> PollTargetResult:
    if not account.target_user_id:
        raise RuntimeError(
            f"{account.target_username} does not have a resolved target_user_id."
        )

    state = fetch_timeline_state(conn, account.target_user_id)
    start_time = None if state and state.since_id else current_jst_midnight_utc()
    posts, users_by_id = client.fetch_user_posts(
        account.target_user_id,
        since_id=state.since_id if state else None,
        start_time=start_time,
    )

    if account.target_user_id not in users_by_id:
        with conn.cursor() as cur:
            cur.execute(
                """
                select payload
                from raw.x_users
                where user_id = %s
                """,
                (account.target_user_id,),
            )
            row = cur.fetchone()
        if row is not None:
            users_by_id[account.target_user_id] = row["payload"]

    with conn.transaction():
        inserted_count, updated_count = upsert_posts(
            conn,
            target_username=account.target_username,
            posts=posts,
            users_by_id=users_by_id,
        )
        sorted_posts = sorted(posts, key=lambda post: post_id_sort_key(post.get("id")))
        newest_post = sorted_posts[-1] if sorted_posts else None
        since_id = (
            newest_post.get("id")
            if newest_post is not None
            else (state.since_id if state is not None else None)
        )
        last_seen_post_id = (
            newest_post.get("id")
            if newest_post is not None
            else (state.last_seen_post_id if state is not None else None)
        )
        last_seen_created_at = (
            parse_api_datetime(newest_post.get("created_at"))
            if newest_post is not None
            else (state.last_seen_created_at if state is not None else None)
        )
        mark_timeline_success(
            conn,
            target_user_id=account.target_user_id,
            since_id=since_id,
            last_seen_post_id=last_seen_post_id,
            last_seen_created_at=last_seen_created_at,
        )
        update_monitored_account_access(
            conn,
            target_username=account.target_username,
            last_error=None,
        )

    return PollTargetResult(
        target_username=account.target_username,
        fetched_count=len(posts),
        inserted_count=inserted_count,
        updated_count=updated_count,
    )


def execute_poll_once(
    conn: psycopg.Connection[Any],
    client: XCollectorClient,
    *,
    run_mode: str,
    target_username: str | None = None,
    accounts: list[MonitoredAccount] | None = None,
) -> int:
    if accounts is None:
        resolved_accounts = fetch_monitored_accounts(conn, resolved_only=True)
        accounts = filter_accounts_by_username(resolved_accounts, target_username)
    if not accounts:
        logging.info(
            "No resolved monitored accounts matched%s. Run sync-targets after inserting usernames.",
            f" @{normalize_username(target_username)}" if target_username else "",
        )
        return 0

    with conn.transaction():
        run_id = start_poll_run(conn, run_mode=run_mode, target_count=len(accounts))

    success_count = 0
    failure_count = 0
    fetched_post_count = 0
    inserted_post_count = 0
    updated_post_count = 0
    last_error: str | None = None

    for account in accounts:
        logging.info("Polling @%s", account.target_username)
        try:
            result = poll_target(conn, client, account)
        except XApiError as exc:
            failure_count += 1
            last_error = str(exc)
            logging.warning("Polling failed for @%s: %s", account.target_username, exc)
            with conn.transaction():
                if account.target_user_id is not None:
                    mark_timeline_failure(
                        conn,
                        target_user_id=account.target_user_id,
                        status_code=exc.status_code,
                        error_message=str(exc),
                    )
                if exc.status_code == 404:
                    clear_monitored_account_resolution(
                        conn,
                        target_username=account.target_username,
                        last_error=str(exc),
                    )
                else:
                    update_monitored_account_access(
                        conn,
                        target_username=account.target_username,
                        last_error=str(exc),
                    )
            continue
        except Exception as exc:
            failure_count += 1
            last_error = str(exc)
            logging.exception("Unexpected error while polling @%s", account.target_username)
            with conn.transaction():
                if account.target_user_id is not None:
                    mark_timeline_failure(
                        conn,
                        target_user_id=account.target_user_id,
                        status_code=None,
                        error_message=str(exc),
                    )
                update_monitored_account_access(
                    conn,
                    target_username=account.target_username,
                    last_error=str(exc),
                )
            continue

        success_count += 1
        fetched_post_count += result.fetched_count
        inserted_post_count += result.inserted_count
        updated_post_count += result.updated_count
        logging.info(
            "Polled @%s: fetched=%s inserted=%s updated=%s",
            result.target_username,
            result.fetched_count,
            result.inserted_count,
            result.updated_count,
        )

    if failure_count == len(accounts):
        status = "failed"
    elif failure_count:
        status = "completed_with_errors"
    else:
        status = "completed"

    with conn.transaction():
        finish_poll_run(
            conn,
            run_id=run_id,
            status=status,
            success_count=success_count,
            failure_count=failure_count,
            fetched_post_count=fetched_post_count,
            inserted_post_count=inserted_post_count,
            updated_post_count=updated_post_count,
            last_error=last_error,
        )

    logging.info(
        "Poll run %s finished with status=%s success=%s failure=%s fetched=%s inserted=%s updated=%s",
        run_id,
        status,
        success_count,
        failure_count,
        fetched_post_count,
        inserted_post_count,
        updated_post_count,
    )
    return 0 if failure_count == 0 else 1


def execute_ensure_current(
    conn: psycopg.Connection[Any],
    client: XCollectorClient,
    *,
    target_username: str | None,
    freshness_minutes: int,
) -> int:
    if freshness_minutes <= 0:
        raise RuntimeError("--freshness-minutes must be positive.")

    resolved_accounts = fetch_monitored_accounts(conn, resolved_only=True)
    accounts = filter_accounts_by_username(resolved_accounts, target_username)
    if not accounts:
        logging.info(
            "No resolved monitored accounts matched%s.",
            f" @{normalize_username(target_username)}" if target_username else "",
        )
        return 0

    cutoff_time = datetime.now(UTC) - timedelta(minutes=freshness_minutes)
    stale_accounts: list[MonitoredAccount] = []

    for account in accounts:
        if account.target_user_id is None:
            stale_accounts.append(account)
            logging.info(
                "@%s has no resolved user id yet and will be polled.",
                account.target_username,
            )
            continue

        status = fetch_timeline_sync_status(conn, account.target_user_id)
        if status is None or status.last_success_at is None:
            stale_accounts.append(account)
            logging.info(
                "@%s has no successful incremental poll yet and will be polled.",
                account.target_username,
            )
            continue

        if status.last_success_at < cutoff_time:
            stale_accounts.append(account)
            logging.info(
                "@%s is stale: last_success_at=%s is older than the %s-minute freshness window.",
                account.target_username,
                status.last_success_at.astimezone(JST).isoformat(),
                freshness_minutes,
            )
            continue

        logging.info(
            "@%s is current: last_success_at=%s is within the %s-minute freshness window. Skipping fetch.",
            account.target_username,
            status.last_success_at.astimezone(JST).isoformat(),
            freshness_minutes,
        )

    if not stale_accounts:
        logging.info(
            "All matched accounts are current within %s minutes. No incremental fetch needed.",
            freshness_minutes,
        )
        return 0

    return execute_poll_once(
        conn,
        client,
        run_mode="poll_once",
        accounts=stale_accounts,
    )


def execute_backfill(
    conn: psycopg.Connection[Any],
    client: XCollectorClient,
    *,
    target_username: str | None,
    days: int,
    end_date: date | None,
) -> int:
    if days <= 0:
        raise RuntimeError("--days must be positive.")

    resolved_accounts = fetch_monitored_accounts(conn, resolved_only=True)
    accounts = filter_accounts_by_username(resolved_accounts, target_username)
    if not accounts:
        logging.info(
            "No resolved monitored accounts matched%s.",
            f" @{normalize_username(target_username)}" if target_username else "",
        )
        return 0

    effective_end_date = end_date or current_jst_date()
    start_date = effective_end_date - timedelta(days=days - 1)
    start_time, end_time = jst_day_bounds_utc(start_date, effective_end_date)

    total_fetched = 0
    total_inserted = 0
    total_updated = 0
    failure_count = 0

    logging.info(
        "Backfilling %s account(s) for JST %s through %s",
        len(accounts),
        start_date.isoformat(),
        effective_end_date.isoformat(),
    )

    for account in accounts:
        logging.info("Backfilling @%s", account.target_username)
        try:
            posts, users_by_id = client.fetch_user_posts(
                account.target_user_id,
                since_id=None,
                start_time=start_time,
                end_time=end_time,
            )
            if account.target_user_id not in users_by_id:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        select payload
                        from raw.x_users
                        where user_id = %s
                        """,
                        (account.target_user_id,),
                    )
                    row = cur.fetchone()
                if row is not None:
                    users_by_id[account.target_user_id] = row["payload"]

            with conn.transaction():
                inserted_count, updated_count = upsert_posts(
                    conn,
                    target_username=account.target_username,
                    posts=posts,
                    users_by_id=users_by_id,
                )
                update_monitored_account_access(
                    conn,
                    target_username=account.target_username,
                    last_error=None,
                )
            total_fetched += len(posts)
            total_inserted += inserted_count
            total_updated += updated_count
            logging.info(
                "Backfilled @%s: fetched=%s inserted=%s updated=%s",
                account.target_username,
                len(posts),
                inserted_count,
                updated_count,
            )
        except XApiError as exc:
            failure_count += 1
            logging.warning("Backfill failed for @%s: %s", account.target_username, exc)
            with conn.transaction():
                update_monitored_account_access(
                    conn,
                    target_username=account.target_username,
                    last_error=str(exc),
                )
        except Exception as exc:
            failure_count += 1
            logging.exception("Unexpected backfill error for @%s", account.target_username)
            with conn.transaction():
                update_monitored_account_access(
                    conn,
                    target_username=account.target_username,
                    last_error=str(exc),
                )

    logging.info(
        "Backfill finished: fetched=%s inserted=%s updated=%s failures=%s",
        total_fetched,
        total_inserted,
        total_updated,
        failure_count,
    )
    return 0 if failure_count == 0 else 1


def upsert_usage_daily(
    conn: psycopg.Connection[Any],
    usage_payload: dict[str, Any],
) -> int:
    data = usage_payload.get("data", {})
    project_id = data.get("project_id")
    project_cap = data.get("project_cap")
    row_count = 0

    with conn.cursor() as cur:
        for day_payload in data.get("daily_project_usage", []):
            usage_date = day_payload.get("date")
            for app_usage in day_payload.get("usage", []):
                raw_payload = {
                    "date": usage_date,
                    "project_id": project_id,
                    "project_cap": project_cap,
                    "usage": app_usage,
                }
                cur.execute(
                    """
                    insert into ingest.x_usage_daily (
                        usage_date,
                        app_id,
                        project_id,
                        project_cap,
                        posts_consumed,
                        raw_payload,
                        fetched_at
                    )
                    values (%s, %s, %s, %s, %s, %s, now())
                    on conflict (usage_date, app_id) do update
                    set project_id = excluded.project_id,
                        project_cap = excluded.project_cap,
                        posts_consumed = excluded.posts_consumed,
                        raw_payload = excluded.raw_payload,
                        fetched_at = excluded.fetched_at
                    """,
                    (
                        date.fromisoformat(usage_date),
                        app_usage.get("app_id"),
                        project_id,
                        project_cap,
                        app_usage.get("tweets_consumed", 0),
                        Jsonb(raw_payload),
                    ),
                )
                row_count += 1

    return row_count


def run_usage(conn: psycopg.Connection[Any], client: XCollectorClient) -> int:
    usage_payload = client.fetch_usage()
    if usage_payload is None:
        logging.info("X_BEARER_TOKEN is not configured; skipping usage fetch.")
        return 0

    with conn.transaction():
        row_count = upsert_usage_daily(conn, usage_payload)
    logging.info("Stored %s usage row(s).", row_count)
    return 0


def next_run_delay_seconds(interval_seconds: int, now: datetime | None = None) -> float:
    reference = now.astimezone(JST) if now else datetime.now(JST)
    if interval_seconds >= 3600 and interval_seconds % 3600 == 0:
        hours = interval_seconds // 3600
        base_hour = (reference.hour // hours) * hours
        boundary = reference.replace(
            hour=base_hour,
            minute=0,
            second=0,
            microsecond=0,
        )
        if boundary <= reference:
            boundary += timedelta(hours=hours)
        return max(1.0, (boundary - reference).total_seconds())

    next_epoch = math.ceil(reference.timestamp() / interval_seconds) * interval_seconds
    return max(1.0, next_epoch - reference.timestamp())


def run_daemon(args: argparse.Namespace, dsn: str) -> int:
    interval_seconds = args.interval_seconds or int(
        os.getenv("X_COLLECT_INTERVAL_SECONDS", str(DEFAULT_INTERVAL_SECONDS))
    )
    if interval_seconds <= 0:
        raise RuntimeError("interval_seconds must be positive.")

    while True:
        client = build_client(args)
        try:
            with connect_db(dsn) as conn:
                execute_poll_once(conn, client, run_mode="daemon")
        except Exception:
            logging.exception("Daemon poll cycle failed.")
        finally:
            client.close()

        delay_seconds = next_run_delay_seconds(interval_seconds)
        logging.info("Sleeping %.1f seconds until next poll cycle.", delay_seconds)
        time.sleep(delay_seconds)


def main() -> int:
    args = parse_args()
    configure_logging(args.log_level)
    dsn = get_dsn(args.dsn)

    if args.command == "daemon":
        return run_daemon(args, dsn)

    client = build_client(args)
    try:
        with connect_db(dsn) as conn:
            if args.command == "sync-targets":
                sync_targets(conn, client)
                return 0
            if args.command == "poll-once":
                return execute_poll_once(
                    conn,
                    client,
                    run_mode="poll_once",
                    target_username=args.target_username,
                )
            if args.command == "ensure-current":
                return execute_ensure_current(
                    conn,
                    client,
                    target_username=args.target_username,
                    freshness_minutes=args.freshness_minutes,
                )
            if args.command == "backfill":
                return execute_backfill(
                    conn,
                    client,
                    target_username=args.target_username,
                    days=args.days,
                    end_date=args.end_date,
                )
            if args.command == "usage":
                return run_usage(conn, client)
    finally:
        client.close()

    raise RuntimeError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
