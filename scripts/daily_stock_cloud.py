"""SSO-free daily review Lambda. Only private numeric price state and one public HTML.

SQLite is an ephemeral working file; a versioned private S3 object is durable state.
No connection to the user's PC/PostgreSQL, no VPC, no subprocesses, no X/Jev calls.
"""
from __future__ import annotations

import csv
from datetime import date, datetime, timedelta
from decimal import Decimal
import gzip
import hashlib
import io
import json
import math
import os
from pathlib import Path
import re
import sqlite3
import tempfile
import time
import urllib.request

from fetch_kabuplus_daily_csv import LISTING_URL, JST, build_auth_header, http_get, parse_listing
from stock_review_page import build_payload, write_html, validate_html

STATE_KEY = "state/prices.sqlite.gz"
MAX_CATCHUP_DAYS = 31
LOCK_KEY = "control/run-lock.json"


def optional_object(s3, bucket: str, key: str):
    try:
        return s3.get_object(Bucket=bucket, Key=key)
    except Exception as exc:
        if getattr(exc, "response", {}).get("Error", {}).get("Code") in ("NoSuchKey", "404"):
            return None
        raise


def acquire_lock(s3, bucket: str, owner: str, *, now: float | None = None) -> str | None:
    timestamp = time.time() if now is None else now
    existing = optional_object(s3, bucket, LOCK_KEY)
    condition = {"IfNoneMatch": "*"}
    if existing:
        current = json.loads(existing["Body"].read())
        if current["expires_at"] > timestamp:
            return None
        condition = {"IfMatch": existing["ETag"]}
    try:
        result = s3.put_object(
            Bucket=bucket, Key=LOCK_KEY, ContentType="application/json",
            Body=json.dumps({"owner": owner, "expires_at": timestamp + 960}).encode(), **condition,
        )
        return result["ETag"]
    except Exception as exc:
        if getattr(exc, "response", {}).get("ResponseMetadata", {}).get("HTTPStatusCode") in (409, 412):
            return None
        raise


def release_lock(s3, bucket: str, owner: str, etag: str) -> None:
    s3.put_object(Bucket=bucket, Key=LOCK_KEY, Body=json.dumps({"owner": owner, "expires_at": 0}).encode(),
                  ContentType="application/json", IfMatch=etag)


def open_state(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        create table if not exists prices (
            sc text not null, day text not null,
            open real, high real, low real, close real, volume real,
            previous_close real, change real, change_pct real, turnover real,
            primary key (sc,day)
        ) without rowid;
        create index if not exists prices_day on prices(day);
        create table if not exists stocks (
            sc text primary key, name text, market text, industry text
        ) without rowid;
        create table if not exists actions (
            sc text not null, day text not null, multiplier real not null,
            source text not null, primary key (sc,day)
        ) without rowid;
        create table if not exists meta (key text primary key, value text not null);
    """)
    return conn


def number(value: str | None) -> float | None:
    if value is None or value.strip() in ("", "-", "--"):
        return None
    result = float(value.replace(",", ""))
    if not math.isfinite(result):
        raise ValueError("Non-finite price in CSV")
    return result


def import_csv(conn: sqlite3.Connection, content: bytes, day: date, *, min_rows: int = 1000) -> int:
    reader = csv.DictReader(io.StringIO(content.decode("cp932")))
    required = {"SC", "日付", "株価", "前日比", "前日終値", "始値", "高値", "安値", "出来高", "売買代金（千円）"}
    if not required.issubset(reader.fieldnames or []):
        raise ValueError("Daily CSV header mismatch")
    rows, companies, seen = [], [], set()
    for row in reader:
        sc = row["SC"].strip()
        if not re.fullmatch(r"[0-9A-Z]{4}", sc):
            raise ValueError("Invalid stock code in CSV")
        if row["日付"].strip() != day.strftime("%Y%m%d"):
            continue  # Suspended/no-current-quote securities must not become today's bars.
        if sc in seen:
            raise ValueError("Duplicate stock code in CSV")
        seen.add(sc)
        values = [number(row.get(key)) for key in (
            "始値", "高値", "安値", "株価", "出来高", "前日終値", "前日比", "前日比（％）", "売買代金（千円）",
        )]
        o, h, low, close, volume, previous, change, change_pct, turnover = values
        if close is not None and close <= 0:
            raise ValueError("Non-positive close in CSV")
        if all(x is not None for x in (o, h, low, close)) and not (0 < low <= min(o, close) <= max(o, close) <= h):
            raise ValueError("Inconsistent OHLC in CSV")
        rows.append((sc, day.isoformat(), *values))
        companies.append((sc, row.get("名称") or "名称不明", row.get("市場"), row.get("業種")))
    if len(rows) < min_rows:
        raise ValueError("Daily CSV is empty, wrong-dated or unexpectedly incomplete")
    with conn:
        conn.executemany("insert or replace into prices values (?,?,?,?,?,?,?,?,?,?,?)", rows)
        conn.executemany("insert or replace into stocks values (?,?,?,?)", companies)
        # KABU+ reports the unadjusted previous close, but its day-change uses the
        # ex-rights comparison base. This detects a split from vendor reference
        # fields, never from the market's OHLC jump. Seeded verified actions win.
        for sc, d, o, h, low, close, volume, previous, change, pct, turnover in rows:
            if close is None or previous is None or previous <= 0 or change is None:
                continue
            ratio = (Decimal(str(close)) - Decimal(str(change))) / Decimal(str(previous))
            if abs(ratio - 1) <= Decimal("0.001"):
                continue
            if not Decimal("0.0001") <= ratio <= Decimal("10000"):
                raise ValueError("Invalid corporate-action comparison base")
            conn.execute(
                "insert or ignore into actions values (?,?,?,?)",
                (sc, d, float(ratio), "kabuplus_change_reference_provisional"),
            )
        conn.execute("insert or replace into meta values ('latest_date',?)", (day.isoformat(),))
    return len(rows)


def make_payload(conn: sqlite3.Connection, day: date, *, threshold: int = 3000, limit: int = 300, lookback: int = 250) -> dict:
    candidates = [dict(row) for row in conn.execute("""
        select p.sc,s.name,s.market,s.industry,p.close as close_price,
            p.change_pct as day_change_pct,p.turnover as turnover_thousand_yen
        from prices p join stocks s using(sc)
        where p.day=? and p.turnover>=? and p.close is not null
        order by p.turnover desc,p.sc
    """, (day.isoformat(), threshold * 1000))]
    matched = len(candidates)
    candidates = candidates[:limit] if limit else candidates
    if not candidates:
        raise ValueError("No current stocks match the chart threshold")
    price_rows = []
    provisional = 0
    start = (day - timedelta(days=max(lookback * 2, 180))).isoformat()
    for stock in candidates:
        actions = conn.execute(
            "select day,multiplier,source from actions where sc=? and day>? and day<=? order by day",
            (stock["sc"], start, day.isoformat()),
        ).fetchall()
        provisional += sum(a["source"] == "kabuplus_change_reference_provisional" for a in actions)
        rows = conn.execute("""
            select * from prices where sc=? and day between ? and ?
              and open is not null and high is not null and low is not null and close is not null
            order by day desc limit ?
        """, (stock["sc"], start, day.isoformat(), lookback)).fetchall()
        for row in reversed(rows):
            factor = Decimal(1)
            for action in actions:
                if action["day"] > row["day"]:
                    factor *= Decimal(str(action["multiplier"]))
            result = {"sc": stock["sc"], "trade_date": date.fromisoformat(row["day"])}
            for key in ("open", "high", "low", "close"):
                result[key + "_price"] = Decimal(str(row[key])) * factor
            result["volume"] = Decimal(str(row["volume"])) / factor if row["volume"] is not None else None
            price_rows.append(result)
    payload = build_payload(
        candidates=candidates, price_rows=price_rows, target_date=day,
        min_turnover_million_yen=Decimal(threshold), lookback_bars=lookback, matched_count=matched,
    )
    payload["meta"].update(source="KABU+ daily / private S3 price history", provisionalAdjustments=provisional)
    return payload


def _json(s3, bucket: str, key: str, value: dict) -> None:
    s3.put_object(Bucket=bucket, Key=key, Body=json.dumps(value, ensure_ascii=False).encode(), ContentType="application/json")


def process(event: dict, context, *, s3, secrets, cloudfront, today: date | None = None) -> dict:
    day = date.fromisoformat(event["target_date"]) if event.get("target_date") else (today or datetime.now(JST).date())
    if day > (today or datetime.now(JST).date()):
        raise ValueError("Future target date is not allowed")
    bucket, site_bucket = os.environ["DATA_BUCKET"], os.environ["SITE_BUCKET"]
    url = os.environ["SITE_URL"].rstrip("/") + "/daily/"
    receipt = {"target_date": day.isoformat(), "request_id": context.aws_request_id, "status": "running", "url": url}
    phase = "load_state"
    try:
        with tempfile.TemporaryDirectory() as directory:
            folder = Path(directory)
            compressed = s3.get_object(Bucket=bucket, Key=STATE_KEY)["Body"].read()
            state_path = folder / "prices.sqlite"
            state_path.write_bytes(gzip.decompress(compressed))
            conn = open_state(state_path)
            try:
                latest_row = conn.execute("select value from meta where key='latest_date'").fetchone()
                if not latest_row:
                    raise ValueError("Cloud history has not been seeded")
                latest = date.fromisoformat(latest_row[0])
                if day < latest:
                    raise ValueError("Refusing to replace a newer trading date")
                if (day - latest).days > MAX_CATCHUP_DAYS:
                    raise ValueError("Cloud price history is over 31 days stale; reseed required")
                if day == latest and not event.get("force_refresh"):
                    previous = optional_object(s3, bucket, "status/last-published.json")
                    if previous:
                        published = json.loads(previous["Body"].read())
                        if published.get("target_date") == day.isoformat():
                            head = s3.head_object(Bucket=site_bucket, Key="daily/index.html").get("Metadata", {})
                            if head.get("trading-date") == day.isoformat() and head.get("sha256") == published.get("sha256"):
                                receipt.update(status="already_current", displayed=published["displayed"], sha256=published["sha256"], phase="done")
                                return receipt
                phase = "listing"
                credentials = json.loads(secrets.get_secret_value(SecretId=os.environ["CSVEX_SECRET_ARN"])["SecretString"])
                auth = build_auth_header(credentials["username"], credentials["password"])
                entries = parse_listing(http_get(LISTING_URL, auth, 30).decode("utf-8"), LISTING_URL)
                if not entries:
                    raise ValueError("KABU+ listing format changed or listing is empty")
                if day not in entries:
                    receipt.update(status="skipped", reason="csv_not_published")
                    return receipt
                phase = "import"
                imported_days = []
                # Include the target even when seeded at this date, to exercise real
                # remote acquisition at deployment and allow provider corrections.
                for trading_day in sorted(d for d in entries if latest < d <= day or d == day):
                    entry = entries[trading_day]
                    content = http_get(entry.download_url, auth, 30)
                    count = import_csv(conn, content, trading_day)
                    s3.put_object(Bucket=bucket, Key=f"raw/daily/{entry.file_name}", Body=content, ContentType="text/csv")
                    imported_days.append({"date": trading_day.isoformat(), "rows": count})
                receipt["imports"] = imported_days
                phase = "generate"
                payload = make_payload(conn, day)
                html = folder / "index.html"
                write_html(Path(__file__).with_name("template.html"), html, payload)
                receipt.update(validate_html(html, day))
                receipt["provisional_adjustments"] = payload["meta"]["provisionalAdjustments"]
                with conn:
                    conn.execute("delete from prices where day<?", ((day - timedelta(days=1000)).isoformat(),))
                conn.execute("vacuum")
            finally:
                conn.close()
            phase = "save_state"
            s3.put_object(Bucket=bucket, Key=STATE_KEY, Body=gzip.compress(state_path.read_bytes(), compresslevel=6), ContentType="application/gzip",
                          Metadata={"trading-date": day.isoformat()})
            phase = "publish"
            # The runtime role can publish exactly this one public object.
            s3.put_object(
                Bucket=site_bucket, Key="daily/index.html", Body=html.read_bytes(),
                ContentType="text/html; charset=utf-8", CacheControl="no-cache,max-age=0,must-revalidate",
                Metadata={"trading-date": day.isoformat(), "sha256": receipt["sha256"]},
            )
            receipt["uploaded"] = True
            response = cloudfront.create_invalidation(
                DistributionId=os.environ["DISTRIBUTION_ID"],
                InvalidationBatch={"Paths": {"Quantity": 2, "Items": ["/daily", "/daily/*"]}, "CallerReference": context.aws_request_id},
            )
            receipt["invalidation_id"] = response["Invalidation"]["Id"]
            phase = "verify_https"
            cloudfront.get_waiter("invalidation_completed").wait(
                DistributionId=os.environ["DISTRIBUTION_ID"], Id=receipt["invalidation_id"],
                WaiterConfig={"Delay": 10, "MaxAttempts": 45},
            )
            with urllib.request.urlopen(urllib.request.Request(url, headers={"Cache-Control": "no-cache"}), timeout=30) as response:
                if hashlib.sha256(response.read()).hexdigest() != receipt["sha256"]:
                    raise ValueError("Published HTTPS body hash mismatch")
            receipt.update(status="published", phase="done", finished_at=datetime.now(JST).isoformat())
            _json(s3, bucket, "status/last-published.json", receipt)
            return receipt
    except Exception as exc:
        # Lambda tracebacks otherwise echo third-party exception messages. Only
        # stage and exception class are safe diagnostics; never persist credentials.
        receipt.update(status="failed", phase=phase, error_type=type(exc).__name__)
        raise RuntimeError(f"Daily stock review failed in {phase}: {type(exc).__name__}") from None
    finally:
        receipt.setdefault("phase", phase)
        receipt["finished_at"] = datetime.now(JST).isoformat()
        _json(s3, bucket, "status/latest-run.json", receipt)
        _json(s3, bucket, f"status/runs/{context.aws_request_id}.json", receipt)
        print(json.dumps(receipt, ensure_ascii=False))


def handler(event, context):
    import boto3
    from botocore.config import Config
    config = Config(retries={"max_attempts": 3, "mode": "standard"}, connect_timeout=10, read_timeout=45)
    s3 = boto3.client("s3", config=config)
    bucket = os.environ["DATA_BUCKET"]
    lease = acquire_lock(s3, bucket, context.aws_request_id)
    if not lease:
        result = {"status": "skipped", "reason": "already_running", "request_id": context.aws_request_id}
        print(json.dumps(result))
        return result
    try:
        return process(event or {}, context, s3=s3,
                       secrets=boto3.client("secretsmanager", config=config), cloudfront=boto3.client("cloudfront", config=config))
    finally:
        release_lock(s3, bucket, context.aws_request_id, lease)
