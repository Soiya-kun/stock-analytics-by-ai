"""Shared self-contained chart payload, HTML rendering and publication validation."""
from __future__ import annotations
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
from pathlib import Path
import re
from typing import Any

JST = timezone(timedelta(hours=9))


def as_number(value: object) -> float | int | None:
    if value is None:
        return None
    if isinstance(value, Decimal):
        return float(value)
    return value  # type: ignore[return-value]


def build_payload(
    *,
    candidates: list[dict[str, Any]],
    price_rows: list[dict[str, Any]],
    target_date: date,
    min_turnover_million_yen: Decimal,
    lookback_bars: int,
    matched_count: int,
) -> dict[str, Any]:
    prices_by_code: dict[str, list[dict[str, Any]]] = {}
    for row in price_rows:
        prices_by_code.setdefault(row["sc"], []).append(
            {
                "date": row["trade_date"].isoformat(),
                "open": as_number(row["open_price"]),
                "high": as_number(row["high_price"]),
                "low": as_number(row["low_price"]),
                "close": as_number(row["close_price"]),
                "volume": as_number(row["volume"]),
            }
        )

    stocks = []
    for row in candidates:
        stocks.append(
            {
                "sc": row["sc"],
                "name": row["name"] or "名称不明",
                "market": row["market"],
                "industry": row["industry"],
                "close": as_number(row["close_price"]),
                "changePct": as_number(row["day_change_pct"]),
                "turnoverMillionYen": (
                    float(row["turnover_thousand_yen"]) / 1000
                    if row["turnover_thousand_yen"] is not None
                    else None
                ),
                "prices": prices_by_code.get(row["sc"], []),
            }
        )

    return {
        "meta": {
            "title": "売買代金チャート一覧",
            "targetDate": target_date.isoformat(),
            "minTurnoverMillionYen": float(min_turnover_million_yen),
            "lookbackBars": lookback_bars,
            "matchedCount": matched_count,
            "displayedCount": len(stocks),
            "generatedAt": datetime.now(tz=JST).isoformat(),
            "source": "raw.kabuplus_records / analytics.effective_price_actions",
            "adjustmentAsOf": target_date.isoformat(),
        },
        "stocks": stocks,
    }


def write_html(template_path: Path, output_path: Path, payload: dict[str, Any]) -> None:
    template = template_path.read_text(encoding="utf-8")
    if "__VIEWER_DATA__" not in template:
        raise ValueError(f"Template has no __VIEWER_DATA__ placeholder: {template_path}")
    serialized = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    serialized = serialized.replace("<", "\\u003c")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        template.replace("__VIEWER_DATA__", serialized), encoding="utf-8"
    )


def validate_html(path: Path, day: date) -> dict:
    content = path.read_text(encoding="utf-8")
    match = re.search(r'<script id="viewer-data" type="application/json">(.*?)</script>', content, re.S)
    if not match:
        raise ValueError("Generated page has no viewer payload.")
    payload = json.loads(match.group(1))
    stocks = payload["stocks"]
    if payload["meta"]["targetDate"] != day.isoformat() or not stocks:
        raise ValueError("Generated page is empty or has the wrong date.")
    if len({stock["sc"] for stock in stocks}) != len(stocks):
        raise ValueError("Duplicate stock codes in generated page.")
    for stock in stocks:
        if not stock["prices"] or stock["prices"][-1]["date"] != day.isoformat():
            raise ValueError(f"Missing current chart candle for {stock['sc']}.")
        if abs(stock["prices"][-1]["close"] - stock["close"]) > 0.01:
            raise ValueError(f"Chart and current-price basis differ for {stock['sc']}.")
    return {
        "target_date": day.isoformat(), "displayed": len(stocks),
        "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }
