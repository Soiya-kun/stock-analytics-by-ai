"""Export one stock's entire daily history to a self-contained interactive HTML."""
from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import math
import os
from pathlib import Path
import re

import psycopg
from psycopg.rows import dict_row

from generate_turnover_chart_viewer import fetch_price_history

ROOT = Path(__file__).resolve().parents[1]
PLOTLY_SHA256 = "122e3be346d66616944d0b83eaaf7242581508c3c1cfa0995a17af0d83eff770"


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sc", default="285A")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--plotly-js", type=Path, default=ROOT / "outputs/stock-history/vendor/plotly-3.1.0.min.js")
    args = parser.parse_args()
    if not re.fullmatch(r"(?:[0-9]{4}|[0-9]{3}[A-Z])", args.sc):
        raise ValueError("Invalid security code")
    library = args.plotly_js.read_bytes()
    if hashlib.sha256(library).hexdigest() != PLOTLY_SHA256:
        raise ValueError("Expected the pinned official Plotly 3.1.0 bundle")
    folder = args.output_dir or ROOT / "outputs/stock-history" / args.sc
    numeric = lambda key: f"util.to_numeric_or_null(payload->>'{key}')"
    sql = "select record_date as day,payload->>'名称' as name," + ",".join(
        numeric(key) + " as " + alias for key, alias in [
            ("始値", "open"), ("高値", "high"), ("安値", "low"), ("株価", "close"), ("出来高", "volume")
        ]
    ) + " from raw.kabuplus_records where dataset_key='japan-all-stock-prices/daily' and security_code=%s order by record_date"
    with psycopg.connect(os.environ["DATABASE_URL"], row_factory=dict_row) as conn:
        conn.execute("set transaction isolation level repeatable read read only")
        raw = conn.execute(sql, (args.sc,)).fetchall()
        if not raw or len({r["day"] for r in raw}) != len(raw):
            raise ValueError("Missing history or duplicate daily records")
        first, last = raw[0]["day"], raw[-1]["day"]
        adjusted = fetch_price_history(conn, stock_codes=[args.sc], target_date=last,
                                       lookback_bars=max(len(raw), (last - first).days + 1))
        if [r["trade_date"] for r in adjusted] != [r["day"] for r in raw]:
            raise ValueError("Adjusted history omits source candles")
        actions = conn.execute("""
            select e.sc,e.action_date,e.price_multiplier,e.evidence_json,
                   case when v.sc is not null then 'verified' else 'inferred' end as source
            from analytics.effective_price_actions e
            left join analytics.verified_price_actions v using(sc,action_date)
            where e.sc=%s and e.action_date<=%s order by e.action_date
        """, (args.sc, last)).fetchall()
    candles = []
    for r, a in zip(raw, adjusted):
        factor = math.prod([x["price_multiplier"] for x in actions if x["action_date"] > r["day"]], start=Decimal(1))
        candle = {"date": r["day"].isoformat(), "raw": {}, "adjusted": {}}
        for k in ("open", "high", "low", "close", "volume"):
            v, av = r[k], a[k if k == "volume" else k + "_price"]
            if v is None or not math.isfinite(float(v)) or v < 0 or (k != "volume" and v == 0):
                raise ValueError(f"Invalid {k} on {r['day']}")
            expected = v / factor if k == "volume" else v * factor
            if av != expected:
                raise ValueError("Raw and adjusted price basis differ")
            candle["raw"][k], candle["adjusted"][k] = float(v), float(av)
        if not r["low"] <= min(r["open"], r["close"]) <= max(r["open"], r["close"]) <= r["high"]:
            raise ValueError("Inconsistent OHLC")
        candles.append(candle)
    if candles[-1]["raw"] != candles[-1]["adjusted"]:
        raise ValueError("Latest candle should be on the current share basis")
    plan = json.loads((ROOT / "config/stock-market-history.json").read_text(encoding="utf8"))
    listing = plan.get("kioxia", {}) if args.sc == "285A" else {}
    if listing and candles[0]["date"] != listing["listing_date"]:
        raise ValueError("History does not start on the verified listing date")
    payload = {
        "sc": args.sc, "name": raw[-1]["name"], "start": first.isoformat(), "end": last.isoformat(),
        "bars": len(candles), "generatedAt": datetime.now(timezone(timedelta(hours=9))).isoformat(),
        "listingSource": listing.get("listing_source"), "candles": candles,
        "actions": [{"date": a["action_date"].isoformat(), "multiplier": float(a["price_multiplier"]),
                     "source": a["source"], "evidence": a["evidence_json"]} for a in actions],
    }
    serialized = json.dumps(payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False).replace("<", "\\u003c")
    template = (ROOT / "site/stock-history/template.html").read_text(encoding="utf8")
    html = template.replace("__HISTORY_DATA__", serialized).replace("__PLOTLY_JS__", library.decode("utf8").replace("</script", "<\\/script"))
    folder.mkdir(parents=True, exist_ok=True)
    (folder / "index.html").write_text(html, encoding="utf8")
    (folder / "prices.json").write_text(json.dumps(payload, ensure_ascii=False, indent=2, allow_nan=False), encoding="utf8")
    manifest = {"sc": args.sc, "first_date": payload["start"], "last_date": payload["end"], "bars": len(candles),
                "raw_preserved": True, "adjustments": payload["actions"], "all_source_candles_included": True,
                "sha256": hashlib.sha256((folder / "index.html").read_bytes()).hexdigest(),
                "plotly_sha256": PLOTLY_SHA256}
    (folder / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf8")
    print(json.dumps({"path": str(folder / "index.html"), "bars": len(candles), "start": payload["start"], "end": payload["end"]}))


if __name__ == "__main__":
    main()
