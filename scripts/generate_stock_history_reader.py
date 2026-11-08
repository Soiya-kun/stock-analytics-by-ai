"""Build a self-contained, scroll-linked stock history reader from reviewed prose."""
from __future__ import annotations

import argparse
from datetime import date
import hashlib
from html import escape
import json
import math
from pathlib import Path
import re

from markdown_it import MarkdownIt
from markdown_it.token import Token

ROOT = Path(__file__).resolve().parents[1]
TEMPLATES = ROOT / "templates/stock-history-reader"


def script_json(value: object) -> str:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).replace(
        "<", "\\u003c").replace("\u2028", "\\u2028").replace("\u2029", "\\u2029")


def validate_prices(source: dict, config: dict) -> list[dict]:
    if source["sc"] != config["sc"]:
        raise ValueError("Security code mismatch")
    candles = source["candles"]
    days = [c["date"] for c in candles]
    if not days or days != sorted(set(days)) or days[-1] != config["price_basis_date"]:
        raise ValueError("Dates must be unique, ascending and end at the configured price basis date")
    if config.get("chart_start_date"):
        date.fromisoformat(config["chart_start_date"])
        candles = [c for c in candles if c["date"] >= config["chart_start_date"]]
        if not candles:
            raise ValueError("No prices on or after chart start date")
    for candle in candles:
        date.fromisoformat(candle["date"])
        for basis in ("raw", "adjusted"):
            values = candle[basis]
            if any(not isinstance(values[k], (int, float)) or not math.isfinite(values[k]) or values[k] <= 0
                   for k in ("open", "high", "low", "close")):
                raise ValueError("Invalid price")
            if not values["low"] <= min(values["open"], values["close"]) <= max(values["open"], values["close"]) <= values["high"]:
                raise ValueError("Inconsistent OHLC")
            if not math.isfinite(values["volume"]) or values["volume"] < 0:
                raise ValueError("Invalid volume")
    # Publish only daily chart values. Do not propagate arbitrary source metadata.
    return [{"date": c["date"], **{basis: {k: c[basis][k] for k in ("open", "high", "low", "close", "volume")}
                                   for basis in ("raw", "adjusted")}} for c in candles]


def format_reader_tables(state) -> None:
    """Keep one semantic table, with visible field labels when rows become cards."""
    result, headers = [], []
    in_table = in_header = False
    column = 0
    for index, token in enumerate(state.tokens):
        if token.type == "table_open":
            in_table, headers = True, []
            wrapper = Token("html_block", "", 0)
            wrapper.content = '<div class="table-wrap">\n'
            result.append(wrapper)
            token.attrSet("role", "table")
        elif in_table and token.type in ("thead_open", "tbody_open"):
            in_header = token.type == "thead_open"
            token.attrSet("role", "rowgroup")
        elif in_table and token.type == "tr_open":
            column = 0
            token.attrSet("role", "row")
        elif in_table and token.type == "th_open":
            token.attrSet("scope", "col")
            token.attrSet("role", "columnheader")
            inline = state.tokens[index + 1]
            headers.append("".join(" " if child.type in ("softbreak", "hardbreak") else child.content
                                   for child in inline.children or []))
        elif in_table and not in_header and token.type == "td_open":
            token.attrSet("role", "cell")
            result.append(token)
            label = Token("html_inline", "", 0)
            label.content = '<span class="cell-label" aria-hidden="true">' + escape(headers[column]) + '</span>'
            result.append(label)
            column += 1
            continue
        result.append(token)
        if token.type == "table_close":
            in_table = False
            wrapper = Token("html_block", "", 0)
            wrapper.content = '</div>\n'
            result.append(wrapper)
    state.tokens = result


def render_reader(article: str, config: dict, prices: dict) -> tuple[str, dict]:
    candles = validate_prices(prices, config)
    parser = MarkdownIt("commonmark", {"html": False, "linkify": False}).enable("table")
    parser.core.ruler.after("inline", "reader_tables", format_reader_tables)
    # Map the section heading explicitly, never financial forecast dates inside paragraphs.
    pieces = re.split(r"(?m)^(#{1,3}) (.+)$", article)
    sections = [{"heading": "相場の流れ", "level": 0, "body": pieces[0],
                 "start": candles[0]["date"], "end": candles[-1]["date"], "kind": "overview"}]
    configured = config["sections"]
    if len(pieces[1:]) // 3 != len(configured):
        raise ValueError("Every heading requires an explicit timeline entry")
    for offset, entry in enumerate(configured):
        level, heading, body = pieces[1 + offset * 3:4 + offset * 3]
        if heading != entry["heading"]:
            raise ValueError(f"Timeline heading mismatch: {heading}")
        start, end = entry["start"], entry["end"]
        date.fromisoformat(start)
        date.fromisoformat(end)
        if start > end or start > candles[-1]["date"] or end < candles[0]["date"] or end > candles[-1]["date"]:
            raise ValueError(f"Timeline date outside available chart: {heading}")
        sections.append({**entry, "level": len(level), "body": body})
    rendered, navigation, segments = [], [], []
    for n, section in enumerate(sections):
        key = f"s-{n}"
        heading = section["heading"]
        label = section["start"].replace("-", "/")
        if section["end"] != section["start"]:
            label += " — " + section["end"].replace("-", "/")
        if section["kind"] == "review":
            label = "振り返り · " + label
        level = 2 if section["level"] <= 1 else 3
        rendered.append(f'<section id="{key}" class="story-section {section["kind"]}" aria-labelledby="{key}-title">'
                        f'<p class="section-date">{escape(label)}</p>'
                        f'<h{level} id="{key}-title">{escape(heading)}</h{level}>'
                        + parser.render(section["body"]) + '</section>')
        navigation.append(f'<a href="#{key}" class="toc-level-{section["level"]}">{escape(heading)}</a>')
        segments.append({"id": key, **{k: section[k] for k in ("heading", "start", "end", "kind")}})
    data = {"sc": config["sc"], "priceBasisDate": config["price_basis_date"], "candles": candles, "segments": segments}
    substitutions = {
        "TITLE": escape(config["title"]), "SUBTITLE": escape(config["subtitle"]),
        "SC": escape(config["sc"]), "NAME": escape(config.get("name", source_name(prices, config))),
        "PRICE_DATE": escape(config["price_basis_date"]), "INFO_DATE": escape(config["information_date"]),
        "ARTICLE": "\n".join(rendered), "TOC": "\n".join(navigation), "DATA": script_json(data),
        "CSS": (TEMPLATES / "reader.css").read_text(encoding="utf8"),
        "JS": (TEMPLATES / "reader.js").read_text(encoding="utf8"),
    }
    html = (TEMPLATES / "reader.html").read_text(encoding="utf8")
    # Single pass: article text is never interpreted as a template directive.
    html = re.sub(r"\{\{([A-Z_]+)\}\}", lambda m: substitutions[m[1]], html)
    manifest = {"schema_version": 1, "sc": config["sc"], "prefix": config["public_path"],
                "price_basis_date": config["price_basis_date"], "information_date": config["information_date"],
                "article_sha256": hashlib.sha256(article.encode()).hexdigest(), "bars": len(candles),
                "sections": len(segments), "files": {"index.html": hashlib.sha256(html.encode()).hexdigest()}}
    return html, manifest


def source_name(prices: dict, config: dict) -> str:
    return prices.get("name", config["title"].removesuffix("の相場史"))


def main() -> None:
    arg = argparse.ArgumentParser(description=__doc__)
    arg.add_argument("--sc", default="285A")
    arg.add_argument("--config", type=Path)
    arg.add_argument("--prices", type=Path)
    arg.add_argument("--output-dir", type=Path)
    args = arg.parse_args()
    if not re.fullmatch(r"(?:[0-9]{4}|[0-9]{3}[A-Z])", args.sc):
        raise ValueError("Invalid security code")
    config_file = args.config or ROOT / f"config/stock-history-reader-{args.sc}.json"
    config = json.loads(config_file.read_text(encoding="utf8"))
    price_file = args.prices or ROOT / "outputs/stock-history" / config["sc"] / "prices.json"
    output_dir = args.output_dir or ROOT / "outputs/stock-history-reader" / config["sc"]
    article = (ROOT / config["article"]).read_text(encoding="utf8")
    html, manifest = render_reader(article, config, json.loads(price_file.read_text(encoding="utf8")))
    output_dir.mkdir(parents=True, exist_ok=True)
    (output_dir / "index.html").write_text(html, encoding="utf8")
    (output_dir / "publish.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf8")
    print(json.dumps({"path": str(output_dir / "index.html"), **manifest}, ensure_ascii=False))


if __name__ == "__main__":
    main()
