from __future__ import annotations

import argparse
import base64
import html
import os
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path


LISTING_URL = "https://csvex.com/kabu.plus/csv/japan-all-stock-prices/daily/"
JST = timezone(timedelta(hours=9), name="JST")
FILE_NAME_RE = re.compile(r"^japan-all-stock-prices_(\d{8})\.csv$")
ROW_RE = re.compile(
    r"<tr><td>(?P<name>[^<]+)</td><td>[^<]*</td><td>(?P<updated>[^<]*)</td><td>(?P<size>[^<]*)</td></tr>",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ListingEntry:
    file_name: str
    file_date: date
    updated_at_text: str
    size_bytes: int | None
    download_url: str


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch a KABU+ daily stock prices CSV after checking the listing page first."
    )
    parser.add_argument(
        "--date",
        default="today",
        help="Target date in YYYY-MM-DD or YYYYMMDD format, or 'today' (JST).",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("stock") / "kabuplus-2026",
        help="Directory where the CSV should be saved.",
    )
    parser.add_argument(
        "--listing-url",
        default=LISTING_URL,
        help="Listing page URL to inspect before downloading.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=30,
        help="HTTP timeout in seconds.",
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite the target file if it already exists.",
    )
    return parser.parse_args()


def require_credentials() -> tuple[str, str]:
    user = os.environ.get("CSVEX_BASIC_USER")
    password = os.environ.get("CSVEX_BASIC_PASSWORD")
    if not user or not password:
        raise ValueError(
            "Set CSVEX_BASIC_USER and CSVEX_BASIC_PASSWORD before downloading."
        )
    return user, password


def build_auth_header(user: str, password: str) -> str:
    token = base64.b64encode(f"{user}:{password}".encode("ascii")).decode("ascii")
    return f"Basic {token}"


def http_get(url: str, auth_header: str, timeout: int) -> bytes:
    request = urllib.request.Request(
        url,
        headers={
            "Authorization": auth_header,
            "User-Agent": "stock-analytics-by-ai/1.0",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return response.read()


def parse_target_date(raw_value: str) -> date:
    normalized = raw_value.strip().lower()
    if normalized == "today":
        return datetime.now(JST).date()
    if re.fullmatch(r"\d{8}", normalized):
        return datetime.strptime(normalized, "%Y%m%d").date()
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", normalized):
        return datetime.strptime(normalized, "%Y-%m-%d").date()
    raise ValueError(f"Unsupported --date value: {raw_value}")


def parse_size(raw_value: str) -> int | None:
    cleaned = raw_value.strip()
    if not cleaned or cleaned == "-":
        return None
    return int(cleaned.replace(",", ""))


def parse_listing(html_text: str, listing_url: str) -> dict[date, ListingEntry]:
    entries: dict[date, ListingEntry] = {}
    for match in ROW_RE.finditer(html_text):
        file_name = html.unescape(match.group("name").strip())
        file_match = FILE_NAME_RE.fullmatch(file_name)
        if not file_match:
            continue
        file_date = datetime.strptime(file_match.group(1), "%Y%m%d").date()
        entries[file_date] = ListingEntry(
            file_name=file_name,
            file_date=file_date,
            updated_at_text=html.unescape(match.group("updated").strip()),
            size_bytes=parse_size(html.unescape(match.group("size"))),
            download_url=urllib.parse.urljoin(listing_url, file_name),
        )
    return entries


def save_file(content: bytes, output_path: Path, overwrite: bool) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists() and not overwrite:
        raise FileExistsError(
            f"{output_path} already exists. Pass --overwrite to replace it."
        )
    output_path.write_bytes(content)


def main() -> int:
    args = parse_args()
    target_date = parse_target_date(args.date)
    user, password = require_credentials()
    auth_header = build_auth_header(user, password)

    try:
        listing_bytes = http_get(args.listing_url, auth_header, args.timeout)
    except urllib.error.HTTPError as exc:
        raise SystemExit(f"Failed to fetch listing page: HTTP {exc.code}") from exc
    except urllib.error.URLError as exc:
        raise SystemExit(f"Failed to fetch listing page: {exc.reason}") from exc

    listing_text = listing_bytes.decode("utf-8", errors="replace")
    entries = parse_listing(listing_text, args.listing_url)
    target_entry = entries.get(target_date)
    if target_entry is None:
        print(
            f"No CSV published for {target_date.isoformat()} on the listing page. Nothing downloaded."
        )
        return 0

    output_path = args.output_dir / target_entry.file_name
    try:
        csv_bytes = http_get(target_entry.download_url, auth_header, args.timeout)
    except urllib.error.HTTPError as exc:
        raise SystemExit(
            f"Listing contained {target_entry.file_name}, but download failed with HTTP {exc.code}."
        ) from exc
    except urllib.error.URLError as exc:
        raise SystemExit(
            f"Listing contained {target_entry.file_name}, but download failed: {exc.reason}"
        ) from exc

    save_file(csv_bytes, output_path, args.overwrite)
    print(f"Saved {target_entry.file_name} to {output_path}")
    if target_entry.updated_at_text:
        print(f"Listing updated_at: {target_entry.updated_at_text}")
    if target_entry.size_bytes is not None:
        print(f"Listing size_bytes: {target_entry.size_bytes}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(2) from exc
