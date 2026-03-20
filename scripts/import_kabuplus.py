from __future__ import annotations

import argparse
import csv
import io
import logging
import os
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path, PurePosixPath
from typing import Iterable
from zipfile import ZipFile, ZipInfo

import psycopg
from psycopg.types.json import Jsonb


DATE_PATTERNS = (
    re.compile(r"(\d{8})(?=\.csv$)"),
    re.compile(r"(\d{6})(?=\.csv$)"),
)
DATE_COLUMNS = ("日付", "分割併合日")


@dataclass(frozen=True)
class DatasetSpec:
    dataset_name: str
    frequency: str
    dataset_key: str
    file_name: str
    file_date: date | None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Import Kabuplus ZIP files into PostgreSQL."
    )
    parser.add_argument(
        "--stock-dir",
        type=Path,
        default=Path("stock"),
        help="Directory containing kabuplus-*.zip files.",
    )
    parser.add_argument(
        "--zip-pattern",
        default="kabuplus-*.zip",
        help="Glob pattern used to find ZIP files.",
    )
    parser.add_argument(
        "--dsn",
        default=None,
        help="PostgreSQL DSN. Defaults to DATABASE_URL.",
    )
    parser.add_argument(
        "--dataset",
        action="append",
        default=[],
        help="Limit import to dataset keys such as japan-all-stock-prices/daily.",
    )
    parser.add_argument(
        "--limit-files",
        type=int,
        default=None,
        help="Stop after importing N CSV files.",
    )
    parser.add_argument(
        "--limit-zips",
        type=int,
        default=None,
        help="Stop after scanning N ZIP files.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-import files even if they were already completed.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity.",
    )
    return parser.parse_args()


def configure_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level),
        format="%(asctime)s %(levelname)s %(message)s",
    )


def discover_zip_files(stock_dir: Path, pattern: str) -> list[Path]:
    if not stock_dir.exists():
        raise FileNotFoundError(f"Stock directory does not exist: {stock_dir}")
    zip_files = sorted(stock_dir.glob(pattern))
    if not zip_files:
        raise FileNotFoundError(
            f"No ZIP files matched pattern '{pattern}' in {stock_dir}"
        )
    return zip_files


def iter_csv_entries(zip_file: ZipFile) -> Iterable[ZipInfo]:
    entries = (
        entry
        for entry in zip_file.infolist()
        if not entry.is_dir() and entry.filename.lower().endswith(".csv")
    )
    return sorted(entries, key=lambda entry: entry.filename)


def extract_dataset_spec(entry_name: str) -> DatasetSpec:
    path = PurePosixPath(entry_name)
    if len(path.parts) < 4:
        raise ValueError(f"Unexpected CSV path layout: {entry_name}")

    dataset_name = path.parts[1]
    frequency = path.parts[2]
    dataset_key = f"{dataset_name}/{frequency}"
    file_name = path.name

    return DatasetSpec(
        dataset_name=dataset_name,
        frequency=frequency,
        dataset_key=dataset_key,
        file_name=file_name,
        file_date=parse_file_date(file_name),
    )


def parse_file_date(file_name: str) -> date | None:
    for pattern in DATE_PATTERNS:
        match = pattern.search(file_name)
        if not match:
            continue
        return parse_compact_date(match.group(1))
    return None


def parse_compact_date(value: str | None) -> date | None:
    if not value:
        return None
    cleaned = value.strip()
    if not cleaned or cleaned == "-":
        return None
    if re.fullmatch(r"\d{8}", cleaned):
        return date(
            int(cleaned[0:4]),
            int(cleaned[4:6]),
            int(cleaned[6:8]),
        )
    if re.fullmatch(r"\d{6}", cleaned):
        return date(
            int(cleaned[0:4]),
            int(cleaned[4:6]),
            1,
        )
    return None


def normalize_cell(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.lstrip("\ufeff").strip()
    return normalized or None


def normalize_row(row: dict[str, str | None]) -> dict[str, str | None]:
    normalized: dict[str, str | None] = {}
    for raw_key, raw_value in row.items():
        key = normalize_cell(raw_key)
        if not key:
            continue
        normalized[key] = normalize_cell(raw_value)
    return normalized


def extract_security_code(payload: dict[str, str | None]) -> str | None:
    return payload.get("SC")


def extract_record_date(
    payload: dict[str, str | None], fallback: date | None
) -> date | None:
    for column in DATE_COLUMNS:
        parsed = parse_compact_date(payload.get(column))
        if parsed:
            return parsed
    return fallback


def should_import(
    conn: psycopg.Connection, source_zip: str, source_entry: str, force: bool
) -> bool:
    if force:
        return True

    with conn.cursor() as cur:
        cur.execute(
            """
            select status
            from ingest.kabuplus_files
            where source_zip = %s and source_entry = %s
            """,
            (source_zip, source_entry),
        )
        row = cur.fetchone()

    return row is None or row[0] != "completed"


def mark_failed(
    conn: psycopg.Connection,
    source_zip: str,
    source_entry: str,
    spec: DatasetSpec,
    zip_size: int,
    zip_crc: int,
    message: str,
) -> None:
    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into ingest.kabuplus_files (
                    source_zip,
                    source_entry,
                    dataset_key,
                    dataset_name,
                    frequency,
                    source_file_name,
                    file_date,
                    file_size,
                    zip_crc,
                    status,
                    imported_rows,
                    imported_at,
                    last_error,
                    created_at,
                    updated_at
                )
                values (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    'failed', null, now(), %s, now(), now()
                )
                on conflict (source_zip, source_entry) do update
                set dataset_key = excluded.dataset_key,
                    dataset_name = excluded.dataset_name,
                    frequency = excluded.frequency,
                    source_file_name = excluded.source_file_name,
                    file_date = excluded.file_date,
                    file_size = excluded.file_size,
                    zip_crc = excluded.zip_crc,
                    status = 'failed',
                    imported_rows = null,
                    imported_at = now(),
                    last_error = excluded.last_error,
                    updated_at = now()
                """,
                (
                    source_zip,
                    source_entry,
                    spec.dataset_key,
                    spec.dataset_name,
                    spec.frequency,
                    spec.file_name,
                    spec.file_date,
                    zip_size,
                    zip_crc,
                    message[:2000],
                ),
            )


def import_entry(
    conn: psycopg.Connection,
    zip_path: Path,
    zip_file: ZipFile,
    entry: ZipInfo,
    spec: DatasetSpec,
) -> int:
    imported_rows = 0

    with conn.transaction():
        with conn.cursor() as cur:
            cur.execute(
                """
                delete from raw.kabuplus_records
                where source_zip = %s and source_entry = %s
                """,
                (zip_path.name, entry.filename),
            )
            cur.execute(
                """
                insert into ingest.kabuplus_files (
                    source_zip,
                    source_entry,
                    dataset_key,
                    dataset_name,
                    frequency,
                    source_file_name,
                    file_date,
                    file_size,
                    zip_crc,
                    status,
                    imported_rows,
                    imported_at,
                    last_error,
                    created_at,
                    updated_at
                )
                values (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    'running', null, null, null, now(), now()
                )
                on conflict (source_zip, source_entry) do update
                set dataset_key = excluded.dataset_key,
                    dataset_name = excluded.dataset_name,
                    frequency = excluded.frequency,
                    source_file_name = excluded.source_file_name,
                    file_date = excluded.file_date,
                    file_size = excluded.file_size,
                    zip_crc = excluded.zip_crc,
                    status = 'running',
                    imported_rows = null,
                    imported_at = null,
                    last_error = null,
                    updated_at = now()
                """,
                (
                    zip_path.name,
                    entry.filename,
                    spec.dataset_key,
                    spec.dataset_name,
                    spec.frequency,
                    spec.file_name,
                    spec.file_date,
                    entry.file_size,
                    entry.CRC,
                ),
            )

            with zip_file.open(entry, "r") as raw_stream:
                text_stream = io.TextIOWrapper(raw_stream, encoding="cp932", newline="")
                reader = csv.DictReader(text_stream)

                if not reader.fieldnames:
                    raise ValueError(f"CSV header not found: {entry.filename}")

                with cur.copy(
                    """
                    copy raw.kabuplus_records (
                        dataset_key,
                        dataset_name,
                        frequency,
                        source_zip,
                        source_entry,
                        source_file_name,
                        file_date,
                        record_date,
                        security_code,
                        row_number,
                        payload
                    )
                    from stdin
                    """
                ) as copy:
                    for row_number, row in enumerate(reader, start=1):
                        payload = normalize_row(row)
                        copy.write_row(
                            (
                                spec.dataset_key,
                                spec.dataset_name,
                                spec.frequency,
                                zip_path.name,
                                entry.filename,
                                spec.file_name,
                                spec.file_date,
                                extract_record_date(payload, spec.file_date),
                                extract_security_code(payload),
                                row_number,
                                Jsonb(payload),
                            )
                        )
                        imported_rows += 1

            cur.execute(
                """
                update ingest.kabuplus_files
                set status = 'completed',
                    imported_rows = %s,
                    imported_at = now(),
                    last_error = null,
                    updated_at = now()
                where source_zip = %s and source_entry = %s
                """,
                (imported_rows, zip_path.name, entry.filename),
            )

    return imported_rows


def main() -> int:
    args = parse_args()
    configure_logging(args.log_level)

    dsn = args.dsn or os.environ.get("DATABASE_URL")
    if not dsn:
        raise ValueError("Pass --dsn or set DATABASE_URL.")

    zip_files = discover_zip_files(args.stock_dir, args.zip_pattern)
    if args.limit_zips is not None:
        zip_files = zip_files[: args.limit_zips]

    dataset_filters = set(args.dataset)
    processed_files = 0
    imported_rows = 0

    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("set timezone to 'Asia/Tokyo'")

        for zip_path in zip_files:
            logging.info("Scanning %s", zip_path.name)
            with ZipFile(zip_path) as zip_file:
                for entry in iter_csv_entries(zip_file):
                    spec = extract_dataset_spec(entry.filename)

                    if dataset_filters and spec.dataset_key not in dataset_filters:
                        continue
                    if args.limit_files is not None and processed_files >= args.limit_files:
                        logging.info("Reached --limit-files=%s", args.limit_files)
                        return 0
                    if not should_import(conn, zip_path.name, entry.filename, args.force):
                        logging.debug(
                            "Skipping already imported file %s from %s",
                            entry.filename,
                            zip_path.name,
                        )
                        continue

                    logging.info(
                        "Importing %s from %s into %s",
                        entry.filename,
                        zip_path.name,
                        spec.dataset_key,
                    )

                    try:
                        row_count = import_entry(conn, zip_path, zip_file, entry, spec)
                    except Exception as exc:
                        mark_failed(
                            conn,
                            zip_path.name,
                            entry.filename,
                            spec,
                            entry.file_size,
                            entry.CRC,
                            str(exc),
                        )
                        raise

                    processed_files += 1
                    imported_rows += row_count
                    logging.info("Completed %s (%s rows)", entry.filename, row_count)

    logging.info(
        "Import finished: %s files, %s rows",
        processed_files,
        imported_rows,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
