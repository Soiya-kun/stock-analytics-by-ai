"""Local KABU+ import and optional chart preview. Public updates run in Lambda."""
from __future__ import annotations

import argparse
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import uuid
from stock_review_page import validate_html

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "outputs" / "daily-stock-review"
JST = timezone(timedelta(hours=9))


def target_date(value: str) -> date:
    if value == "today":
        return datetime.now(JST).date()
    if re.fullmatch(r"\d{8}", value):
        return datetime.strptime(value, "%Y%m%d").date()
    return date.fromisoformat(value)


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


def command(args: list[str], *, timeout: int = 900) -> str:
    result = subprocess.run(
        args, cwd=ROOT, capture_output=True, text=True, encoding="utf-8",
        errors="replace", timeout=timeout,
        env={**os.environ, "AWS_PAGER": "", "AWS_CLI_AUTO_PROMPT": "off"},
    )
    if result.returncode:
        # Never persist command stderr: third-party exceptions may include secrets.
        raise RuntimeError(f"{args[0]} {args[1]} failed (exit {result.returncode}).")
    return result.stdout.strip()


def docker_python(script: str, *args: str, service: str = "analysis") -> str:
    return command([
        "docker", "compose", "run", "--rm", "-T", "--entrypoint", "python",
        service, f"/workspace/scripts/{script}", *args,
    ])


def check_import(day: date) -> dict:
    # This action runs only inside the existing importer/analysis image.
    import psycopg

    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                select exists (
                    select 1 from ingest.kabuplus_files f
                    where f.dataset_key = 'japan-all-stock-prices/daily'
                      and f.file_date = %s and f.status = 'completed'
                      and f.imported_rows > 0
                      and f.imported_rows = (
                          select count(*) from raw.kabuplus_records r
                          where r.source_zip = f.source_zip
                            and r.source_entry = f.source_entry
                            and r.record_date = %s
                      )
                )
                """, (day, day),
            )
            return {"target_date": day.isoformat(), "imported": cur.fetchone()[0]}


def ensure_imported(day: date, skip_fetch: bool) -> bool:
    def imported() -> bool:
        result = json.loads(docker_python("daily_stock_review.py", "check-import", "--date", day.isoformat()))
        return result["target_date"] == day.isoformat() and result["imported"] is True

    if imported():
        return True
    if skip_fetch:
        raise RuntimeError(f"No verified completed import for {day.isoformat()}.")

    file_name = f"japan-all-stock-prices_{day:%Y%m%d}.csv"
    csv_path = ROOT / "stock" / "kabuplus-2026" / file_name
    if not csv_path.is_file():
        command([
            "docker", "compose", "run", "--rm", "-T",
            "-e", "CSVEX_BASIC_USER", "-e", "CSVEX_BASIC_PASSWORD",
            "--entrypoint", "python", "importer",
            "/workspace/scripts/fetch_kabuplus_daily_csv.py", "--date", day.isoformat(),
            "--output-dir", "/workspace/stock/kabuplus-2026",
        ])
        if not csv_path.is_file():
            return False  # Listing checked, but this date has not been published.

    command([
        "docker", "compose", "run", "--rm", "-T", "importer", "--csv-file",
        f"/workspace/stock/kabuplus-2026/{file_name}",
    ])
    if not imported():
        raise RuntimeError("Import did not produce a completed file with the expected row count/date.")
    return True


@contextmanager
def run_lock():
    OUTPUT.mkdir(parents=True, exist_ok=True)
    lock_path = OUTPUT / "pipeline.lock"
    try:
        descriptor = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        raise RuntimeError("Daily pipeline already running. Check pipeline.lock before retrying.") from None
    try:
        with os.fdopen(descriptor, "w") as stream:
            stream.write(str(os.getpid()))
        yield
    finally:
        lock_path.unlink()


def run(args: argparse.Namespace) -> int:
    day = target_date(args.date)
    run_id = datetime.now(JST).strftime("%Y%m%dT%H%M%S") + "-" + uuid.uuid4().hex[:8]
    run_dir = OUTPUT / "runs" / run_id
    run_dir.mkdir(parents=True)
    receipt = {"run_id": run_id, "target_date": day.isoformat(), "status": "running"}
    with run_lock():
        try:
            receipt["phase"] = "import"
            if not ensure_imported(day, args.skip_fetch):
                receipt.update(status="skipped", reason="csv_not_published")
                return 0
            if args.action == "import":
                receipt.update(status="imported", phase="done")
                return 0
            receipt["phase"] = "generate"
            html_path = run_dir / "index.html"
            container_path = "/workspace/" + html_path.relative_to(ROOT).as_posix()
            docker_python(
                "generate_turnover_chart_viewer.py", "--date", day.isoformat(),
                "--min-turnover-million-yen", str(args.min_turnover_million_yen),
                "--lookback-bars", str(args.lookback_bars), "--limit", str(args.limit),
                "--output", container_path,
            )
            receipt.update(validate_html(html_path, day))
            receipt["html"] = str(html_path)
            receipt["status"] = "generated"
            receipt["phase"] = "done"
            return 0
        except Exception as exc:
            receipt.update(status="failed", error=str(exc))
            return 1
        finally:
            receipt["finished_at"] = datetime.now(JST).isoformat()
            write_json(run_dir / "result.json", receipt)
            write_json(OUTPUT / "latest-run.json", receipt)
            print(json.dumps(receipt, ensure_ascii=False))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["run", "import", "check-import"], nargs="?", default="run")
    parser.add_argument("--date", default="today", help="today (JST), YYYY-MM-DD or YYYYMMDD; never falls back.")
    parser.add_argument("--skip-fetch", action="store_true", help="Require this date to be imported already.")
    parser.add_argument("--min-turnover-million-yen", type=int, default=3000)
    parser.add_argument("--lookback-bars", type=int, default=250)
    parser.add_argument("--limit", type=int, default=300)
    args = parser.parse_args()
    if args.action == "check-import":
        print(json.dumps(check_import(target_date(args.date))))
        return 0
    return run(args)


if __name__ == "__main__":
    raise SystemExit(main())
