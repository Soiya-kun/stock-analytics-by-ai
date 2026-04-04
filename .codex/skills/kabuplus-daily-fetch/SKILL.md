---
name: kabuplus-daily-fetch
description: Fetch and import KABU+ daily stock price CSV files for this repository. Use when Codex needs to check the japan-all-stock-prices daily listing page first, determine whether a target date file exists, save the CSV under stock/kabuplus-2026, and load it into the Docker PostgreSQL database without guessing direct CSV URLs.
---

# Kabuplus Daily Fetch

## Overview

Use this skill to fetch `japan-all-stock-prices/daily` CSV files through the repository's guarded workflow. The workflow always checks the directory listing first, saves confirmed files under `stock/kabuplus-2026`, and then imports them through the existing Docker importer.

## Workflow

1. Check the current time in JST if the request is about `today`.
2. Read [workflow.md](references/workflow.md) for the command sequence and guardrails.
3. Require `CSVEX_BASIC_USER` and `CSVEX_BASIC_PASSWORD` from the environment. Do not store credentials in the repo or in the skill.
4. Run `python scripts/fetch_kabuplus_daily_csv.py --date ... --output-dir stock/kabuplus-2026`.
5. If the listing page does not contain the requested date, stop. Do not fall back to the previous business day.
6. If the CSV is saved, run `docker compose run --rm importer --csv-file /workspace/stock/kabuplus-2026/<file-name>.csv`.
7. Verify the import with `analytics.import_status` or a date-filtered `analytics.stock_prices_daily` query.

## Rules

- Always inspect `https://csvex.com/kabu.plus/csv/japan-all-stock-prices/daily/` before any CSV download.
- Treat `today` as `Asia/Tokyo`.
- Use `stock/kabuplus-2026` as the download destination.
- Use importer CSV mode for direct CSV files instead of inventing a one-off loader.
- Preserve idempotency by keeping the saved path stable and letting the importer reuse `source_zip = 'kabuplus-daily-csv'`.

## Repo Touchpoints

- Downloader script: `scripts/fetch_kabuplus_daily_csv.py`
- Importer: `scripts/import_kabuplus.py`
- Import destination: `stock/kabuplus-2026/`
- Verification view: `analytics.import_status`

## Validation

Run the skill validator after editing:

```powershell
python C:\Users\djmaa\.codex\skills\.system\skill-creator\scripts\quick_validate.py .codex\skills\kabuplus-daily-fetch
```
