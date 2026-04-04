# Kabuplus Daily CSV Workflow

## Inputs

- `CSVEX_BASIC_USER`
- `CSVEX_BASIC_PASSWORD`
- Target date as `today`, `YYYY-MM-DD`, or `YYYYMMDD`

## Commands

Fetch a CSV only after confirming the date exists on the daily listing page:

```powershell
python scripts/fetch_kabuplus_daily_csv.py --date today --output-dir stock/kabuplus-2026
```

Import a saved CSV into PostgreSQL:

```powershell
docker compose run --rm importer --csv-file /workspace/stock/kabuplus-2026/japan-all-stock-prices_YYYYMMDD.csv
```

Verify the import:

```powershell
docker compose exec db psql -U stock -d stock_analytics -c "select dataset_key, last_file_date, last_imported_at from analytics.import_status where dataset_key = 'japan-all-stock-prices/daily';"
docker compose exec db psql -U stock -d stock_analytics -c "select count(*) from analytics.stock_prices_daily where trade_date = date 'YYYY-MM-DD';"
```

## Guardrails

- Always check the listing page before downloading any CSV.
- Treat `today` as JST.
- If the target date is not on the listing page, stop without downloading.
- Save files under `stock/kabuplus-2026/`.
- Use `--overwrite` only when replacing an existing local CSV is intentional.
