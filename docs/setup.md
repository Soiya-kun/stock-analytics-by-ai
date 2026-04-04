# Setup

## Components

- `db`: PostgreSQL 16. Data is persisted in `./pgdata`.
- `importer`: Python container that mounts the repository and imports ZIP files or a direct CSV file from `./stock`.
- `raw.kabuplus_records`: immutable raw layer stored as `jsonb`.
- `analytics.inferred_price_actions`: inferred split / reverse-split events derived from one-day integer OHLC jumps.
- `analytics.stock_prices_adjusted_daily`: non-destructive adjusted OHLCV view with raw and adjusted columns.
- `research.*`: durable breakout study runs, cases, and hypothesis records.
- `analytics.*`: typed SQL views for the main daily, weekly, and monthly datasets.

## Boot

1. Copy `.env.example` to `.env` if you want to change the default credentials.
2. Start PostgreSQL.

```powershell
docker compose up -d db
```

3. Run a smoke import on a small sample.

```powershell
docker compose run --rm importer --limit-files 5
```

4. Run the full import.

```powershell
docker compose run --rm importer
```

The full import is long-running because the yearly ZIP files contain many CSV files. Use `--limit-files` or `--dataset` first, then run the full load when you are ready.

## Daily CSV Flow

Use the listing-first downloader when you want the current KABU+ daily prices CSV.

```powershell
$env:CSVEX_BASIC_USER = "your-user"
$env:CSVEX_BASIC_PASSWORD = "your-password"
python scripts/fetch_kabuplus_daily_csv.py --date today --output-dir stock/kabuplus-2026
```

The downloader always checks `https://csvex.com/kabu.plus/csv/japan-all-stock-prices/daily/` before downloading. If the target date is not listed yet, it exits without writing a file.

After a CSV is saved, import it with:

```powershell
docker compose run --rm importer --csv-file /workspace/stock/kabuplus-2026/japan-all-stock-prices_YYYYMMDD.csv
```

The CSV mode stores `source_zip = 'kabuplus-daily-csv'` and keeps `source_entry` as the repository-relative CSV path so reruns stay idempotent.

To run the full import in the background:

```powershell
docker compose up -d importer
docker compose logs -f importer
```

When the import service exits, the load is finished. Progress can also be checked with:

```powershell
docker compose exec db psql -U stock -d stock_analytics -c "select * from analytics.import_status order by dataset_key;"
```

## Query

Open `psql` inside the database container:

```powershell
docker compose exec db psql -U stock -d stock_analytics
```

Examples:

```sql
select * from analytics.import_status order by dataset_key;
select * from analytics.stock_prices_daily where sc = '1301' order by trade_date desc limit 5;
```

Sample SQL is stored in `sql/queries/sample_queries.sql`.

## Analysis

Run the multi-year range-breakout grid search inside Docker:

```powershell
docker compose run --rm analysis grid-search --processes 4
```

Scan the latest market day for current candidates:

```powershell
docker compose run --rm analysis scan --lookback-years 3 --max-range-width-pct 0.50 --breakout-buffer-pct 0.01 --min-volume-ratio 1.50
```

Run the durable 2018+ breakout labeling study:

```powershell
docker compose run --rm analysis label-study
```

`grid-search` and `scan` write under `outputs/range-breakout/`. `label-study` writes durable artifacts under `research/range-breakout-2018-study/`.

Run the non-destructive split-adjustment and 6-month breakout research pipeline inside Docker:

```powershell
docker compose run --rm analysis infer-price-actions
docker compose run --rm analysis prepare-adjusted-prices
docker compose run --rm analysis build-entry-dataset
docker compose run --rm analysis mine-entry-hypotheses
docker compose run --rm analysis evaluate-entry-hypotheses
```

- `infer-price-actions`: writes inferred split / reverse-split events into `analytics.inferred_price_actions`.
- `prepare-adjusted-prices`: audits the adjusted-price layer and writes md/yaml under `research/inferred-price-actions/`.
- `build-entry-dataset`: writes durable md/yaml/csv under `research/entry-breakout-6m/` and persists rows into `research.entry_study_runs` / `research.entry_cases`.
- `mine-entry-hypotheses`: mines interpretable threshold rules from the train split and stores them in `research.entry_hypotheses`.
- `evaluate-entry-hypotheses`: applies the mined rules to the validation split and writes durable evaluation artifacts.

When `pgdata` already exists, apply the incremental migration before the first run of the new pipeline:

```powershell
docker compose exec -T db psql -U stock -d stock_analytics -f /workspace/sql/migrations/20260404_entry_breakout_setup.sql
```

## Useful Import Options

- `--dataset japan-all-stock-prices/daily`: import only one dataset family.
- `--limit-zips 1`: scan only one yearly ZIP file.
- `--limit-files 20`: import only the first 20 CSV files.
- `--force`: delete and re-import files even if they were already completed.
- `--csv-file /workspace/stock/kabuplus-2026/japan-all-stock-prices_YYYYMMDD.csv`: import one CSV directly.
- `--csv-dataset japan-all-stock-prices/daily`: dataset key used with `--csv-file`.
- `--csv-source kabuplus-daily-csv`: synthetic source label used with `--csv-file`.

Example:

```powershell
docker compose run --rm importer --dataset japan-all-stock-prices/daily --limit-zips 1 --force
```

## Reset

To rebuild the database from scratch, stop containers and remove `./pgdata`.

```powershell
docker compose down
Remove-Item -Recurse -Force pgdata
docker compose up -d db
docker compose run --rm importer
```

## Notes

- ZIP files are read directly. There is no unzip step on disk.
- CSVs are decoded as `cp932`.
- Initial SQL in `sql/init` is applied when PostgreSQL starts with an empty `./pgdata`.
- Existing `./pgdata` does not replay `sql/init`; use `sql/migrations/20260404_entry_breakout_setup.sql` to add the research objects to an already-populated DB.
