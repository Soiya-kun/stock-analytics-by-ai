# Setup

## Components

- `db`: PostgreSQL 16. Data is persisted in `./pgdata`.
- `importer`: Python container that mounts the repository and imports ZIP files from `./stock`.
- `raw.kabuplus_records`: immutable raw layer stored as `jsonb`.
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

## Useful Import Options

- `--dataset japan-all-stock-prices/daily`: import only one dataset family.
- `--limit-zips 1`: scan only one yearly ZIP file.
- `--limit-files 20`: import only the first 20 CSV files.
- `--force`: delete and re-import files even if they were already completed.

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
