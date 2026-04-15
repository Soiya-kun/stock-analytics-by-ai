# Setup

## Components

- `db`: PostgreSQL 16. Data is persisted in `./pgdata`.
- `importer`: Python container that mounts the repository and imports ZIP files or a direct CSV file from `./stock`.
- `xcollector`: Python container that monitors configured X accounts and stores posts into PostgreSQL.
- `raw.kabuplus_records`: immutable raw layer stored as `jsonb`.
- `raw.x_posts`: one row per collected X post. Re-fetches update the latest payload and public metrics.
- `ingest.x_*`: monitored-account config, timeline checkpoint state, poll run audit log, and optional usage snapshots.
- `research.tweet_*`: durable tweet-to-stock analysis runs and `tweet x company-code` mention rows.
- `research.x_*`: canonical `post x stock-code` signal rows, review state, and account trust scores.
- `analytics.inferred_price_actions`: inferred split / reverse-split events derived from one-day integer OHLC jumps.
- `analytics.stock_prices_adjusted_daily`: non-destructive adjusted OHLCV view with raw and adjusted columns.
- `analytics.x_bullish_stock_signals`: canonical bullish `post x stock-code` signal view.
- `analytics.x_account_trust_latest`: latest trust score per candidate account.
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

## X Monitoring Flow

1. Add your X credentials to `.env`.

```powershell
X_API_KEY=...
X_API_KEY_SECRET=...
X_ACCESS_TOKEN=...
X_ACCESS_TOKEN_SECRET=...
X_BEARER_TOKEN=...
X_COLLECT_INTERVAL_SECONDS=3600
```

2. If `pgdata` already exists, apply the incremental migration for the X collector objects.

```powershell
docker compose exec -T db psql -U stock -d stock_analytics -f /workspace/sql/migrations/20260407_x_collector_setup.sql
```

If `pgdata` already exists and you want benchmark / candidate trust scoring, also apply:

```powershell
docker compose exec -T db psql -U stock -d stock_analytics -f /workspace/sql/migrations/20260412_x_account_trust_setup.sql
```

3. Insert the fixed monitored usernames.

```powershell
docker compose exec db psql -U stock -d stock_analytics -c "insert into ingest.x_monitored_accounts (target_username) values ('example_user') on conflict do nothing;"
```

Candidate evaluation uses the same table with `account_role='candidate'`.

```powershell
docker compose exec db psql -U stock -d stock_analytics -c "insert into ingest.x_monitored_accounts (target_username, account_role) values ('yuzz__', 'candidate') on conflict (target_username) do update set account_role = excluded.account_role;"
```

4. Resolve usernames to user IDs and verify access through the authenticated X account.

```powershell
docker compose run --rm xcollector sync-targets
```

Candidate only:

```powershell
docker compose run --rm xcollector sync-targets --account-role candidate
```

5. Run a one-shot poll to capture only the current JST day's posts on first load.

```powershell
docker compose run --rm xcollector poll-once
```

6. Start the long-running collector. It polls immediately on startup, then aligns later polls to the next hourly JST boundary.

```powershell
docker compose up -d xcollector
docker compose logs -f xcollector
```

7. Optionally store usage snapshots when `X_BEARER_TOKEN` is configured.

```powershell
docker compose run --rm xcollector usage
```

Collector state lives in:

- `ingest.x_monitored_accounts`: configured usernames and latest resolution/access state
- `ingest.x_monitored_accounts.account_role`: `benchmark` / `candidate`
- `ingest.x_timeline_state`: `since_id` checkpoint and per-target polling status
- `ingest.x_poll_runs`: aggregate audit rows for each polling run
- `ingest.x_usage_daily`: optional usage snapshots from `/2/usage/tweets`
- `analytics.monitored_x_posts`: typed query view for collected posts

## Tweet Stock Analysis Flow

1. If `pgdata` already exists, apply the incremental migration for the tweet-analysis objects.

```powershell
docker compose exec -T db psql -U stock -d stock_analytics -f /workspace/sql/migrations/20260407_tweet_stock_analysis_setup.sql
```

2. Prepare the durable tweet-analysis template for a date range.

```powershell
docker compose run --rm analysis prepare-tweet-analysis --start-date 2026-04-07 --end-date 2026-04-07 --target-username 4th_skywalker
```

3. Open `research/tweet-stock-analysis/<run-id>/analysis_template.yaml`, identify Japanese listed companies and codes for each tweet, and fill `mentions`.

4. Enrich the annotated file with stock price / volume context.

```powershell
docker compose run --rm analysis enrich-tweet-analysis --input-file /workspace/research/tweet-stock-analysis/<run-id>/analysis_template.yaml
```

5. Review `enriched_analysis.yaml`, set `volume_spike_flag`, `price_jump_flag`, and the rationale fields, then persist the result.

```powershell
docker compose run --rm analysis persist-tweet-analysis --input-file /workspace/research/tweet-stock-analysis/<run-id>/enriched_analysis.yaml
```

6. Query the saved rows.

```powershell
docker compose exec db psql -U stock -d stock_analytics -c "select run_id, sc, company_name, volume_spike_flag, price_jump_flag from research.tweet_stock_mentions order by created_at desc limit 20;"
```

## X Account Trust Flow

1. Prepare a review batch for unanalyzed posts.

```powershell
docker compose run --rm analysis prepare-x-signal-analysis --start-date 2026-01-13 --end-date 2026-04-12 --account-role all
```

2. Open `research/x-signal-analysis/<run-id>/analysis_template.yaml` and fill `signals` for each post.

3. Add market context.

```powershell
docker compose run --rm analysis enrich-x-signal-analysis --input-file /workspace/research/x-signal-analysis/<run-id>/analysis_template.yaml
```

4. Persist canonical signals and review state.

```powershell
docker compose run --rm analysis persist-x-signal-analysis --input-file /workspace/research/x-signal-analysis/<run-id>/enriched_analysis.yaml
```

5. Evaluate a candidate account against benchmark clusters.

```powershell
docker compose run --rm analysis evaluate-x-account-trust --candidate-username yuzz__ --start-date 2026-01-13 --end-date 2026-04-12
```

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
- Existing `./pgdata` does not replay `sql/init`; use the dated migration files such as `sql/migrations/20260404_entry_breakout_setup.sql` and `sql/migrations/20260407_x_collector_setup.sql` to add new objects to an already-populated DB.
