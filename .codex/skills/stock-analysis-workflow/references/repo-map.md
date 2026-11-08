# Repo Map

- `scripts/daily_stock_cloud.py`: independent daily Lambda: listing-first KABU+ acquisition, private S3 SQLite history, raw CSV archive, shared chart generation, single-object public upload, CloudFront invalidation and HTTPS SHA-256 verification. Execution-role auth, no PC/PostgreSQL/SSO/X/Jev. Weekdays16:40/17:10/18:10 JST via `infra/static-site/bin/daily-review.ts` / `lib/daily-review-stack.ts`. `export_daily_stock_seed.py` creates a read-only PostgreSQL seed; `deploy_daily_stock_cloud.py` deploys disabled, registers credentials privately, seeds, verifies live, then enables. `stock_review_page.py` shares payload/HTML/validation with local generation.
- `scripts/daily_stock_review.py`: local import/preview only. Existing weekday16:40 JST `kabuplus-daily-import` calls `import --date today` to keep Docker PostgreSQL ingestion. No AWS auth/publication. Default run also generates a local chart; `--skip-fetch` requires the exact date to exist. `generate_turnover_chart_viewer.py` retains the local verified-precedence action source through the display date.

- `config/stock-market-history.json`: user-ordered stock history writing queue (13 stocks), long/mid/short perspectives, Kioxia recollections, and the writing policy. Before writing or editing a market-history document, read its `writing_policy` and `time_reference_policy` and apply `natural-japanese`. State the information publication/as-of date, target calendar period, comparison period and actual/forecast status together. Omit collection counts, implementation details and authoring plans from the narrative; preserve source limits that affect interpretation. `scripts/generate_stock_history.py` exports all daily candles for one code, reusing the verified-precedence adjustment reader with no bar truncation. `site/stock-history/template.html` embeds pinned Plotly and raw/adjusted OHLCV into an offline HTML under `outputs/stock-history/<SC>/`. Separate from paused momentum experiments; no X/Jev calls.

## Data Stack

- `docs/stock-history-reader.md`: per-stock Page and scroll-linked mobile reader workflow. `scripts/generate_stock_history_reader.py --sc 285A|4062` uses each stock's explicit section/date mapping and raw/adjusted prices. `deploy-report.ps1 -HistoryReader -SecurityCode 285A|4062` publishes only its single HTML under the allowlisted `market-history/kioxia/` or `market-history/ibiden/` prefix and verifies HTTPS SHA-256.

- `docker-compose.yml`: PostgreSQL and importer services.
- `sql/init/00_schemas.sql`: schema creation.
- `sql/init/01_tables.sql`: raw and ingest tables.
- `sql/init/03_x_tables.sql`: monitored-account config, checkpoint, audit, and raw X storage tables.
- `sql/init/04_tweet_analysis_tables.sql`: durable tweet-analysis run and mention tables.
- `sql/init/02_helpers.sql`: casting helpers for Japanese CSV values.
- `sql/init/10_views.sql`: curated SQL views.
- `sql/init/11_x_views.sql`: curated X monitoring views.
- `sql/init/12_tweet_analysis_views.sql`: company lookup views for tweet analysis.
- `scripts/import_kabuplus.py`: ZIP-to-PostgreSQL importer.
- `scripts/x_collector.py`: X API v2 collector for fixed monitored accounts.
- `scripts/tweet_stock_research.py`: tweet-range export, market-context enrichment, and persistence helpers.

## Documentation

- `README.md`: entry point and quick start.
- `docs/setup.md`: operational setup and reset.
- `docs/data-catalog.md`: dataset inventory and curated views.
- `docs/analysis-principles.md`: long-lived analysis principles and parameter defaults.
- `docs/skills/README.md`: skill governance for this repository.
