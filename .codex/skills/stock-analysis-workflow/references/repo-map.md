# Repo Map

## Data Stack

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
