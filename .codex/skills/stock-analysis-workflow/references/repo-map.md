# Repo Map

## Data Stack

- `docker-compose.yml`: PostgreSQL and importer services.
- `sql/init/00_schemas.sql`: schema creation.
- `sql/init/01_tables.sql`: raw and ingest tables.
- `sql/init/02_helpers.sql`: casting helpers for Japanese CSV values.
- `sql/init/10_views.sql`: curated SQL views.
- `scripts/import_kabuplus.py`: ZIP-to-PostgreSQL importer.

## Documentation

- `README.md`: entry point and quick start.
- `docs/setup.md`: operational setup and reset.
- `docs/data-catalog.md`: dataset inventory and curated views.
- `docs/analysis-principles.md`: long-lived analysis principles and parameter defaults.
- `docs/skills/README.md`: skill governance for this repository.
