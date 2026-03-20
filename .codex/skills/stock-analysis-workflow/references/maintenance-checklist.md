# Maintenance Checklist

## When Ingestion Changes

- Update `docker-compose.yml` if service startup or runtime arguments change.
- Update `scripts/import_kabuplus.py` if file discovery, metadata extraction, or import semantics change.
- Update `sql/init/*` if new raw tables, helper functions, or curated views are introduced.
- Update `docs/setup.md` and `docs/data-catalog.md`.

## When Analysis Principles Change

- Write the principle in `docs/analysis-principles.md`.
- Keep thresholds, lookbacks, and ranking counts as named parameters.
- Add or update SQL views, SQL query files, or scripts that implement the rule.
- Keep analysis execution reproducible through Docker services.
- Document new curated outputs in `docs/data-catalog.md`.

## Validation

- Run the skill validator after editing the skill.
- Run at least a smoke import with `--limit-files`.
- Verify `analytics.import_status` and one curated view with SQL.
