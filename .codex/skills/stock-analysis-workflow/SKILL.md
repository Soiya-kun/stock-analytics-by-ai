---
name: stock-analysis-workflow
description: Repository workflow for stock-analytics-by-ai. Use when Codex is asked to ingest Kabuplus ZIP files or direct daily CSV files into PostgreSQL, maintain docker compose or SQL views for this repo, or turn user-provided stock analysis principles into parameterized queries, scripts, and documentation that should persist across future sessions.
---

# Stock Analysis Workflow

## Overview

Use this skill to keep the repository in a stable shape while stock analysis rules evolve. The repo is organized so that raw Kabuplus ingestion stays generic, fixed X monitoring state lives in `ingest.x_*` / `raw.x_*`, durable tweet/company analysis and other research outputs live in `research.*`, typed SQL access lives in `analytics.*`, and user-provided analysis principles can be added later without reworking the base pipeline.

## Workflow

1. Read `docs/analysis-principles.md` before implementing new screening logic, ranking logic, or parameter changes.
2. Treat `raw.kabuplus_records` as the immutable raw layer. Do not push strategy-specific logic into ingestion.
3. Keep raw prices immutable. If split or reverse-split handling is needed, persist inferred events separately and apply them in an adjusted view.
4. Put reusable typed access in `sql/init/10_views.sql` or new SQL files under `sql/`.
5. Keep strategy parameters explicit and mirror them in `docs/analysis-principles.md`.
6. When the repository workflow changes, update this skill and the references listed below.
6. Use the separate `kabuplus-daily-fetch` project skill for listing-first daily CSV download flow instead of hand-crafting direct CSV URLs.

## Repository Rules

- Use `scripts/import_kabuplus.py` for data loading instead of ad hoc import scripts.
- Use `docker compose run --rm analysis ...` for reusable analysis runs instead of local-only one-off commands.
- Before tweet analysis on recent dates, use `docker compose run --rm xcollector ensure-current ...` so X API calls only happen when the 60-minute freshness window has expired.
- Keep SQL-queryable entities split into `ingest`, `raw`, and `analytics` layers.
- Persist durable case datasets and hypothesis outputs into `research.*` tables and matching md/yaml/csv artifacts.
- Update `docs/data-catalog.md` whenever a new curated view is added.
- Update `README.md` and `docs/setup.md` when startup or import commands change.
- Validate the skill after editing it.

```powershell
python C:\Users\djmaa\.codex\skills\.system\skill-creator\scripts\quick_validate.py .codex\skills\stock-analysis-workflow
```

## References

- For change discipline and update targets, read `references/maintenance-checklist.md`.
- For repository layout and preferred extension points, read `references/repo-map.md`.
