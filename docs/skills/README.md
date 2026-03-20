# Skills Management

## Managed Skill

- Project skill: `.codex/skills/stock-analysis-workflow`

This repository keeps one project skill that explains how to turn user-provided stock analysis principles into durable code and documentation.

## Source of Truth

- Workflow and trigger definition: `.codex/skills/stock-analysis-workflow/SKILL.md`
- Durable repository-specific guidance: `.codex/skills/stock-analysis-workflow/references/*`
- Principle definitions: `docs/analysis-principles.md`
- Data layout and curated SQL views: `docs/data-catalog.md`

## Update Flow

1. Add or update the principle in `docs/analysis-principles.md`.
2. Implement or adjust SQL views, scripts, or parameters.
3. Reflect any workflow change in `.codex/skills/stock-analysis-workflow/SKILL.md` or its `references`.
4. Validate the skill.

```powershell
python C:\Users\djmaa\.codex\skills\.system\skill-creator\scripts\quick_validate.py .codex\skills\stock-analysis-workflow
```

## Guardrails

- Do not bury decision rules only in chat history.
- Do not add analysis logic directly into raw ingestion.
- Keep raw, typed, and strategy layers separate so parameter tuning stays cheap.
- When a curated view is added, document it in `docs/data-catalog.md`.
- Run analysis scripts via `docker compose run --rm analysis ...` so the execution path stays reproducible.
