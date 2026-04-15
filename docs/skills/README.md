# Skills Management

## Managed Skill

- Project skill: `.codex/skills/stock-analysis-workflow`
- Project skill: `.codex/skills/kabuplus-daily-fetch`
- Project skill: `.codex/skills/tweet-stock-analysis`
- Project skill: `.codex/skills/x-account-trust-evaluation`

This repository keeps one project skill that explains how to turn user-provided stock analysis principles into durable code and documentation.
This repository also keeps one project skill for listing-first download and import of KABU+ daily stock price CSV files.
This repository also keeps one project skill for turning collected X posts into durable tweet-to-stock reaction rows.
This repository also keeps one project skill for evaluating candidate X accounts against trusted benchmark signal history.

## Source of Truth

- Workflow and trigger definition: `.codex/skills/stock-analysis-workflow/SKILL.md`
- Workflow and trigger definition: `.codex/skills/kabuplus-daily-fetch/SKILL.md`
- Workflow and trigger definition: `.codex/skills/tweet-stock-analysis/SKILL.md`
- Workflow and trigger definition: `.codex/skills/x-account-trust-evaluation/SKILL.md`
- Durable repository-specific guidance: `.codex/skills/stock-analysis-workflow/references/*`
- Daily CSV workflow guidance: `.codex/skills/kabuplus-daily-fetch/references/*`
- Tweet-analysis workflow guidance: `.codex/skills/tweet-stock-analysis/references/*`
- X account trust workflow guidance: `.codex/skills/x-account-trust-evaluation/references/*`
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
