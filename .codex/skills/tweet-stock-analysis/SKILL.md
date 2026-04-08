---
name: tweet-stock-analysis
description: Analyze collected X posts from PostgreSQL over a requested date range, identify Japanese listed companies and stock codes mentioned in each tweet, attach tweet URLs, enrich with stock-price and volume context, set LLM-reviewed reaction flags, and persist the final rows into research.tweet_* tables.
---

# Tweet Stock Analysis

## Overview

Use this skill when the user wants tweet data already stored in PostgreSQL to be turned into durable `tweet x listed-company` analysis rows. The workflow is staged so Codex can do the company-code identification and flag judgment, while the repository scripts handle repeatable export, market-context enrichment, and DB persistence.

## Workflow

1. Read [workflow.md](references/workflow.md) before starting.
2. Before analyzing recent dates, run `docker compose run --rm xcollector ensure-current --target-username USERNAME`.
3. Treat the tweet set as current when the target's `last_success_at` is within the last 60 minutes. If it is older or missing, let `ensure-current` fetch only the incremental gap.
4. Prepare a range-scoped analysis template with `docker compose run --rm analysis prepare-tweet-analysis ...`.
5. Work from `research/tweet-stock-analysis/<run-id>/analysis_template.yaml`.
6. For each tweet, identify only Japanese listed companies and stock codes. Leave `mentions: []` when no listed company is relevant.
7. Record `match_confidence` as `high`, `medium`, or `low`, plus `extraction_rationale`.
8. Run `docker compose run --rm analysis enrich-tweet-analysis --input-file ...` to add price and volume context.
9. Review `enriched_analysis.yaml`, set `volume_spike_flag`, `price_jump_flag`, their rationale fields, and `analysis_summary`.
10. Persist with `docker compose run --rm analysis persist-tweet-analysis --input-file ...`.

## Rules

- Use `analytics.listed_companies_latest` as the company-code lookup source unless the user explicitly asks for another universe.
- Keep one persisted row per `tweet x listed-company code`.
- Preserve the original tweet URL and text exactly as exported from the template.
- For current-day or near-real-time analysis, always perform the freshness check first and skip X API calls when the last successful poll is already within the 60-minute budget window.
- Do not force a mapping for ETFs, mutual funds, private companies, or themes when no Japanese listed company is actually mentioned.
- Use the enriched market context as evidence for the final flags, but keep the final flag judgment as an explicit LLM decision with rationale.
- When editing the analysis file, follow `schemas/tweet-stock-analysis.schema.yaml`.

## Repo Touchpoints

- Analysis entrypoint: `scripts/analyze_range_breakout.py`
- Tweet-analysis helper: `scripts/tweet_stock_research.py`
- Source tweets: `analytics.monitored_x_posts`
- Company lookup view: `analytics.listed_companies_latest`
- Output tables: `research.tweet_analysis_runs`, `research.tweet_stock_mentions`

## Validation

Run the skill validator after editing:

```powershell
python C:\Users\djmaa\.codex\skills\.system\skill-creator\scripts\quick_validate.py .codex\skills\tweet-stock-analysis
```
