---
name: tweet-stock-analysis
description: Analyze collected X posts from PostgreSQL over a requested date range, identify Japanese listed companies and stock codes mentioned in each tweet, attach tweet URLs, enrich with stock-price and volume context, set LLM-reviewed reaction flags, and persist the final rows into research.tweet_* tables.
---

# Tweet Stock Analysis

## Overview

Use this skill when the user wants tweet data already stored in PostgreSQL to be turned into durable `tweet x listed-company` analysis rows. The workflow is staged so Codex can do the company-code identification and flag judgment, while the repository scripts handle repeatable export, market-context enrichment, and DB persistence.

This skill should also treat the same company being mentioned by multiple monitored users in the same analysis window as a higher-priority signal than ordinary single-user chatter.

## Workflow

1. Read [workflow.md](references/workflow.md) before starting.
2. Before starting containers, check whether the repository already has a running PostgreSQL container with the expected data. Prefer reusing that DB instead of recreating it.
3. If `stock-analytics-db` is already running and mounted to the intended checkout, use it as-is. Do not run `docker compose up -d db` from another checkout just to "be safe", because that can point you at a different `pgdata` directory and hide the real monitored accounts / tweet history.
4. Before analyzing recent dates, run `docker compose run --rm xcollector ensure-current --target-username USERNAME`.
5. Treat the tweet set as current when the target's `last_success_at` is within the last 60 minutes. If it is older or missing, let `ensure-current` fetch only the incremental gap.
6. Prepare a range-scoped analysis template with `docker compose run --rm analysis prepare-tweet-analysis ...`.
7. Work from `research/tweet-stock-analysis/<run-id>/analysis_template.yaml`.
8. For each tweet, identify only Japanese listed companies and stock codes. Leave `mentions: []` when no listed company is relevant.
9. Record `match_confidence` as `high`, `medium`, or `low`, plus `extraction_rationale`.
10. Run `docker compose run --rm analysis enrich-tweet-analysis --input-file ...` to add price and volume context.
11. Review `enriched_analysis.yaml`, set `volume_spike_flag`, `price_jump_flag`, their rationale fields, and `analysis_summary`.
12. If the requested post date is a weekend, holiday, or otherwise lacks a same-day/future trade row, cite the most recent available trade date in `market_context` instead of leaving the market evidence blank.
13. After persistence, aggregate the range by `sc` and count `distinct target_username` so that cross-user confirmation is visible.
14. If 2 or more monitored users independently mention the same listed company in the range, raise that company in the final report even when the raw mention count is still small.
15. Persist with `docker compose run --rm analysis persist-tweet-analysis --input-file ...`.

## Rules

- If multiple checkouts of the repository exist, prefer the checkout that owns the already-running `stock-analytics-db` container and has the live `.env` / `pgdata`.
- When in doubt, verify the active DB with `docker inspect stock-analytics-db` and confirm the bind mount for `/var/lib/postgresql/data` before running collector or analysis commands.
- Use `analytics.listed_companies_latest` as the company-code lookup source unless the user explicitly asks for another universe.
- Keep one persisted row per `tweet x listed-company code`.
- Preserve the original tweet URL and text exactly as exported from the template.
- For current-day or near-real-time analysis, always perform the freshness check first and skip X API calls when the last successful poll is already within the 60-minute budget window.
- Do not force a mapping for ETFs, mutual funds, private companies, or themes when no Japanese listed company is actually mentioned.
- Use the enriched market context as evidence for the final flags, but keep the final flag judgment as an explicit LLM decision with rationale.
- When no trade exists for the tweet date yet, reuse the latest available trade date for that symbol and state that fallback explicitly in the rationale/summary.
- Treat `distinct monitored-user count` as a strong recommendation signal. A company mentioned by multiple monitored users should be surfaced ahead of single-user names with similar or moderately higher raw mention totals.
- When reporting findings, explicitly call out the participating usernames for any company that reached `count(distinct target_username) >= 2`.
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
