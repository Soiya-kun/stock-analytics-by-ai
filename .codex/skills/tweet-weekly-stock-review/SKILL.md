---
name: tweet-weekly-stock-review
description: Review persisted tweet-stock analysis results for the exact trailing seven JST calendar dates including the execution date, or for a requested weekly date range, compute statistics on monitored X stock mentions, identify which Japanese listed-stock ideas worked or failed using post-event price and volume context, and produce a concise retrospective report. Use when Codex is asked for a weekly review, current seven-day review, last-seven-days retrospective, or how monitored X stock picks performed based on research.tweet_stock_mentions.
---

# Tweet Weekly Stock Review

## Overview

Use this skill after `tweet-stock-analysis` has persisted `tweet x listed-company` rows. The weekly review is a reporting/statistics layer over `research.tweet_stock_mentions`; it should not re-identify company mentions unless the requested week has not been analyzed yet.

## Workflow

1. Read [workflow.md](references/workflow.md) before starting.
2. By default, use the exact trailing seven JST calendar dates including the execution date. For example, if run on 2026-05-23 JST, review 2026-05-17 through 2026-05-23.
3. Confirm the active PostgreSQL container and checkout before running commands. Reuse the existing `stock-analytics-db` when it is mounted to this checkout.
4. Immediately before every current seven-day review, run `docker compose run --rm xcollector ensure-current` with no target filter so the DB is checked for posts up to the current moment. Let the command skip X API calls if the collector state is already fresh.
5. Check whether `research.tweet_stock_mentions` already has rows for the target range. If rows are missing or obviously incomplete, run `tweet-stock-analysis` for that range after the freshness check.
6. Run the bundled review script for the target range:

```powershell
docker compose run --rm --entrypoint python analysis /workspace/.codex/skills/tweet-weekly-stock-review/scripts/weekly_tweet_stock_review.py --trailing-week
```

For an explicit range:

```powershell
docker compose run --rm --entrypoint python analysis /workspace/.codex/skills/tweet-weekly-stock-review/scripts/weekly_tweet_stock_review.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD
```

7. Use the script output as the statistical backbone, then write a concise Japanese report unless the user asked for another language.
8. Because the default range includes today, state whether today's tweets were freshness-checked and whether 5d or 20d forward-return statistics are incomplete because future trade rows are not available yet.

## Reporting Rules

- Lead with the weekly verdict: whether monitored X mentions were broadly effective, mixed, or weak.
- Include coverage: source post count, posts with stock mentions, mention rows, unique stocks, and monitored users.
- Include hit-rate style metrics for `max_close_return_5d_pct` and `max_close_return_20d_pct` when available.
- Surface cross-user confirmation names (`distinct target_username >= 2`) before single-user ideas with similar performance.
- Show best and worst examples. Include stock code, company name, users, mention count, 5d/20d return context, and the short thesis from `analysis_summary`.
- Compare `price_jump_flag` and `volume_spike_flag` cohorts when there is enough data, because these flags indicate whether the original tweet excitement matched market action.
- Avoid treating duplicate persisted runs as separate ideas. The bundled script deduplicates by latest `(post_id, sc)` by default.
- If there are zero persisted mention rows, do not invent statistics. Tell the user the week must be processed with `tweet-stock-analysis` first.
- Do not use the previous Monday-Sunday week unless the user explicitly asks for calendar last week.

## Repo Touchpoints

- Weekly review script: `scripts/weekly_tweet_stock_review.py` in this skill folder
- Upstream analysis skill: `.codex/skills/tweet-stock-analysis`
- Source persisted rows: `research.tweet_stock_mentions`
- Source post coverage view: `analytics.monitored_x_posts`
- Relevant fields: `volume_spike_flag`, `price_jump_flag`, `event_day_return_pct`, `max_close_return_5d_pct`, `max_close_return_20d_pct`, `volume_ratio_20d`

## Validation

Run the skill validator after editing:

```powershell
python C:\Users\djmaa\.codex\skills\.system\skill-creator\scripts\quick_validate.py .codex\skills\tweet-weekly-stock-review
```
