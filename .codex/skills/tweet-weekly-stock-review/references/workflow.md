# Tweet Weekly Stock Review Workflow

## Date Rules

- Default weekly review: exact trailing seven JST calendar dates including the execution date.
- Example: if the command runs on 2026-05-23 JST, use 2026-05-17 through 2026-05-23.
- Requests for the last seven days or the most recent week use the same trailing seven-date rule.
- Previous calendar week / last calendar week: previous JST Monday through Sunday, only when the user explicitly asks for calendar-week semantics.
- If the user supplies dates, use those exact dates and state them in the report.

## Preflight

Confirm the DB container and mounted checkout before analysis:

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
docker inspect stock-analytics-db
```

Check persisted coverage:

```sql
select
    min(post_date_jst) as first_date,
    max(post_date_jst) as last_date,
    count(*) as mention_rows,
    count(distinct post_id) as posts_with_mentions,
    count(distinct sc) as unique_stocks,
    count(distinct target_username) as monitored_users
from research.tweet_stock_mentions
where post_date_jst between date 'YYYY-MM-DD' and date 'YYYY-MM-DD';
```

For the default current seven-day review, always check/fetch current tweets before judging coverage:

```powershell
docker compose run --rm xcollector ensure-current
```

Run it across all active monitored accounts. Do not add `--target-username` unless the user explicitly limits the review to one account. The command itself decides whether an X API fetch is needed based on `ingest.x_timeline_state.last_success_at`.

If this returns zero rows, or the range has not been processed, use `tweet-stock-analysis` first:

```powershell
docker compose run --rm xcollector ensure-current
docker compose run --rm analysis prepare-tweet-analysis --start-date YYYY-MM-DD --end-date YYYY-MM-DD
docker compose run --rm analysis enrich-tweet-analysis --input-file /workspace/research/tweet-stock-analysis/<run-id>/analysis_template.yaml
docker compose run --rm analysis persist-tweet-analysis --input-file /workspace/research/tweet-stock-analysis/<run-id>/enriched_analysis.yaml
```

For the default current seven-day workflow, run `xcollector ensure-current` even if persisted rows already exist, because today's posts may have arrived after the last analysis run.

## Run the Review

Default current seven JST dates, including the execution date:

```powershell
docker compose run --rm --entrypoint python analysis /workspace/.codex/skills/tweet-weekly-stock-review/scripts/weekly_tweet_stock_review.py --trailing-week
```

Explicit range:

```powershell
docker compose run --rm --entrypoint python analysis /workspace/.codex/skills/tweet-weekly-stock-review/scripts/weekly_tweet_stock_review.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD
```

JSON output for follow-up processing:

```powershell
docker compose run --rm --entrypoint python analysis /workspace/.codex/skills/tweet-weekly-stock-review/scripts/weekly_tweet_stock_review.py --trailing-week --format json
```

## Interpretation

- `max_close_return_5d_pct > 0`: the idea had at least one positive close in the first five trading sessions after the event date.
- `max_close_return_5d_pct >= 0.05`: strong short-term follow-through of at least +5%.
- `max_close_return_20d_pct >= 0.10`: meaningful swing follow-through of at least +10%.
- `event_day_return_pct`: immediate reaction on the event trade date.
- `volume_ratio_20d`: event volume divided by 20-day average volume.
- `price_jump_flag` and `volume_spike_flag`: LLM-reviewed flags persisted by `tweet-stock-analysis`; use them as cohorts, not as ground truth by themselves.

## Report Shape

Use this order unless the user asks for raw tables:

1. Target dates and data completeness.
2. One-paragraph verdict.
3. Coverage and outcome statistics.
4. Cross-user confirmation candidates.
5. Best and worst examples.
6. Account-level observations.
7. Practical next actions for watchlist refinement.

Do not dump every mention row by default. Focus on the 5-10 most decision-useful stocks.
