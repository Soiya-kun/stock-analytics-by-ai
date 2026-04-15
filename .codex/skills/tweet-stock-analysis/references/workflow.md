# Tweet Stock Analysis Workflow

## Commands

Confirm whether an existing DB container should be reused before starting anything:

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
docker inspect stock-analytics-db
```

If `stock-analytics-db` is already running against the intended checkout's `pgdata`, reuse it. Avoid recreating the DB from another worktree unless you explicitly intend to switch databases.

Ensure the monitored tweet set is fresh enough before analyzing recent dates:

```powershell
docker compose run --rm xcollector ensure-current --target-username USERNAME
```

Prepare the analysis template:

```powershell
docker compose run --rm analysis prepare-tweet-analysis --start-date YYYY-MM-DD --end-date YYYY-MM-DD --target-username USERNAME
```

Enrich the annotated file with market context:

```powershell
docker compose run --rm analysis enrich-tweet-analysis --input-file /workspace/research/tweet-stock-analysis/<run-id>/analysis_template.yaml
```

Persist the final judgments:

```powershell
docker compose run --rm analysis persist-tweet-analysis --input-file /workspace/research/tweet-stock-analysis/<run-id>/enriched_analysis.yaml
```

## Editing Checklist

Before `prepare-tweet-analysis`:

- Reuse the already-running `stock-analytics-db` when it has the intended monitored accounts and tweet history.
- If there are multiple repository checkouts, confirm the running DB container's bind mounts before issuing any `docker compose up -d db`.
- If the requested range includes today or otherwise depends on recent tweets, run `xcollector ensure-current`.
- Treat tweets as current when the target's last successful incremental poll is within the last 60 minutes.
- If `ensure-current` decides the account is stale, let it perform the incremental fetch before continuing.

When filling `analysis_template.yaml`:

- Keep `post_id`, `tweet_url`, and `text` unchanged.
- Add one `mentions` item per Japanese listed company actually mentioned.
- Fill `sc`, `company_name`, `match_confidence`, and `extraction_rationale`.
- Use `mentions: []` when no listed company applies.

When reviewing `enriched_analysis.yaml`:

- Read `market_context.event_trade_date`, `event_day_return_pct`, `max_close_return_5d_pct`, `max_close_return_20d_pct`, and `volume_ratio_20d`.
- If the tweet landed on a non-trading day or before the next trade row exists, use the latest available `market_context.event_trade_date` for that symbol instead of treating market context as unavailable.
- Set `volume_spike_flag` and `price_jump_flag` as booleans.
- Explain both flags in plain Japanese or English with evidence from the market context.
- Fill `analysis_summary` with a short conclusion suitable for later SQL querying.

When preparing the final report:

- Aggregate the persisted rows by `sc` and compute `count(distinct target_username)`.
- Treat any symbol with `count(distinct target_username) >= 2` as a cross-user confirmation candidate.
- Surface cross-user confirmation candidates prominently even when their total mention count is low.
- Name the participating monitored users explicitly so the report makes the independent confirmations obvious.

## Verification Queries

Latest persisted rows:

```sql
select
    run_id,
    sc,
    company_name,
    volume_spike_flag,
    price_jump_flag,
    tweet_url
from research.tweet_stock_mentions
order by created_at desc
limit 20;
```

Run summary:

```sql
select
    run_id,
    start_date,
    end_date,
    target_username,
    created_at
from research.tweet_analysis_runs
order by created_at desc
limit 20;
```

Cross-user confirmation candidates:

```sql
select
    sc,
    company_name,
    count(*) as mention_rows,
    count(distinct target_username) as distinct_users,
    array_agg(distinct target_username order by target_username) as mentioned_by
from research.tweet_stock_mentions
where post_date_jst between date 'YYYY-MM-DD' and date 'YYYY-MM-DD'
group by sc, company_name
having count(distinct target_username) >= 2
order by distinct_users desc, mention_rows desc, sc;
```
