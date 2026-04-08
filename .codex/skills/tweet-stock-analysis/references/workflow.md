# Tweet Stock Analysis Workflow

## Commands

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
- Set `volume_spike_flag` and `price_jump_flag` as booleans.
- Explain both flags in plain Japanese or English with evidence from the market context.
- Fill `analysis_summary` with a short conclusion suitable for later SQL querying.

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
