# X Account Trust Workflow

## Boot And Migration

Reuse the existing DB container when it already points at this checkout's `pgdata`.

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
docker inspect stock-analytics-db
docker compose exec -T db psql -U stock -d stock_analytics -f /workspace/sql/migrations/20260412_x_account_trust_setup.sql
```

## Account Setup

Add candidate accounts into the same monitored-account table and keep existing trusted accounts as benchmarks.

```powershell
docker compose exec db psql -U stock -d stock_analytics -c "insert into ingest.x_monitored_accounts (target_username, account_role) values ('yuzz__', 'candidate') on conflict (target_username) do update set account_role = excluded.account_role;"
docker compose run --rm xcollector sync-targets --account-role candidate
docker compose run --rm xcollector backfill --account-role all --days 90
```

## Signal Review

Export only posts that have not been reviewed yet:

```powershell
docker compose run --rm analysis prepare-x-signal-analysis --start-date YYYY-MM-DD --end-date YYYY-MM-DD --account-role all
```

Review `research/x-signal-analysis/<run-id>/analysis_template.yaml`.

Per post:

- Keep `post_id`, `tweet_url`, and `text` unchanged.
- Add one item in `signals` per Japanese listed company actually mentioned.
- Fill:
  - `sc`
  - `company_name`
  - `match_confidence`
  - `signal_label`
  - `signal_confidence`
  - `extraction_rationale`
  - `signal_rationale`
- Leave `signals: []` when no listed company is relevant.

## Enrich And Persist

```powershell
docker compose run --rm analysis enrich-x-signal-analysis --input-file /workspace/research/x-signal-analysis/<run-id>/analysis_template.yaml
docker compose run --rm analysis persist-x-signal-analysis --input-file /workspace/research/x-signal-analysis/<run-id>/enriched_analysis.yaml
```

Persistence updates:

- `research.x_post_stock_signals`
- `research.x_signal_analysis_post_reviews`

## Trust Evaluation

```powershell
docker compose run --rm analysis evaluate-x-account-trust --candidate-username yuzz__ --start-date YYYY-MM-DD --end-date YYYY-MM-DD
```

The report must show:

- benchmark overlap symbols
- symbols where the candidate was earlier than benchmark
- candidate-only winners
- failed unique picks

## Verification Queries

Canonical bullish signals:

```sql
select
    target_username,
    sc,
    company_name,
    post_date_jst,
    max_close_return_20d_pct
from analytics.x_bullish_stock_signals
order by post_created_at desc
limit 20;
```

Latest trust score:

```sql
select
    candidate_username,
    trust_score,
    verdict,
    bullish_cluster_count,
    unique_pick_count
from analytics.x_account_trust_latest
order by run_created_at desc;
```
