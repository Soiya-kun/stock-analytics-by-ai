# Data Catalog

## Storage Layers

- `raw.kabuplus_records`: one row per CSV row. Original Japanese headers and values are stored in `payload jsonb`.
- `raw.x_users`: latest profile snapshot for each monitored X account.
- `raw.x_posts`: one row per collected X post. Latest payload and public metrics overwrite on re-fetch.
- `ingest.kabuplus_files`: one row per imported CSV file with status, row counts, and failure message.
- `ingest.x_monitored_accounts`: fixed monitored usernames and their resolved X user IDs.
- `ingest.x_monitored_accounts.account_role`: `benchmark` / `candidate` role attached to each monitored username.
- `ingest.x_timeline_state`: per-target polling checkpoint and failure state.
- `ingest.x_poll_runs`: aggregate poll-run audit log.
- `ingest.x_usage_daily`: optional snapshots from `GET /2/usage/tweets`.
- `analytics.inferred_price_actions`: inferred split / reverse-split events detected from one-day integer OHLC jumps.
- `research.x_signal_analysis_post_reviews`: one row per reviewed X post, including zero-signal reviews.
- `research.x_post_stock_signals`: canonical one row per `post x listed-company code` signal.
- `research.x_account_trust_*`: durable candidate trust runs, clusters, and scores.
- `research.*`: durable study runs, breakout cases, and hypothesis records.
- `analytics.*`: typed views for datasets that are likely to be queried first.

## Curated Views

| View | Dataset key | Cadence | Purpose |
| --- | --- | --- | --- |
| `analytics.stock_prices_daily` | `japan-all-stock-prices/daily` | Daily | Close, open, high, low, change, volume, turnover |
| `analytics.stock_prices_adjusted_daily` | `analytics.stock_prices_daily` + `analytics.inferred_price_actions` | Daily | Non-destructive split-adjusted OHLCV with raw and adjusted columns |
| `analytics.stock_prices_daily_extended` | `japan-all-stock-prices-2/daily` | Daily | `stock_prices_daily` plus VWAP and year-to-date ranges |
| `analytics.stock_snapshot_daily` | `japan-all-stock-data/daily` | Daily | Valuation and snapshot metrics such as PER/PBR/dividend yield |
| `analytics.tosho_stock_ohlc_daily` | `tosho-stock-ohlc/daily` | Daily | TSE OHLC data including AM/PM session breakdown |
| `analytics.financial_results_monthly` | `japan-all-stock-financial-results/monthly` | Monthly | Revenue, profit, balance sheet, ROE/ROA |
| `analytics.listing_information_monthly` | `japan-all-stock-information/monthly` | Monthly | Listing month master data |
| `analytics.margin_transactions_weekly` | `japan-all-stock-margin-transactions/weekly` | Weekly | Margin buy/sell balances |
| `analytics.corporate_actions_monthly` | `corporate-action/monthly` | Monthly | Split and reverse split events |
| `analytics.import_status` | `ingest.kabuplus_files` | Derived | Import coverage and status |
| `analytics.monitored_x_posts` | `raw.x_posts` + `ingest.x_monitored_accounts` | Near real-time | Fixed monitored X accounts with JST timestamps and public metrics |
| `analytics.listed_companies_latest` | `analytics.stock_prices_daily` | Latest market day | Listed-company snapshot for LLM company-code lookup |
| `analytics.x_bullish_stock_signals` | `research.x_post_stock_signals` | Derived | Bullish-only canonical signal rows used for trust scoring |
| `analytics.x_account_trust_latest` | `research.x_account_trust_scores` | Derived | Latest trust score and verdict for each candidate account |

## Research Tables

| Relation | Purpose |
| --- | --- |
| `research.entry_study_runs` | One row per durable 6-month breakout study run |
| `research.entry_cases` | Wide case table for breakout-point features and trend labels |
| `research.entry_hypotheses` | Interpretable threshold rules stored for train / validation stages |
| `research.tweet_analysis_runs` | One row per tweet-analysis run over a requested date range |
| `research.tweet_stock_mentions` | One row per `tweet x listed-company` mention with reaction flags and rationale |
| `research.x_signal_analysis_runs` | One row per canonical X signal review batch |
| `research.x_signal_analysis_post_reviews` | One row per reviewed post, including posts with zero relevant stock signals |
| `research.x_post_stock_signals` | Canonical one row per `post x listed-company code` with bullish / non-bullish / irrelevant signal labels |
| `research.x_account_trust_runs` | One row per candidate trust evaluation run |
| `research.x_account_trust_clusters` | One row per `run x candidate x symbol-cluster` comparison result |
| `research.x_account_trust_scores` | One row per `run x candidate` trust score and verdict |

## Dataset Families Observed in `stock/kabuplus-2025.zip`

- `corporate-action/monthly`
- `japan-all-stock-data/daily`
- `japan-all-stock-financial-results/monthly`
- `japan-all-stock-information/monthly`
- `japan-all-stock-margin-transactions/weekly`
- `japan-all-stock-prices/daily`
- `japan-all-stock-prices-2/daily`
- `jsf-balance-data/daily`
- `jsf-gyakuhibu-data/daily`
- `tosho-etf-margin-transactions/weekly`
- `tosho-etf-margin-transactions-2/weekly`
- `tosho-etf-ohlc/daily`
- `tosho-etf-stock-prices/code`
- `tosho-etf-stock-prices/daily`
- `tosho-fund-and-others-margin-transactions/weekly`
- `tosho-fund-and-others-margin-transactions-2/weekly`
- `tosho-fund-and-others-ohlc/daily`
- `tosho-fund-and-others-stock-prices/code`
- `tosho-fund-and-others-stock-prices/daily`
- `tosho-index-data/code`
- `tosho-index-data/daily`
- `tosho-reit-margin-transactions/weekly`
- `tosho-reit-margin-transactions-2/weekly`
- `tosho-reit-ohlc/daily`
- `tosho-reit-stock-prices/code`
- `tosho-reit-stock-prices/daily`
- `tosho-stock-margin-transactions-2/weekly`
- `tosho-stock-ohlc/daily`

All of these are imported into `raw.kabuplus_records`, even if a curated typed view has not been added yet.

## Raw Query Pattern

For a dataset without a curated view, query the raw layer directly:

```sql
select
    security_code,
    record_date,
    payload
from raw.kabuplus_records
where dataset_key = 'jsf-balance-data/daily'
order by record_date desc
limit 20;
```
