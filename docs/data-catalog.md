# Data Catalog

## Storage Layers

- `raw.kabuplus_records`: one row per CSV row. Original Japanese headers and values are stored in `payload jsonb`.
- `ingest.kabuplus_files`: one row per imported CSV file with status, row counts, and failure message.
- `analytics.*`: typed views for datasets that are likely to be queried first.

## Curated Views

| View | Dataset key | Cadence | Purpose |
| --- | --- | --- | --- |
| `analytics.stock_prices_daily` | `japan-all-stock-prices/daily` | Daily | Close, open, high, low, change, volume, turnover |
| `analytics.stock_prices_daily_extended` | `japan-all-stock-prices-2/daily` | Daily | `stock_prices_daily` plus VWAP and year-to-date ranges |
| `analytics.stock_snapshot_daily` | `japan-all-stock-data/daily` | Daily | Valuation and snapshot metrics such as PER/PBR/dividend yield |
| `analytics.tosho_stock_ohlc_daily` | `tosho-stock-ohlc/daily` | Daily | TSE OHLC data including AM/PM session breakdown |
| `analytics.financial_results_monthly` | `japan-all-stock-financial-results/monthly` | Monthly | Revenue, profit, balance sheet, ROE/ROA |
| `analytics.listing_information_monthly` | `japan-all-stock-information/monthly` | Monthly | Listing month master data |
| `analytics.margin_transactions_weekly` | `japan-all-stock-margin-transactions/weekly` | Weekly | Margin buy/sell balances |
| `analytics.corporate_actions_monthly` | `corporate-action/monthly` | Monthly | Split and reverse split events |
| `analytics.import_status` | `ingest.kabuplus_files` | Derived | Import coverage and status |

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
