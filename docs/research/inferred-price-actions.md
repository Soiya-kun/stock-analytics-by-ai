# Inferred Price Actions

## Goal

Track obvious stock split and reverse-split events without mutating the raw daily price history.

## Detection Rule

- Source: `analytics.stock_prices_daily`
- Candidate factor range: integer `2..10`
- Compare breakout-day `open/high/low/close` against the reported previous close
- Detect `split` when all OHLC values cluster around `1 / k`
- Detect `reverse_split` when all OHLC values cluster around `k`
- Choose the candidate with the lowest OHLC error when multiple factors fit

## Strictness

- Median relative OHLC error threshold: `0.06`
- Max per-field relative OHLC error threshold: `0.12`
- Evidence is stored in `evidence_json`

## Persistence

- DB table: `analytics.inferred_price_actions`
- Audit outputs: `research/inferred-price-actions/*_summary.yaml`, `*_split_audit.md`, `*_events.csv`
- Adjusted-price view: `analytics.stock_prices_adjusted_daily`

## Docker Commands

```powershell
docker compose run --rm analysis infer-price-actions
docker compose run --rm analysis prepare-adjusted-prices
```
