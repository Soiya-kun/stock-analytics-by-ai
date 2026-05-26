# Aotenjo Discovery Workflow

## Meaning

For this repository, "青天井" means a stock entered price territory with no prior adjusted-price overhead:

```text
target_day_basis_price > max(prior_day_basis_price for the same sc)
```

Use adjusted prices from `analytics.stock_prices_adjusted_daily`. Raw prices can create false positives around splits and reverse splits.

## Parameters

| Parameter | Default | Notes |
| --- | --- | --- |
| `as_of_date` | latest imported trading day | The scan uses the latest trading day on or before this date. |
| `breakout_basis` | `high` | Use `high` for intraday discovery, `close` for close-confirmed discovery. |
| `breakout_buffer_pct` | `0.0` | Minimum excess above prior all-time high. |
| `min_history_bars` | `500` | Avoids IPO/new-listing false positives. |
| `volume_lookback_bars` | `20` | Prior bars used for average volume. |
| `min_volume_ratio` | `1.0` | Target-day volume divided by prior average volume. |
| `min_turnover_thousand_yen` | `0` | Liquidity floor using KABU+ turnover. |
| `limit` | `50` | Rows to report or write. |

## Standard Command

```powershell
docker compose run --rm --entrypoint python analysis /workspace/.codex/skills/aotenjo-stock-discovery/scripts/scan_aotenjo.py --as-of-date YYYY-MM-DD --output-dir /workspace/outputs/aotenjo
```

Useful variants:

```powershell
docker compose run --rm --entrypoint python analysis /workspace/.codex/skills/aotenjo-stock-discovery/scripts/scan_aotenjo.py --as-of-date YYYY-MM-DD --breakout-basis close --min-volume-ratio 1.5
docker compose run --rm --entrypoint python analysis /workspace/.codex/skills/aotenjo-stock-discovery/scripts/scan_aotenjo.py --latest --min-turnover-thousand-yen 100000 --limit 100
```

## Query Shape

Use this shape for custom SQL. Keep the basis expression restricted to known columns; do not interpolate user text into SQL.

```sql
with target_day as (
    select max(trade_date) as trade_date
    from analytics.stock_prices_adjusted_daily
    where trade_date <= :as_of_date
),
target_rows as (
    select
        sc,
        name,
        market,
        industry,
        trade_date,
        high_price,
        close_price,
        volume,
        turnover_thousand_yen,
        day_change_pct
    from analytics.stock_prices_adjusted_daily
    where trade_date = (select trade_date from target_day)
      and high_price is not null
      and close_price is not null
),
history_ranked as (
    select
        sc,
        trade_date,
        high_price as basis_price,
        volume,
        row_number() over (
            partition by sc
            order by trade_date desc
        ) as reverse_seq
    from analytics.stock_prices_adjusted_daily
    where high_price is not null
      and trade_date < (select trade_date from target_day)
),
history_summary as (
    select
        sc,
        count(*) as history_bars,
        max(basis_price) as prior_all_time_high,
        avg(volume) filter (where reverse_seq <= 20) as avg_volume_20
    from history_ranked
    group by sc
)
select *
from target_rows t
join history_summary h on h.sc = t.sc
where h.history_bars >= :min_history_bars
  and h.prior_all_time_high is not null
  and t.high_price > h.prior_all_time_high * (1 + :breakout_buffer_pct)
  and coalesce(t.volume / nullif(h.avg_volume_20, 0), 0) >= :min_volume_ratio
order by coalesce(t.volume / nullif(h.avg_volume_20, 0), 0) desc,
         (t.high_price / h.prior_all_time_high - 1) desc;
```

## Reporting

For each highlighted candidate, include:

- `sc`, `name`, `market`, `industry`
- target trading day and whether the signal is intraday-high or close-confirmed
- target basis price, prior all-time high, and prior all-time-high date
- breakout margin percentage
- volume ratio versus the prior average
- turnover and day-change context

When no candidates appear, report the target trading day that was actually scanned and the active filters.
