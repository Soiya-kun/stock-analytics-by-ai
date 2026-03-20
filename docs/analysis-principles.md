# Analysis Principles

## Current State

This file is the source of truth for analysis logic that must survive across future sessions. When a principle changes, update this file first and then update the implementation.

## Principle 001 - Multi-Year Range Breakout

- Intent: Find ways to capture stocks that spent multiple years inside a range and then break above that range.
- Universe: Listed stocks with enough daily history and non-null price and volume data.
- Base dataset/view: `analytics.stock_prices_daily`
- Required filters:
  - Sufficient lookback history exists for the range window.
  - Historical range width stays below a configurable cap.
  - Breakout day exceeds the prior range high by a configurable buffer.
  - Breakout day volume is above a configurable recent-average multiple.
  - Repeated signals inside the cooldown window are suppressed.
- Parameters and defaults:
  - `lookback_years_grid`: `2,3,4,5`
  - `max_range_width_pct_grid`: `0.30,0.50,0.80`
  - `breakout_buffer_pct_grid`: `0.00,0.01,0.02`
  - `min_volume_ratio_grid`: `1.00,1.50`
  - `volume_lookback_bars`: `20`
  - `cooldown_bars`: `60`
  - `breakout_basis`: `close`
  - `range_high_basis`: `close`
  - `range_low_basis`: `close`
  - `forward_bars`: `20,60,120`
- Signal or scoring logic:
  - For each code and date, compute the historical upper and lower bound over the prior `lookback_years`.
  - Treat the date as a breakout candidate when the breakout basis exceeds the prior upper bound by `breakout_buffer_pct`.
  - Reject the candidate when the historical range is too wide or breakout-day volume is too weak.
  - Remove repeated signals for the same code inside `cooldown_bars`.
  - Evaluate parameter sets by forward returns over `forward_bars`.
- Validation query or backtest method:
  - Historical grid search: `docker compose run --rm analysis grid-search`
  - Latest candidate scan: `docker compose run --rm analysis scan`
  - Output directory: `outputs/range-breakout/`
- Open questions:
  - Whether price series should be adjusted for split or reverse split events.
  - Whether close-based or high-based breakout judgment is better.
  - Whether to add liquidity filters, retest conditions, or multi-day confirmation.

## Principle 002 - Breakout To Long-Trend Labeling

- Intent: Split breakout cases into `trend` and `non_trend` first, using only price and volume, so later parameter tuning can learn from both groups.
- Universe: Breakout cases detected from `analytics.stock_prices_daily`.
- Base dataset/view: `analytics.stock_prices_daily`
- Required filters:
  - Pre-breakout range is defined over a fixed bar window.
  - Breakout day exceeds the pre-breakout range high by a fixed buffer.
  - Breakout day volume exceeds a recent average multiple.
  - Repeated cases for the same code are suppressed by a cooldown rule.
- Parameters and defaults:
  - `candidate_start_date`: `2018-07-01`
  - `candidate_end_date`: `2020-12-30`
  - `range_lookback_bars`: `120`
  - `max_range_width_pct`: `0.35`
  - `breakout_buffer_pct`: `0.02`
  - `min_volume_ratio`: `1.20`
  - `volume_lookback_bars`: `20`
  - `cooldown_bars`: `60`
  - `trend_confirm_bars`: `120`
  - `trend_eval_bars`: `240`
  - `failure_drawdown_bars`: `60`
  - `trend_min_return_pct`: `0.40`
  - `trend_min_confirm_return_pct`: `0.20`
  - `failure_drawdown_pct`: `-0.10`
- Signal or scoring logic:
  - Detect breakout candidates from a bar-based range definition.
  - Label `trend` when forward max return and confirm-horizon return clear the trend thresholds and early drawdown stays above the failure threshold.
  - Label `non_trend` when early drawdown breaches the failure threshold or the breakout never advances enough.
  - Label `neutral` when the breakout advances but not enough to qualify as a long trend.
  - Label `incomplete` when there are not enough future bars to judge the case.
- Validation query or backtest method:
  - Durable study run: `docker compose run --rm analysis label-study`
  - Durable output directory: `research/range-breakout-2018-study/`
  - Method document: `docs/research/2018-range-breakout-methodology.md`
- Open questions:
  - Whether the trend threshold should be based on max return, end return, or moving-average slope.
  - Whether the failure rule should use close-only drawdown, intraday drawdown, or time-under-breakout.
  - Whether the breakout lookback should stay at `120` bars or be tuned across `120/180/240`.

## Principle Template

Use the following template for each principle:

```markdown
## Principle 00X - Name

- Intent:
- Universe:
- Base dataset/view:
- Required filters:
- Parameters and defaults:
- Signal or scoring logic:
- Validation query or backtest method:
- Open questions:
```

## Parameter Rule

- Every threshold, lookback window, or ranking size must have an explicit parameter name.
- Defaults live in the implementation and are mirrored in this document.
- If a parameter changes, update both the implementation and this document in the same change.

## Implementation Rule

- Keep raw ingestion logic in `scripts/import_kabuplus.py`.
- Keep typed SQL access in `analytics.*` views.
- Run reusable analysis logic through Docker services, not ad hoc local-only scripts.
- Put reusable analysis logic in SQL files or scripts, not in ad hoc shell history.
- Update the project skill when the workflow for adding or validating principles changes.
