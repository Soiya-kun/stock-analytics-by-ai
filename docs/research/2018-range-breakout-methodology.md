# 2018 Range-Breakout Methodology

## Goal

Use only numeric price and volume behavior to find breakout cases and split them into:

- `trend`: breakout turned into a long-term uptrend
- `non_trend`: breakout failed
- `neutral`: breakout advanced, but not enough to call it a long trend
- `incomplete`: not enough future bars to judge

## Why Start With Labeling

Before tuning breakout parameters, the useful first step is to separate:

- breakouts that actually transitioned into long trends
- breakouts that looked similar at the breakout point but did not transition

This produces a durable case dataset that can later be mined for threshold differences.

## Baseline Numeric Rule

### 1. Breakout Candidate Detection

- Use `analytics.stock_prices_daily`.
- Define the pre-breakout range over `range_lookback_bars`.
- Require the breakout day to exceed the prior range high by `breakout_buffer_pct`.
- Reject cases whose pre-breakout range width is too wide.
- Require breakout-day volume to exceed the recent average by `min_volume_ratio`.
- Suppress repeated cases for the same code inside `cooldown_bars`.

### 2. Long-Trend Label

Label a case as `trend` when all of the following hold:

- forward max return over `trend_eval_bars` is at least `trend_min_return_pct`
- return at `trend_confirm_bars` is at least `trend_min_confirm_return_pct`
- worst return over `failure_drawdown_bars` stays above `failure_drawdown_pct`

### 3. Non-Trend Label

Label a case as `non_trend` when any of the following hold:

- worst return over `failure_drawdown_bars` breaches `failure_drawdown_pct`
- forward max return over `trend_eval_bars` fails to reach even half of the trend threshold
- return at `trend_confirm_bars` is non-positive

### 4. Neutral / Incomplete

- `neutral`: enough future bars exist, but the case is neither a clear trend nor a clear failure
- `incomplete`: not enough future bars exist to apply the rule

## Durable Files

- YAML summary: `research/range-breakout-2018-study/*_summary.yaml`
- Markdown report: `research/range-breakout-2018-study/*_report.md`
- All cases CSV: `research/range-breakout-2018-study/*_cases_all.csv`
- Per-label CSV: `research/range-breakout-2018-study/*_cases_trend.csv`, `*_cases_non_trend.csv`, `*_cases_neutral.csv`, `*_cases_incomplete.csv`

## Docker Command

```powershell
docker compose run --rm analysis label-study
```

## Next Tuning Axes

- vary `range_lookback_bars` across `120/180/240`
- vary `breakout_buffer_pct`
- vary `trend_min_return_pct`
- vary `failure_drawdown_pct`
- compare `close` vs `high` breakout basis
- flag or adjust split-driven price jumps because raw, unadjusted prices can create false mega-trend labels
