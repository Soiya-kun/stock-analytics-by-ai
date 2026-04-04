# 6-Month Breakout Entry Methodology

## Goal

Use only adjusted price and volume behavior to find 4-6 month range breakouts and persist:

- the breakout cases themselves
- the feature values observed at entry
- the forward label (`trend`, `non_trend`, `neutral`, `incomplete`)
- the interpretable threshold rules mined from the train split

## Data Layers

- Raw source: `analytics.stock_prices_daily`
- Inferred split events: `analytics.inferred_price_actions`
- Analysis source: `analytics.stock_prices_adjusted_daily`
- Durable study tables: `research.entry_study_runs`, `research.entry_cases`, `research.entry_hypotheses`

## Non-Destructive Adjustment Rule

- Raw prices remain unchanged.
- Split / reverse-split candidates are inferred only from one-day integer OHLC jumps.
- The adjusted view applies the cumulative inferred multipliers only at read time.
- Past prices are adjusted to the latest basis for each stock.

## Case Definition

- One case = one stock code on one breakout date.
- Entry price = breakout-day close.
- Range window = prior `120` trading bars.
- Breakout condition = breakout basis exceeds the prior range high by `2%`.
- Range width cap = `35%`.
- Volume filter = breakout-day volume at least `1.20x` the trailing 20-bar average.
- Cooldown = suppress repeated cases for the same stock inside `60` bars.

## Feature Set

- Breakout margin
- Day return and gap
- Candle body ratio
- Upper / lower wick ratio
- Volume ratio
- Bullish flag
- Prior bullish / up-day counts across `10/20/60`
- High-volume bullish counts across `20/60`
- Long upper / lower wick counts across `20/60`
- Prior 20 / 60 bar returns
- MA gap and MA slope for `20/60`
- Range width and range-high touch count
- Higher-high / higher-low counts
- ATR context

## Label Rule

- `trend`
  - `future_max_return_240d_pct >= 0.40`
  - `return_120d_pct >= 0.20`
  - `future_min_return_60d_pct > -0.10`
- `non_trend`
  - early drawdown breaches `-0.10`, or
  - future max return never reaches half the trend threshold, or
  - 120-bar return is non-positive
- `neutral`
  - enough future bars exist, but the case is neither `trend` nor `non_trend`
- `incomplete`
  - not enough future bars exist to apply the label rule

## Time Split

- Train: `2018-01-01` to `2020-12-31`
- Validation: `2021-01-01` to `2022-12-30`

## Hypothesis Mining Rule

- Mine only interpretable threshold rules.
- Start with one feature at a time.
- Keep rules only when precision beats the base train trend rate and selected cases exceed the minimum size.
- Build two-feature `AND` rules only from the top univariate candidates.
- Evaluate the frozen train rules on the validation split without refitting.

## Durable Artifacts

- `research/entry-breakout-6m/*_manifest.yaml`
- `research/entry-breakout-6m/*_summary.md`
- `research/entry-breakout-6m/*_cases_all.csv`
- `research/entry-breakout-6m/*_cases_train.csv`
- `research/entry-breakout-6m/*_cases_validation.csv`
- `research/entry-breakout-6m/*_hypotheses.yaml`
- `research/entry-breakout-6m/*_hypotheses_validation.yaml`
- `research/entry-breakout-6m/*_evaluation.md`

## Docker Commands

```powershell
docker compose run --rm analysis build-entry-dataset
docker compose run --rm analysis mine-entry-hypotheses
docker compose run --rm analysis evaluate-entry-hypotheses
```
