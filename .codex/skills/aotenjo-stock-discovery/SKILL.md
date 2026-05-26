---
name: aotenjo-stock-discovery
description: Discover Japanese listed stocks that have just entered all-time-high "aotenjo" territory using adjusted KABU+ daily prices. Use when Codex needs to scan a target trading day for stocks whose adjusted high or close first exceeds the prior all-time high, rank the candidates by breakout margin, volume confirmation, and liquidity, or explain the exact blue-sky breakout moment from this repository's PostgreSQL data.
---

# Aotenjo Stock Discovery

## Overview

Use this skill to find stocks that became "aotenjo" on a target trading day: today's adjusted high or close exceeds the highest adjusted price seen before that day. This is a first-breakout scan, not a multi-month range-breakout study.

## Workflow

1. Read [workflow.md](references/workflow.md) before changing thresholds or writing a custom query.
2. Normalize the user request into `as_of_date`, `breakout_basis`, optional liquidity filters, and output size.
3. Use `analytics.stock_prices_adjusted_daily` as the source relation so split and reverse-split effects do not create false all-time highs.
4. Treat the signal as valid only when the target-day basis price is greater than the prior all-time high computed from dates before the target day.
5. Require enough pre-signal history; default to `min_history_bars = 500` to avoid flagging newly listed stocks as blue-sky breakouts.
6. Prefer the bundled script for ordinary scans:

```powershell
docker compose run --rm --entrypoint python analysis /workspace/.codex/skills/aotenjo-stock-discovery/scripts/scan_aotenjo.py --as-of-date YYYY-MM-DD --output-dir /workspace/outputs/aotenjo
```

7. If the user asks for "今日" or "最新", use the latest imported trading day on or before that date; do not assume same-day KABU+ data is already loaded.
8. Return a concise candidate report with the target trading day, basis, prior all-time-high date, breakout margin, volume ratio, day change, and liquidity context.

## Signal Definition

- `aotenjo moment`: `basis_price > prior_all_time_high * (1 + breakout_buffer_pct)` where `prior_all_time_high` is computed only from earlier trading days for the same `sc`.
- Default `breakout_basis`: `high`, because intraday high is the earliest daily OHLC evidence that a stock entered blue-sky territory.
- Use `close` when the user asks for close-confirmed blue-sky breakouts.
- Default `breakout_buffer_pct`: `0.0`; raise it when the user wants fewer near-tie candidates.
- Default `min_volume_ratio`: `1.0`; raise it when the user wants volume-confirmed breakouts only.
- Default `min_turnover_thousand_yen`: `0`; raise it to remove illiquid stocks.

## Output Rules

- Sort by volume confirmation and breakout margin unless the user requests another ranking.
- Separate "intraday aotenjo" from "close-confirmed aotenjo" when the basis is ambiguous.
- Call out stale-data risk when the latest imported date is earlier than the user's requested date.
- Do not mix this with 4-6 month or multi-year range-breakout labels unless the user explicitly asks to combine signals.

## Repo Touchpoints

- Source view: `analytics.stock_prices_adjusted_daily`
- Existing breakout study: `scripts/analyze_range_breakout.py`
- Output directory: `outputs/aotenjo/`
- Analysis principle: `docs/analysis-principles.md`

## Validation

Run the validator after editing:

```powershell
python C:\Users\djmaa\.codex\skills\.system\skill-creator\scripts\quick_validate.py .codex\skills\aotenjo-stock-discovery
```
