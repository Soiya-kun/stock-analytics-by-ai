---
name: x-account-trust-evaluation
description: Evaluate candidate X accounts against trusted benchmark stock-signal history. Use when Codex needs to manage `benchmark` / `candidate` monitored accounts, refresh recent or 90-day X post history, turn posts into canonical bullish/non-bullish stock-signal rows, and score whether a candidate account is trustworthy enough to watch.
---

# X Account Trust Evaluation

## Overview

Use this skill to run the repository's durable X account trust workflow. The goal is not ad hoc tweet reaction review, but a repeatable candidate-vs-benchmark scoring pipeline backed by PostgreSQL tables, md/yaml/csv artifacts, and Docker commands.

## Workflow

1. Read [workflow.md](references/workflow.md) before touching the DB or collector.
2. Reuse the already-running `stock-analytics-db` container when it exists.
3. Apply `sql/migrations/20260412_x_account_trust_setup.sql` before using the trust commands on an existing `pgdata`.
4. Keep trusted monitored accounts as `account_role='benchmark'` and add evaluation targets as `account_role='candidate'`.
5. Refresh candidate or benchmark data with `xcollector`, using `--account-role` filters instead of ad hoc username lists when possible.
6. Export only unanalyzed posts with `prepare-x-signal-analysis`.
7. Review each post and write only Japanese listed-company signals. Leave `signals: []` when nothing relevant exists.
8. Run `enrich-x-signal-analysis` before persistence so each signal gets adjusted-price market context.
9. Persist canonical rows with `persist-x-signal-analysis`. This also marks zero-signal posts as reviewed.
10. Run `evaluate-x-account-trust` and keep the md/yaml/csv outputs under `research/account-trust/`.

## Rules

- Treat `research.x_post_stock_signals` as the canonical signal layer for this workflow.
- Preserve the original post text and URL exactly as exported.
- Use only `signal_label='bullish'` when computing trust scores.
- Keep the 30-day clustering rule and the default trust-score weights unless the user explicitly changes the principle.
- Surface four report sections for the candidate:
  - overlap with benchmark
  - earlier than benchmark
  - candidate-only winners
  - failed unique picks
- Use `analytics.stock_prices_adjusted_daily` market context only. Do not bypass the adjusted-price layer with raw prices.

## Repo Touchpoints

- Collector: `scripts/x_collector.py`
- Analysis entrypoint: `scripts/analyze_range_breakout.py`
- Trust helper: `scripts/x_account_trust_research.py`
- Source posts: `analytics.monitored_x_posts`
- Canonical bullish view: `analytics.x_bullish_stock_signals`
- Latest score view: `analytics.x_account_trust_latest`
- Durable tables:
  - `research.x_signal_analysis_runs`
  - `research.x_signal_analysis_post_reviews`
  - `research.x_post_stock_signals`
  - `research.x_account_trust_runs`
  - `research.x_account_trust_clusters`
  - `research.x_account_trust_scores`

## Validation

Run the validator after editing:

```powershell
python C:\Users\djmaa\.codex\skills\.system\skill-creator\scripts\quick_validate.py .codex\skills\x-account-trust-evaluation
```
