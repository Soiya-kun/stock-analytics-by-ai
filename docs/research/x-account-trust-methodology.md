# X Account Trust Methodology

## Goal

Track a candidate X account, compare its bullish stock signals against trusted benchmark accounts, and decide whether the candidate is worth monitoring more closely.

## Signal Layer

- Source posts: `analytics.monitored_x_posts`
- Canonical signal table: `research.x_post_stock_signals`
- Review state table: `research.x_signal_analysis_post_reviews`
- Bullish-only view: `analytics.x_bullish_stock_signals`

One reviewed post can map to zero or more `post x stock-code` signal rows.
Posts with no listed-company signal still get a review row so `prepare-x-signal-analysis` can skip them on later runs.

## Annotation Rule

- Annotate only Japanese listed companies.
- Persist one row per `post x stock-code`.
- `signal_label` is one of:
  - `bullish`
  - `non_bullish`
  - `irrelevant`
- Required LLM fields for each signal:
  - `sc`
  - `company_name`
  - `match_confidence`
  - `signal_label`
  - `signal_confidence`
  - `extraction_rationale`
  - `signal_rationale`

## Market Context Rule

`enrich-x-signal-analysis` adds:

- `tweet_session`
- `event_trade_date`
- `previous_close_price`
- `event_close_price`
- `volume_ratio_20d`
- `max_close_return_5d_pct`
- `max_close_return_20d_pct`

The enrichment uses `analytics.stock_prices_adjusted_daily`, so split-adjusted prices are used throughout the trust study.

## Trust Scoring Rule

- Use only `signal_label = 'bullish'`.
- Group bullish signals by `sc` into 30-day clusters.
- Each cluster is anchored at the earliest bullish post timestamp in that cluster.

Per candidate account, compute:

- `benchmark_overlap_rate`
  - Share of candidate clusters where at least one benchmark account was also bullish in the same cluster.
- `early_overlap_rate`
  - Share of overlap clusters where the candidate's first bullish post was earlier than the earliest benchmark bullish post.
- `unique_pick_success_rate`
  - Share of candidate-only clusters where the candidate's earliest bullish post led to `max_close_return_20d_pct >= 10%`.

Initial score:

- `trust_score = 0.35 * overlap + 0.35 * early + 0.30 * unique`

Initial verdict thresholds:

- `insufficient_data`
  - `bullish_cluster_count < 15` or `unique_pick_count < 5`
- `trusted_candidate`
  - `trust_score >= 0.60`
- `watch`
  - `0.35 <= trust_score < 0.60`
- `low_confidence`
  - `trust_score < 0.35`

## Durable Outputs

- Signal-analysis artifacts: `research/x-signal-analysis/<run-id>/`
- Trust-evaluation artifacts: `research/account-trust/`
- DB tables:
  - `research.x_signal_analysis_runs`
  - `research.x_signal_analysis_post_reviews`
  - `research.x_post_stock_signals`
  - `research.x_account_trust_runs`
  - `research.x_account_trust_clusters`
  - `research.x_account_trust_scores`

## Standard Commands

```powershell
docker compose run --rm xcollector sync-targets --account-role candidate
docker compose run --rm xcollector backfill --account-role all --days 90
docker compose run --rm analysis prepare-x-signal-analysis --start-date 2026-01-13 --end-date 2026-04-12 --account-role all
docker compose run --rm analysis enrich-x-signal-analysis --input-file /workspace/research/x-signal-analysis/<run-id>/analysis_template.yaml
docker compose run --rm analysis persist-x-signal-analysis --input-file /workspace/research/x-signal-analysis/<run-id>/enriched_analysis.yaml
docker compose run --rm analysis evaluate-x-account-trust --candidate-username yuzz__ --start-date 2026-01-13 --end-date 2026-04-12
```
