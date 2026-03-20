# range_breakout_2018_study_20260319_151001

## Method

- Detect breakout candidates from a numeric price-only range definition.
- Label each case as `trend`, `non_trend`, `neutral`, or `incomplete` using forward returns and early drawdown only.
- Suppress repeated cases for the same code inside the cooldown window.

## Parameters

- Candidate window: 2018-07-01 to 2020-12-30
- Range lookback bars: 120
- Max range width pct: 0.35
- Breakout buffer pct: 0.02
- Min volume ratio: 1.2
- Trend confirm bars: 120
- Trend eval bars: 240
- Failure drawdown bars: 60
- Trend min return pct: 0.4
- Trend min confirm return pct: 0.2
- Failure drawdown pct: -0.1

## Label Counts

- Candidate count: 3393
- Trend: 249
- Non-trend: 2696
- Neutral: 410
- Incomplete: 38
- Trend rate excluding incomplete: 0.07421758569299552

## Chronological Trend Cases

| breakout_date | sc | name | breakout_pct | return_120d | future_max_return | reason |
| --- | --- | --- | --- | --- | --- | --- |
| 2018-07-30 | 6371 | 椿本チエイン | 10.32% | 284.06% | 419.08% | future_max_and_confirm_return_cleared_thresholds |
| 2018-08-01 | 8396 | 十八銀行 | 5.41% | 696.68% | 1105.44% | future_max_and_confirm_return_cleared_thresholds |
| 2018-08-03 | 7812 | クレステック | 3.07% | 21.35% | 45.24% | future_max_and_confirm_return_cleared_thresholds |
| 2018-08-08 | 1813 | 不動テトラ | 2.83% | 668.35% | 879.36% | future_max_and_confirm_return_cleared_thresholds |
| 2018-08-10 | 5902 | ホッカンホールディングス | 2.48% | 317.39% | 470.05% | future_max_and_confirm_return_cleared_thresholds |
| 2018-08-13 | 9063 | 岡山県貨物運送 | 3.70% | 737.91% | 1000.27% | future_max_and_confirm_return_cleared_thresholds |
| 2018-08-17 | 4800 | オリコン | 5.84% | 36.34% | 323.06% | future_max_and_confirm_return_cleared_thresholds |
| 2018-08-20 | 3776 | ブロードバンドタワー | 10.14% | 24.12% | 77.63% | future_max_and_confirm_return_cleared_thresholds |
| 2018-08-24 | 6709 | 明星電気 | 2.65% | 432.76% | 801.72% | future_max_and_confirm_return_cleared_thresholds |
| 2018-09-03 | 2164 | 地域新聞社 | 12.72% | 502.54% | 694.91% | future_max_and_confirm_return_cleared_thresholds |
| 2018-09-06 | 1776 | 三井住建道路 | 2.69% | 97.91% | 102.62% | future_max_and_confirm_return_cleared_thresholds |
| 2018-09-07 | 3698 | CRI・ミドルウェア | 7.08% | 30.23% | 59.13% | future_max_and_confirm_return_cleared_thresholds |
| 2018-09-18 | 6504 | 富士電機 | 2.01% | 265.28% | 412.05% | future_max_and_confirm_return_cleared_thresholds |
| 2018-09-19 | 9303 | 住友倉庫 | 3.13% | 80.38% | 93.42% | future_max_and_confirm_return_cleared_thresholds |
| 2018-09-20 | 4041 | 日本曹達 | 3.94% | 321.21% | 409.83% | future_max_and_confirm_return_cleared_thresholds |

## Chronological Non-Trend Cases

| breakout_date | sc | name | breakout_pct | return_120d | future_min_return | reason |
| --- | --- | --- | --- | --- | --- | --- |
| 2018-07-04 | 4355 | ロングライフホールディング | 3.99% | -38.57% | -9.87% | future_max_return_never_reached_half_trend_threshold |
| 2018-07-04 | 9265 | ヤマシタヘルスケアホールディングス | 2.77% | -33.27% | -16.76% | early_drawdown_breached_failure_threshold |
| 2018-07-05 | 3065 | ライフフーズ | 2.08% | -16.16% | -16.58% | early_drawdown_breached_failure_threshold |
| 2018-07-09 | 1805 | 飛島建設 | 5.50% | 580.57% | -18.48% | early_drawdown_breached_failure_threshold |
| 2018-07-09 | 2445 | エスアールジータカミヤ | 2.70% | -5.26% | -16.29% | early_drawdown_breached_failure_threshold |
| 2018-07-09 | 3080 | ジェーソン | 2.21% | -0.72% | -1.44% | confirm_horizon_return_non_positive |
| 2018-07-09 | 3964 | オークネット | 3.15% | -41.46% | -25.46% | early_drawdown_breached_failure_threshold |
| 2018-07-09 | 4044 | セントラル硝子 | 6.28% | -22.59% | -10.21% | early_drawdown_breached_failure_threshold |
| 2018-07-09 | 4734 | ビーイング | 3.36% | 20.07% | -12.48% | early_drawdown_breached_failure_threshold |
| 2018-07-09 | 6091 | ウエスコホールディングス | 2.00% | -27.67% | -16.78% | early_drawdown_breached_failure_threshold |
| 2018-07-10 | 2139 | 中広 | 2.09% | -33.86% | -9.52% | future_max_return_never_reached_half_trend_threshold |
| 2018-07-10 | 2923 | サトウ食品工業 | 2.12% | -18.18% | -2.86% | future_max_return_never_reached_half_trend_threshold |
| 2018-07-10 | 3905 | データセクション | 7.96% | -10.80% | -9.78% | confirm_horizon_return_non_positive |
| 2018-07-10 | 6403 | 水道機工 | 5.34% | -20.51% | -12.58% | early_drawdown_breached_failure_threshold |
| 2018-07-10 | 6965 | 浜松ホトニクス | 2.73% | -24.95% | -15.95% | early_drawdown_breached_failure_threshold |

## Notes

- This study is price-only. No fundamentals or news are used.
- `neutral` means the breakout advanced, but not enough to satisfy the long-trend rule.
- `incomplete` means there were not enough future bars to apply the label rule.
