# Inferred Price Action Audit

## Method

- Raw prices remain untouched in `analytics.stock_prices_daily`.
- One-day integer jump inference uses only `open/high/low/close` against the previous close.
- Inferred events are stored in `analytics.inferred_price_actions` and applied later in `analytics.stock_prices_adjusted_daily`.

## Counts

- Event count: 1374
- Split: 1182
- Reverse split: 192
- Matched official same date: 0
- Inferred only same date: 1374
- Official only same date: 0

## Inferred-Only Samples

| action_date | sc | action_type | integer_factor |
| --- | --- | --- | --- |
| 2018-01-12 | 3452 | split | 2 |
| 2018-01-29 | 3484 | split | 4 |
| 2018-01-29 | 3750 | split | 5 |
| 2018-01-29 | 3926 | split | 3 |
| 2018-01-29 | 3988 | split | 2 |
| 2018-01-29 | 4344 | split | 2 |
| 2018-01-29 | 6188 | split | 3 |
| 2018-01-29 | 6552 | split | 2 |
| 2018-01-29 | 7810 | split | 2 |
| 2018-02-09 | 2930 | split | 3 |
| 2018-02-26 | 2162 | split | 2 |
| 2018-02-26 | 2406 | split | 2 |
| 2018-02-26 | 2471 | split | 5 |
| 2018-02-26 | 2736 | reverse_split | 10 |
| 2018-02-26 | 3045 | split | 2 |
| 2018-02-26 | 3168 | split | 2 |
| 2018-02-26 | 3415 | split | 3 |
| 2018-02-26 | 3433 | split | 4 |
| 2018-02-26 | 3557 | split | 2 |
| 2018-02-26 | 3558 | split | 2 |

## Official-Only Samples

| action_date | sc | split_ratio |
| --- | --- | --- |
