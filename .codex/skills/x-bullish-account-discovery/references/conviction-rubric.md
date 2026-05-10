# Conviction Rubric

## Labels

Use these labels for each post about the target stock:

- `high_conviction_bullish`: clear forward-looking claim that the stock should rise, is a buy, is a main position, has a meaningful catalyst, or is expected to break out.
- `medium_bullish`: positive but less decisive; watchlist, light accumulation, mild catalyst interest, or conditional bullishness.
- `weak_or_neutral`: news sharing, price/volume fact, vague interest, chart observation without stance, or non-committal mention.
- `exclude`: not the target company, negative, sarcastic, pure repost, unrelated hashtag spam, after-the-fact victory lap outside the search intent, or bot-like aggregation.

## High-conviction evidence

Prefer accounts whose posts include one or more:

- explicit action: buying, bought, adding, holding as main position
- explicit forecast: should rise, target price, breakout expectation, big upside
- explicit catalyst thesis: earnings, upward revision, order win, policy tailwind, sector rotation, valuation gap
- timing: posted before the target date and before the move being investigated
- repeated reinforcement: multiple posts across days without being spammy
- accountable wording: a concrete reason, scenario, or risk/reward statement

## Downgrades

Downgrade or reject:

- "監視", "メモ", "気になる" with no thesis
- simple news headline forwarding
- screenshots with no interpretable text unless the image is explicitly reviewed
- posts after the relevant stock move when the task is to find prior conviction
- engagement bait without a stock-specific reason
- accounts that mention many tickers in one generic list

## Account scoring

Start from post-level labels, then rank accounts:

```text
account_score =
  3 * high_conviction_bullish_count
  + 1 * medium_bullish_count
  + 1 * unique_bullish_day_count
  + 1 * earliest_bonus
  - 2 * spam_or_list_penalty
```

Use this as a ranking aid, not a mechanical verdict. A single excellent high-conviction post with a clear thesis can outrank several weak watchlist mentions.

## Rationale format

For every surfaced account, write:

- what the account claimed
- why it is bullish rather than neutral
- whether the claim was before the user-specified date
- whether the account deserves trust evaluation or only monitoring
