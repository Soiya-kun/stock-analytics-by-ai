# Discovery Workflow

## Inputs

Require or infer:

- `target_date`: JST date supplied by the user
- `stock_code`: Japanese listed stock code
- `company_name`: official or common listed-company name
- `aliases`: short names, brand names, old names, ticker-like forms, or investor slang
- `lookback_days`: default 35
- `max_budget_usd`: default to a conservative budget when the user does not specify one

Use a JST date window of `[target_date - lookback_days, target_date]`. For X API UTC timestamps, convert the start to `00:00:00 JST` and the end to `23:59:59 JST`, then express both in UTC.

## Local-first search

Before using X API search, inspect already collected data. Useful patterns:

```sql
select target_username, created_at_jst, post_id, tweet_url, text
from analytics.monitored_x_posts
where post_date_jst between :start_date and :target_date
  and (
    text ilike '%' || :stock_code || '%'
    or text ilike '%' || :company_name || '%'
  )
order by created_at_jst;
```

```sql
select target_username, sc, company_name, tweet_url, post_created_at, extraction_rationale
from research.tweet_stock_mentions
where post_date_jst between :start_date and :target_date
  and (sc = :stock_code or company_name ilike '%' || :company_name || '%')
order by post_created_at;
```

```sql
select target_username, sc, company_name, post_created_at_jst, tweet_url, signal_confidence, signal_rationale
from analytics.x_bullish_stock_signals
where post_date_jst between :start_date and :target_date
  and (sc = :stock_code or company_name ilike '%' || :company_name || '%')
order by post_created_at_jst;
```

If local results already include multiple high-conviction posts from accounts not yet trusted, stop external discovery and recommend trust evaluation.

## External X search stages

1. Build query families from `query-strategy.md`.
2. Use counts by day for each query family.
3. Drop broad queries that produce too many irrelevant posts.
4. Fetch a first tranche from:
   - direct stock-code queries
   - exact company/alias queries with bullish verbs
   - the 7-14 days closest to the target date if the 35-day window is large
5. Review post text before resolving author usernames.
6. Resolve only surviving authors.
7. Optionally fetch one narrow follow-up batch for authors or aliases that look promising.

## Stopping conditions

Stop early when:

- the local DB has enough candidates
- counts imply the budget will be exceeded
- fetched posts are mostly irrelevant after the first tranche
- at least 5-10 strong candidate accounts are found
- only weak/neutral posts remain after query tightening

## Handoff

For each account worth deeper work, add it as a `candidate` only after the user accepts the extra collection cost. Then use `x-account-trust-evaluation` to backfill and score the account.
