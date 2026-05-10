# X API Cost Model

## Baseline

As of 2026-05-10, X's public pricing page describes pay-per-use pricing with approximate resource costs:

- Post Read: `$0.005 / resource`
- User Read: `$0.010 / resource`
- Recent Search Counts: `$0.005 / request`
- Full Archive Search Counts: `$0.010 / request`

Pricing and plan access can change. Before a paid run, prefer the current official X docs and the account's available plan over these baseline numbers.

## Discovery cost posture

Use this order:

1. local DB rows: free
2. counts requests: cheap planning probes
3. post search reads: spend only after counts look useful
4. user reads: spend only for candidate authors
5. timeline backfills: avoid during discovery

## Estimation

Estimate before fetching:

```text
estimated_cost =
  counts_requests * count_request_price
  + posts_to_fetch * post_read_price
  + authors_to_resolve * user_read_price
```

Use a conservative `authors_to_resolve` estimate of `min(posts_to_fetch, 50)` before post review, then revise after classification.

## Budget gates

If the user has not specified a budget, use a low default and show the estimate before external search. Refuse to proceed automatically when:

- projected post reads exceed the budget
- broad queries dominate the estimate
- useful candidates require timeline backfills rather than search result snippets

## Usage tracking

When available, run the repository's existing usage command before and after a paid search session:

```powershell
docker compose run --rm xcollector usage
```

Record the before/after usage snapshot in the final report if it is available.
