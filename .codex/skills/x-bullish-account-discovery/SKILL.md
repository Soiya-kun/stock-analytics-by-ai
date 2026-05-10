---
name: x-bullish-account-discovery
description: Discover X accounts that made high-conviction bullish posts about a specified Japanese listed stock during the month leading into a user-specified date, while minimizing X API cost. Use when Codex needs to search beyond already monitored accounts, plan cost-aware X recent/all search queries, identify positive stock mentions from historical posts, and produce candidate accounts to feed into x-account-trust-evaluation.
---

# X Bullish Account Discovery

## Overview

Use this skill to find candidate X accounts that strongly and positively mentioned a target stock before a target date. This is the upstream discovery workflow for `x-account-trust-evaluation`: discover candidates first, then evaluate trust only for accounts worth spending more API budget on.

The main constraint is X API cost. Always prefer local data and cheap count probes before fetching posts or user profiles.

## Workflow

1. Read [workflow.md](references/workflow.md) before using X API endpoints.
2. Normalize the user request into `stock_code`, `company_name`, optional `aliases`, `target_date`, and `lookback_days` defaulting to 35.
3. Query local repository data first:
   - `analytics.monitored_x_posts`
   - `research.tweet_stock_mentions`
   - `research.x_post_stock_signals`
   - `analytics.x_bullish_stock_signals`
4. If local data is enough, report candidates without external X search.
5. If external search is needed, read [query-strategy.md](references/query-strategy.md) and run the query planner:

```powershell
python .codex\skills\x-bullish-account-discovery\scripts\plan_discovery.py --target-date YYYY-MM-DD --stock-code CODE --company-name NAME --alias ALIAS
```

6. Use X counts endpoints before post search. Stop before fetching posts if the projected cost exceeds the user budget or if a query is too broad to be useful.
7. Fetch posts in small batches from the highest-signal query slices first. Do not look up author profiles until posts have passed the bullish-conviction review.
8. Classify posts with [conviction-rubric.md](references/conviction-rubric.md).
9. Resolve usernames only for surviving author IDs, then rank accounts by high-conviction evidence.
10. Recommend only worthwhile candidates for `x-account-trust-evaluation`.

## Cost Rules

- Treat external X API usage as scarce. Do not fetch timelines or profiles as a first step.
- Re-check current X pricing before a new paid search run when practical; the cost notes in [x-api-cost-model.md](references/x-api-cost-model.md) are a baseline, not a permanent contract.
- Counts requests are the gate. Use them to prune queries and date slices before post reads.
- Prefer fewer precise queries over broad company-name searches when the stock name is noisy.
- Use `User Read` only after post text proves the author is a candidate.
- Never backfill a candidate account timeline during discovery unless the user explicitly accepts the extra budget. Leave that to the trust-evaluation phase.

## Output

Return a compact candidate report, not a raw dump:

- search scope, date window, and query groups used
- estimated X API cost and actual known usage, if available
- ranked candidate accounts with `username`, `author_id`, post count, high-conviction count, and first/latest bullish post date
- representative post URLs or IDs with short rationale
- exclusions for noisy queries or rejected accounts
- recommended next step: `evaluate_with_x-account-trust`, `monitor_candidate`, or `discard`

## Repo Touchpoints

- Existing monitored posts: `analytics.monitored_x_posts`
- Existing tweet mentions: `research.tweet_stock_mentions`
- Canonical signals: `research.x_post_stock_signals`
- Candidate trust workflow: `.codex/skills/x-account-trust-evaluation`
- Query planner: `scripts/plan_discovery.py`

## Validation

Run the validator after editing:

```powershell
python C:\Users\djmaa\.codex\skills\.system\skill-creator\scripts\quick_validate.py .codex\skills\x-bullish-account-discovery
```
