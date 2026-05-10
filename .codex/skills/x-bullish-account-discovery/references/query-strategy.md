# Query Strategy

## Query families

Generate queries from narrow to broad:

1. Stock-code direct:
   - `7203 lang:ja -is:retweet`
   - `7203 株 lang:ja -is:retweet`
   - `7203 買い OR 上がる OR 強い lang:ja -is:retweet`
2. Exact company or alias:
   - `"トヨタ自動車" lang:ja -is:retweet`
   - `"トヨタ" 株 lang:ja -is:retweet`
3. Bullish-conviction phrases:
   - `"トヨタ" 買い lang:ja -is:retweet`
   - `"トヨタ" 本命 lang:ja -is:retweet`
   - `"トヨタ" 初動 lang:ja -is:retweet`
   - `"トヨタ" 上値 OR ブレイク OR 仕込み lang:ja -is:retweet`
4. Catalyst phrases when relevant:
   - 決算, 上方修正, 材料, 増配, 受注, 提携, 国策, 半導体, 防衛, AI

Use exact phrase quotes for company names and aliases that collide with common words. Use the stock code when company names are too noisy.

## Positive lexicon

Prefer terms that imply forward-looking conviction:

- 買い, 買った, 買い増し, 仕込み, 本命, 主力
- 上がる, 上抜け, ブレイク, 初動, 強い, 化ける
- 目標, テンバガー, 大相場, 来る, 確信
- 決算期待, 上方修正期待, 増配期待, 材料, 需給改善

Weak terms like `気になる`, `監視`, `メモ`, `チェック` can be used only after tighter direct queries fail.

## Noise controls

Add exclusions only after checking what they remove:

- `-is:retweet` by default
- Consider `-is:reply` only when replies are noisy; replies can contain useful early conviction
- Exclude common false-positive aliases with `-"..."` terms
- Avoid broad market words without the stock code or exact company/alias

## Counts-first pruning

For each query family, collect daily counts before fetching posts. Keep:

- low-count direct hits even when sparse
- medium-count bullish phrase queries
- high-count queries only if the top terms are very stock-specific

Drop or tighten:

- company aliases with generic meanings
- queries where most hits are news headlines or official PR reposts
- queries with large counts but no conviction terms

## Fetch order

Fetch in this order:

1. direct stock code plus bullish terms
2. exact company/alias plus bullish terms
3. direct stock code without bullish terms
4. exact company/alias without bullish terms
5. weak watchlist terms only when the first four are too sparse

Within each query, fetch from the period closest to `target_date` first unless the user asks for earliest discovery.
