# Setup

## Components

- `db`: PostgreSQL 16. Data is persisted in `./pgdata`.
- `importer`: Python container that mounts the repository and imports ZIP files or a direct CSV file from `./stock`.
- `xcollector`: Python container that monitors configured X accounts and stores posts into PostgreSQL.
- `raw.kabuplus_records`: immutable raw layer stored as `jsonb`.
- `raw.x_posts`: one row per collected X post. Re-fetches update the latest payload and public metrics.
- `ingest.x_*`: monitored-account config, timeline checkpoint state, poll run audit log, and optional usage snapshots.
- `research.tweet_*`: durable tweet-to-stock analysis runs and `tweet x company-code` mention rows.
- `research.x_*`: canonical `post x stock-code` signal rows, review state, and account trust scores.
- `analytics.inferred_price_actions`: inferred split / reverse-split events derived from one-day integer OHLC jumps.
- `analytics.stock_prices_adjusted_daily`: non-destructive adjusted OHLCV view with raw and adjusted columns.
- `analytics.x_bullish_stock_signals`: canonical bullish `post x stock-code` signal view.
- `analytics.x_account_trust_latest`: latest trust score per candidate account.
- `research.*`: durable breakout study runs, cases, and hypothesis records.
- `analytics.*`: typed SQL views for the main daily, weekly, and monthly datasets.

## Boot

1. Copy `.env.example` to `.env` if you want to change the default credentials.
2. Start PostgreSQL.

```powershell
docker compose up -d db
```

3. Run a smoke import on a small sample.

```powershell
docker compose run --rm importer --limit-files 5
```

4. Run the full import.

```powershell
docker compose run --rm importer
```

The full import is long-running because the yearly ZIP files contain many CSV files. Use `--limit-files` or `--dataset` first, then run the full load when you are ready.

## X Monitoring Flow

1. Add your X credentials to `.env`.

```powershell
X_API_KEY=...
X_API_KEY_SECRET=...
X_ACCESS_TOKEN=...
X_ACCESS_TOKEN_SECRET=...
X_BEARER_TOKEN=...
X_COLLECT_INTERVAL_SECONDS=3600
```

2. If `pgdata` already exists, apply the incremental migration for the X collector objects.

```powershell
docker compose exec -T db psql -U stock -d stock_analytics -f /workspace/sql/migrations/20260407_x_collector_setup.sql
```

If `pgdata` already exists and you want benchmark / candidate trust scoring, also apply:

```powershell
docker compose exec -T db psql -U stock -d stock_analytics -f /workspace/sql/migrations/20260412_x_account_trust_setup.sql
```

3. Insert the fixed monitored usernames.

```powershell
docker compose exec db psql -U stock -d stock_analytics -c "insert into ingest.x_monitored_accounts (target_username) values ('example_user') on conflict do nothing;"
```

Candidate evaluation uses the same table with `account_role='candidate'`.

```powershell
docker compose exec db psql -U stock -d stock_analytics -c "insert into ingest.x_monitored_accounts (target_username, account_role) values ('yuzz__', 'candidate') on conflict (target_username) do update set account_role = excluded.account_role;"
```

4. Resolve usernames to user IDs and verify access through the authenticated X account.

```powershell
docker compose run --rm xcollector sync-targets
```

Candidate only:

```powershell
docker compose run --rm xcollector sync-targets --account-role candidate
```

5. Run a one-shot poll to capture only the current JST day's posts on first load.

```powershell
docker compose run --rm xcollector poll-once
```

6. Start the long-running collector. It polls immediately on startup, then aligns later polls to the next hourly JST boundary.

```powershell
docker compose up -d xcollector
docker compose logs -f xcollector
```

7. Optionally store usage snapshots when `X_BEARER_TOKEN` is configured.

```powershell
docker compose run --rm xcollector usage
```

Collector state lives in:

- `ingest.x_monitored_accounts`: configured usernames and latest resolution/access state
- `ingest.x_monitored_accounts.account_role`: `benchmark` / `candidate`
- `ingest.x_timeline_state`: `since_id` checkpoint and per-target polling status
- `ingest.x_poll_runs`: aggregate audit rows for each polling run
- `ingest.x_usage_daily`: optional usage snapshots from `/2/usage/tweets`
- `analytics.monitored_x_posts`: typed query view for collected posts

## Tweet Stock Analysis Flow

1. If `pgdata` already exists, apply the incremental migration for the tweet-analysis objects.

```powershell
docker compose exec -T db psql -U stock -d stock_analytics -f /workspace/sql/migrations/20260407_tweet_stock_analysis_setup.sql
```

2. Prepare the durable tweet-analysis template for a date range.

```powershell
docker compose run --rm analysis prepare-tweet-analysis --start-date 2026-04-07 --end-date 2026-04-07 --target-username 4th_skywalker
```

3. Open `research/tweet-stock-analysis/<run-id>/analysis_template.yaml`, identify Japanese listed companies and codes for each tweet, and fill `mentions`.

4. Enrich the annotated file with stock price / volume context.

```powershell
docker compose run --rm analysis enrich-tweet-analysis --input-file /workspace/research/tweet-stock-analysis/<run-id>/analysis_template.yaml
```

5. Review `enriched_analysis.yaml`, set `volume_spike_flag`, `price_jump_flag`, and the rationale fields, then persist the result.

```powershell
docker compose run --rm analysis persist-tweet-analysis --input-file /workspace/research/tweet-stock-analysis/<run-id>/enriched_analysis.yaml
```

6. Query the saved rows.

```powershell
docker compose exec db psql -U stock -d stock_analytics -c "select run_id, sc, company_name, volume_spike_flag, price_jump_flag from research.tweet_stock_mentions order by created_at desc limit 20;"
```

## X Account Trust Flow

1. Prepare a review batch for unanalyzed posts.

```powershell
docker compose run --rm analysis prepare-x-signal-analysis --start-date 2026-01-13 --end-date 2026-04-12 --account-role all
```

2. Open `research/x-signal-analysis/<run-id>/analysis_template.yaml` and fill `signals` for each post.

3. Add market context.

```powershell
docker compose run --rm analysis enrich-x-signal-analysis --input-file /workspace/research/x-signal-analysis/<run-id>/analysis_template.yaml
```

4. Persist canonical signals and review state.

```powershell
docker compose run --rm analysis persist-x-signal-analysis --input-file /workspace/research/x-signal-analysis/<run-id>/enriched_analysis.yaml
```

5. Evaluate a candidate account against benchmark clusters.

```powershell
docker compose run --rm analysis evaluate-x-account-trust --candidate-username yuzz__ --start-date 2026-01-13 --end-date 2026-04-12
```

## Daily CSV Flow

Use the listing-first downloader when you want the current KABU+ daily prices CSV.

```powershell
$env:CSVEX_BASIC_USER = "your-user"
$env:CSVEX_BASIC_PASSWORD = "your-password"
python scripts/fetch_kabuplus_daily_csv.py --date today --output-dir stock/kabuplus-2026
```

The downloader always checks `https://csvex.com/kabu.plus/csv/japan-all-stock-prices/daily/` before downloading. If the target date is not listed yet, it exits without writing a file.

After a CSV is saved, import it with:

```powershell
docker compose run --rm importer --csv-file /workspace/stock/kabuplus-2026/japan-all-stock-prices_YYYYMMDD.csv
```

The CSV mode stores `source_zip = 'kabuplus-daily-csv'` and keeps `source_entry` as the repository-relative CSV path so reruns stay idempotent.

To run the full import in the background:

```powershell
docker compose up -d importer
docker compose logs -f importer
```

When the import service exits, the load is finished. Progress can also be checked with:

```powershell
docker compose exec db psql -U stock -d stock_analytics -c "select * from analytics.import_status order by dataset_key;"
```

## チャート付き相場史

本文と日足がスクロールで連動するスマホ向けHTMLは、[更新手順](stock-history-reader.md) に従って生成・公開します。キオクシアは既存サイトの `market-history/kioxia/`、イビデンは `market-history/ibiden/` へ配置し、日次ページや全日足HTMLとは別に管理します。

## 個別銘柄の全日足

`config/stock-market-history.json` に、相場史を書く目的・長期／中期／短期の視点・ユーザー指定の分析順と記憶を保存します。`scripts/generate_stock_history.py` はローカルPostgreSQLを読取専用で参照し、銘柄の全日足を単独HTMLへ出力します。500本や1年で打ち切らず、元データと同数・同日付の足を含むことを検証します。公式確認値を優先して表示最終日までの分割を反映し、当時の未調整OHLCVも保持します。

Plotly 3.1.0の公式配布 `https://cdn.plot.ly/plotly-3.1.0.min.js` を `outputs/stock-history/vendor/plotly-3.1.0.min.js` に配置してから実行します。生成時にSHA-256 `122e3be346d66616944d0b83eaaf7242581508c3c1cfa0995a17af0d83eff770` と照合し、HTML内へ埋め込むため閲覧時のCDN接続は不要です。

```powershell
docker compose run --rm -T --entrypoint python analysis `
  /workspace/scripts/generate_stock_history.py --sc 285A
```

`outputs/stock-history/285A/index.html` がチャート、`prices.json` が原値／調整値、`manifest.json` が件数・期間・分割根拠・hashです。キオクシアは公式上場日2024-12-18との一致も検証します。内部ブラウザでHTTP表示する場合は `python -m http.server 8877 --bind 127.0.0.1 --directory outputs/stock-history` でこの生成物のディレクトリだけを配信し、`http://127.0.0.1:8877/285A/` を開きます。AWSへの公開や定期更新は行いません。

## Daily Stock Review Pipeline

公開ページの定期更新は `StockAnalyticsDailyReview` スタックのAWS Lambda `stock-analytics-daily-review` が担当します。EventBridge Schedulerが平日16:40・17:10・18:10 JSTに実行します。PCやDocker、個人のAWS SSOログインには依存しません。VPC/NAT Gatewayや常設DBは使いません。

Lambdaは初回だけローカルDBから複製した価格履歴を、非公開S3の `state/prices.sqlite.gz` に保存します。SQLiteはLambda内の一時作業ファイルとして使い、永続化はS3が担当します。KABU+当日一覧を先に確認し、対象日が無ければ過去日に切り替えず `skipped / csv_not_published` として終了します。公開済みの日は再取得を省略でき、未反映の公開済みCSVがあれば最大31日分を補います。

同時実行はS3の条件付き書き込みによる `control/run-lock.json` のリースで排他します。リースはLambdaの15分タイムアウトより長い960秒で失効し、異常終了時も次の定期確認で回復できます。公開済み状態と公開オブジェクトのhashが一致する日は `already_current` として再取得・再公開を省略します。

取り込んだCSVは非公開S3の `raw/daily/` に保持します。初回は `analytics.effective_price_actions` の公式確認値優先で分割補正を引き継ぎます。以後の新規分割は、KABU+の株価・前日比が示す比較基準と前日終値の比から暫定補正を追加します。OHLCの急騰急落だけで分割を推測しません。既に公式確認済みの補正は優先し、暫定補正は公開payloadにも件数を保持します。後日公式値を更新する場合はローカルDBで確認した上で価格履歴を再export・deployします。

対象日・CSV件数・価格・重複・現在値と最新足の一致を検証してから、既存サイトの `daily/index.html` だけを公開します。CloudFrontキャッシュ無効化の完了と公開HTTPS本文のSHA-256一致まで成功条件に含みます。生CSV、SQLite、認証情報、実行記録は公開しません。実行ロールの公開先権限はこの1オブジェクトと対象CloudFront配信に限定します。KABU+ Basic認証はSecrets Managerに保存し、Lambda環境変数やCDKテンプレートへ値を書きません。

### 分割補正のDB定義

新規DBには `sql/init/05_price_action_overrides.sql` が公式確認値を優先するテーブルとビューを作成します。既存DBには、seedのexportより先に次の定義を適用します。

```powershell
docker compose exec -T db psql -v ON_ERROR_STOP=1 -U stock -d stock_analytics -f /workspace/sql/migrations/20260928_verified_price_actions.sql
docker compose exec -T db psql -v ON_ERROR_STOP=1 -U stock -d stock_analytics -f /workspace/sql/init/10_views.sql
```

`20261013_kioxia_shipping_verified_splits.sql` と `20261107_daily_review_verified_splits.sql` は確認済みの個別補正です。後者は2026年9月29日のKABU+原値との一致を検査するため、その日のCSVを取り込んだDBへ適用します。生の株価は書き換えません。

### 初回デプロイ・再デプロイ

デプロイ作業だけはAWSへログインした端末で実行します。平常運用のLambdaはこのSSO認証を使いません。初回は `infra/static-site` で `npm ci` を実行し、既存の `StockAnalyticsStaticSite` スタックを用意してから、リポジトリルートで次の操作を行います。

```powershell
aws sso login --profile 373117960731_AdministratorAccess

# 毎回新しい出力ディレクトリを指定。PostgreSQLは読み取り専用で参照する。
docker compose run --rm -T --entrypoint python analysis `
  /workspace/scripts/export_daily_stock_seed.py `
  --output-dir /workspace/outputs/daily-stock-cloud/seed-YYYYMMDD-v1

python -X utf8 scripts/deploy_daily_stock_cloud.py `
  --seed-dir outputs/daily-stock-cloud/seed-YYYYMMDD-v1
```

デプロイスクリプトはCDK build/synth/diff/deploy、Docker環境からの認証情報登録、価格履歴のアップロード、実LambdaによるKABU+取得・公開、別経路でのHTTPS本文検証を実行します。検証に成功してから定期実行を有効にします。`.daily-content/` はコード3ファイルとチャートテンプレート1ファイルだけを梱包し、`.env`・DB・研究生成物は含めません。出力は `infra/static-site/cdk.daily.out/deployed-outputs.json` とseedディレクトリの `deployment.json` です。

### 状態・障害の確認

非公開データバケット名とロググループ名はCDK出力にあります。`status/latest-run.json` が最新結果、`status/last-published.json` が最後の公開検証成功、`status/runs/<request-id>.json` が各実行記録です。失敗時は `phase` と例外クラスだけを記録し、秘密情報や外部例外メッセージは残しません。LambdaのエラーはCloudWatch Alarm、配送・再試行失敗はSQSのdead-letter queueで確認できます。

データバケットとSecretはスタック削除時も保持します。日次取得CSVは保持し、価格状態はS3バージョニングで復旧可能です。31日以上更新できず古い状態になった場合は、ローカル価格履歴を再exportしてから再デプロイします。

### ローカルDockerへの取り込み

既存のローカルスケジュール `kabuplus-daily-import`（平日16:40 JST）は次を実行します。こちらは従来通りPC・アプリ・Dockerの稼働が必要です。公開はLambdaが独立して担当し、ローカル処理はAWS APIやSSOを使いません。

```powershell
python -X utf8 scripts/daily_stock_review.py import --date today
```

`run`（既定）にすればチャートのローカル生成・検証も行います。`--skip-fetch` は指定日の取り込み完了を必須にします。`--min-turnover-million-yen` / `--limit` / `--lookback-bars` で表示条件を変更できます。実行結果とHTMLは `outputs/daily-stock-review/` に保存します。

## Query

Open `psql` inside the database container:

```powershell
docker compose exec db psql -U stock -d stock_analytics
```

Examples:

```sql
select * from analytics.import_status order by dataset_key;
select * from analytics.stock_prices_daily where sc = '1301' order by trade_date desc limit 5;
```

Sample SQL is stored in `sql/queries/sample_queries.sql`.

## Analysis

Run the multi-year range-breakout grid search inside Docker:

```powershell
docker compose run --rm analysis grid-search --processes 4
```

Scan the latest market day for current candidates:

```powershell
docker compose run --rm analysis scan --lookback-years 3 --max-range-width-pct 0.50 --breakout-buffer-pct 0.01 --min-volume-ratio 1.50
```

Run the durable 2018+ breakout labeling study:

```powershell
docker compose run --rm analysis label-study
```

`grid-search` and `scan` write under `outputs/range-breakout/`. `label-study` writes durable artifacts under `research/range-breakout-2018-study/`.

Run the non-destructive split-adjustment and 6-month breakout research pipeline inside Docker:

```powershell
docker compose run --rm analysis infer-price-actions
docker compose run --rm analysis prepare-adjusted-prices
docker compose run --rm analysis build-entry-dataset
docker compose run --rm analysis mine-entry-hypotheses
docker compose run --rm analysis evaluate-entry-hypotheses
```

- `infer-price-actions`: writes inferred split / reverse-split events into `analytics.inferred_price_actions`.
- `prepare-adjusted-prices`: audits the adjusted-price layer and writes md/yaml under `research/inferred-price-actions/`.
- `build-entry-dataset`: writes durable md/yaml/csv under `research/entry-breakout-6m/` and persists rows into `research.entry_study_runs` / `research.entry_cases`.
- `mine-entry-hypotheses`: mines interpretable threshold rules from the train split and stores them in `research.entry_hypotheses`.
- `evaluate-entry-hypotheses`: applies the mined rules to the validation split and writes durable evaluation artifacts.

When `pgdata` already exists, apply the incremental migration before the first run of the new pipeline:

```powershell
docker compose exec -T db psql -U stock -d stock_analytics -f /workspace/sql/migrations/20260404_entry_breakout_setup.sql
```

## Useful Import Options

- `--dataset japan-all-stock-prices/daily`: import only one dataset family.
- `--limit-zips 1`: scan only one yearly ZIP file.
- `--limit-files 20`: import only the first 20 CSV files.
- `--force`: delete and re-import files even if they were already completed.
- `--csv-file /workspace/stock/kabuplus-2026/japan-all-stock-prices_YYYYMMDD.csv`: import one CSV directly.
- `--csv-dataset japan-all-stock-prices/daily`: dataset key used with `--csv-file`.
- `--csv-source kabuplus-daily-csv`: synthetic source label used with `--csv-file`.

Example:

```powershell
docker compose run --rm importer --dataset japan-all-stock-prices/daily --limit-zips 1 --force
```

## Reset

To rebuild the database from scratch, stop containers and remove `./pgdata`.

```powershell
docker compose down
Remove-Item -Recurse -Force pgdata
docker compose up -d db
docker compose run --rm importer
```

## Notes

- ZIP files are read directly. There is no unzip step on disk.
- CSVs are decoded as `cp932`.
- Initial SQL in `sql/init` is applied when PostgreSQL starts with an empty `./pgdata`.
- Existing `./pgdata` does not replay `sql/init`; use `sql/migrations/20260404_entry_breakout_setup.sql` to add the research objects to an already-populated DB.
- Existing `./pgdata` does not replay `sql/init`; use the dated migration files such as `sql/migrations/20260404_entry_breakout_setup.sql` and `sql/migrations/20260407_x_collector_setup.sql` to add new objects to an already-populated DB.
