# stock-analytics-by-ai

Kabuplus の年次 ZIP と日次 CSV を PostgreSQL に取り込み、SQL で株価分析を進めるための初期基盤です。`docker compose` で DB を起動し、ZIP または CSV を `raw` レイヤへ投入し、`analytics` レイヤの typed view から分析を始めます。株式分割・併合は raw 価格を壊さず、`analytics.inferred_price_actions` と `analytics.stock_prices_adjusted_daily` で非破壊に扱います。加えて、固定の X 監視対象アカウントについては専用 collector で当日投稿を PostgreSQL へ蓄積できます。

## Quick Start

```powershell
docker compose up -d db
docker compose run --rm importer --limit-files 5
docker compose run --rm importer
docker compose exec db psql -U stock -d stock_analytics
```

最初の 2 行は DB 起動とスモークテスト、3 行目がフル投入、4 行目で SQL に入ります。
フル投入は CSV 数が多いため長時間かかります。まず `--limit-files` で疎通確認してから本投入に進める前提です。

バックグラウンドで全量投入する場合は次です。

```powershell
docker compose up -d importer
docker compose logs -f importer
```

日次 CSV を一覧ページ確認付きで取得して投入する場合は次です。

```powershell
$env:CSVEX_BASIC_USER = "your-user"
$env:CSVEX_BASIC_PASSWORD = "your-password"
python scripts/fetch_kabuplus_daily_csv.py --date today --output-dir stock/kabuplus-2026
docker compose run --rm importer --csv-file /workspace/stock/kabuplus-2026/japan-all-stock-prices_YYYYMMDD.csv
```

`today` は JST 基準です。対象日の CSV が一覧ページに無い場合は、ダウンロードも取り込みも行わず終了します。

## Main Files

- `docker-compose.yml`: PostgreSQL と importer の構成
- `scripts/import_kabuplus.py`: ZIP または CSV を読み込む importer
- `scripts/fetch_kabuplus_daily_csv.py`: 一覧ページ確認後に日次 CSV を保存する downloader
- `scripts/analyze_range_breakout.py`: Docker 内で実行する分析 entrypoint
- `scripts/tweet_stock_research.py`: tweet 範囲抽出、株価文脈 enrich、DB 保存を行う tweet-stock analysis helper
- `scripts/x_collector.py`: X API v2 で固定監視対象の当日投稿を収集する collector
- `scripts/entry_breakout_research.py`: split 推定、補正価格監査、6か月 breakout dataset / hypothesis pipeline
- `sql/init`: スキーマ、テーブル、helper 関数、typed view
- `sql/migrations/20260404_entry_breakout_setup.sql`: 既存 `pgdata` へ研究用オブジェクトを追い適用する migration
- `sql/migrations/20260407_x_collector_setup.sql`: 既存 `pgdata` へ X collector 用オブジェクトを追い適用する migration
- `sql/migrations/20260407_tweet_stock_analysis_setup.sql`: 既存 `pgdata` へ tweet-stock analysis 用オブジェクトを追い適用する migration
- `sql/queries/sample_queries.sql`: すぐ叩ける SQL 例
- `docs/setup.md`: 起動とリセット手順
- `docs/data-catalog.md`: データ種別と curated view
- `docs/analysis-principles.md`: 今後の分析原則の保管場所
- `docs/skills/README.md`: project skill の運用ルール
- `.codex/skills/stock-analysis-workflow`: このリポジトリ専用の skill

## Data Model

- `ingest.kabuplus_files`: CSV ごとの投入ステータス
- `raw.kabuplus_records`: 元データを `jsonb` で保持する raw レイヤ
- `analytics.inferred_price_actions`: 整数倍率ジャンプから推定した split / reverse split event
- `analytics.stock_prices_adjusted_daily`: raw と adjusted の両方を持つ分析用 price view
- `research.*`: 6か月 breakout の run / case / hypothesis を永続化する研究レイヤ
- `ingest.x_*` / `raw.x_*`: 固定監視対象アカウントと X 投稿収集の運用レイヤ
- `research.tweet_*`: tweet と日本株コードの紐付け、および価格反応の durable 分析レイヤ

初期の分析原則として「数年レンジ相場の上抜け」と「6か月レンジ上抜け entry study」を追加済みです。今後は `docs/analysis-principles.md` に原則を明文化し、その内容に合わせて SQL やスクリプトを追加していきます。

## X Monitoring

X collector は `Home timeline` ではなく、監視対象ごとの `/2/users/:id/tweets` を 1 時間おきに巡回します。`OAuth 1.0a User Context` を前提にしているため、専用の認証アカウントが監視対象をフォローしていれば、非公開アカウントも収集対象にできます。

初回セットアップは次です。

```powershell
docker compose up -d db
docker compose exec -T db psql -U stock -d stock_analytics -f /workspace/sql/migrations/20260407_x_collector_setup.sql
docker compose exec db psql -U stock -d stock_analytics -c "insert into ingest.x_monitored_accounts (target_username) values ('example_user') on conflict do nothing;"
docker compose run --rm xcollector sync-targets
docker compose run --rm xcollector poll-once
docker compose up -d xcollector
```

必要な認証情報は `.env` に次を設定します。

```powershell
X_API_KEY=...
X_API_KEY_SECRET=...
X_ACCESS_TOKEN=...
X_ACCESS_TOKEN_SECRET=...
X_BEARER_TOKEN=...
X_COLLECT_INTERVAL_SECONDS=3600
```

`X_BEARER_TOKEN` は `usage` サブコマンドで `/2/usage/tweets` を取得するときだけ使います。collector 本体の収集は user context 認証だけで動きます。

分析前に「今この時点で tweet が十分そろっているか」を確認したい場合は次を使います。

```powershell
docker compose run --rm xcollector ensure-current --target-username example_user
```

このコマンドは `ingest.x_timeline_state.last_success_at` を見て、直近 60 分以内に増分取得が成功していれば X API を呼ばずに終了します。60 分を超えて古い、または incremental poll がまだ一度も走っていない場合だけ、`since_id` ベースで不足分を取りに行きます。

## Tweet Stock Analysis

tweet を date range 単位で分析し、日本の上場会社名・会社コード・tweet URL・株価反応フラグを DB に保存するワークフローも追加しました。分析は `docker compose run --rm analysis ...` で再現でき、tweet 抽出、株価文脈 enrich、最終 persist を段階的に進めます。

```powershell
docker compose exec -T db psql -U stock -d stock_analytics -f /workspace/sql/migrations/20260407_tweet_stock_analysis_setup.sql
docker compose run --rm xcollector ensure-current --target-username 4th_skywalker
docker compose run --rm analysis prepare-tweet-analysis --start-date 2026-04-07 --end-date 2026-04-07 --target-username 4th_skywalker
docker compose run --rm analysis enrich-tweet-analysis --input-file /workspace/research/tweet-stock-analysis/<run-id>/analysis_template.yaml
docker compose run --rm analysis persist-tweet-analysis --input-file /workspace/research/tweet-stock-analysis/<run-id>/enriched_analysis.yaml
```

## Analysis

分析スクリプトも Docker 内で実行します。

```powershell
docker compose run --rm analysis grid-search --processes 4
docker compose run --rm analysis scan
docker compose run --rm analysis label-study
docker compose run --rm analysis infer-price-actions
docker compose run --rm analysis prepare-adjusted-prices
docker compose run --rm analysis build-entry-dataset
docker compose run --rm analysis mine-entry-hypotheses
docker compose run --rm analysis evaluate-entry-hypotheses
```

6か月 breakout 研究の標準順は次です。

```powershell
docker compose run --rm analysis infer-price-actions
docker compose run --rm analysis prepare-adjusted-prices
docker compose run --rm analysis build-entry-dataset
docker compose run --rm analysis mine-entry-hypotheses
docker compose run --rm analysis evaluate-entry-hypotheses
```
