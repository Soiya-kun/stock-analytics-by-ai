# stock-analytics-by-ai

Kabuplus の年次 ZIP と日次 CSV を PostgreSQL に取り込み、SQL で株価分析を進めるための初期基盤です。`docker compose` で DB を起動し、ZIP または CSV を `raw` レイヤへ投入し、`analytics` レイヤの typed view から分析を始めます。株式分割・併合は raw 価格を壊さず、`analytics.inferred_price_actions` と `analytics.stock_prices_adjusted_daily` で非破壊に扱います。加えて、固定の X 監視対象アカウントについては専用 collector で投稿を PostgreSQL へ蓄積し、benchmark / candidate の役割を分けた trust evaluation まで進められます。

## 個別銘柄の相場史

個別銘柄ごとに、決算・業界ニュース・指定投資家の投稿と当時価格を時系列で記録し、同じ文書へ追記します。

| 銘柄・対象期間 | チャート付きHTML | 追記用の文書 |
| --- | --- | --- |
| キオクシア・上場から2026年11月まで | [読む](https://d3fvqjccwn6gec.cloudfront.net/market-history/kioxia/) | [Page](https://chatgpt.com/space/page_4209db1a12688191816d1c17d8d03f48) |
| イビデン・2025年1月から2026年11月まで | [読む](https://d3fvqjccwn6gec.cloudfront.net/market-history/ibiden/) | [Page](https://chatgpt.com/space/page_1e3b2dd4ed3081918c01290bb9c2a7e0) |

スマホ向けHTMLは画面上部に日足を固定し、読んでいる節に合わせて表示期間が切り替わります。表は見出し付きのカードに並べ替え、横スクロールなしで読めます。この表示は今後追加する銘柄にも共通です。本文は当時価格、チャートは2026年11月6日基準の分割調整後です。タップで両方の価格を確認できます。[更新手順](docs/stock-history-reader.md) を参照してください。

この相場史の執筆・推敲には [natural-japanese](.codex/skills/natural-japanese/SKILL.md) を使います。[文体方針](style-profile.md) に従って当時の事実と判断材料を直接書き、読み手の誤解を先回りした念押しや弁明を省きます。数値や出典、事実と考察の区別を保ちながら、自然な日本語に整えます。

業績や市況予想は「いつ公表された情報で、何年何月の期をどう予想していたか」を明記します。比較対象の期、通期・四半期単独・累計、実績と予想も区別します。「次の四半期」「翌期」だけでは書きません。保存本数・取得件数・集計手順や更新予定などの作業説明は本文から省き、出典や価格基準など内容の理解に必要な情報を残します。詳しい執筆ルールは [分析候補と優先順](config/stock-market-history.json) に保存しています。

[分析候補と優先順](config/stock-market-history.json) に、長期・中期・短期の視点で値動きと当時の判断を記録する方針を保存しています。[キオクシアの全日足HTML](outputs/stock-history/285A/index.html) は、分割調整後／当時価格の切り替え・期間拡大・対数表示に対応します。生成方法は [セットアップ](docs/setup.md#個別銘柄の全日足) を参照してください。

上値のブレイクと局所的な天井付近では、当時のニュースとXの強気・警戒・目標の変化を追います。高値より前の発言と、下落後の諦めや撤退を分けて記録します。

## 毎日の株価確認

[日次株価ページ](https://d3fvqjccwn6gec.cloudfront.net/daily/) は、AWS Lambdaが平日16:40 JSTに更新します。17:10・18:10にも再確認します。KABU+の当日CSV一覧確認・取得、非公開S3の価格履歴更新、売買代金30億円以上の上位300銘柄・250営業日のチャート生成、既存CloudFrontへの公開までAWS内で完結し、PC起動やSSOログインは不要です。未公開日は前回ページを保持し、公開後はHTTPS本文のSHA-256一致を確認します。

ローカルの `Kabuplus daily import` は従来通り平日16:40にDocker PostgreSQLへ当日株価を取り込みます。こちらはPC・アプリ・Dockerの稼働が必要ですが、AWS認証は使いません。

```powershell
# ローカルDBへの取り込みだけを実行
python -X utf8 scripts/daily_stock_review.py import --date today

# 取り込み済みの株価でローカルHTMLを確認（公開は行わない）
python -X utf8 scripts/daily_stock_review.py --date 2026-11-06 --skip-fetch
```

初回デプロイ、価格履歴の準備、認証情報の登録、障害確認は [Setup](docs/setup.md#daily-stock-review-pipeline) を参照してください。過去研究の自動再開やX/Jevの追加取得は行いません。

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
- `scripts/x_account_trust_research.py`: X signal canonicalization と candidate trust scoring helper
- `scripts/x_collector.py`: X API v2 で固定監視対象の当日投稿を収集する collector
- `scripts/entry_breakout_research.py`: split 推定、補正価格監査、6か月 breakout dataset / hypothesis pipeline
- `sql/init`: スキーマ、テーブル、helper 関数、typed view
- `sql/migrations/20260404_entry_breakout_setup.sql`: 既存 `pgdata` へ研究用オブジェクトを追い適用する migration
- `sql/migrations/20260407_x_collector_setup.sql`: 既存 `pgdata` へ X collector 用オブジェクトを追い適用する migration
- `sql/migrations/20260407_tweet_stock_analysis_setup.sql`: 既存 `pgdata` へ tweet-stock analysis 用オブジェクトを追い適用する migration
- `sql/migrations/20260412_x_account_trust_setup.sql`: 既存 `pgdata` へ benchmark / candidate 管理と account trust 用オブジェクトを追い適用する migration
- `sql/queries/sample_queries.sql`: すぐ叩ける SQL 例
- `docs/setup.md`: 起動とリセット手順
- `docs/data-catalog.md`: データ種別と curated view
- `docs/analysis-principles.md`: 今後の分析原則の保管場所
- `docs/skills/README.md`: project skill の運用ルール
- `.codex/skills/stock-analysis-workflow`: このリポジトリ専用の skill
- `.codex/skills/x-account-trust-evaluation`: X account trust evaluation 用の project skill

## Data Model

- `ingest.kabuplus_files`: CSV ごとの投入ステータス
- `raw.kabuplus_records`: 元データを `jsonb` で保持する raw レイヤ
- `analytics.inferred_price_actions`: 整数倍率ジャンプから推定した split / reverse split event
- `analytics.stock_prices_adjusted_daily`: raw と adjusted の両方を持つ分析用 price view
- `research.*`: 6か月 breakout の run / case / hypothesis を永続化する研究レイヤ
- `ingest.x_*` / `raw.x_*`: 固定監視対象アカウントと X 投稿収集の運用レイヤ
- `research.tweet_*`: tweet と日本株コードの紐付け、および価格反応の durable 分析レイヤ
- `research.x_*`: canonical signal review と account trust evaluation の durable レイヤ

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

## X Account Trust Evaluation

candidate を benchmark と比較しながら評価する継続フローも追加しました。`ingest.x_monitored_accounts.account_role` で `benchmark` / `candidate` を分け、canonical signal と trust score は `research.x_*` に保存します。

```powershell
docker compose exec -T db psql -U stock -d stock_analytics -f /workspace/sql/migrations/20260412_x_account_trust_setup.sql
docker compose exec db psql -U stock -d stock_analytics -c "insert into ingest.x_monitored_accounts (target_username, account_role) values ('yuzz__', 'candidate') on conflict (target_username) do update set account_role = excluded.account_role;"
docker compose run --rm xcollector sync-targets --account-role candidate
docker compose run --rm xcollector backfill --account-role all --days 90
docker compose run --rm analysis prepare-x-signal-analysis --start-date 2026-01-13 --end-date 2026-04-12 --account-role all
docker compose run --rm analysis enrich-x-signal-analysis --input-file /workspace/research/x-signal-analysis/<run-id>/analysis_template.yaml
docker compose run --rm analysis persist-x-signal-analysis --input-file /workspace/research/x-signal-analysis/<run-id>/enriched_analysis.yaml
docker compose run --rm analysis evaluate-x-account-trust --candidate-username yuzz__ --start-date 2026-01-13 --end-date 2026-04-12
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
docker compose run --rm analysis prepare-x-signal-analysis --start-date 2026-01-13 --end-date 2026-04-12 --account-role all
docker compose run --rm analysis enrich-x-signal-analysis --input-file /workspace/research/x-signal-analysis/<run-id>/analysis_template.yaml
docker compose run --rm analysis persist-x-signal-analysis --input-file /workspace/research/x-signal-analysis/<run-id>/enriched_analysis.yaml
docker compose run --rm analysis evaluate-x-account-trust --candidate-username yuzz__ --start-date 2026-01-13 --end-date 2026-04-12
```

6か月 breakout 研究の標準順は次です。

```powershell
docker compose run --rm analysis infer-price-actions
docker compose run --rm analysis prepare-adjusted-prices
docker compose run --rm analysis build-entry-dataset
docker compose run --rm analysis mine-entry-hypotheses
docker compose run --rm analysis evaluate-entry-hypotheses
```
