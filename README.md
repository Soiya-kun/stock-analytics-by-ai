# stock-analytics-by-ai

Kabuplus の年次 ZIP と日次 CSV を PostgreSQL に取り込み、SQL で株価分析を進めるための初期基盤です。`docker compose` で DB を起動し、ZIP または CSV を `raw` レイヤへ投入し、`analytics` レイヤの typed view から分析を始めます。株式分割・併合は raw 価格を壊さず、`analytics.inferred_price_actions` と `analytics.stock_prices_adjusted_daily` で非破壊に扱います。

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
- `scripts/entry_breakout_research.py`: split 推定、補正価格監査、6か月 breakout dataset / hypothesis pipeline
- `sql/init`: スキーマ、テーブル、helper 関数、typed view
- `sql/migrations/20260404_entry_breakout_setup.sql`: 既存 `pgdata` へ研究用オブジェクトを追い適用する migration
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

初期の分析原則として「数年レンジ相場の上抜け」と「6か月レンジ上抜け entry study」を追加済みです。今後は `docs/analysis-principles.md` に原則を明文化し、その内容に合わせて SQL やスクリプトを追加していきます。

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
