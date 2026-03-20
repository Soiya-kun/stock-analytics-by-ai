# stock-analytics-by-ai

Kabuplus の年次 ZIP を PostgreSQL に取り込み、SQL で株価分析を進めるための初期基盤です。`docker compose` で DB を起動し、ZIP を直接読んで `raw` レイヤへ投入し、`analytics` レイヤの typed view から分析を始めます。

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

## Main Files

- `docker-compose.yml`: PostgreSQL と importer の構成
- `scripts/import_kabuplus.py`: ZIP を直接読み込む importer
- `scripts/analyze_range_breakout.py`: 数年レンジ上抜けの grid-search / 最新スキャン
- `sql/init`: スキーマ、テーブル、helper 関数、typed view
- `sql/queries/sample_queries.sql`: すぐ叩ける SQL 例
- `docs/setup.md`: 起動とリセット手順
- `docs/data-catalog.md`: データ種別と curated view
- `docs/analysis-principles.md`: 今後の分析原則の保管場所
- `docs/skills/README.md`: project skill の運用ルール
- `.codex/skills/stock-analysis-workflow`: このリポジトリ専用の skill

## Data Model

- `ingest.kabuplus_files`: CSV ごとの投入ステータス
- `raw.kabuplus_records`: 元データを `jsonb` で保持する raw レイヤ
- `analytics.*`: `stock_prices_daily` などの typed view

初期の分析原則として「数年レンジ相場の上抜け」を追加済みです。今後は `docs/analysis-principles.md` に原則を明文化し、その内容に合わせて SQL やスクリプトを追加していきます。

## Analysis

分析スクリプトも Docker 内で実行します。

```powershell
docker compose run --rm analysis grid-search --processes 4
docker compose run --rm analysis scan
docker compose run --rm analysis label-study
```
