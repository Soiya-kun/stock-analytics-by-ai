# S3 + CloudFront static site

AWS account `373117960731` に、分析 HTML を HTTPS 公開する CDK スタックです。
S3 バケットは非公開のままにし、CloudFront Origin Access Control 経由でのみ配信します。
既定リージョンは `ap-northeast-1` です。変更時のみ `STACK_REGION` を指定してください。

既定では `outputs/` 配下の全 HTML を再帰的に収集し、相対パスを保って一括デプロイします。
ルート URL には公開済み HTML の一覧ページを自動生成します。CloudFront Function が拡張子なしの
URL を `.html` に解決するため、たとえば次の両方で同じレポートを表示できます。

- `/tweet-analysis-viewer/2026-08-12`
- `/tweet-analysis-viewer/2026-08-12.html`

## 日次株価ページの更新

[日次株価ページ](https://d3fvqjccwn6gec.cloudfront.net/daily/) は別スタック `StockAnalyticsDailyReview` のLambdaとEventBridge Schedulerで更新します。PC起動・個人のSSOログインは不要です。平日16:40 JSTを基本に17:10・18:10に再確認し、非公開S3の価格履歴から生成した `daily/index.html` だけを公開します。既存サイト全体のCDK再デプロイは日次更新に使いません。

コード入口は `bin/daily-review.ts` / `lib/daily-review-stack.ts`、運用入口はリポジトリルートの `scripts/deploy_daily_stock_cloud.py` です。初回はスケジュールを無効にして作成し、価格履歴・Secrets Managerを準備し、実Lambdaで取得・公開・HTTPS本文hashを検証してから有効にします。[運用手順](../../docs/setup.md#daily-stock-review-pipeline) を参照してください。

## 初回セットアップ

```powershell
cd infra/static-site
npm install
aws configure sso --profile 373117960731_AdministratorAccess
aws sso login --profile 373117960731_AdministratorAccess
npx cdk bootstrap aws://373117960731/ap-northeast-1 --profile 373117960731_AdministratorAccess
```

## 確認とデプロイ

```powershell
npm run build
npm run synth
npm run diff -- --profile 373117960731_AdministratorAccess
npm run deploy -- --profile 373117960731_AdministratorAccess
```

デプロイ完了時の `SiteUrl` が公開 URL です。CloudFront の反映には数分かかることがあります。

別のディレクトリ一式を公開する場合は、相対パスまたは絶対パスを指定します。

```powershell
$env:SITE_SOURCE_DIR = "../../some-html-directory"
npm run deploy -- --profile 373117960731_AdministratorAccess
Remove-Item Env:SITE_SOURCE_DIR
```

単一 HTML だけを公開する場合は `SITE_FILE` を指定します。

```powershell
$env:SITE_FILE = "../../path/to/report.html"
npm run deploy -- --profile 373117960731_AdministratorAccess
Remove-Item Env:SITE_FILE
```

スタックを削除しても、誤削除防止のため S3 バケットとオブジェクトは保持されます。
