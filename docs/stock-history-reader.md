# チャート付き相場史の更新

キオクシアの相場史を、スマホで本文と日足を一緒に読める形で公開する。公開先は [market-history/kioxia/](https://d3fvqjccwn6gec.cloudfront.net/market-history/kioxia/)。本文を読むと、画面上部のチャートがその節の時期へ切り替わる。

本文の加筆は既存のPageで行い、公開する内容を `docs/market-history/kioxia.md` へ反映する。見出しを変更・追加したら、`config/stock-history-reader-285A.json` の同じ見出しと対象期間も更新する。株価基準日と情報基準日を分け、業績予想の将来年月をチャートの対象期間に使わない。本文の非公開投稿は要旨だけとし、原文や取得用キャッシュを追加しない。

## 生成と確認

株価は `outputs/stock-history/285A/prices.json` を使う。最新の日足へ更新する場合は、先に [全日足の生成手順](setup.md#個別銘柄の全日足) を実行する。本文と設定の基準日も合わせて更新する。

```powershell
docker compose build analysis
docker compose run --rm --no-deps --entrypoint python analysis /workspace/scripts/generate_stock_history_reader.py
docker compose run --rm --no-deps --entrypoint python analysis -m unittest discover -s tests -p test_stock_history_reader.py -v
python -m http.server 8878 --bind 127.0.0.1 --directory outputs/stock-history-reader
```

`http://127.0.0.1:8878/285A/` をスマホ幅で開く。上場時、突破、高値、下落の節へ目次から移り、表示期間が対応することを確認する。下へ読んだ後に上へ戻る場合、横長画面、表の横スクロールも確認する。チャートをタップすると調整後と当時のOHLCを表示し、矢印キーでも日を選べる。表示期間の選択、対数目盛、本文との連動停止・再開も使える。

チャートは通常、節の最終営業日までを表示する。選択した期間よりも本文の期間が長い場合は、本文の期間を優先する。「全期間」では全日足を表示し、本文の対象期間を緑で示す。本文の価格は取引当時、チャートと出来高は株価基準日までの分割を調整した値である。

## S3へ公開

```powershell
cd infra/static-site
npm ci
cd ../..
# 作業用SSOが切れた場合だけ実行する。
aws sso login --profile 373117960731_AdministratorAccess
./infra/static-site/scripts/deploy-report.ps1 -HistoryReader
```

`StockAnalyticsHistoryReader285A` は既存サイトの非公開S3とCloudFrontを参照する。`publish.json` とHTMLのSHA-256を照合し、梱包するのは `index.html` だけ。配置先は `market-history/kioxia/` に固定し、削除同期は行わない。既存サイトのトップ・日次ページ・他のレポートを再配置しない。

スクリプトは型チェック、CDK synth、AWSアカウント・既存リソースの確認、diff、deployを行い、最後に公開HTTPSから取得したHTMLのSHA-256を照合する。スタックを削除しても公開オブジェクトは保持する。本文や日足の更新は、この生成・公開手順を再実行して反映する。
