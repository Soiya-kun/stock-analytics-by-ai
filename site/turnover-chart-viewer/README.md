# 売買代金チャート一覧

KABU+ の日足から、指定日の売買代金が閾値以上だった銘柄を抽出し、
ローソク足をまとめて閲覧できる自己完結HTMLを生成します。

```powershell
docker compose run --rm --entrypoint python analysis `
  /workspace/scripts/generate_turnover_chart_viewer.py `
  --date latest `
  --min-turnover-million-yen 3000
```

出力先は既定で
`outputs/turnover-chart-viewer/YYYY-MM-DD-turnover-3000m.html` です。
ブラウザで直接開け、外部JavaScriptやローカルサーバーは不要です。

主なオプション:

- `--date YYYY-MM-DD`: 対象日。既定は `latest`
- `--min-turnover-million-yen 3000`: 売買代金の下限（百万円）
- `--lookback-bars 250`: 1チャートに表示する営業日数（約1年）
- `--limit 300`: 表示上限。`0` で上限なし
- `--output PATH`: 出力先を明示

HTML内では、コード・銘柄名検索、売買代金/騰落率/コード順の並べ替え、
1年分のデータから描画期間を選べる両端レンジバー、PC表示密度の切り替えができます。
期間レンジは画面下部にフローティング表示されます。スマートフォンでは自動的に1列表示になり、
レンジバーも画面幅いっぱいに表示されます。レンジを1年未満に絞った状態では、任意のチャートを
指またはマウスで左右にドラッグすると、期間の長さを保ったまま全チャートの表示期間を移動できます。
