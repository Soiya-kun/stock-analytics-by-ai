# kabuステーションAPIの操作・接続確認

このPCでkabuステーションを操作する人と、別チャットで接続確認を引き継ぐCodex向けの手順。
2026年11月7日に、本番APIの認証と株価取得に成功した。常時収集やDB保存はまだ設定していない。

1. 口座側の利用状態、本体へのログイン、本体のAPI設定を順に確認する。
2. HTTP接続には `localhost` を使う。
3. パスワードはユーザーがローカル画面に入力する。チャット・Git・ログへ残さない。
4. 接続テストは認証と情報取得までとする。

初回は「利用条件」から「接続テスト」までを読む。再接続時は「本体の起動・API設定」、問題が起きたら「症状から確認する」を参照する。

## 確認済みの状態

次の表は2026年11月7日の作業記録。記録日である2026年11月15日には、口座状態やAPIへの再接続を確認していない。

| 確認したもの | 結果 |
| --- | --- |
| インストーラー | 公式配布の `kabuStation_Installer.exe`。バージョン5.48.0.0。三菱UFJ eスマート証券のデジタル署名は有効 |
| 利用申込 | APIの利用設定済みを確認 |
| Professionalプラン | 利用可。画面の利用期間は2026年12月1日まで |
| kabuステーションAPI | 口座画面で利用可 |
| 本体のAPI設定 | ユーザーがAPIを有効化し、本番用APIパスワードを設定。再起動・再ログイン後に接続できた |
| 認証・株価取得 | 2026年11月7日12:58 JSTに、`POST /token` と `GET /board/8306@1` がHTTP 200で成功 |
| 取得データの時刻 | 三菱UFJ（8306）の株価・出来高は2026年11月6日15:30 JST時点 |
| 秘密情報 | 接続テストではパスワードとトークンをファイルへ保存していない |
| 継続取得 | 単発の接続確認まで。常時収集・DB保存・自動再接続・スケジュール起動は未設定。PUSH受信も未検証 |

口座番号、パスワード、トークン、取得した価格・出来高の値、API応答の原本は、この公開リポジトリへ追加しない。

## 利用条件

- APIはProfessionalプラン以上の適用で無料。Professionalプランは、信用取引口座または先物・オプション口座の開設と、対象期間の約定実績が必要。[公式の料金案内](https://kabu.com/item/kabustation_api/default.html)
- 約定の判定対象は前々々月から前営業日・前々営業日まで。必要回数は1回以上で、締め日は商品により異なる。現物株式の売買も対象になる。[利用プラン・判定ルール](https://kabu.com/kabustation/services.html)
- 国内株式は約定日の当日に判定する。日次判定は毎日19時頃で、判定後すぐ、または翌日から適用される。[公式の判定タイミング](https://kabu.com/kabustation/services.html)
- 口座画面の「利用設定済」と、プラン条件を満たした「利用可」を確認する。今回の作業では、11月6日の約定後、11月7日にAPIが利用可へ変わった。
- 利用条件は変更されることがある。操作時に公式案内と口座画面を確認する。

## 口座画面での確認

PC版のメンバーズサイトで確認する。

1. 「設定・申込」→「電子契約」を開く。
2. 「4．取引ツール」でProfessionalまたはPremiumプランと、kabuステーションAPIが「利用可」か確認する。
3. kabuステーションAPIの設定画面で「ご利用設定状態」を確認する。

既存の申込が「利用設定済」なら、プランの適用状況を先に確認する。ログイン切れの場合は、ユーザーにログインを引き継ぐ。[公式の利用設定手順](https://kabucom.github.io/kabusapi/ptal/howto.html)

## 本体の起動・API設定

Windowsの本体を起動し、APIを有効にする。

1. スタートメニューまたはショートカットからkabuステーションを起動する。このPCの実行ファイルは `%LOCALAPPDATA%\kabuStation\KabuS.exe`。
2. 本体へのログインをユーザーに依頼する。ブラウザでログイン済みでも、本体で認証が必要になることがある。
3. 右上の「</>」を右クリックし、「APIシステム設定」を開く。
4. 「APIを利用する」を有効にする。
5. ユーザーが本番用APIパスワードを設定する。公式の指定は英数字6～16桁。
6. ソフトリミット（ワンショット上限）の設定値を確認する。情報取得のテストのために上限を引き上げない。
7. 「OK」で保存し、案内に従って本体を再起動する。
8. ユーザーが再ログインする。
9. 右上のAPIアイコンが緑になったことを確認する。

設定変更後は、再起動と再ログインが完了してから接続を確認する。[公式のAPIシステム設定](https://kabucom.github.io/kabusapi/ptal/howto.html)

### 画面操作を引き継ぐとき

- Windows操作には、その時点で利用可能なcomputer-useスキルとツールを使う。HTTPリクエストの確認はCLIから行える。
- アプリとウィンドウ名を取得し、現在の画面を確認してから操作する。
- この環境では、本体の最小化、ウィンドウのアクティブ化失敗、アクセシビリティ情報の取得失敗があった。
- キャプチャに別アプリが写った場合は、その画像の座標で操作しない。本体を前面に出す操作をユーザーに引き継ぐ。
- ログインとAPIパスワードの設定・入力は、ユーザー自身が画面で行う。

## 接続テスト

まずTCP接続を確認し、その後にHTTP・認証・情報取得を確認する。

| 環境 | REST API | PUSH（WebSocket） |
| --- | --- | --- |
| 本番 | `http://localhost:18080/kabusapi` | `ws://localhost:18080/kabusapi/websocket` |
| 検証 | `http://localhost:18081/kabusapi` | `ws://localhost:18081/kabusapi/websocket` |

2026年11月7日のテストでは、本番APIを使った。検証環境は接続未確認。

### TCP接続の確認

WindowsホストのPythonから確認する。

```powershell
python -c "import socket; s=socket.create_connection(('127.0.0.1',18080),timeout=3); print('TCP OK'); s.close()"
```

TCP確認での `127.0.0.1` は利用できた。HTTPのURLには `localhost` を指定する。HTTPを `127.0.0.1` で呼んだ際はHTTP 400、`localhost` ではAPIから応答が返った。

`Get-NetTCPConnection` の表示だけでAPIの利用可否を断定せず、実際のTCP・HTTP応答まで確認する。

### 認証と株価取得

本体が起動・ログイン済みで、ほかの取得プログラムがトークンを使っていないことを確認してから実行する。

1. APIパスワードをローカルのマスク付き入力画面で受け取る。ログイン用パスワードとAPIパスワードを取り違えない。
2. `POST /token` にJSONの `APIPassword` を渡す。
3. HTTP 200、`ResultCode = 0`、トークンの発行を確認する。
4. 取得したトークンを `X-API-KEY` ヘッダーに付け、`GET /board/8306@1` を1回呼ぶ。`8306@1` は三菱UFJの東証銘柄。
5. HTTP 200と銘柄コードを確認し、価格・出来高の取得時刻を読む。休場日の応答には、直前の営業日の値が含まれる。
6. 結果には実行日時、HTTPステータス、結果コード、銘柄、取得時刻を記録する。パスワードとトークンは出力しない。

接続テストで呼ぶのは `POST /token` と `GET /board` まで。発注・取消は行わない。失敗時の再試行回数を決め、パスワードの総当たりや無制限のリトライを避ける。

今回のテストには、Python標準ライブラリとTkinterのマスク付き入力画面を使った。HTTPクライアントはプロキシを無効化し、リダイレクトを追わない設定にした。呼び出せるメソッドとパスは、上記の認証・株価取得に限定した。

このPCのローカル生成物は、次の場所にある。ほかの環境ではファイルの存在を確認する。

```text
C:\Users\djmaa\.codex\visualizations\2026\11\05\01a1aa0a-f133-7212-b28e-df67926bfaf4\
  kabu_api_connection_test.py
  kabu_api_connection_test.result.json
```

テスト用スクリプトは実行のたびにパスワードを尋ね、結果JSONを上書きする。再実行する際は、コードの接続先と対象APIを確認し、必要な過去の結果を退避する。テスト用スクリプトとAPI応答の原本はGit管理外に置いている。

## 継続取得の条件と未実装の部分

- このPCと本体を起動し、ログインとAPI有効化を維持する。
- トークンは本体の終了・ログアウト・別トークンの発行で無効になる。早朝には強制ログアウトがあるため、再ログイン後にトークンを発行する。リクエストごとの再発行は不要。[認証仕様](https://raw.githubusercontent.com/kabucom/kabusapi/master/reference/kabu_STATION_API.yaml)
- API登録銘柄はRESTとPUSHを合わせて最大50銘柄。情報取得のリクエストでも自動登録される。既存の登録銘柄を無断で一括解除しない。[REST仕様](https://raw.githubusercontent.com/kabucom/kabusapi/master/reference/kabu_STATION_API.yaml)
- 情報系RESTの流量制限は秒間10件程度。取得対象・頻度と再試行の上限を決めてから継続取得を実装する。[REST仕様](https://raw.githubusercontent.com/kabucom/kabusapi/master/reference/kabu_STATION_API.yaml)
- PUSHは登録した銘柄の値が更新されたときに配信される。昼休み・引け後は配信されない。[PUSH仕様](https://kabucom.github.io/kabusapi/ptal/push.html)
- 常時収集、DB保存、安全な認証情報の保存、自動再接続、スケジュール起動は、今回の作業では設定していない。PUSHの受信・保存も未検証。
- kabuステーションのローカルAPIを使うには、接続するプロセスから本体へ到達できる必要がある。Dockerの `localhost` はコンテナ自身を指す。Docker・AWSからの接続は未検証。
- KABU+の日次CSV取り込みとクラウド公開は、既存の独立した処理を維持する。kabuステーションの導入を理由に、休止中の研究を再開しない。
- API取得データは本人利用の範囲で扱う。第三者への提供や公開への転用を始める前に、[公式の利用条件](https://kabu.com/item/kabustation_api/default.html)を確認する。

## 症状から確認する

| 症状 | 確認・対応 |
| --- | --- |
| 口座画面がログイン画面に戻る | ブラウザのログインをユーザーに依頼する |
| APIは利用設定済みだが利用不可 | Professional以上の適用と判定時刻を確認する |
| 本体がログイン画面を表示する | 本体へのログインをユーザーに依頼する |
| TCP接続に失敗する | 本体の起動・ログイン、「APIを利用する」、再起動完了、環境に合うポートを確認する |
| HTTP 400でHTMLが返る | HTTP接続先が `localhost` か確認する。このPCでは `127.0.0.1` 指定で発生した |
| 認証なしでHTTP 401、コード4001009が返る | APIは応答している。トークンを発行し、`X-API-KEY` に付けて確認する |
| 認証後もHTTP 401になる | 本番用・検証用のAPIパスワードと接続先、ヘッダー、トークンの失効を確認する |
| 本体の画面を自動操作できない | 新しいウィンドウ情報を取得する。前面に出せなければ、画面操作をユーザーに引き継ぐ |
| 休場日に以前の価格が返る | `CurrentPriceTime` と `TradingVolumeTime` を確認し、その日時の値として記録する |

## 公式資料・問い合わせ

- [ダウンロード案内](https://kabu.com/kabustation/default.html)
- [API利用設定](https://kabucom.github.io/kabusapi/ptal/howto.html)
- [REST APIリファレンス](https://kabucom.github.io/kabusapi/reference/index.html)
- [PUSH配信](https://kabucom.github.io/kabusapi/ptal/push.html)
- [公式トラブルシューティング](https://kabu.com/kabustation/support.html)
- [APIコミュニティ](https://github.com/kabucom/kabusapi)
- [証券会社への問い合わせ](https://kabu.com/support/detail.html)

記録日・公式仕様の確認日：2026年11月15日。実機での最終接続確認：2026年11月7日12:58 JST。
