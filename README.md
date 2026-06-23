# Nostr License — Nostr免許証メーカー

NIP-07 / npub で Nostr のプロフィール（名前・アイコン）を読み込み、運転免許証風のカード画像を生成して PNG ダウンロードできる単一ページWebアプリ。

## 🌐 Live demo

**https://kojira.github.io/nostr-license/**

ビルド不要のバニラ HTML/CSS/JS。GitHub Pages で静的配信しています（バックエンドなし・すべてブラウザ内で完結）。

## 使い方

公開サイト（上記 Live demo）をブラウザで開くだけ。ローカルで動かす場合は、ESモジュールの都合で `file://` 直開きは不可なので、任意の静的サーバー経由で開きます:

```bash
cd nostr-license
python3 -m http.server 8899   # 例。npx serve など何でも可
# → http://localhost:8899/index.html
```

- **発行**: `npub1...`（または64桁hex）を貼り付けて「発行」。そのプロフィールでカードを生成。
- **NIP-07からnpubを取得**: nos2x / Alby などの拡張機能から自分の公開鍵を取得して入力欄に入れるだけ（取得元が NIP-07 でも手入力でも処理は同一・秘密鍵は不要）。

## リレー（データ取得先）

- デフォルトは `wss://r.kojira.io` / `wss://x.kojira.io` / `wss://yabu.me`（nostr.band は使わない）。
- 画面の「データ取得リレー」で**自由に追加・削除**できる（最低1つ必須）。
  フォロワー/WoT/最古投稿は取得先リレーの保持範囲に依存するので、網羅性を上げたいときは大手リレーを足す。
- 「対象の公開リレーリスト(kind:10002)も取得先に加える」をONにすると、対象が公開している
  リレーも問い合わせ先に合流し、より多くのデータを拾える。

## カードに載る情報（リレー＋primal の実データ）

- 氏名（display_name / name）、npub（全桁・自動縮小で表示）、NIP-05、アイコン写真、QR（njump.me）
- **利用開始 / SINCE**：最古イベントの推定。**年単位で過去へジャンプ探索**（kind:1/0/3）して大まかな年代を特定し、
  最古付近を `until` ページングで詰める。密な reaction に阻まれず数年前まで遡れる。
- **有効期限 / EXPIRES**：最終利用（最新イベント）+ 3年
- **ランク**：利用開始からの年数・フォロワー数・投稿数で算出
- **★パラメータ（5本・実データ）**
  - Communication … 投稿数（kind:1、`until` ページングでリレー上限を越えて過去も集計）
  - Web of Trust / Followers … NIP-07接続時は「あなたのフォロー ∩ 対象のフォロワー」（自己発行なら相互フォロー数）。npub貼り付け時はフォロワー数
  - Relay Handling … 利用リレー数（kind:10002、無ければ legacy kind:3）
  - Zap Received … 受信 zap の件数・合計 sats（**primal 経由**。下記「Zap 統計」参照）
  - Zap Sent … 送信 zap の件数・合計 sats（**primal 経由**。下記「Zap 統計」参照）

> 注: 取得は選択リレーの保持範囲に依存します（古いイベントが prune されていれば SINCE は新しめに出ます）。
> 投稿・フォロワーは `until` を遡ってページ収集しますが、ページ上限があるため超大量アカウントでは一部が頭打ち（`+` 表記）になります。

## Zap 統計は primal に依存している（理由）

裏面カードの **ZAP RECEIVED / ZAP SENT（件数・合計 sats）は、選択リレーではなく
primal の caching service（`wss://cache2.primal.net/v1`）から取得**している。これは
意図的な設計で、**zap（特に送信）は通常の Nostr リレー直クエリでは正確に集計できない**ため。

実測で確認した理由:

- **送信 zap を引く `#P`（大文字、送信者）タグは receipt の約 0.4% にしか付いていない。**
  zap receipt（kind:9735）の送信者は本来 `description`（埋め込まれた kind:9734 zap request）の
  `pubkey` に入っており 98% 以上で取れるが、**リレーは description 本文で検索できない**ので、
  送信者でのフィルタは事実上 `#P` タグ頼みになり、ほとんど拾えない。
- **送信 zap の receipt は「受信者（支払先）側のリレー」に配信される**ため、自分のリレーには
  相手が同じリレーを使った分しか存在しない。kojira の例では、自分の 3 リレーの全 zap 履歴
  21.9 万件を description まで精査しても送信は 41 件・全て ≤777 sats しか無く、
  オフ会の割り勘などの大口は 1 件も無かった（実際は primal 集計で送信約 210 万 sats）。
- 受信 zap（必須の `#p` タグ）は比較的拾えるが、それでも自分の 3 リレーでは全ネットワークの
  約 6 割しか取れていなかった（primal: 6,295 件 / リレー直: 3,054 件）。

primal は全ネットワークの zap receipt を `description.pubkey` まで含めて index 済みなので、
送受信ともに正確な集計が取れる。取得方法:

- **送信**: `user_zaps {sender}` をページング（`kind:10000129` の `amount_sats` を合算）。
  primal は `since`/`until` をサーバー側で解釈するので、**時間範囲を分割して並列ページング**し
  全体時間を短縮している。
- **受信（件数・合計 sats）**: `user_profile` の `total_zap_count` / `total_satszapped`
  （1 リクエストの権威集計値。ページング合算はチャンク途中終了で大口を取りこぼし得るため
  集計には使わない）。
- **受信の時系列（グラフ用）**: `user_zaps {receiver}` のページング。

> 旧実装はリレーから `#P`/`#p` を窓走査していたが、件数が桁違いに過少な上、ほぼ空の窓を
> 大量に往復してリレー取得全体を遅くしていたため**完全に廃止**した。primal が落ちている／
> 応答しない場合、zap は「取得不能」として 0 表示にする（誤解を招くリレー値はフォールバックに使わない）。
>
> nostr.band も同種の集計を持っていたが現在は稼働していない。primal が停止すると zap 統計は出なくなる
> （投稿・フォロワー等のリレー取得には影響しない）。

## 構成

- `index.html` / `style.css` / `app.js`（バニラJS）
- `nostr-tools`（nip19 / bech32）と `qrcode` は esm.sh から動的import
- アバターは CORS 不許可ホスト対策に images.weserv.nl プロキシへフォールバック（canvas書き出しを保証）
- **外部依存**: 投稿・フォロワー等は選択リレー（WebSocket）、**zap 統計は primal の
  caching service（`wss://cache2.primal.net/v1`）**。理由は上記「Zap 統計」を参照。

これは遊びのファンカードであり、公的な身分証ではありません。
