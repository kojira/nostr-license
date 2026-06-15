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

## カードに載る情報（すべてリレーからの実データ）

- 氏名（display_name / name）、npub（全桁・自動縮小で表示）、NIP-05、アイコン写真、QR（njump.me）
- **利用開始 / SINCE**：最古イベントの推定。**年単位で過去へジャンプ探索**（kind:1/0/3）して大まかな年代を特定し、
  最古付近を `until` ページングで詰める。密な reaction に阻まれず数年前まで遡れる。
- **有効期限 / EXPIRES**：最終利用（最新イベント）+ 3年
- **ランク**：利用開始からの年数・フォロワー数・投稿数で算出
- **★パラメータ（5本・実データ）**
  - Communication … 投稿数（kind:1、`until` ページングでリレー上限を越えて過去も集計）
  - Web of Trust / Followers … NIP-07接続時は「あなたのフォロー ∩ 対象のフォロワー」（自己発行なら相互フォロー数）。npub貼り付け時はフォロワー数
  - Relay Handling … 利用リレー数（kind:10002、無ければ legacy kind:3）
  - Zap Received … 受信 zap 件数（kind:9735 の `#p`）
  - Zap Sent … 送信 zap 件数（kind:9735 の `#P`、対応リレーでのみ取得可）

> 注: 取得は選択リレーの保持範囲に依存します（古いイベントが prune されていれば SINCE は新しめに出ます）。
> 投稿・フォロワー・zap は `until` を遡ってページ収集しますが、ページ上限があるため超大量アカウントでは一部が頭打ち（`+` 表記）になります。

## 構成

- `index.html` / `style.css` / `app.js`（バニラJS）
- `nostr-tools`（nip19 / bech32）と `qrcode` は esm.sh から動的import
- アバターは CORS 不許可ホスト対策に images.weserv.nl プロキシへフォールバック（canvas書き出しを保証）

これは遊びのファンカードであり、公的な身分証ではありません。
