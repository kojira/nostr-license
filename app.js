// Nostr License — NIP-07 でプロフィールを読み込み、運転免許証風カードを生成する
import { nip19 } from "https://esm.sh/nostr-tools@2.10.4";

// ===== デフォルトリレー（初期値。ユーザーが編集可能。nostr.band は使わない）=====
const DEFAULT_RELAYS = [
  "wss://r.kojira.io",
  "wss://x.kojira.io",
  "wss://yabu.me",
];

const $ = (id) => document.getElementById(id);
const canvas = $("license-canvas");
const ctx = canvas.getContext("2d");

// 画面のリレーエディタから現在のリレー一覧を収集（空・重複・不正は除外）
function getActiveRelays() {
  const inputs = [...document.querySelectorAll("#relay-list .relay-input")];
  const seen = new Set();
  const out = [];
  for (const el of inputs) {
    let v = el.value.trim();
    if (!v) continue;
    if (!/^wss?:\/\//i.test(v)) v = "wss://" + v;
    v = v.replace(/\/+$/, "");
    if (seen.has(v)) continue;
    seen.add(v);
    out.push(v);
  }
  return out;
}

let lastData = null; // 直近に描画したデータ（テーマ切替・再描画用）

function setStatus(msg, kind = "") {
  const el = $("status");
  el.textContent = msg;
  el.className = "status" + (kind ? " " + kind : "");
}

// ===== リレーへ REQ を投げてイベントを集める =====
function queryRelays(filters, { relays = getActiveRelays(), timeoutMs = 4500 } = {}) {
  return new Promise((resolve) => {
    const events = new Map();
    const sockets = [];
    let settled = false;
    let doneCount = 0;
    const subId = "nl-" + Math.random().toString(36).slice(2, 10);
    const total = relays.length;

    const finish = () => {
      if (settled) return;
      settled = true;
      for (const ws of sockets) {
        try { ws.close(); } catch {}
      }
      resolve([...events.values()]);
    };

    if (total === 0) { resolve([]); return; }
    const timer = setTimeout(finish, timeoutMs);

    relays.forEach((url) => {
      let ws;
      try {
        ws = new WebSocket(url);
      } catch {
        doneCount++;
        return;
      }
      sockets.push(ws);

      ws.onopen = () => {
        try {
          ws.send(JSON.stringify(["REQ", subId, ...filters]));
        } catch {}
      };
      ws.onmessage = (e) => {
        let data;
        try { data = JSON.parse(e.data); } catch { return; }
        if (!Array.isArray(data)) return;
        if (data[0] === "EVENT" && data[1] === subId && data[2]) {
          const ev = data[2];
          events.set(ev.id, ev);
        } else if (data[0] === "EOSE" && data[1] === subId) {
          try { ws.close(); } catch {}
        }
      };
      const markDone = () => {
        doneCount++;
        if (doneCount >= total) {
          clearTimeout(timer);
          finish();
        }
      };
      ws.onclose = markDone;
      ws.onerror = () => { try { ws.close(); } catch {} };
    });
  });
}

// ===== ユーティリティ：イベント集合の最新/最古 created_at =====
function maxCreatedAt(...lists) {
  let m = 0;
  for (const list of lists) for (const ev of list) if (ev && ev.created_at > m) m = ev.created_at;
  return m;
}
// 最新の置換可能イベント（kind:0 / 3 / 10002 など）を1つ選ぶ
function latest(events) {
  let best = null;
  for (const ev of events) if (!best || ev.created_at > best.created_at) best = ev;
  return best;
}
function tagValues(ev, name) {
  if (!ev) return [];
  return ev.tags.filter((t) => t[0] === name).map((t) => t[1]).filter(Boolean);
}

// ===== until を遡って全件をページ収集（リレーの limit 上限を越えて過去も集める）=====
// filterBase: {kinds, authors, "#p" ...}（limit/until は内部で付与）
async function fetchAllPaged(filterBase, relays, { maxPages = 5, pageLimit = 500, timeoutMs = 3500 } = {}) {
  const all = new Map();
  let until = null;
  for (let i = 0; i < maxPages; i++) {
    const f = { ...filterBase, limit: pageLimit };
    if (until != null) f.until = until - 1;
    const evs = await queryRelays([f], { relays, timeoutMs });
    if (!evs.length) break;
    let minTs = Infinity;
    for (const e of evs) {
      if (!all.has(e.id)) all.set(e.id, e);
      if (e.created_at < minTs) minTs = e.created_at;
    }
    if (until != null && minTs >= until) break; // 進捗なし
    until = minTs;
    if (evs.length < pageLimit) break;          // これ以上古いものは無い
  }
  return [...all.values()];
}

// ===== 最古イベント（利用開始の推定）=====
// 1) 3ヶ月刻みで until を過去へ下げ、イベントのある最古帯まで一気に降りる
//    （新しい順しか返らない relay でも、密なリアクションに阻まれず最古帯を掴める）
// 2) 最古帯から limit:500 のページングで「これ以上古いものが無い」所まで詰める
//    探索対象は kind:1/0/3（投稿・プロフィール作成・初期フォロー）。reaction(7) は除外。
const MONTH_SEC = 30.44 * 24 * 3600;
async function findEarliest(pubkeyHex, relays, seedMin) {
  const now = Math.floor(Date.now() / 1000);
  let earliest = seedMin != null ? seedMin : now;
  const kinds = [1, 0, 3];

  // 1) 粗探索：3ヶ月刻み（最大 ~12年）。境界より古い until は空になる。
  //    ただしリレーの一時的な空応答で早期打ち切りしないよう、2回連続で空の時のみ終了。
  let empties = 0;
  for (let mo = 3; mo <= 144; mo += 3) {
    const until = Math.floor(now - mo * MONTH_SEC);
    const evs = await queryRelays(
      [{ kinds, authors: [pubkeyHex], until, limit: 200 }],
      { relays, timeoutMs: 4500 }
    );
    if (!evs.length) { if (++empties >= 2) break; continue; }
    empties = 0;
    const m = Math.min(...evs.map((e) => e.created_at));
    if (m < earliest) earliest = m;
  }

  // 2) 仕上げ drill：最古帯からさらに限界まで遡る
  let until = earliest;
  for (let i = 0; i < 12; i++) {
    const evs = await queryRelays(
      [{ kinds, authors: [pubkeyHex], until: until - 1, limit: 500 }],
      { relays, timeoutMs: 5000 }
    );
    if (!evs.length) break;
    const m = Math.min(...evs.map((e) => e.created_at));
    if (m >= until) break;
    earliest = m;
    until = m;
    if (evs.length < 500) break;
  }
  return earliest;
}

// ===== プロフィール＋実データ指標を取得（2フェーズ）=====
// opts.useUserRelays: true なら対象の kind:10002 を取得先リレーにも合流させる。
async function fetchProfile(pubkeyHex, { useUserRelays = false } = {}) {
  const t = pubkeyHex;
  const selected = getActiveRelays();

  // --- Phase A: 選択リレーで軽量取得（profile / relay list）---
  setStatus("リレーからプロフィールとリレーリストを取得中…");
  const [metaEvents, relayListEvents] = await Promise.all([
    queryRelays([{ kinds: [0], authors: [t], limit: 5 }], { relays: selected }),
    queryRelays([{ kinds: [10002], authors: [t], limit: 5 }], { relays: selected }),
  ]);

  const meta = latest(metaEvents);
  let profile = {};
  if (meta) { try { profile = JSON.parse(meta.content) || {}; } catch {} }

  // 対象が公開しているリレー（NIP-65 kind:10002）
  const relayListEv = latest(relayListEvents);
  const userRelays = tagValues(relayListEv, "r").map((u) => u.replace(/\/+$/, ""));

  // 取得に使う working リレー（トグル ON なら対象リレーを合流）
  const working = useUserRelays && userRelays.length
    ? [...new Set([...selected, ...userRelays])]
    : selected;

  // --- Phase B: working リレーで本取得（過去も遡って集計）---
  // まず投稿の先頭ページを取り、利用開始推定の起点(seedMin)にする
  setStatus("活動・フォロワー・Zap を過去まで取得中…");
  const notes0 = await queryRelays([{ kinds: [1], authors: [t], limit: 500 }], { relays: working });
  const seedMin = notes0.length ? Math.min(...notes0.map((e) => e.created_at)) : (meta ? meta.created_at : null);

  // ページング取得（リレー上限を越えて過去も）＋最古推定を並列実行
  const [noteEvents, followerEvents, zapRecvEvents, zapSentEvents, contactsEvents, sinceAt] = await Promise.all([
    fetchAllPaged({ kinds: [1], authors: [t] }, working, { maxPages: 5 }),
    fetchAllPaged({ kinds: [3], "#p": [t] }, working, { maxPages: 3 }),
    fetchAllPaged({ kinds: [9735], "#p": [t] }, working, { maxPages: 6 }), // Zap 受信
    fetchAllPaged({ kinds: [9735], "#P": [t] }, working, { maxPages: 6 }), // Zap 送信（大文字 P）
    queryRelays([{ kinds: [3], authors: [t], limit: 5 }], { relays: working }),
    findEarliest(t, working, seedMin),
  ]);

  // フォロワー集合（kind:3 の発行者。自分は除外）
  const followers = new Set(followerEvents.map((e) => e.pubkey));
  followers.delete(t);

  // 対象の follow（最新 kind:3 の p タグ）
  const contactsEv = latest(contactsEvents);
  const followList = new Set(tagValues(contactsEv, "p"));

  // Relay Handling 用のリレー数：kind:10002 優先、無ければ legacy kind:3 content
  let relayCount = userRelays.length;
  if (!relayCount && contactsEv) {
    try { relayCount = Object.keys(JSON.parse(contactsEv.content || "{}")).length; } catch {}
  }

  // Zap 件数（送受信を分離。重複は id で除去済み）
  const zapRecv = zapRecvEvents.length;
  const zapSent = zapSentEvents.length;

  // WoT = 相互フォロー数（対象の follow ∩ 対象の follower）。
  // npub だけで計算でき、秘密鍵も NIP-07 も不要（取得元が違っても結果は同じ）。
  let mutual = 0;
  for (const pk of followers) if (followList.has(pk)) mutual++;
  const wotValue = mutual;

  const lastActivity = maxCreatedAt(noteEvents, contactsEvents, metaEvents, relayListEvents);

  return {
    pubkeyHex: t,
    npub: nip19.npubEncode(t),
    name: profile.display_name || profile.name || "NO NAME",
    handle: profile.name || "",
    picture: profile.picture || "",
    nip05: profile.nip05 || "",
    about: profile.about || "",
    hasNip05: !!profile.nip05,
    // 実データ指標
    activity: noteEvents.length,        // 投稿数（ページ収集後）
    activityCapped: noteEvents.length >= 2500,
    followers: followers.size,
    following: followList.size,
    relayCount,
    zapRecv,
    zapSent,
    wotValue,
    sinceAt,                            // 利用開始（最古イベント推定、取れなければ null）
    lastActivity: lastActivity || sinceAt || Math.floor(Date.now() / 1000),
    usedRelays: working,
  };
}

// ===== ランク（実データ基準）=====
function computeRank(d) {
  const ageYears = d.sinceAt ? (Date.now() / 1000 - d.sinceAt) / (365.25 * 24 * 3600) : 0;
  if (ageYears >= 3 || d.followers >= 300) return "NOSTR VETERAN";
  if (d.followers >= 50 || d.activity >= 300) return "NOSTR CITIZEN";
  if (d.activity >= 10 || d.hasNip05) return "RELAY EXPLORER";
  return "BEGINNER NOSTR USER";
}

// 実数 → ★(1..5)。log スケール。
function starFrom(x, k, base = 1) {
  const n = Math.round(Math.log10((x || 0) + 1) * k) + base;
  return Math.max(1, Math.min(5, n));
}
// ステータス（5項目・すべて実データ）。2列グリッドで表示。各 {label, n, note, icon}
function computeStars(d) {
  return [
    { label: "Communication", icon: "bubble", n: starFrom(d.activity, 1.6), note: d.activity + (d.activityCapped ? "+" : "") },
    { label: "Web of Trust", icon: "shield", n: starFrom(d.wotValue, 1.6), note: String(d.wotValue) },
    { label: "Relay Handling", icon: "relay", n: starFrom(d.relayCount, 2.4), note: String(d.relayCount) },
    { label: "Zap Received", icon: "bolt", n: starFrom(d.zapRecv, 1.6), note: String(d.zapRecv) },
    { label: "Zap Sent", icon: "bolt", n: starFrom(d.zapSent, 1.6), note: String(d.zapSent) },
  ];
}

// ===== 画像ロード（CORS対策のため weserv プロキシにフォールバック）=====
function loadImage(url, { crossOrigin = true } = {}) {
  return new Promise((resolve, reject) => {
    const img = new Image();
    if (crossOrigin) img.crossOrigin = "anonymous";
    img.onload = () => resolve(img);
    img.onerror = reject;
    img.src = url;
  });
}
async function loadAvatar(url) {
  if (!url) return null;
  // 1) 直接 (CORS許可されていればそのまま)
  try { return await loadImage(url); } catch {}
  // 2) weserv プロキシ経由（CORSヘッダ付きで返るため canvas 書き出し可）
  try {
    const proxied = "https://images.weserv.nl/?url=" + encodeURIComponent(url) + "&w=480&h=480&fit=cover";
    return await loadImage(proxied);
  } catch {}
  return null;
}

// ===== QRコード生成（njump へのリンク）。失敗したら null =====
async function makeQR(text) {
  try {
    const QR = (await import("https://esm.sh/qrcode@1.5.4")).default;
    const dataUrl = await QR.toDataURL(text, { margin: 1, width: 300, errorCorrectionLevel: "H", color: { dark: "#16233a", light: "#ffffff" } });
    return await loadImage(dataUrl);
  } catch {
    return null;
  }
}

// ===== 描画ユーティリティ =====
function roundRect(c, x, y, w, h, r) {
  c.beginPath();
  c.moveTo(x + r, y);
  c.arcTo(x + w, y, x + w, y + h, r);
  c.arcTo(x + w, y + h, x, y + h, r);
  c.arcTo(x, y + h, x, y, r);
  c.arcTo(x, y, x + w, y, r);
  c.closePath();
}
function fmtDate(ts) {
  const dt = new Date(ts * 1000);
  return `${dt.getFullYear()}年${String(dt.getMonth() + 1).padStart(2, "0")}月${String(dt.getDate()).padStart(2, "0")}日`;
}
function fmtISO(ts) {
  const dt = new Date(ts * 1000);
  const p = (n) => String(n).padStart(2, "0");
  return `${dt.getFullYear()}-${p(dt.getMonth() + 1)}-${p(dt.getDate())}`;
}
function licenseNo(d) {
  const m = (d.nip05 || "").match(/\d{2,}/);
  const num = m ? m[0].slice(0, 4) : String(parseInt(d.pubkeyHex.slice(0, 4), 16) % 10000).padStart(3, "0");
  return `NSTR-${num}-${new Date().getFullYear()}`;
}

// ===== ホログラム/ギロシェ用ヘルパー =====
function hexPath(c, cx, cy, r) {
  c.beginPath();
  for (let i = 0; i < 6; i++) {
    const a = (Math.PI / 180) * (60 * i - 90);
    const x = cx + r * Math.cos(a), y = cy + r * Math.sin(a);
    i ? c.lineTo(x, y) : c.moveTo(x, y);
  }
  c.closePath();
}
// スピログラフ状のギロシェ曲線
function guilloche(c, cx, cy, R, amp, k, turns, color, alpha, lw = 1) {
  c.save();
  c.globalAlpha = alpha;
  c.strokeStyle = color;
  c.lineWidth = lw;
  c.beginPath();
  const steps = turns * 160;
  for (let i = 0; i <= steps; i++) {
    const t = (i / steps) * turns * Math.PI * 2;
    const r = R + amp * Math.cos(k * t);
    const x = cx + r * Math.cos(t), y = cy + r * Math.sin(t);
    i ? c.lineTo(x, y) : c.moveTo(x, y);
  }
  c.stroke();
  c.restore();
}
// 六角形 N ロゴ
function drawHexLogo(c, cx, cy, s, colA, colB) {
  c.save();
  hexPath(c, cx, cy, s);
  const g = c.createLinearGradient(cx - s, cy - s, cx + s, cy + s);
  g.addColorStop(0, colA);
  g.addColorStop(1, colB);
  c.fillStyle = g;
  c.fill();
  c.lineWidth = Math.max(1, s * 0.05);
  c.strokeStyle = "rgba(255,255,255,0.55)";
  c.stroke();
  c.fillStyle = "#ffffff";
  c.textAlign = "center";
  c.textBaseline = "middle";
  c.font = `800 ${s * 1.0}px 'Hiragino Sans','Yu Gothic',sans-serif`;
  c.fillText("N", cx, cy + s * 0.06);
  c.restore();
}
// シールド＋鍵（ホログラム）
function drawShield(c, cx, cy, w, h, t) {
  c.save();
  const x = cx - w / 2, y = cy - h / 2;
  c.beginPath();
  c.moveTo(cx, y);
  c.lineTo(x + w, y + h * 0.2);
  c.lineTo(x + w, y + h * 0.55);
  c.quadraticCurveTo(x + w, y + h * 0.9, cx, y + h);
  c.quadraticCurveTo(x, y + h * 0.9, x, y + h * 0.55);
  c.lineTo(x, y + h * 0.2);
  c.closePath();
  const g = c.createLinearGradient(x, y, x + w, y + h);
  g.addColorStop(0, "#dfe8ff");
  g.addColorStop(0.5, "#ead9ff");
  g.addColorStop(1, "#ffe2ef");
  c.fillStyle = g;
  c.fill();
  c.lineWidth = 2.5;
  c.strokeStyle = t.border;
  c.globalAlpha = 0.75;
  c.stroke();
  c.globalAlpha = 1;
  // 鍵
  const lw = w * 0.26, lh = h * 0.2, lx = cx - lw / 2, ly = cy - lh * 0.1;
  c.fillStyle = t.accent;
  roundRect(c, lx, ly, lw, lh, 4);
  c.fill();
  c.lineWidth = w * 0.07;
  c.strokeStyle = t.accent;
  c.beginPath();
  c.arc(cx, ly, lw * 0.32, Math.PI, 0);
  c.stroke();
  c.restore();
}
// ホログラム印（虹色ロゼット＋N）
function drawHoloSeal(c, cx, cy, r) {
  c.save();
  const hues = ["#bfe3ff", "#d9c7ff", "#ffc9e6", "#c9ffe8", "#fff0c2"];
  for (let i = 0; i < 5; i++) guilloche(c, cx, cy, r - 6 - i * 3, 6 + i * 2, 7 + i, 3, hues[i], 0.55, 1.3);
  c.beginPath();
  c.arc(cx, cy, r, 0, Math.PI * 2);
  c.lineWidth = 2;
  c.strokeStyle = "rgba(120,140,220,0.5)";
  c.stroke();
  drawHexLogo(c, cx, cy, r * 0.34, "#7b3ff2", "#3a5bd0");
  c.restore();
}
// ステータスアイコン（アクセント色の簡易ベクター）
function drawStatIcon(c, name, x, y, s, color) {
  c.save();
  c.fillStyle = color;
  c.strokeStyle = color;
  c.lineWidth = s * 0.12;
  c.lineCap = "round";
  c.lineJoin = "round";
  if (name === "bubble") {
    roundRect(c, x, y, s, s * 0.78, s * 0.22);
    c.fill();
    c.beginPath();
    c.moveTo(x + s * 0.25, y + s * 0.72);
    c.lineTo(x + s * 0.18, y + s);
    c.lineTo(x + s * 0.45, y + s * 0.72);
    c.closePath();
    c.fill();
  } else if (name === "relay") {
    const pts = [[x + s * 0.5, y + s * 0.16], [x + s * 0.14, y + s * 0.84], [x + s * 0.86, y + s * 0.84]];
    c.beginPath();
    c.moveTo(pts[0][0], pts[0][1]); c.lineTo(pts[1][0], pts[1][1]);
    c.moveTo(pts[0][0], pts[0][1]); c.lineTo(pts[2][0], pts[2][1]);
    c.moveTo(pts[1][0], pts[1][1]); c.lineTo(pts[2][0], pts[2][1]);
    c.stroke();
    for (const p of pts) { c.beginPath(); c.arc(p[0], p[1], s * 0.13, 0, Math.PI * 2); c.fill(); }
  } else if (name === "shield") {
    c.beginPath();
    c.moveTo(x + s * 0.5, y);
    c.lineTo(x + s, y + s * 0.22);
    c.lineTo(x + s, y + s * 0.55);
    c.quadraticCurveTo(x + s, y + s * 0.92, x + s * 0.5, y + s);
    c.quadraticCurveTo(x, y + s * 0.92, x, y + s * 0.55);
    c.lineTo(x, y + s * 0.22);
    c.closePath();
    c.fill();
    c.strokeStyle = "#fff";
    c.lineWidth = s * 0.1;
    c.beginPath();
    c.moveTo(x + s * 0.3, y + s * 0.52);
    c.lineTo(x + s * 0.45, y + s * 0.68);
    c.lineTo(x + s * 0.72, y + s * 0.34);
    c.stroke();
  } else if (name === "bolt") {
    c.beginPath();
    c.moveTo(x + s * 0.56, y);
    c.lineTo(x + s * 0.16, y + s * 0.56);
    c.lineTo(x + s * 0.46, y + s * 0.56);
    c.lineTo(x + s * 0.4, y + s);
    c.lineTo(x + s * 0.84, y + s * 0.42);
    c.lineTo(x + s * 0.52, y + s * 0.42);
    c.closePath();
    c.fill();
  } else if (name === "person") {
    c.beginPath();
    c.arc(x + s * 0.5, y + s * 0.28, s * 0.22, 0, Math.PI * 2);
    c.fill();
    c.beginPath();
    c.moveTo(x + s * 0.1, y + s);
    c.quadraticCurveTo(x + s * 0.5, y + s * 0.5, x + s * 0.9, y + s);
    c.closePath();
    c.fill();
  }
  c.restore();
}
// スター評価（ダークなので白地でも視認性◎）
function drawStarRating(c, x, y, n, size, fill, empty) {
  c.textAlign = "left";
  c.textBaseline = "middle";
  c.font = `${size}px 'Hiragino Sans','Apple Color Emoji',sans-serif`;
  for (let i = 0; i < 5; i++) {
    c.fillStyle = i < n ? fill : empty;
    c.fillText(i < n ? "★" : "☆", x + i * size * 0.96, y);
  }
}
// 角丸長方形のラベル（カプセル型ではなく控えめなR）
function drawPill(c, text, x, y, { bg, fg, font, padX = 14, h = 34, r = 7 }) {
  c.font = font;
  c.textAlign = "left";
  c.textBaseline = "middle";
  const w = c.measureText(text).width + padX * 2;
  roundRect(c, x, y, w, h, r);
  c.fillStyle = bg;
  c.fill();
  c.fillStyle = fg;
  c.fillText(text, x + padX, y + h / 2 + 1);
  return w;
}

// 車アイコン（横向きセダンのシルエット）。cy は中心の縦位置。
function drawCar(c, x, cy, color) {
  c.save();
  c.translate(x, cy);
  c.fillStyle = color;
  c.beginPath();
  c.moveTo(2, 4);
  c.lineTo(2, -2);
  c.quadraticCurveTo(2, -5, 8, -6);
  c.lineTo(15, -6);
  c.quadraticCurveTo(20, -16, 31, -16);
  c.lineTo(40, -16);
  c.quadraticCurveTo(49, -15, 54, -6);
  c.lineTo(60, -5);
  c.quadraticCurveTo(64, -4, 64, 0);
  c.lineTo(64, 4);
  c.quadraticCurveTo(64, 7, 60, 7);
  c.lineTo(6, 7);
  c.quadraticCurveTo(2, 7, 2, 4);
  c.closePath();
  c.fill();
  // 窓
  c.fillStyle = "rgba(255,255,255,0.78)";
  c.beginPath();
  c.moveTo(19, -6); c.lineTo(23, -14); c.lineTo(31, -14); c.lineTo(31, -6); c.closePath(); c.fill();
  c.beginPath();
  c.moveTo(34, -6); c.lineTo(34, -14); c.lineTo(39, -14); c.quadraticCurveTo(46, -13, 49, -6); c.closePath(); c.fill();
  // タイヤ
  c.fillStyle = "#2a3550";
  c.beginPath(); c.arc(18, 8, 7, 0, Math.PI * 2); c.fill();
  c.beginPath(); c.arc(49, 8, 7, 0, Math.PI * 2); c.fill();
  c.fillStyle = "#e7ecf6";
  c.beginPath(); c.arc(18, 8, 3, 0, Math.PI * 2); c.fill();
  c.beginPath(); c.arc(49, 8, 3, 0, Math.PI * 2); c.fill();
  c.restore();
}

const THEMES = {
  jp:    { accent: "#2b4fce", accent2: "#7b3ff2", ink: "#16233a", sub: "#43507a", line: "#9fb1e6", border: "#3a5bd0", gold1: "#dcc07f", gold2: "#b48a3c", paper: ["#f3f6fd", "#eef0fb", "#fbf2f5"] },
  cyber: { accent: "#0a9fc0", accent2: "#d6249f", ink: "#142539", sub: "#3a5066", line: "#9bd3e2", border: "#0a9fc0", gold1: "#bcae72", gold2: "#8c7a38", paper: ["#eafaff", "#eef0fb", "#fde8f6"] },
  gold:  { accent: "#b4863a", accent2: "#9a6b1e", ink: "#2a2206", sub: "#5a4d22", line: "#dcc79a", border: "#b4863a", gold1: "#e6cd84", gold2: "#b4863a", paper: ["#fffaf0", "#fff4e2", "#fdeed6"] },
};

// ===== カード描画（高級ホログラム調 / 英語表記）=====
async function renderCard(d, theme = "jp") {
  const t = THEMES[theme] || THEMES.jp;
  const c = ctx;
  const W = canvas.width, H = canvas.height; // 1568 x 984
  c.clearRect(0, 0, W, H);
  c.lineCap = "round";
  c.lineJoin = "round";

  // Material Symbols（車アイコン）のフォント読み込みを待つ
  let carFont = false;
  try {
    await document.fonts.load('400 46px "Material Symbols Outlined"', "electric_car");
    carFont = document.fonts.check('400 46px "Material Symbols Outlined"');
  } catch {}

  // ===== ホログラム/セキュリティ用紙の背景 =====
  // ベースのイリデッセント・グラデーション
  const bg = c.createLinearGradient(0, 0, W, H);
  bg.addColorStop(0, t.paper[0]);
  bg.addColorStop(0.5, t.paper[1]);
  bg.addColorStop(1, t.paper[2]);
  roundRect(c, 0, 0, W, H, 24);
  c.fillStyle = bg;
  c.fill();

  // パネル内にクリップしてギロシェ等を描く
  c.save();
  roundRect(c, 0, 0, W, H, 24);
  c.clip();

  // 斜めの虹色シーン（ホログラム反射）
  const sheen = c.createLinearGradient(0, H, W, 0);
  sheen.addColorStop(0.0, "rgba(120,180,255,0.10)");
  sheen.addColorStop(0.35, "rgba(180,150,255,0.06)");
  sheen.addColorStop(0.6, "rgba(255,160,210,0.07)");
  sheen.addColorStop(0.85, "rgba(160,255,220,0.06)");
  sheen.addColorStop(1.0, "rgba(255,230,170,0.08)");
  c.fillStyle = sheen;
  c.fillRect(0, 0, W, H);

  // 全面の細密ギロシェ・メッシュ（2方向のサイン波が干渉して紙幣/ホログラム風に）
  // 上部ほど濃く、下に向かって淡くフェード
  c.save();
  for (let i = 0; i < 78; i++) {
    const yy = 26 + i * 12.4;
    c.globalAlpha = Math.max(0.06, 0.6 * (1 - yy / (H * 1.18)));
    c.strokeStyle = i % 2 ? t.line : t.accent;
    c.lineWidth = 0.7;
    c.beginPath();
    for (let x = 24; x <= W - 24; x += 5) {
      const y2 = yy
        + Math.sin(x / 44 + i * 0.55) * 7
        + Math.sin(x / 128 - i * 0.32) * 5
        + Math.cos(x / 320 + i * 0.12) * 3;
      x === 24 ? c.moveTo(x, y2) : c.lineTo(x, y2);
    }
    c.stroke();
  }
  // 縦方向の波（横波と交差させて織り地＝ギロシェ風に）
  for (let j = 0; j < 50; j++) {
    const xx = 24 + j * 31;
    c.globalAlpha = 0.09;
    c.strokeStyle = j % 2 ? t.accent : t.line;
    c.lineWidth = 0.6;
    c.beginPath();
    for (let y = 24; y <= H - 24; y += 6) {
      const x2 = xx + Math.sin(y / 50 + j * 0.5) * 6 + Math.sin(y / 150 - j * 0.3) * 4;
      y === 24 ? c.moveTo(x2, y) : c.lineTo(x2, y);
    }
    c.stroke();
  }
  c.restore();

  // ホログラムのロゼット紋様（重ねて立体感）
  guilloche(c, W * 0.20, H * 0.34, 230, 74, 9, 26, t.accent, 0.08, 0.9);
  guilloche(c, W * 0.20, H * 0.34, 150, 52, 14, 26, t.accent2, 0.07, 0.9);
  guilloche(c, W * 0.50, H * 0.50, 380, 104, 7, 30, t.accent, 0.05, 0.9);
  guilloche(c, W * 0.50, H * 0.50, 250, 84, 17, 26, t.accent2, 0.04, 0.9);
  guilloche(c, W * 0.83, H * 0.72, 210, 66, 11, 24, t.accent2, 0.07, 0.9);
  guilloche(c, W * 0.83, H * 0.72, 130, 46, 16, 24, t.accent, 0.06, 0.9);
  // 四隅の小ロゼット
  for (const [px, py] of [[110, 120], [W - 120, 120], [120, H - 110], [W - 120, H - 110]]) {
    guilloche(c, px, py, 70, 26, 13, 18, t.accent, 0.06, 0.8);
  }

  // 斜めの虹色シーン（ホログラム反射のきらめき）
  const streak = c.createLinearGradient(0, 0, W, H);
  streak.addColorStop(0.30, "rgba(255,255,255,0)");
  streak.addColorStop(0.44, "rgba(150,200,255,0.16)");
  streak.addColorStop(0.50, "rgba(210,170,255,0.18)");
  streak.addColorStop(0.56, "rgba(255,170,220,0.14)");
  streak.addColorStop(0.70, "rgba(255,255,255,0)");
  c.fillStyle = streak;
  c.fillRect(0, 0, W, H);

  // 透かしの大きな六角 N（名前と写真の間あたり）
  c.save();
  c.globalAlpha = 0.06;
  hexPath(c, W * 0.46, 330, 132);
  c.lineWidth = 10;
  c.strokeStyle = t.accent;
  c.stroke();
  c.fillStyle = t.accent;
  c.textAlign = "center";
  c.textBaseline = "middle";
  c.font = "800 150px 'Hiragino Sans',sans-serif";
  c.fillText("N", W * 0.46, 345);
  c.restore();

  c.restore(); // unclip

  // ===== 枠線（二重） =====
  c.lineWidth = 5;
  c.strokeStyle = t.border;
  roundRect(c, 10, 10, W - 20, H - 20, 20);
  c.stroke();
  c.lineWidth = 1.5;
  c.strokeStyle = "rgba(58,91,208,0.45)";
  roundRect(c, 22, 22, W - 44, H - 44, 14);
  c.stroke();

  const PAD = 70;

  // ===== ヘッダー =====
  c.textAlign = "left";
  c.textBaseline = "alphabetic";
  c.fillStyle = "#11151c";
  c.font = "800 76px 'Hiragino Sans','Yu Gothic','Arial Black',sans-serif";
  c.fillText("NOSTR LICENSE", PAD, 118);
  c.fillStyle = t.accent;
  c.font = "italic 600 30px 'Hiragino Sans','Georgia',serif";
  c.fillText("Your keys, your identity.", PAD + 4, 158);

  // NOSTR NETWORK + ロゴ（右上）
  c.textAlign = "right";
  c.fillStyle = t.accent;
  c.font = "800 30px 'Hiragino Sans',sans-serif";
  c.fillText("NOSTR NETWORK", W - PAD - 86, 102);
  drawHexLogo(c, W - PAD - 36, 90, 40, t.accent2, t.accent);

  // ヘッダー下の区切り線
  c.strokeStyle = t.line;
  c.lineWidth = 2;
  c.beginPath();
  c.moveTo(PAD, 182);
  c.lineTo(W * 0.62, 182);
  c.stroke();
  c.setLineDash([6, 8]);
  c.beginPath();
  c.moveTo(W * 0.62, 182);
  c.lineTo(W - PAD, 182);
  c.stroke();
  c.setLineDash([]);

  const rank = computeRank(d);

  // ===== 写真（中央右）=====
  const phX = 850, phY = 202, phW = 360, phH = 468, phR = 16;
  c.save();
  c.shadowColor = "rgba(30,40,80,0.28)";
  c.shadowBlur = 26;
  c.shadowOffsetY = 10;
  roundRect(c, phX, phY, phW, phH, phR);
  c.fillStyle = "#e7ecf6";
  c.fill();
  c.restore();
  c.save();
  roundRect(c, phX, phY, phW, phH, phR);
  c.clip();
  if (d._avatar) {
    const img = d._avatar;
    const ratio = Math.max(phW / img.width, phH / img.height);
    const dw = img.width * ratio, dh = img.height * ratio;
    c.drawImage(img, phX + (phW - dw) / 2, phY + (phH - dh) / 2, dw, dh);
  } else {
    c.fillStyle = t.sub;
    c.textAlign = "center";
    c.textBaseline = "middle";
    c.font = "22px sans-serif";
    c.fillText("NO IMAGE", phX + phW / 2, phY + phH / 2);
  }
  c.restore();
  c.lineWidth = 3;
  c.strokeStyle = "rgba(255,255,255,0.9)";
  roundRect(c, phX + 2, phY + 2, phW - 4, phH - 4, phR - 2);
  c.stroke();
  c.lineWidth = 2;
  c.strokeStyle = t.border;
  roundRect(c, phX, phY, phW, phH, phR);
  c.stroke();

  // ===== 左カラム：フィールド =====
  const lx = PAD;
  const fieldMaxW = phX - 40 - lx;
  c.textAlign = "left";
  c.textBaseline = "alphabetic";

  // NAME（他フィールドと同じピル・ラベルに統一）
  drawPill(c, "NAME", lx, 208, { bg: t.accent, fg: "#fff", font: "700 22px 'Hiragino Sans',sans-serif", h: 34 });
  c.fillStyle = t.ink;
  c.textAlign = "left";
  c.font = "800 58px 'Hiragino Sans','Yu Gothic',sans-serif";
  c.fillText(d.name, lx, 292);          // NAME ピルとの間を詰める
  if (d.handle) {
    c.fillStyle = t.accent;
    c.font = "600 28px 'Hiragino Sans',sans-serif";
    c.fillText(d.handle, lx, 348);      // 名前との間を広げる
  }

  // ID (npub)：ピルと値を近づけて1グループに見せる
  drawPill(c, "ID (npub)", lx, 386, { bg: t.accent, fg: "#fff", font: "700 22px 'Hiragino Sans',sans-serif", h: 34 });
  c.fillStyle = t.ink;
  let np = 28;
  while (np > 14) {
    c.font = `600 ${np}px 'SF Mono','Menlo','Consolas',monospace`;
    if (c.measureText(d.npub).width <= fieldMaxW) break;
    np -= 1;
  }
  c.fillText(d.npub, lx, 444);

  // NIP-05（前グループとの間を広めに取って区切る）
  drawPill(c, "NIP-05", lx, 496, { bg: t.accent, fg: "#fff", font: "700 22px 'Hiragino Sans',sans-serif", h: 34 });
  c.fillStyle = t.ink;
  let n5 = 32;
  const nip05Text = d.nip05 ? d.nip05 + "  ✓" : "— not set —";
  while (n5 > 14) {
    c.font = `600 ${n5}px 'SF Mono','Menlo','Consolas',monospace`;
    if (c.measureText(nip05Text).width <= fieldMaxW) break;
    n5 -= 1;
  }
  c.fillText(nip05Text, lx, 552);

  // 下段3カラム（ISSUED / FIRST SEEN / LICENSE CLASS）
  // 日付は控えめに細く小さく。パネルとの間に余白を確保。
  const THREE_YEARS = 3 * 365.25 * 24 * 3600;
  const rowY = 614;
  const col = [lx, lx + 230, lx + 450];
  c.fillStyle = t.sub;
  c.font = "700 19px 'Hiragino Sans',sans-serif";
  c.fillText("ISSUED", col[0], rowY);
  c.fillText("FIRST SEEN", col[1], rowY);
  c.fillText("LICENSE CLASS", col[2], rowY);
  c.fillStyle = t.ink;
  c.font = "400 25px 'Hiragino Sans',sans-serif";
  c.fillText(fmtISO(Math.floor(Date.now() / 1000)), col[0], rowY + 32);
  c.fillText(d.sinceAt ? fmtISO(d.sinceAt) : "—", col[1], rowY + 32);
  drawPill(c, rank, col[2], rowY + 20, { bg: t.accent2, fg: "#fff", font: "700 21px 'Hiragino Sans',sans-serif", h: 34 });

  // ===== 右カラム：LICENSE NO / VALID THRU / シールド / QR =====
  const rlx = 1250;            // 右カラムの左揃え位置
  const rcx = 1392;            // 右カラム中央（シールド/IDENTITY/QR用）
  c.textAlign = "left";
  c.fillStyle = t.sub;
  c.font = "700 22px 'Hiragino Sans',sans-serif";
  c.fillText("LICENSE NO.", rlx, 222);
  c.fillStyle = t.ink;
  c.font = "500 25px 'Hiragino Sans',sans-serif";
  c.fillText(licenseNo(d), rlx, 260);
  c.fillStyle = t.sub;
  c.font = "700 22px 'Hiragino Sans',sans-serif";
  c.fillText("VALID THRU", rlx, 316);
  c.fillStyle = t.ink;
  c.font = "500 25px 'Hiragino Sans',sans-serif";
  c.fillText(fmtISO(d.lastActivity + THREE_YEARS), rlx, 354);

  // シールド
  drawShield(c, rcx, 442, 96, 116, t);
  c.fillStyle = t.sub;
  c.textAlign = "center";
  c.font = "700 22px 'Hiragino Sans',sans-serif";
  c.fillText("SELF-SOVEREIGN", rcx, 528);
  c.fillText("IDENTITY", rcx, 554);

  // QR（中央に N ロゴ）
  if (d._qr) {
    const qs = 150, qx = rcx - qs / 2, qy = 588;
    c.fillStyle = "#fff";
    roundRect(c, qx - 10, qy - 10, qs + 20, qs + 20, 14);
    c.fill();
    c.drawImage(d._qr, qx, qy, qs, qs);
    drawHexLogo(c, qx + qs / 2, qy + qs / 2, 21, t.accent2, t.accent);
  }

  // ===== ステータス・パネル（DRIVER PROFILE）=====
  const pnX = 60, pnY = 712, pnW = 1000, pnH = 182;
  c.save();
  c.shadowColor = "rgba(80,60,20,0.18)";
  c.shadowBlur = 16;
  c.shadowOffsetY = 6;
  const pg = c.createLinearGradient(pnX, pnY, pnX, pnY + pnH);
  pg.addColorStop(0, "#f6efdc");
  pg.addColorStop(1, "#efe6cf");
  roundRect(c, pnX, pnY, pnW, pnH, 12);
  c.fillStyle = pg;
  c.fill();
  c.restore();
  c.lineWidth = 1.5;
  c.strokeStyle = t.gold2;
  c.globalAlpha = 0.6;
  roundRect(c, pnX, pnY, pnW, pnH, 12);
  c.stroke();
  c.globalAlpha = 1;

  // タブ・バナー（右肩が長く下に向かって斜めに切れるリボン。文字との余白を確保）
  c.save();
  const tbW = 404, tbH = 46, tbX = pnX + 16, tbY = pnY - 20;
  c.beginPath();
  c.moveTo(tbX, tbY + 12);
  c.arcTo(tbX, tbY, tbX + 12, tbY, 12);
  c.lineTo(tbX + tbW, tbY);             // 上辺は右端まで（右肩が突き出る）
  c.lineTo(tbX + tbW - 28, tbY + tbH);  // 斜めに下りて下辺は短い
  c.lineTo(tbX + 12, tbY + tbH);
  c.arcTo(tbX, tbY + tbH, tbX, tbY + tbH - 12, 12);
  c.closePath();
  const tg = c.createLinearGradient(tbX, tbY, tbX, tbY + tbH);
  tg.addColorStop(0, t.gold1);
  tg.addColorStop(1, t.gold2);
  c.fillStyle = tg;
  c.fill();
  c.fillStyle = "#3a2c08";
  c.textAlign = "left";
  c.textBaseline = "middle";
  c.font = "800 24px 'Hiragino Sans',sans-serif";
  c.fillText("NOSTR DRIVER PROFILE", tbX + 24, tbY + tbH / 2 + 1);
  c.restore();

  // ステータス 2列×3行
  const stats = computeStars(d);
  const colX = [pnX + 40, pnX + 510];
  const rowsY = [pnY + 56, pnY + 106, pnY + 156];
  for (let i = 0; i < stats.length; i++) {
    const s = stats[i];
    const cxp = colX[i % 2];
    const cyp = rowsY[Math.floor(i / 2)];
    drawStatIcon(c, s.icon, cxp, cyp - 15, 28, t.accent);
    c.fillStyle = t.ink;
    c.textAlign = "left";
    c.textBaseline = "middle";
    c.font = "700 27px 'Hiragino Sans',sans-serif";
    c.fillText(s.label, cxp + 44, cyp);
    drawStarRating(c, cxp + 296, cyp, s.n, 28, "#1e2a5a", "#b9c1d7");
  }

  // ===== 署名・AUTHORIZED・ホロ印（パネル右）=====
  c.fillStyle = "#1b2336";
  c.textAlign = "center";
  c.textBaseline = "alphabetic";
  c.font = "italic 600 46px 'Snell Roundhand','Apple Chancery','Brush Script MT',cursive";
  c.fillText(d.handle || d.name, 1285, 828);
  c.fillStyle = t.sub;
  c.font = "700 20px 'Hiragino Sans',sans-serif";
  c.fillText("AUTHORIZED BY NOSTR", 1285, 866);
  drawHoloSeal(c, 1486, 832, 48);

  // ===== 最下段：キャッチコピー（パネル・枠線に被らない高さに）=====
  const capY = 936;
  if (carFont) {
    c.fillStyle = t.accent;
    c.textAlign = "left";
    c.textBaseline = "middle";
    c.font = '400 40px "Material Symbols Outlined"';
    c.fillText("electric_car", PAD, capY);
  } else {
    drawCar(c, PAD, capY, t.accent);
  }
  c.fillStyle = "#2a3550";
  c.textAlign = "left";
  c.textBaseline = "middle";
  c.font = "600 25px 'Hiragino Sans',sans-serif";
  c.fillText("Drive the decentralized future.", PAD + 64, capY);

  c.fillStyle = t.accent;
  c.textAlign = "right";
  c.font = "800 25px 'Hiragino Sans',sans-serif";
  c.fillText("BUCKLE UP, STAY DECENTRALIZED.", W - PAD, capY);

  $("download-btn").disabled = false;
}

// ===== 発行フロー（取得元が NIP-07 でも手入力でも完全に同じ処理）=====
async function issueFor(pubkeyHex) {
  try {
    if (getActiveRelays().length === 0) {
      throw new Error("リレーを最低1つ指定してください");
    }
    const useUserRelays = $("use-user-relays").checked;
    const data = await fetchProfile(pubkeyHex, { useUserRelays });
    setStatus("アバター / QR を生成中…");
    const [avatar, qr] = await Promise.all([
      loadAvatar(data.picture),
      makeQR("https://njump.me/" + data.npub),
    ]);
    data._avatar = avatar;
    data._qr = qr;
    lastData = data;

    await renderCard(data, $("theme-select").value);
    setStatus(
      `発行完了：${data.name}｜投稿 ${data.activity}${data.activityCapped ? "+" : ""} / WoT ${data.wotValue} / リレー ${data.relayCount} / Zap受信 ⚡${data.zapRecv} 送信 ⚡${data.zapSent}`,
      "ok"
    );
  } catch (err) {
    console.error(err);
    setStatus("エラー: " + (err?.message || err), "error");
  }
}

// npub 文字列/hex を hex pubkey に正規化
function toHexPubkey(raw) {
  raw = raw.trim();
  if (raw.startsWith("npub1")) return nip19.decode(raw).data;
  if (/^[0-9a-f]{64}$/i.test(raw)) return raw.toLowerCase();
  throw new Error("npub1... または64桁hexを入力してください");
}

// ===== イベント =====
// NIP-07：公開鍵を取得して入力欄に入れるだけ（解析はしない）
$("nip07-btn").addEventListener("click", async () => {
  if (!window.nostr) {
    setStatus("NIP-07拡張機能が見つかりません（nos2x / Alby などをインストールしてください）", "error");
    return;
  }
  try {
    setStatus("NIP-07で公開鍵を取得中…");
    const pk = await window.nostr.getPublicKey();
    $("npub-input").value = nip19.npubEncode(pk);
    setStatus("npub を取得しました。「発行」を押してください。", "ok");
  } catch (err) {
    setStatus("エラー: " + (err?.message || err), "error");
  }
});

// 発行
$("manual-btn").addEventListener("click", async () => {
  const raw = $("npub-input").value.trim();
  if (!raw) { setStatus("npub を入力してください", "error"); return; }
  try {
    await issueFor(toHexPubkey(raw));
  } catch (err) {
    setStatus("エラー: " + (err?.message || err), "error");
  }
});

$("theme-select").addEventListener("change", () => {
  if (lastData) renderCard(lastData, $("theme-select").value);
});

// ===== リレーエディタ =====
function addRelayRow(value = "") {
  const list = $("relay-list");
  const row = document.createElement("div");
  row.className = "relay-row";
  const input = document.createElement("input");
  input.type = "text";
  input.className = "relay-input";
  input.placeholder = "wss://relay.example.com";
  input.value = value;
  const del = document.createElement("button");
  del.className = "btn relay-del";
  del.type = "button";
  del.textContent = "×";
  del.title = "このリレーを削除";
  del.addEventListener("click", () => {
    row.remove();
    if ($("relay-list").children.length === 0) addRelayRow(); // 最低1行は残す
  });
  row.append(input, del);
  list.appendChild(row);
}

$("add-relay-btn").addEventListener("click", () => addRelayRow());

// 初期リレー行（デフォルト3つ）
DEFAULT_RELAYS.forEach((r) => addRelayRow(r));

// 初期プレースホルダ描画
(function initPlaceholder() {
  const t = THEMES.jp;
  const g = ctx.createLinearGradient(0, 0, canvas.width, canvas.height);
  g.addColorStop(0, t.paper[0]);
  g.addColorStop(0.5, t.paper[1]);
  g.addColorStop(1, t.paper[2]);
  roundRect(ctx, 0, 0, canvas.width, canvas.height, 24);
  ctx.fillStyle = g;
  ctx.fill();
  ctx.lineWidth = 5;
  ctx.strokeStyle = t.border;
  roundRect(ctx, 10, 10, canvas.width - 20, canvas.height - 20, 20);
  ctx.stroke();
  guilloche(ctx, canvas.width * 0.5, canvas.height * 0.5, 320, 90, 7, 18, t.accent, 0.06, 1);
  ctx.fillStyle = t.sub;
  ctx.font = "700 30px 'Hiragino Sans','Noto Sans JP',sans-serif";
  ctx.textAlign = "center";
  ctx.textBaseline = "middle";
  ctx.fillText("Enter an npub and press 発行 to issue", canvas.width / 2, canvas.height / 2);
})();

$("download-btn").addEventListener("click", () => {
  try {
    const url = canvas.toDataURL("image/png");
    const a = document.createElement("a");
    a.href = url;
    a.download = "nostr-license.png";
    a.click();
  } catch (err) {
    setStatus("ダウンロード失敗（アバター画像のCORS制限の可能性）: " + err.message, "error");
  }
});
