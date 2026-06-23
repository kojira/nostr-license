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

// Nostr の created_at は唯一の時刻でユーザーが詐称しうる（rkey-TID のような代替が無い）。
// 緩い範囲外（2020以前・未来）は時系列集計から除外する best-effort なガード。
const NOSTR_MIN = Date.parse("2020-01-01T00:00:00Z") / 1000;
const saneSec = (s) => Number.isFinite(s) && s >= NOSTR_MIN && s <= Date.now() / 1000 + 86400;

// bolt11 インボイスの HRP から金額を sats で取り出す。
// HRP = ln + 通貨(bc/tb/...) + 金額 + 倍率(m/u/n/p) + "1"(区切り)。
// 倍率→BTC: m=1e-3, u=1e-6, n=1e-9, p=1e-12。sats = BTC * 1e8。
// 倍率なし = 整数 BTC。金額なし（lnbc1...）は null。
function bolt11Sats(inv) {
  if (typeof inv !== "string") return null;
  const m = /^ln(?:bc|tb|bcrt|sb)(\d+)([munp])?1/i.exec(inv.toLowerCase());
  if (!m) return null;
  const num = parseInt(m[1], 10);
  if (!Number.isFinite(num)) return null;
  const factor = { "": 1e8, m: 1e5, u: 1e2, n: 0.1, p: 1e-4 };
  return Math.round(num * factor[m[2] || ""]);
}

// kind:9735（Zap receipt）1件の金額を sats で返す。
// 金額の真正な情報源は bolt11（実際に支払われたインボイス額）。description の
// amount タグなどへのフォールバックは付けない（不具合を隠すため）。bolt11 が
// 無い／パースできない受領は 0 とし、取りこぼしはそのまま表面化させる。
function zapSats(ev) {
  const tags = ev && ev.tags;
  if (!Array.isArray(tags)) return 0;
  const bolt11 = tags.find((x) => x[0] === "bolt11")?.[1];
  const s = bolt11Sats(bolt11);
  return s == null ? 0 : s;
}

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
    // primal cache service は標準リレーではない（独自 cache プロトコル）。
    // 通常のフィルタ取得には混ぜず、zap 集計だけ専用パス（fetchPrimalZaps）で使う。
    if (/(?:^|\.)primal\.net/i.test(v)) continue;
    out.push(v);
  }
  return out;
}

// ===== Primal cache service（zap 集計専用）=====
// 標準リレーと違い ["REQ", id, {cache:[method, params]}] という独自プロトコル。
// 送信 zap は description.pubkey で全ネットワーク index 済み（リレー直クエリの #P は
// receipt の 0.4% しか拾えないため、ここで正確な送信総額を取る）。
const PRIMAL_URL = "wss://cache2.primal.net/v1";
function openPrimal() {
  let ws;
  try { ws = new WebSocket(PRIMAL_URL); } catch { return null; }
  const subs = new Map();
  const ready = new Promise((res, rej) => {
    ws.onopen = () => res();
    ws.onerror = () => rej(new Error("primal open failed"));
  });
  ws.onmessage = (e) => {
    let d; try { d = JSON.parse(e.data); } catch { return; }
    if (!Array.isArray(d)) return;
    const s = subs.get(d[1]); if (!s) return;
    if (d[0] === "EVENT" && d[2]) s.evs.push(d[2]);
    else if (d[0] === "EOSE") s.done();
  };
  const req = (method, params, timeoutMs = 12000) => new Promise((resolve) => {
    const sub = "pz" + Math.random().toString(36).slice(2, 9);
    const evs = []; let fin = false;
    const done = () => { if (fin) return; fin = true; clearTimeout(timer); subs.delete(sub); try { ws.send(JSON.stringify(["CLOSE", sub])); } catch {} resolve(evs); };
    const timer = setTimeout(done, timeoutMs);
    subs.set(sub, { evs, done });
    try { ws.send(JSON.stringify(["REQ", sub, { cache: [method, params] }])); } catch { done(); }
  });
  const close = () => { try { ws.close(); } catch {} };
  return { ready, req, close };
}

// 指定方向（sender=送信 / receiver=受信）の zap を primal からページング集計する。
// user_zaps は 1 リクエスト 50 件程度なので直列だと遅い。primal は since/until を
// サーバー側で解釈するため、時間範囲を N 分割して【各チャンクを並列ページング】し
// 全体時間を短縮する。重複は zap_receipt_id でグローバル排除。専用接続 1 本で多重化。
async function primalZapDir(hex, dirKey) {
  const conn = openPrimal();
  if (!conn) return null;
  try {
    await Promise.race([conn.ready, new Promise((_, rej) => setTimeout(() => rej(new Error("timeout")), 8000))]);
    const seen = new Map();   // id -> { sats, t }
    const now = Math.floor(Date.now() / 1000);
    const floor = Math.floor(Date.parse("2021-01-01T00:00:00Z") / 1000);
    const N = 8;
    const step = Math.ceil((now - floor) / N);
    const chunk = async (cSince, cUntil) => {
      let until = cUntil;
      for (let pg = 0; pg < 80; pg++) {
        const evs = await conn.req("user_zaps", { [dirKey]: hex, since: cSince, until, limit: 1000 });
        const recs = evs.filter((e) => e.kind === 10000129).map((e) => { try { return JSON.parse(e.content); } catch { return null; } }).filter(Boolean);
        if (!recs.length) break;
        let oldest = Infinity, fresh = 0;
        for (const z of recs) {
          const id = z.zap_receipt_id || z.event_id;
          if (!seen.has(id)) { seen.set(id, { sats: Number(z.amount_sats) || 0, t: z.created_at }); fresh++; }
          if (z.created_at < oldest) oldest = z.created_at;
        }
        if (fresh === 0 || !isFinite(oldest) || oldest <= cSince || oldest >= until) break;
        until = oldest - 1;
      }
    };
    const tasks = [];
    for (let i = 0; i < N; i++) {
      const cSince = Math.min(now, floor + step * i);
      const cUntil = i === N - 1 ? now : Math.min(now, floor + step * (i + 1));
      if (cUntil > cSince) tasks.push(chunk(cSince, cUntil));
    }
    await Promise.all(tasks);
    let count = 0, sats = 0; const times = [];
    for (const v of seen.values()) { count++; sats += v.sats; times.push(v.t); }
    return { count, sats, times };
  } catch {
    return null;
  } finally {
    conn.close();
  }
}

// primal の user_profile から受信 zap の権威集計（total_zap_count / total_satszapped）。
// ページング合算はチャンクが途中終了すると取りこぼし得る（大口が欠けると合計が大きく
// ズレる）ため、受信の件数・金額はこの集計値を使う（1 リクエストで正確）。
async function primalProfileZaps(hex) {
  const conn = openPrimal();
  if (!conn) return null;
  try {
    await Promise.race([conn.ready, new Promise((_, rej) => setTimeout(() => rej(new Error("timeout")), 8000))]);
    const prof = await conn.req("user_profile", { pubkey: hex });
    const stat = prof.find((e) => e.kind === 10000105);
    if (!stat) return null;
    const j = JSON.parse(stat.content);
    return { count: j.total_zap_count || 0, sats: j.total_satszapped || 0 };
  } catch {
    return null;
  } finally {
    conn.close();
  }
}

// zap 送受信を primal（全ネットワーク index）から取得。3 接続を並列に走らせる：
//  - 送信: user_zaps{sender} ページング（件数/金額/時系列。送信の集計 API は無い）
//  - 受信時系列: user_zaps{receiver} ページング（グラフ用。多少の取りこぼしは shape に影響小）
//  - 受信集計: user_profile（件数/金額の権威値。取りこぼさない）
// primal が全滅なら null（リレー直クエリの #P/#p は receipt の 0.4% しか拾えず誤解を
// 招くため zap のフォールバックには使わない＝取得不能として扱う）。
async function fetchPrimalZaps(hex) {
  const [sent, recvTimes, recvAgg] = await Promise.all([
    primalZapDir(hex, "sender"),
    primalZapDir(hex, "receiver"),
    primalProfileZaps(hex),
  ]);
  if (!sent && !recvTimes && !recvAgg) return null;
  const recv = recvAgg
    ? { count: recvAgg.count, sats: recvAgg.sats, times: recvTimes?.times || [] }
    : (recvTimes || null);   // 集計が取れなければページング値で代用
  return { sent, recv };
}

let lastData = null; // 直近に描画したデータ（テーマ切替・再描画用）

function setStatus(msg, kind = "") {
  const el = $("status");
  el.textContent = msg;
  el.className = "status" + (kind ? " " + kind : "");
}

// リレーごとの取得進捗をライブ表示する（複数プールの stats を URL で合算）。
function renderRelayProgress(pools, done, walkState) {
  const el = document.getElementById("fetch-progress");
  if (!el) return;
  const byUrl = new Map();
  for (const p of pools) {
    for (const s of p.stats()) {
      const cur = byUrl.get(s.url) || { url: s.url, events: 0, reqs: 0, active: 0, states: [] };
      cur.events += s.events; cur.reqs += s.reqs; cur.active += s.active; cur.states.push(s.state);
      byUrl.set(s.url, cur);
    }
  }
  const rows = [...byUrl.values()].map((r) => {
    const open = r.states.some((st) => st === "open");
    const allFailed = r.states.length && r.states.every((st) => st === "failed");
    // 窓走査（最長処理）が始まって active が 0 に戻ったリレーは EOSE で取り切り＝完了。
    const w = walkState && walkState.get(r.url);
    const walkDone = w && w.started && w.active === 0;
    const walking = w && w.active > 0;
    let label, cls;
    if (allFailed) { label = "接続失敗"; cls = "rs-fail"; }
    else if (done || walkDone) { label = r.events > 0 ? "完了" : "投稿なし"; cls = r.events > 0 ? "rs-ok" : "rs-empty"; }
    else if (r.active > 0 || walking) { label = "取得中…"; cls = "rs-active"; }
    else if (open) { label = "待機"; cls = "rs-idle"; }
    else { label = "接続中…"; cls = "rs-idle"; }
    const host = r.url.replace(/^wss?:\/\//, "");
    return `<li class="relay-prog ${cls}"><span class="rp-dot"></span><span class="rp-host">${host}</span><span class="rp-status">${label}</span><span class="rp-meta">${r.events.toLocaleString()} 件</span></li>`;
  }).join("");
  const total = [...byUrl.values()].reduce((a, r) => a + r.events, 0);
  el.hidden = false;
  el.innerHTML = `<div class="rp-head"><span>${done ? "取得完了" : "リレーから取得中…"}</span><span class="rp-total">受信 ${total.toLocaleString()} 件</span></div><ul class="relay-prog-list">${rows}</ul>`;
}

// ===== リレー接続プール =====
// リレーごとに WebSocket を1本だけ永続化し、サブIDでクエリを多重化する。
// 「呼び出しごとに新規ソケットを大量に開く」接続ストームを避けることで、
// リレー側の切断・取りこぼしを減らし、取得件数を安定させる。
function createPool(relays, { maxInflight = 16 } = {}) {
  const conns = relays.map((url) => {
    // state: connecting / open / failed（接続できず）/ closed（一度開いて閉じた）。
    // events: このリレーから受信したイベント総数（進捗表示用）。reqs: 投げた REQ 数。
    const c = { url, ws: null, ready: false, everReady: false, closed: false, subs: new Map(), queue: [], state: "connecting", events: 0, reqs: 0 };
    const connect = () => {
      if (c.closed) return;
      let ws;
      try { ws = new WebSocket(url); } catch { c.state = "failed"; return; }
      c.ws = ws;
      ws.onopen = () => {
        c.ready = true; c.everReady = true; c.state = "open";
        for (const m of c.queue) { try { ws.send(m); } catch {} }
        c.queue = [];
      };
      ws.onmessage = (e) => {
        let d; try { d = JSON.parse(e.data); } catch { return; }
        if (!Array.isArray(d)) return;
        const sub = c.subs.get(d[1]); if (!sub) return;
        if (d[0] === "EVENT" && d[2]) { c.events++; sub.onEvent(d[2]); }
        else if (d[0] === "EOSE") sub.onEose();
        else if (d[0] === "CLOSED") sub.onClosed();   // リレーがサブを拒否/打ち切り＝不完全
      };
      ws.onclose = () => {
        c.ready = false; c.ws = null;
        c.state = c.everReady ? "closed" : "failed";
        for (const sub of [...c.subs.values()]) sub.onDrop();
      };
      ws.onerror = () => { if (!c.everReady) c.state = "failed"; };
    };
    c.send = (m) => {
      if (m.startsWith('["REQ')) c.reqs++;
      if (c.ready && c.ws) { try { c.ws.send(m); } catch { c.queue.push(m); } }
      else { c.queue.push(m); if (!c.ws && !c.closed) connect(); }
    };
    connect();
    return c;
  });

  let subN = 0;

  // 同時サブスク数を制限（接続あたりの上限超過で CLOSED 拒否される取りこぼしを防ぐ）。
  let inflight = 0;
  const waiters = [];
  const acquire = () => new Promise((res) => {
    if (inflight < maxInflight) { inflight++; res(); } else waiters.push(res);
  });
  const release = () => {
    inflight--;
    if (waiters.length && inflight < maxInflight) { inflight++; waiters.shift()(); }
  };

  // 全イベントを収集して返す。戻り値の .complete = 接続できた全リレーが EOSE を返したか。
  // CLOSED（拒否/打ち切り）は EOSE と区別し、不完全扱い → queryStable が再取得する。
  const rawQuery = (filters, { timeoutMs = 9000 } = {}) => new Promise((resolve) => {
    const subId = "q" + (subN++);
    const events = new Map();
    const eosed = new Set();
    const finished = new Set();
    let settled = false;
    const total = conns.length;
    const finish = () => {
      if (settled) return; settled = true; clearTimeout(timer);
      for (const c of conns) {
        c.subs.delete(subId);
        if (c.ready && c.ws) { try { c.ws.send(JSON.stringify(["CLOSE", subId])); } catch {} }
      }
      const expected = conns.filter((c) => c.everReady).length;
      const out = [...events.values()];
      out.complete = expected > 0 && eosed.size >= expected;
      resolve(out);
    };
    if (total === 0) { const o = []; o.complete = true; return resolve(o); }
    const timer = setTimeout(finish, timeoutMs);
    for (const c of conns) {
      const mark = () => { finished.add(c.url); if (finished.size >= total) finish(); };
      c.subs.set(subId, {
        onEvent: (ev) => { events.set(ev.id, ev); },
        onEose: () => { eosed.add(c.url); mark(); },
        onClosed: () => { mark(); },   // eosed に入れない = 不完全
        onDrop: () => { mark(); },
      });
      c.send(JSON.stringify(["REQ", subId, ...filters]));
    }
  });

  // 1件でも EVENT が来たら即 true、全 EOSE/CLOSED/タイムアウトで false（Streak 日次判定用）。
  const rawHas = (filter, { timeoutMs = 2500 } = {}) => new Promise((resolve) => {
    const subId = "h" + (subN++);
    const finished = new Set();
    let settled = false;
    const total = conns.length;
    const done = (val) => {
      if (settled) return; settled = true; clearTimeout(timer);
      for (const c of conns) {
        c.subs.delete(subId);
        if (c.ready && c.ws) { try { c.ws.send(JSON.stringify(["CLOSE", subId])); } catch {} }
      }
      resolve(val);
    };
    if (total === 0) return resolve(false);
    const timer = setTimeout(() => done(false), timeoutMs);
    for (const c of conns) {
      const mark = () => { finished.add(c.url); if (finished.size >= total) done(false); };
      c.subs.set(subId, { onEvent: () => done(true), onEose: mark, onClosed: mark, onDrop: mark });
      c.send(JSON.stringify(["REQ", subId, filter]));
    }
  });

  // 単一リレーへのクエリ（リレーごとに独立ページングするため）。.complete = そのリレーが EOSE したか。
  const rawQueryOne = (url, filters, { timeoutMs = 9000 } = {}) => new Promise((resolve) => {
    const c = conns.find((x) => x.url === url);
    if (!c) { const o = []; o.complete = false; return resolve(o); }
    const subId = "o" + (subN++);
    const events = new Map();
    let settled = false, eosed = false;
    const finish = () => {
      if (settled) return; settled = true; clearTimeout(timer);
      c.subs.delete(subId);
      if (c.ready && c.ws) { try { c.ws.send(JSON.stringify(["CLOSE", subId])); } catch {} }
      const out = [...events.values()]; out.complete = eosed; resolve(out);
    };
    const timer = setTimeout(finish, timeoutMs);
    c.subs.set(subId, {
      onEvent: (ev) => { events.set(ev.id, ev); },
      onEose: () => { eosed = true; finish(); },
      onClosed: () => { finish(); },   // 拒否/打ち切り = 不完全
      onDrop: () => { finish(); },
    });
    c.send(JSON.stringify(["REQ", subId, ...filters]));
  });

  const query = async (filters, opts) => {
    await acquire();
    try { return await rawQuery(filters, opts); } finally { release(); }
  };
  const has = async (filter, opts) => {
    await acquire();
    try { return await rawHas(filter, opts); } finally { release(); }
  };
  const queryOne = async (url, filters, opts) => {
    await acquire();
    try { return await rawQueryOne(url, filters, opts); } finally { release(); }
  };

  // リレーごとの進捗スナップショット（state / 受信イベント数 / 進行中サブ数 / REQ数）。
  const stats = () => conns.map((c) => ({ url: c.url, state: c.state, events: c.events, reqs: c.reqs, active: c.subs.size }));

  const close = () => { for (const c of conns) { c.closed = true; try { c.ws && c.ws.close(); } catch {} } };
  return { query, has, queryOne, close, stats, size: relays.length, urls: relays.slice() };
}

// 全リレー merged の未完了なら再取得してマージ（単発クエリ・浅い探索用）。
async function queryStable(pool, filters, { timeoutMs = 9000, retries = 2 } = {}) {
  let evs = await pool.query(filters, { timeoutMs });
  let t = 0;
  while (!evs.complete && t < retries) {
    t++;
    const more = await pool.query(filters, { timeoutMs: timeoutMs + 3000 * t });
    const m = new Map();
    for (const e of evs) m.set(e.id, e);
    for (const e of more) m.set(e.id, e);
    const merged = [...m.values()];
    merged.complete = more.complete;
    evs = merged;
  }
  return evs;
}

// 単一リレーの未完了（EOSE 前にタイムアウト/CLOSED）なら再取得してマージ。
async function queryStableOne(pool, url, filters, { timeoutMs = 9000, retries = 2 } = {}) {
  let evs = await pool.queryOne(url, filters, { timeoutMs });
  let t = 0;
  while (!evs.complete && t < retries) {
    t++;
    const more = await pool.queryOne(url, filters, { timeoutMs: timeoutMs + 3000 * t });
    const m = new Map();
    for (const e of evs) m.set(e.id, e);
    for (const e of more) m.set(e.id, e);
    const merged = [...m.values()];
    merged.complete = more.complete;
    evs = merged;
  }
  return evs;
}

// ===== until を遡って全件をページ収集（リレーの limit 上限を越えて過去も集める）=====
// 【リレーごとに独立してページング】する。merged で全リレー共通の until を使うと、
// 保持密度の違うリレーが混ざったとき、疎なリレーの古いイベントが until を引き下げて
// 密なリレーの中間データを丸ごと飛ばす（=大量取りこぼし）ため。各リレーを単独で
// 末尾まで辿り、最後に id で統合する。
async function fetchAllPaged(filterBase, pool, { maxPages = 6, pageLimit = 500, timeoutMs = 9000 } = {}) {
  const all = new Map();
  await Promise.all(pool.urls.map(async (url) => {
    let until = null;
    for (let i = 0; i < maxPages; i++) {
      const f = { ...filterBase, limit: pageLimit };
      if (until != null) f.until = until - 1;
      const evs = await queryStableOne(pool, url, [f], { timeoutMs });
      let minTs = Infinity;
      for (const e of evs) {
        if (!all.has(e.id)) all.set(e.id, e);
        if (e.created_at < minTs) minTs = e.created_at;
      }
      if (!evs.length) break;
      if (until != null && minTs >= until) break;          // 進捗なし
      until = minTs;
      // 単一リレーなので「完全応答で上限未満 = そのリレーは枯渇」と判断してよい。
      if (evs.complete && evs.length < pageLimit) break;
    }
  }));
  return [...all.values()];
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

// ===== Streak 計測：最新投稿日から1日ずつ limit:1 で「その日に投稿があるか」を並列チェックし、
//        最初のギャップ（投稿の無い日）で停止。共有プールで安定取得する。
//        各日は EVENT 即 true、空なら再クエリで確認（瞬間的な取りこぼしでギャップ誤検出しない）。=====
async function measureStreak(pubkeyHex, pool, latestTs, { maxDays = 2000, budgetMs = 55000, batch = 45, perTimeout = 2500, retries = 2 } = {}) {
  const anchorDay = Math.floor((latestTs || Math.floor(Date.now() / 1000)) / 86400);
  if (!pool || !pool.size) return { streak: 0, capped: false, reason: "no-open", opens: 0 };

  const dayHasPost = async (k) => {
    const dayStart = (anchorDay - k) * 86400;
    const filter = { kinds: [1], authors: [pubkeyHex], since: dayStart, until: dayStart + 86399, limit: 1 };
    for (let a = 0; a <= retries; a++) {
      if (await pool.has(filter, { timeoutMs: perTimeout * (a + 1) })) return true;
    }
    return false;
  };

  const t0 = Date.now();
  let streak = 0, capped = false, k = 0;
  while (k < maxDays) {
    if (Date.now() - t0 > budgetMs) { capped = true; break; }
    const ks = []; for (let j = 0; j < batch && k + j < maxDays; j++) ks.push(k + j);
    const res = await Promise.all(ks.map(dayHasPost));
    let gap = -1;
    for (let j = 0; j < res.length; j++) { if (res[j]) streak = ks[j] + 1; else { gap = ks[j]; break; } }
    if (gap >= 0) return { streak: gap, capped: false, reason: "gap@" + gap, opens: pool.size };
    k += batch;
  }
  if (k >= maxDays) capped = true;
  return { streak, capped, reason: capped ? "cap" : "end", opens: pool.size };
}

// 2) 最古帯から limit:500 のページングで「これ以上古いものが無い」所まで詰める
//    探索対象は kind:1/0/3（投稿・プロフィール作成・初期フォロー）。reaction(7) は除外。
const MONTH_SEC = 30.44 * 24 * 3600;
async function findEarliest(pubkeyHex, pool, seedMin) {
  const now = Math.floor(Date.now() / 1000);
  let earliest = seedMin != null ? seedMin : now;
  const kinds = [1, 0, 3];

  // 1) 粗探索：3ヶ月刻み（最大 ~12年）。境界より古い until は空になる。
  //    ただしリレーの一時的な空応答で早期打ち切りしないよう、2回連続で空の時のみ終了。
  let empties = 0;
  for (let mo = 3; mo <= 144; mo += 3) {
    const until = Math.floor(now - mo * MONTH_SEC);
    const evs = await queryStable(pool,
      [{ kinds, authors: [pubkeyHex], until, limit: 200 }],
      { timeoutMs: 7000 }
    );
    if (!evs.length) { if (++empties >= 2) break; continue; }
    empties = 0;
    const m = evs.reduce((mn, e) => e.created_at < mn ? e.created_at : mn, Infinity);
    if (m < earliest) earliest = m;
  }

  // 2) 仕上げ drill：最古帯からさらに限界まで遡る
  let until = earliest;
  for (let i = 0; i < 12; i++) {
    const evs = await queryStable(pool,
      [{ kinds, authors: [pubkeyHex], until: until - 1, limit: 500 }],
      { timeoutMs: 7000 }
    );
    if (!evs.length) break;
    const m = evs.reduce((mn, e) => e.created_at < mn ? e.created_at : mn, Infinity);
    if (m >= until) break;
    earliest = m;
    until = m;
    if (evs.length < 500) break;
  }
  return earliest;
}

// ===== プロフィール＋実データ指標を取得（2フェーズ）=====
// opts.useUserRelays: true なら対象の kind:10002 を取得先リレーにも合流させる。
async function fetchProfile(pubkeyHex, { useUserRelays = false, fetchSince = null } = {}) {
  const t = pubkeyHex;
  const selected = getActiveRelays();

  // --- Phase A: 選択リレーで軽量取得（profile / relay list）---
  // 接続プールを1つ張り、以降の全取得をこの永続接続に多重化する（接続ストーム回避）。
  setStatus("Fetching profile and relay list…");
  const poolA = createPool(selected);
  const [metaEvents, relayListEvents] = await Promise.all([
    queryStable(poolA, [{ kinds: [0], authors: [t], limit: 5 }], { timeoutMs: 7000 }),
    queryStable(poolA, [{ kinds: [10002], authors: [t], limit: 5 }], { timeoutMs: 7000 }),
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

  // working が selected と同じなら Phase A のプールを使い回す。違えば張り直す。
  const pool = (working === selected) ? poolA : createPool(working);
  if (pool !== poolA) poolA.close();
  // Streak は大量の has() を撃つので、取得系と接続を分けて互いに干渉させない（高速化）。
  const streakPool = createPool(working);

  // リレーごとの取得進捗をライブ表示（200ms 間隔でポーリング描画）。
  // walkState: 各リレーの「窓走査（メインの最長処理）」状態。started=一度でも走査開始、
  // active=現在進行中の走査数。active が 0 に戻れば、そのリレーは EOSE で全窓を取り切り＝完了。
  const walkState = new Map(working.map((u) => [u, { started: false, active: 0 }]));
  const progPools = [pool, streakPool];
  renderRelayProgress(progPools, false, walkState);
  const progTimer = setInterval(() => renderRelayProgress(progPools, false, walkState), 200);

  try {
  // --- Phase B: working リレーで本取得（過去も遡って集計）---
  // まず投稿の先頭ページを取り、利用開始推定の起点(seedMin)にする
  setStatus("Fetching activity, followers, and zaps through history…");
  const notes0 = await queryStable(pool, [{ kinds: [1], authors: [t], limit: 500 }], { timeoutMs: 8000 });
  const seedMin = notes0.length ? notes0.reduce((mn, e) => e.created_at < mn ? e.created_at : mn, Infinity) : (meta ? meta.created_at : null);

  // 最新投稿日（Streak ウォークの起点）
  const latestTs = notes0.length ? notes0.reduce((mx, e) => e.created_at > mx ? e.created_at : mx, 0) : Math.floor(Date.now() / 1000);

  // Communication 用：リアクション送受信は【直近30日】の同一窓で取る（件数打ち切りだと
  // 送信/受信でカバー期間がズレて比率が歪むため）。重量級ユーザーでも収まるようページ多め。
  const reactWindowDays = 30;
  const reactSince = Math.floor(Date.now() / 1000) - reactWindowDays * 86400;

  // 全履歴取得：pool.queryOne を since/until で明示した 30 日窓で遡る。
  // 無期限レンジ（{}）だとリレーが EOSE を送るタイミングを判断できずハングするが、
  // 窓を区切れば各窓で確実に EOSE が返る（窓ごと queryStableOne が .complete を確認）。
  // pool を共有するので進捗 UI（fetch-progress）が実際の取得状態をそのまま反映する。
  // haijin-checker の設計に倣い、リレーごと独立・30 日窓を順に遡る・全リレー並列。
  const NOSTR_FLOOR = Math.floor(Date.parse("2021-01-01T00:00:00Z") / 1000);
  const sinceMin = fetchSince ?? NOSTR_FLOOR;
  const WINDOW_SEC = 30 * 86400;   // 30 日窓
  const WIN_TIMEOUT = 12000;       // 1 窓あたり最大 12 秒（EOSE 中央値 60ms・最大 8s なので十分）
  const RELAY_TIMEOUT = 240000;    // リレー 1 本あたり最大 4 分

  const fetchInWindows = async (filter) => {
    const evs = new Map();
    const now = Math.floor(Date.now() / 1000);

    const PAGE_LIMIT = 500;   // リレーの 1 REQ あたり返却上限の目安
    const runRelay = async (relayUrl) => {
      const st = walkState.get(relayUrl);
      if (st) { st.started = true; st.active++; }
      const deadline = Date.now() + RELAY_TIMEOUT;
      try {
        for (let wUntil = now; wUntil > sinceMin; wUntil -= WINDOW_SEC) {
          const wSince = Math.max(sinceMin, wUntil - WINDOW_SEC);
          if (Date.now() >= deadline) break;
          // 窓内を until で遡ってページング：密な窓（>PAGE_LIMIT）でも全件取り切る。
          // limit 未指定だとリレー既定（~500）で EOSE し打ち切られるため limit を明示。
          let pageUntil = wUntil;
          for (let guard = 0; guard < 200; guard++) {
            if (Date.now() >= deadline) break;
            const f = { ...filter, since: wSince, until: pageUntil, limit: PAGE_LIMIT };
            const evArr = await queryStableOne(pool, relayUrl, [f], { timeoutMs: WIN_TIMEOUT, retries: 1 });
            let oldest = Infinity;
            for (const ev of evArr) { evs.set(ev.id, ev); if (ev.created_at < oldest) oldest = ev.created_at; }
            // 取得数が上限未満 = この窓は取り切った。境界の取りこぼしを防ぐため
            // pageUntil は oldest（-1 しない）にし、重複は id 重複排除で吸収する。
            if (evArr.length < PAGE_LIMIT || !isFinite(oldest) || oldest <= wSince) break;
            pageUntil = oldest;
          }
        }
      } finally {
        if (st) st.active--;   // この窓走査が終わった（active が 0 に戻れば完了表示へ）
      }
    };

    await Promise.allSettled(working.map(relayUrl => runRelay(relayUrl)));
    return [...evs.values()];
  };

  // zap 集計は primal（全ネットワーク index）から並行取得。リレー取得と同時に走らせる。
  const primalP = fetchPrimalZaps(t).catch(() => null);

  // zap（kind:9735）はリレー直クエリでは正確に取れない（送信は #P タグが receipt の
  // 0.4% にしか付かず、受信も相手リレーに散る）ため、primal（全ネットワーク index）に
  // 一本化した。リレーからの zap 窓走査は冗長＋ pool 帯域を圧迫して他取得まで遅くする
  // ので【完全に廃止】。zap は上で並行起動した primalP からのみ取得する。
  const [noteEvents, followerEvents, reactSentEvents, reactRecvEvents, repostSentEvents, repostRecvEvents, contactsEvents, sinceAt, nip05Verified, streakInfo] = await Promise.all([
    fetchInWindows({ kinds: [1], authors: [t] }),
    fetchAllPaged({ kinds: [3], "#p": [t] }, pool, { maxPages: 3 }),
    fetchAllPaged({ kinds: [7], authors: [t], since: reactSince }, pool, { maxPages: 6 }), // リアクション送信（直近30日）
    fetchAllPaged({ kinds: [7], "#p": [t], since: reactSince }, pool, { maxPages: 6 }),    // リアクション受信（直近30日）
    fetchAllPaged({ kinds: [6], authors: [t], since: reactSince }, pool, { maxPages: 6 }), // リポスト送信（直近30日）
    fetchAllPaged({ kinds: [6], "#p": [t], since: reactSince }, pool, { maxPages: 6 }),    // リポスト受信（直近30日）
    queryStable(pool, [{ kinds: [3], authors: [t], limit: 5 }], { timeoutMs: 7000 }),
    findEarliest(t, pool, seedMin),
    verifyNip05(profile.nip05, t),
    measureStreak(t, streakPool, latestTs).catch(() => ({ streak: 0, capped: false })),
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

  // Zap 集計は primal（全ネットワーク index＝正確）に一本化。relay 取得と並行で
  // 走らせており（primalP は Promise.all の前に起動済み）、ここで結果を待ち合わせる。
  // primal が落ちていれば zap は取得不能（0／source=unavailable）として正直に扱う。
  const primalZ = await primalP;
  const zapRecv = primalZ?.recv ? primalZ.recv.count : 0;
  const zapRecvSats = primalZ?.recv ? primalZ.recv.sats : 0;
  const zapSent = primalZ?.sent ? primalZ.sent.count : 0;
  const zapSentSats = primalZ?.sent ? primalZ.sent.sats : 0;
  const zapSource = primalZ ? "primal" : "unavailable";
  // グラフ用の zap 時系列（best-effort・サニタイズ済）。primal が無ければ空。
  const zapRecvTimesSrc = (primalZ?.recv?.times || []).filter(saneSec);
  const zapSentTimesSrc = (primalZ?.sent?.times || []).filter(saneSec);

  // Communication（直近30日・活動量ベース）。日本の Nostr はタグ無しエアリプで会話する
  // ため、構造的に拾える「リアクション/リポストの送受信」だけだと会話量を取りこぼす。
  // そこで【投稿量50% + インタラクション量50%】で算出する（投稿の多さ＝エアリプ会話の代理）。
  const reactionsSent = reactSentEvents.length;
  const reactionsRecv = reactRecvEvents.length;
  const repostSent = repostSentEvents.length;
  const repostRecv = repostRecvEvents.length;
  // 直近30日の投稿数（エアリプ会話の代理指標）
  const postsRecent = noteEvents.filter((e) => e.created_at >= reactSince).length;
  // インタラクション総量と、向き（INFLUENCER/SUPPORTER 判定用）の受信・送信合計
  const interactions = reactionsSent + reactionsRecv + repostSent + repostRecv;
  const commInbound = reactionsRecv + repostRecv;   // 受け取った絡み
  const commOutbound = reactionsSent + repostSent;   // 自分から絡んだ量

  // 連続投稿日数：日次ウォークの結果を採用（取れなければ収集ノートからフォールバック）。
  // capped=true は「上限/時間で打ち切り＝実際はもっと長いかも（+表示）」。
  let streak = (streakInfo && streakInfo.streak) || 0;
  const streakCapped = !!(streakInfo && streakInfo.capped);
  let streakReason = (streakInfo && streakInfo.reason) || "none";
  const streakOpens = (streakInfo && streakInfo.opens) || 0;
  if (!streak) { streak = longestStreak(noteEvents.map((e) => e.created_at)); streakReason = "fallback(" + streakReason + ")"; }

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
    nip05Verified,                      // true=検証OK / false=不一致 / null=確認不能
    about: profile.about || "",
    hasNip05: !!profile.nip05,
    // 実データ指標
    activity: noteEvents.length,        // 投稿数（nostr-fetchで全件取得）
    activityCapped: false,              // nostr-fetchはリレー保持分を全件取るのでcapなし
    followers: followers.size,
    following: followList.size,
    relayCount,
    zapRecv,
    zapSent,
    zapRecvSats,
    zapSentSats,
    reactionsSent,
    reactionsRecv,
    repostSent,
    repostRecv,
    postsRecent,
    interactions,
    commInbound,
    commOutbound,
    streak,
    streakCapped,
    streakReason,
    streakOpens,
    wotValue,
    sinceAt,                            // 利用開始（最古イベント推定、取れなければ null）
    lastActivity: lastActivity || sinceAt || Math.floor(Date.now() / 1000),
    velocity: (() => {                  // 1日あたり投稿数
      // Nostr は全投稿を取り切れない（リレー保持範囲＋ページング上限）ため、
      // 「全期間で割る」と多投稿者ほど過小評価される。代わりに【収集できた直近
      // ノートが実際にカバーした期間】で割り、最近の投稿ペースを推定する。
      if (!noteEvents.length) return 0;
      const ts = noteEvents.map((e) => e.created_at);
      const spanDays = (ts.reduce((mx, v) => v > mx ? v : mx, 0) - ts.reduce((mn, v) => v < mn ? v : mn, Infinity)) / 86400;
      return noteEvents.length / Math.max(spanDays, 0.5);
    })(),
    peakUTC: peakBand(noteEvents.map((e) => e.created_at)),  // 最も活発な時間帯
    // 裏面グラフ用：取れた範囲のタイムスタンプ（best-effort・サニタイズ済）。
    fetchSince,   // グラフのx軸開始点（null = 全件）
    noteTimes: noteEvents.map((e) => e.created_at).filter(saneSec),
    zapRecvTimes: zapRecvTimesSrc,
    zapSentTimes: zapSentTimesSrc,
    zapSource,
    usedRelays: working,
  };
  } finally {
    clearInterval(progTimer);
    renderRelayProgress(progPools, true, walkState);   // 最終スナップショット（完了表示）
    pool.close();
    streakPool.close();
  }
}

// ===== NIP-05 検証 =====
// `<local>@<domain>`（@省略時は local="_"）について
// https://<domain>/.well-known/nostr.json?name=<local> を引き、pubkey 一致を確認。
// 返り値: true=検証OK / false=不一致 or 該当名なし / null=取得不能（CORS/ネットワーク等で確認不可）
async function verifyNip05(nip05, pubkeyHex) {
  if (!nip05 || !nip05.includes(".")) return null;
  let name = "_", domain = nip05;
  if (nip05.includes("@")) [name, domain] = nip05.split("@");
  domain = (domain || "").trim();
  if (!domain) return null;
  const ctrl = new AbortController();
  const to = setTimeout(() => ctrl.abort(), 6000);
  try {
    const url = `https://${domain}/.well-known/nostr.json?name=${encodeURIComponent(name)}`;
    const res = await fetch(url, { signal: ctrl.signal, redirect: "follow" });
    if (!res.ok) return null;
    const json = await res.json();
    const got = json && json.names && json.names[name];
    if (!got) return false;
    return String(got).toLowerCase() === pubkeyHex.toLowerCase();
  } catch {
    return null;
  } finally {
    clearTimeout(to);
  }
}

// ===== ランク（実データ基準）=====
// LICENSE CLASS = 在籍期間のみ（ダチョウのライフサイクル）。Nostr は作成日が無いので
// 「取得できた最古イベント」基準の best-effort（古参ほど過小評価され得る＝Nostr的に許容）。
// 新人期は Day1/7/30 の離脱クリフに連動。
function computeRank(d) {
  const c = d.sinceAt;
  if (!c) return "EGG";
  const ageDays = (Date.now() / 1000 - c) / 86400;
  if (ageDays >= 365 * 3) return "NOSTR OG";   // 3年+
  if (ageDays >= 365) return "CITIZEN";        // 1〜3年
  if (ageDays >= 30) return "EXPLORER";        // 1ヶ月〜1年（一人前のダチョウ）
  if (ageDays >= 7) return "JUVENILE";         // 7〜30日（走り回る若鳥）
  if (ageDays >= 1) return "CHICK";            // 1〜7日（孵化したヒナ）
  return "EGG";                                // 〜1日（卵）
}

// 実数 → ★(1..5)。log スケール。
function starFrom(x, k, base = 1) {
  const n = Math.round(Math.log10((x || 0) + 1) * k) + base;
  return Math.max(1, Math.min(5, n));
}
// 最も活発な2時間帯（UTC）。"HH–HH UTC"。
function peakBand(timestamps) {
  if (!timestamps.length) return "—";
  const h = new Array(24).fill(0);
  for (const t of timestamps) h[new Date(t * 1000).getUTCHours()]++;
  let bi = 0, bv = -1;
  for (let i = 0; i < 24; i++) { const v = h[i] + h[(i + 1) % 24]; if (v > bv) { bv = v; bi = i; } }
  const p = (n) => String(n).padStart(2, "0");
  return `${p(bi)}–${p((bi + 2) % 24)} UTC`;
}
// 最長連続投稿日数（連続した「日」のラン）
function longestStreak(timestamps) {
  const days = [...new Set(timestamps.map((t) => Math.floor(t / 86400)))].sort((a, b) => a - b);
  if (!days.length) return 0;
  let best = 1, cur = 1;
  for (let i = 1; i < days.length; i++) {
    if (days[i] === days[i - 1] + 1) { cur++; best = Math.max(best, cur); } else { cur = 1; }
  }
  return best;
}
// Streak の★：90日で★5（厳しめスケール）
function streakStar(s) {
  return s >= 90 ? 5 : s >= 60 ? 4 : s >= 30 ? 3 : s >= 14 ? 2 : 1;
}
// 90日超のプレステージ強化：100日ごとに星を1つずつ（1→2→…→5→1…）レベルアップ。
// 各星のレベル = floor((steps + 4 - i) / 5)。0=通常。190日未満は強化なし(null)。
function streakPrestige(s) {
  if (s < 90) return null;
  const steps = Math.floor((s - 90) / 100);
  if (steps <= 0) return null;
  const levels = [];
  for (let i = 0; i < 5; i++) levels.push(Math.floor((steps + 4 - i) / 5));
  return levels;
}

// ステータス（6項目・すべて実データ）。2列×3行で表示。各 {label, n, note, icon}
function computeStars(d) {
  // Communication = 投稿量50% + インタラクション量50%（直近30日・対数スケール）。
  // 投稿の多さはタグ無しエアリプ会話の代理。インタラクションはリアクション/リポストの送受信合計。
  const postStar = starFrom(d.postsRecent || 0, 1.5);
  const interactionStar = starFrom(d.interactions || 0, 1.5);
  const commStar = Math.max(1, Math.min(5, Math.round(0.5 * postStar + 0.5 * interactionStar)));
  return [
    { label: "Communication", icon: "person", n: commStar, note: `${d.postsRecent || 0}p+${d.interactions || 0}i` },
    { label: "Web of Trust", icon: "shield", n: starFrom(d.wotValue, 1.6), note: String(d.wotValue) },
    { label: "Velocity", icon: "relay", n: starFrom(d.velocity, 2.5), note: (d.velocity || 0).toFixed(1) + "/d" },
    { label: "Streak", icon: "bubble", n: streakStar(d.streak || 0), note: (d.streak || 0) + "d" + (d.streakCapped ? "+" : ""), prestige: streakPrestige(d.streak || 0) },
    { label: "Zap Received", icon: "bolt", n: starFrom(d.zapRecv, 1.6), note: String(d.zapRecv) },
    { label: "Zap Sent", icon: "bolt", n: starFrom(d.zapSent, 1.6), note: String(d.zapSent) },
  ];
}

// ENDORSEMENT = 突出した特性のアーキタイプ（★4以上の突出のみ・横並びの型）
function computeEndorsement(d) {
  const st = {};
  for (const x of computeStars(d)) st[x.label] = x.n;
  const c = st["Communication"], w = st["Web of Trust"], v = st["Velocity"],
        s = st["Streak"], zr = st["Zap Received"], zs = st["Zap Sent"];
  const max = Math.max(c, w, v, s, zr, zs);
  if (v >= 5 && s >= 5) return "TERMINALLY ONLINE";          // 廃人：投稿速度・連続ともMAX
  if ([c, w, v, s, zr, zs].every((x) => x >= 4)) return "ALL-ROUNDER";
  // 絡みの「向き」で称号（受信＝リアクション+リポスト受信、送信＝同送信・volume gate付き）
  const out = d.commOutbound || 0, inb = d.commInbound || 0, VOL = 100;
  if (inb >= 2 * Math.max(out, 1) && inb >= VOL) return "INFLUENCER"; // 受信≫送信＝反応される側
  if (out >= 2 * Math.max(inb, 1) && out >= VOL) return "SUPPORTER";  // 送信≫受信＝盛り上げる側
  if (max <= 2) return "LURKER";
  if (max <= 3) return "CASUAL";
  const cand = [
    [v, "SPEEDSTER"], [s, "MARATHONER"], [c, "COMMUNICATOR"],
    [zr, "ZAP MAGNET"], [zs, "ZAPPER"], [w, "CONNECTOR"],
  ];
  cand.sort((a, b) => b[0] - a[0]);
  return cand[0][1];
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
// プレステージStreak：各星をレベルに応じて大きく・濃く（ヒートランプ＋発光）。
// levels[i] = 強化レベル（0=通常）。レイアウト維持のためサイズ上限あり。
const PRESTIGE_RAMP = ["#1e2a5a", "#caa11e", "#e8722a", "#d6402f", "#c01038", "#9a2bd0", "#6a1fb0"];
function drawPrestigeStars(c, x, y, baseSize, levels) {
  c.textAlign = "left";
  c.textBaseline = "middle";
  let cx = x;
  for (let i = 0; i < 5; i++) {
    const lv = levels[i] || 0;
    const size = baseSize + Math.min(lv, 6) * 2.6;       // レベルで拡大（上限）
    const col = PRESTIGE_RAMP[Math.min(lv, PRESTIGE_RAMP.length - 1)];
    c.save();
    if (lv >= 2) { c.shadowColor = col; c.shadowBlur = Math.min(lv, 6) * 3; }
    c.font = `${size}px 'Hiragino Sans','Apple Color Emoji',sans-serif`;
    c.fillStyle = col;
    c.fillText("★", cx, y);
    c.restore();
    cx += size * 0.92;
  }
}
// 角丸長方形のラベル（カプセル型ではなく控えめなR）
function drawPill(c, text, x, y, { bg, fg, font, padX = 14, h = 34, r = 7, maxW = null }) {
  c.font = font;
  c.textAlign = "left";
  c.textBaseline = "middle";
  if (maxW) { // 長いラベルは枠内に収まるまでフォント縮小
    const m = font.match(/(\d+)px/);
    let size = m ? +m[1] : 21;
    while (size > 12 && c.measureText(text).width + padX * 2 > maxW) {
      size -= 1;
      c.font = font.replace(/\d+px/, size + "px");
    }
  }
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
  purple: { accent: "#7b3ff2", accent2: "#b14ddb", ink: "#221636", sub: "#574a78", line: "#cdbcf3", border: "#7b3ff2", gold1: "#dcc07f", gold2: "#b48a3c", paper: ["#f7f3fe", "#f1ecfb", "#faf0fb"], motif: "ostrich" },
  pink:  { accent: "#e0249f", accent2: "#ff5fa2", ink: "#3a1030", sub: "#7a3a60", line: "#f3c0dd", border: "#e0249f", gold1: "#dcc07f", gold2: "#b48a3c", paper: ["#fdeef7", "#fbeef5", "#fde8f0"] },
  sunset: { accent: "#e8722a", accent2: "#d6402f", ink: "#3a1d0a", sub: "#7a4a2a", line: "#f3cda0", border: "#e8722a", gold1: "#e6cd84", gold2: "#b4863a", paper: ["#fff4e6", "#ffeede", "#fde6d8"] },
  green: { accent: "#1f9e55", accent2: "#3fb37a", ink: "#0e2a1a", sub: "#3a5a48", line: "#bfe0c8", border: "#1f9e55", gold1: "#dcc07f", gold2: "#b48a3c", paper: ["#eefaf0", "#eef7f0", "#e8f5ec"] },
  galaxy: { accent: "#9b7bff", accent2: "#ff6ad5", ink: "#eef0ff", sub: "#aab0e0", line: "#3a3a72", border: "#6b4bd6", gold1: "#e6cd84", gold2: "#b4863a", paper: ["#0b0a1e", "#141033", "#211046"], motif: "galaxy", dark: true },
};

// ===== カード描画（高級ホログラム調 / 英語表記）=====
// "#rrggbb" + alpha → "rgba(...)"
function hexA(hex, a) {
  const n = parseInt(hex.slice(1), 16);
  return `rgba(${(n >> 16) & 255}, ${(n >> 8) & 255}, ${n & 255}, ${a})`;
}

// 透かしのダチョウ・シルエット（Nostr マスコット）。faint watermark.
function drawOstrich(c, cx, cy, s, color, alpha) {
  c.save();
  c.globalAlpha = alpha;
  c.fillStyle = color; c.strokeStyle = color;
  c.lineCap = "round"; c.lineJoin = "round";
  // 脚
  c.lineWidth = 7 * s;
  c.beginPath(); c.moveTo(cx - 6 * s, cy + 40 * s); c.lineTo(cx - 14 * s, cy + 120 * s); c.stroke();
  c.beginPath(); c.moveTo(cx + 16 * s, cy + 40 * s); c.lineTo(cx + 12 * s, cy + 122 * s); c.stroke();
  c.lineWidth = 5 * s;
  c.beginPath(); c.moveTo(cx - 14 * s, cy + 120 * s); c.lineTo(cx - 28 * s, cy + 128 * s); c.stroke();
  c.beginPath(); c.moveTo(cx + 12 * s, cy + 122 * s); c.lineTo(cx + 26 * s, cy + 130 * s); c.stroke();
  // 胴体
  c.beginPath(); c.ellipse(cx, cy, 72 * s, 54 * s, 0, 0, Math.PI * 2); c.fill();
  // 尾羽（右後ろ）
  c.beginPath(); c.ellipse(cx + 64 * s, cy - 16 * s, 28 * s, 21 * s, -0.5, 0, Math.PI * 2); c.fill();
  // 首（太い曲線・左上へ）
  c.lineWidth = 20 * s;
  c.beginPath(); c.moveTo(cx - 34 * s, cy - 22 * s);
  c.quadraticCurveTo(cx - 74 * s, cy - 58 * s, cx - 80 * s, cy - 120 * s); c.stroke();
  // 頭
  c.beginPath(); c.ellipse(cx - 86 * s, cy - 128 * s, 17 * s, 14 * s, -0.2, 0, Math.PI * 2); c.fill();
  // くちばし
  c.beginPath(); c.moveTo(cx - 100 * s, cy - 130 * s); c.lineTo(cx - 120 * s, cy - 126 * s); c.lineTo(cx - 101 * s, cy - 120 * s); c.closePath(); c.fill();
  c.restore();
}

// 銀河・宇宙背景（決定論的な星＋ネビュラ）
function drawGalaxy(c, W, H, t) {
  for (const [x, y, r, col] of [
    [W * 0.28, H * 0.34, 380, t.accent2],
    [W * 0.72, H * 0.52, 440, t.accent],
    [W * 0.5, H * 0.82, 320, "#3b6bd0"],
  ]) {
    const g = c.createRadialGradient(x, y, 0, x, y, r);
    g.addColorStop(0, hexA(col, 0.32));
    g.addColorStop(1, hexA(col, 0));
    c.fillStyle = g; c.fillRect(0, 0, W, H);
  }
  let s = 2246; // 固定シードで端末非依存
  const rnd = () => ((s = (s * 1103515245 + 12345) & 0x7fffffff) / 0x7fffffff);
  for (let i = 0; i < 460; i++) {
    const x = rnd() * W, y = rnd() * H, r = rnd() * 1.6 + 0.3, a = rnd() * 0.7 + 0.2;
    c.fillStyle = `rgba(255,255,255,${a.toFixed(2)})`;
    c.beginPath(); c.arc(x, y, r, 0, Math.PI * 2); c.fill();
  }
}

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

  // 全面の細密ギロシェ（織り地＋ロゼット）。
  // ※ 端末（描画エンジン: Skia/CoreGraphics 等）で「半透明線の重なり蓄積」の出方が違い、
  //    濃さがブレる。これを防ぐため、別キャンバスに【不透明で一度だけ】描いてから、
  //    最後に【1回だけ】薄く合成する（透明度操作を1回に集約 → 端末差が出にくい）。
  if (t.motif === "galaxy") {
    drawGalaxy(c, W, H, t);
  } else {
    const off = document.createElement("canvas");
    off.width = W; off.height = H;
    const g = off.getContext("2d");
    g.lineCap = "round"; g.lineJoin = "round";
    // 横波メッシュ（不透明・線幅1）
    for (let i = 0; i < 78; i++) {
      const yy = 26 + i * 12.4;
      g.strokeStyle = i % 2 ? t.line : t.accent;
      g.lineWidth = 1;
      g.beginPath();
      for (let x = 24; x <= W - 24; x += 5) {
        const y2 = yy
          + Math.sin(x / 44 + i * 0.55) * 7
          + Math.sin(x / 128 - i * 0.32) * 5
          + Math.cos(x / 320 + i * 0.12) * 3;
        x === 24 ? g.moveTo(x, y2) : g.lineTo(x, y2);
      }
      g.stroke();
    }
    // 縦波メッシュ（交差させて織り地に）
    for (let j = 0; j < 50; j++) {
      const xx = 24 + j * 31;
      g.strokeStyle = j % 2 ? t.accent : t.line;
      g.lineWidth = 1;
      g.beginPath();
      for (let y = 24; y <= H - 24; y += 6) {
        const x2 = xx + Math.sin(y / 50 + j * 0.5) * 6 + Math.sin(y / 150 - j * 0.3) * 4;
        y === 24 ? g.moveTo(x2, y) : g.lineTo(x2, y);
      }
      g.stroke();
    }
    // ロゼット紋様（不透明・線幅1）
    guilloche(g, W * 0.20, H * 0.34, 230, 74, 9, 26, t.accent, 1, 1);
    guilloche(g, W * 0.20, H * 0.34, 150, 52, 14, 26, t.accent2, 1, 1);
    guilloche(g, W * 0.50, H * 0.50, 380, 104, 7, 30, t.accent, 1, 1);
    guilloche(g, W * 0.50, H * 0.50, 250, 84, 17, 26, t.accent2, 1, 1);
    guilloche(g, W * 0.83, H * 0.72, 210, 66, 11, 24, t.accent2, 1, 1);
    guilloche(g, W * 0.83, H * 0.72, 130, 46, 16, 24, t.accent, 1, 1);
    for (const [px, py] of [[110, 120], [W - 120, 120], [120, H - 110], [W - 120, H - 110]]) {
      guilloche(g, px, py, 70, 26, 13, 18, t.accent, 1, 1);
    }
    // 上を濃く・下を淡く（決定論的なフェード。destination-out で一様に削る）
    g.globalCompositeOperation = "destination-out";
    const fade = g.createLinearGradient(0, 0, 0, H);
    fade.addColorStop(0.0, "rgba(0,0,0,0)");
    fade.addColorStop(1.0, "rgba(0,0,0,0.5)");
    g.fillStyle = fade;
    g.fillRect(0, 0, W, H);
    g.globalCompositeOperation = "source-over";
    // まとめて薄く合成（透明度操作はここ1回だけ。濃さはこの値だけで全端末一括調整）
    c.save();
    c.globalAlpha = 0.20;
    c.drawImage(off, 0, 0);
    c.restore();
  }

  // ダチョウ透かし（紫テーマ等の motif）。写真枠・パネルを避けた中央左に配置。
  if (t.motif === "ostrich") {
    drawOstrich(c, W * 0.33, H * 0.50, 1.95, t.accent, 0.12);
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
  c.fillStyle = t.ink;
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
  // 正方形モード：枠を正方形にして元のポートレート枠(202..670)内で縦中央寄せ。
  // アイコン(1:1)を左右切り取りなしで全体表示できる。幅は通常と同じ360で右カラム余白を維持。
  const squareAvatar = !!$("square-avatar")?.checked;
  const phX = 850, phR = 16;
  const phW = 360;
  const phH = squareAvatar ? 360 : 468;
  const phY = squareAvatar ? 202 + (468 - phH) / 2 : 202;
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
    // 正方形モードは contain（全体表示）、通常はポートレート枠に cover（はみ出し切り取り）
    const ratio = squareAvatar
      ? Math.min(phW / img.width, phH / img.height)
      : Math.max(phW / img.width, phH / img.height);
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
  let nameSz = 58; // 長い表示名は枠内に収まるまで縮小
  while (nameSz > 20) {
    c.font = `800 ${nameSz}px 'Hiragino Sans','Yu Gothic',sans-serif`;
    if (c.measureText(d.name).width <= fieldMaxW) break;
    nameSz -= 2;
  }
  c.fillText(d.name, lx, 292);          // NAME ピルとの間を詰める
  if (d.handle) {
    c.fillStyle = t.accent;
    let hSz = 28; // 長いハンドルも縮小
    while (hSz > 14) {
      c.font = `600 ${hSz}px 'Hiragino Sans',sans-serif`;
      if (c.measureText(d.handle).width <= fieldMaxW) break;
      hSz -= 1;
    }
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
  // 検証マーク分の余白を確保してアドレス本体を自動縮小
  const markW = d.nip05 && d.nip05Verified !== null ? 36 : 0;
  let n5 = 32;
  const nip05Addr = d.nip05 || "— not set —";
  while (n5 > 14) {
    c.font = `600 ${n5}px 'SF Mono','Menlo','Consolas',monospace`;
    if (c.measureText(nip05Addr).width <= fieldMaxW - markW) break;
    n5 -= 1;
  }
  c.fillStyle = t.ink;
  c.fillText(nip05Addr, lx, 552);
  // 実際に検証した結果だけマークを出す（true=緑✓ / false=赤✗ / null=確認不能なので無印）
  if (d.nip05 && d.nip05Verified !== null) {
    const aw = c.measureText(nip05Addr).width;
    c.font = "700 28px 'Hiragino Sans',sans-serif";
    if (d.nip05Verified === true) { c.fillStyle = "#1c9e57"; c.fillText("✓", lx + aw + 12, 551); }
    else { c.fillStyle = "#d23b3b"; c.fillText("✗", lx + aw + 12, 551); }
  }

  // 下段：ISSUED / FIRST SEEN / CLASS / ENDORSEMENT を等間隔フローで（写真枠 phX 手前まで）。
  const THREE_YEARS = 3 * 365.25 * 24 * 3600;
  const rowY = 614;
  const GAP = 46;
  const fields = [
    { label: "ISSUED",      kind: "date", text: fmtISO(Math.floor(Date.now() / 1000)) },
    { label: "FIRST SEEN",  kind: "date", text: d.sinceAt ? fmtISO(d.sinceAt) : "—" },
    { label: "CLASS",       kind: "pill", text: rank, bg: t.accent2 },
    { label: "ENDORSEMENT", kind: "pill", text: computeEndorsement(d), bg: t.accent },
  ];
  let fx = lx;
  for (const f of fields) {
    c.textAlign = "left"; c.textBaseline = "alphabetic";
    c.fillStyle = t.sub; c.font = "700 19px 'Hiragino Sans',sans-serif";
    c.fillText(f.label, fx, rowY);
    const lw = c.measureText(f.label).width;
    let vw;
    if (f.kind === "date") {
      c.fillStyle = t.ink; c.font = "400 22px 'Hiragino Sans',sans-serif";
      c.fillText(f.text, fx, rowY + 30);
      vw = c.measureText(f.text).width;
    } else {
      vw = drawPill(c, f.text, fx, rowY + 14, { bg: f.bg, fg: "#fff", font: "700 20px 'Hiragino Sans',sans-serif", h: 32, maxW: (phX - 16) - fx });
    }
    fx += Math.max(lw, vw) + GAP;
  }

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
  const pnX = 60, pnY = 712, pnW = 1000, pnH = 192;
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

  // パネルは常にクリーム地なので、文字色はテーマに依らず常に濃色にする（galaxy対策）
  const panelInk = t.dark ? "#22243f" : t.ink;
  const panelSub = t.dark ? "#6b5e44" : t.sub;
  const panelIcon = t.dark ? "#5e44b8" : t.accent;

  // ステータス 2列×3行（6項目）
  const stats = computeStars(d);
  const colX = [pnX + 40, pnX + 510];
  const rowsY = [pnY + 48, pnY + 88, pnY + 128];
  for (let i = 0; i < stats.length; i++) {
    const s = stats[i];
    const cxp = colX[i % 2];
    const cyp = rowsY[Math.floor(i / 2)];
    drawStatIcon(c, s.icon, cxp, cyp - 15, 26, panelIcon);
    c.fillStyle = panelInk;
    c.textAlign = "left";
    c.textBaseline = "middle";
    c.font = "700 25px 'Hiragino Sans',sans-serif";
    c.fillText(s.label, cxp + 42, cyp);
    if (s.prestige) drawPrestigeStars(c, cxp + 300, cyp, 26, s.prestige);
    else drawStarRating(c, cxp + 300, cyp, s.n, 26, "#1e2a5a", "#b9c1d7");
  }

  // パネル内フッター：PEAK のみ（Nostr は投稿数を正確に数えられないため MILEAGE は出さない）
  c.strokeStyle = t.gold2; c.globalAlpha = 0.4; c.lineWidth = 1;
  c.beginPath(); c.moveTo(pnX + 40, pnY + 152); c.lineTo(pnX + pnW - 40, pnY + 152); c.stroke();
  c.globalAlpha = 1;
  const fy = pnY + 176;
  c.textAlign = "left"; c.textBaseline = "alphabetic";
  c.fillStyle = panelSub; c.font = "700 18px 'Hiragino Sans',sans-serif";
  c.fillText("PEAK (UTC)", colX[0], fy);
  c.fillStyle = panelInk; c.font = "700 22px 'Hiragino Sans',sans-serif";
  c.fillText(d.peakUTC || "—", colX[0] + 144, fy);

  // ===== 署名・AUTHORIZED・ホロ印（パネル右）=====
  c.fillStyle = t.ink;
  c.textAlign = "center";
  c.textBaseline = "alphabetic";
  const sigText = d.handle || d.name || "";
  const sigMaxW = 290; // ホロ印（左端~1438）に被らない範囲
  let sigSize = 46;
  while (sigSize > 18) {
    c.font = `italic 600 ${sigSize}px 'Snell Roundhand','Apple Chancery','Brush Script MT',cursive`;
    if (c.measureText(sigText).width <= sigMaxW) break;
    sigSize -= 2;
  }
  c.fillText(sigText, 1285, 828);
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
  c.fillStyle = t.ink;
  c.textAlign = "left";
  c.textBaseline = "middle";
  c.font = "600 25px 'Hiragino Sans',sans-serif";
  c.fillText("Drive the decentralized future.", PAD + 64, capY);

  c.fillStyle = t.accent;
  c.textAlign = "right";
  c.font = "800 25px 'Hiragino Sans',sans-serif";
  c.fillText("BUCKLE UP, STAY DECENTRALIZED.", W - PAD, capY);

  $("download-btn").disabled = false;
  const pb = $("post-btn");
  if (pb) pb.disabled = false;
}

// ===== 裏面（ACTIVITY RECORD）: 取れた範囲の週次グラフ＋統計を免許証の裏風に =====
// グラフ3系列の色（白地でもダーク地でも読める）。投稿=青 / Zap受信=琥珀⚡ / Zap送信=緑↗
const NCHART = { post: "#3b82f6", zapR: "#f59e0b", zapS: "#22c55e" };
function pctOf(a, b) { return b ? Math.round(100 * a / b) + "%" : "—"; }

// Code128(コードセットB) — ハンドル/npub を実エンコード（スキャンで読める）
const _C128 = ["212222","222122","222221","121223","121322","131222","122213","122312","132212","221213","221312","231212","112232","122132","122231","113222","123122","123221","223211","221132","221231","213212","223112","312131","311222","321122","321221","312212","322112","322211","212123","212321","232121","111323","131123","131321","112313","132113","132311","211313","231113","231311","112133","112331","132131","113123","113321","133121","313121","211331","231131","213113","213311","213131","311123","311321","331121","312113","312311","332111","314111","221411","431111","111224","111422","121124","121421","141122","141221","112214","112412","122114","122411","142112","142211","241211","221114","413111","241112","134111","111242","121142","121241","114212","124112","124211","411212","421112","421211","212141","214121","412121","111143","111341","131141","114113","114311","411113","411311","113141","114131","311141","411131","211412","211214","211232","2331112"];
function _code128B(text) {
  const codes = [104]; let sum = 104, pos = 1;
  for (let i = 0; i < text.length; i++) { const v = text.charCodeAt(i) - 32; if (v < 0 || v > 95) continue; codes.push(v); sum += v * pos; pos++; }
  codes.push(sum % 103); codes.push(106);
  return codes.map((v) => _C128[v]);
}
function drawBarcode(c, text, x, y, h, color, { mod = 3, maxW = 1000 } = {}) {
  const pats = _code128B(String(text || "")); const quiet = 10;
  let total = quiet * 2; for (const p of pats) for (const ch of p) total += +ch;
  const mw = Math.min(mod, maxW / total);
  c.save(); c.fillStyle = color; let cx = x + quiet * mw;
  for (const p of pats) for (let k = 0; k < p.length; k++) { const ww = (+p[k]) * mw; if (k % 2 === 0) c.fillRect(cx, y, ww, h); cx += ww; }
  c.restore(); return total * mw;
}

// 週次集計（best-effort・取れた範囲）。posts/zapR/zapS の3系列
function nWeekly(d) {
  const W = 604800;
  const now = Math.floor(Date.now() / 1000);
  const posts = d.noteTimes || [], zR = d.zapRecvTimes || [], zS = d.zapSentTimes || [];
  // グラフのx軸範囲は「投稿(posts)が最初に取れた時点〜今日」にする。
  // zapはposts/zapsで取得深度が違うため、zap最古を基準にするとposts空白の期間が大量に生まれる。
  // posts が主軸なので posts の範囲に合わせ、zapはその期間内だけ表示する。
  if (!posts.length && !zR.length && !zS.length) return { n: 0 };
  const postMin = posts.length ? posts.reduce((mn, v) => v < mn ? v : mn, Infinity) : Infinity;
  const zapAll = [...zR, ...zS];
  const zapMin  = zapAll.length ? zapAll.reduce((mn, v) => v < mn ? v : mn, Infinity) : Infinity;
  // posts があれば posts 基準、なければ zap 基準
  const mn = isFinite(postMin) ? postMin : zapMin;
  if (!isFinite(mn)) return { n: 0 };
  const mx = now;
  // RETRIEVED RANGE ラベル用の実データ最古・最新
  let dataMin = mn, dataMax = 0;
  for (const arr of [posts, zR, zS]) for (const t of arr) { if (t < dataMin) dataMin = t; if (t > dataMax) dataMax = t; }
  const start = Math.floor(mn / W) * W, end = Math.floor(mx / W) * W;
  const n = Math.floor((end - start) / W) + 1;
  const mk = (arr) => { const a = new Array(n).fill(0); for (const t of arr) { const wi = Math.floor((t - start) / W); if (wi >= 0 && wi < n) a[wi]++; } return a; };
  return { posts: mk(posts), zapR: mk(zR), zapS: mk(zS), n, weekSec: W, start, first: dataMin, last: dataMax };
}

// 裏面の地紋：表（直交）とは違う【対角の織り地】＋ギロシェ
function drawBackBg(c, t, W, H) {
  const off = document.createElement("canvas"); off.width = W; off.height = H;
  const g = off.getContext("2d"); g.lineCap = "round"; g.lineJoin = "round";
  g.save(); g.translate(W / 2, H / 2); g.rotate(Math.PI / 4);
  const D = Math.ceil((W + H) / 1.3); let ri = 0;
  for (let i = -D; i <= D; i += 12.4) { g.strokeStyle = (ri++ % 2) ? t.line : t.accent; g.lineWidth = 1; g.beginPath(); for (let x = -D; x <= D; x += 5) { const y2 = i + Math.sin(x / 44 + i * 0.04) * 7 + Math.sin(x / 128) * 5 + Math.cos(x / 320) * 3; x === -D ? g.moveTo(x, y2) : g.lineTo(x, y2); } g.stroke(); }
  let rj = 0;
  for (let j = -D; j <= D; j += 31) { g.strokeStyle = (rj++ % 2) ? t.accent : t.line; g.lineWidth = 1; g.beginPath(); for (let y = -D; y <= D; y += 6) { const x2 = j + Math.sin(y / 50) * 6 + Math.sin(y / 150) * 4; y === -D ? g.moveTo(x2, y) : g.lineTo(x2, y); } g.stroke(); }
  g.restore();
  guilloche(g, W * 0.78, H * 0.26, 300, 90, 8, 28, t.accent, 1, 1);
  guilloche(g, W * 0.78, H * 0.26, 190, 64, 13, 24, t.accent2, 1, 1);
  guilloche(g, W * 0.24, H * 0.64, 260, 80, 11, 26, t.accent2, 1, 1);
  guilloche(g, W * 0.24, H * 0.64, 160, 56, 16, 22, t.accent, 1, 1);
  for (const [px, py] of [[110, 120], [W - 120, 120], [120, H - 110], [W - 120, H - 110]]) guilloche(g, px, py, 64, 24, 13, 18, t.accent2, 1, 1);
  g.globalCompositeOperation = "destination-out";
  const fade = g.createLinearGradient(0, 0, 0, H); fade.addColorStop(0, "rgba(0,0,0,0)"); fade.addColorStop(1, "rgba(0,0,0,0.5)");
  g.fillStyle = fade; g.fillRect(0, 0, W, H); g.globalCompositeOperation = "source-over";
  c.save(); c.globalAlpha = t.dark ? 0.32 : 0.20; c.drawImage(off, 0, 0); c.restore();
}

function drawBackChart(c, d, t, x, y, w, h) {
  const s = nWeekly(d);
  c.save();
  c.fillStyle = t.dark ? "#181530" : "#ffffff"; roundRect(c, x, y, w, h, 12); c.fill();
  c.globalAlpha = 0.6; c.strokeStyle = t.line; c.lineWidth = 1; roundRect(c, x, y, w, h, 12); c.stroke(); c.globalAlpha = 1;
  if (!s.n) {
    c.fillStyle = t.sub; c.textAlign = "center"; c.textBaseline = "middle"; c.font = "600 20px 'Hiragino Sans',sans-serif";
    c.fillText("no retrievable activity", x + w / 2, y + h / 2); c.restore(); return;
  }
  const padX = 16, padY = 30, plotW = w - padX * 2, plotH = h - padY - 26, ox = x + padX, oy = y + padY;
  const xOf = (wi) => ox + (s.n <= 1 ? plotW / 2 : (wi / (s.n - 1)) * plotW);
  // 系列ごとに自分の最小〜最大の幅で正規化する（共通スケールだと投稿に対して
  // zap が潰れて見えなくなるため）。min をその系列の底、max を天井に割り当てる。
  const line = (arr, col, lw) => {
    let mn = Infinity, mx = -Infinity;
    for (const v of arr) { if (v < mn) mn = v; if (v > mx) mx = v; }
    if (!isFinite(mn)) { mn = 0; mx = 0; }
    const span = (mx - mn) || 1;
    const yOf = (v) => oy + plotH - ((v - mn) / span) * plotH;
    c.strokeStyle = col; c.lineWidth = lw; c.beginPath();
    for (let wi = 0; wi < s.n; wi++) { const X = xOf(wi), Y = yOf(arr[wi]); wi ? c.lineTo(X, Y) : c.moveTo(X, Y); }
    c.stroke();
  };
  line(s.zapR, NCHART.zapR, 1.5); line(s.zapS, NCHART.zapS, 1.5); line(s.posts, NCHART.post, 2);
  const ink = t.dark ? "#cfd2f5" : t.sub;
  c.fillStyle = ink; c.font = "600 15px 'Hiragino Sans',sans-serif"; c.textBaseline = "alphabetic"; c.textAlign = "center";
  let ly = null; for (let wi = 0; wi < s.n; wi++) { const yr = new Date((s.start + wi * s.weekSec) * 1000).getUTCFullYear(); if (yr !== ly) { ly = yr; c.fillText(String(yr), xOf(wi), y + h - 8); } }
  c.textAlign = "left";
  let lx = x + 16; for (const [col, lbl] of [[NCHART.post, "posts"], [NCHART.zapR, "zap in"], [NCHART.zapS, "zap out"]]) { c.fillStyle = col; c.fillRect(lx, y + 12, 12, 12); c.fillStyle = ink; c.fillText(lbl, lx + 16, y + 23); lx += 34 + c.measureText(lbl).width; }
  c.restore();
}

function renderBack(d, theme = "jp") {
  const cv = document.getElementById("back-canvas");
  if (!cv) return;
  const t = THEMES[theme] || THEMES.jp;
  const c = cv.getContext("2d");
  const W = cv.width, H = cv.height, PAD = 56;
  c.clearRect(0, 0, W, H); c.lineCap = "round"; c.lineJoin = "round";
  const bg = c.createLinearGradient(0, 0, W, H);
  bg.addColorStop(0, t.paper[0]); bg.addColorStop(0.5, t.paper[1]); bg.addColorStop(1, t.paper[2]);
  roundRect(c, 0, 0, W, H, 24); c.fillStyle = bg; c.fill();
  c.save(); roundRect(c, 0, 0, W, H, 24); c.clip();
  drawBackBg(c, t, W, H);
  const sheen = c.createLinearGradient(0, H, W, 0);
  sheen.addColorStop(0.3, "rgba(255,255,255,0)"); sheen.addColorStop(0.5, t.dark ? "rgba(150,130,255,0.10)" : "rgba(170,200,255,0.12)"); sheen.addColorStop(0.7, "rgba(255,255,255,0)");
  c.fillStyle = sheen; c.fillRect(0, 0, W, H);
  c.restore();
  c.lineWidth = 5; c.strokeStyle = t.border; roundRect(c, 10, 10, W - 20, H - 20, 20); c.stroke();
  // 磁気ストライプ風
  c.fillStyle = "#15131f"; c.fillRect(34, 58, W - 68, 92);
  c.fillStyle = "rgba(255,255,255,0.6)"; c.textBaseline = "middle"; c.textAlign = "left";
  c.font = "700 22px 'Hiragino Sans',sans-serif"; c.fillText("NOSTR LICENSE · DATA STRIPE", PAD, 104);
  c.textAlign = "right"; c.font = "600 18px 'SF Mono','Menlo',monospace"; c.fillText((d.npub || "").slice(0, 24) + "…", W - PAD, 104);
  // ヘッダ
  c.textBaseline = "alphabetic"; c.textAlign = "left";
  c.fillStyle = t.ink; c.font = "800 44px 'Hiragino Sans',sans-serif"; c.fillText("ACTIVITY RECORD", PAD, 214);
  c.fillStyle = t.sub; c.font = "600 25px 'Hiragino Sans',sans-serif"; c.fillText(d.name + (d.handle ? "  @" + d.handle : ""), PAD, 250);
  c.textAlign = "right"; c.fillStyle = t.sub; c.font = "600 22px 'Hiragino Sans',sans-serif"; c.fillText("NO. " + licenseNo(d), W - PAD, 214);
  // RETRIEVED（取れた範囲＝best-effort）
  const s = nWeekly(d);
  const dd = (sec) => sec ? fmtISO(sec) : "—";
  c.textAlign = "left"; c.fillStyle = t.sub; c.font = "700 22px 'Hiragino Sans',sans-serif"; c.fillText("RETRIEVED RANGE (best-effort)", PAD, 296);
  c.fillStyle = t.accent2; c.font = "800 28px 'SF Mono','Menlo','Consolas',monospace"; c.fillText(s.n ? dd(s.first) + "  〜  " + dd(s.last) : "—", PAD, 332);
  // 週次チャート
  drawBackChart(c, d, t, PAD, 356, W - PAD * 2, 226);
  // 統計グリッド 3×3
  const days = new Set((d.noteTimes || []).map((x) => Math.floor(x / 86400)));
  const dayCount = new Map(); for (const x of (d.noteTimes || [])) { const k = Math.floor(x / 86400); dayCount.set(k, (dayCount.get(k) || 0) + 1); }
  let bDay = null, bN = 0; for (const [k, n] of dayCount) if (n > bN) { bN = n; bDay = k; }
  const cap = (v, f) => (v || 0).toLocaleString() + (f ? "+" : "");
  const grid = [
    ["POSTS", cap(d.activity, d.activityCapped)],
    ["LONGEST STREAK", (d.streak || 0) + " d" + (d.streakCapped ? "+" : "")],
    ["ACTIVE DAYS", cap(days.size) + " (seen)"],
    ["AVG PACE", (d.velocity || 0).toFixed(1) + " /day"],
    ["ZAP RECEIVED", "⚡" + cap(d.zapRecvSats) + "  (" + cap(d.zapRecv) + ")"],
    ["ZAP SENT", "⚡" + cap(d.zapSentSats) + "  (" + cap(d.zapSent) + ")"],
    ["WEB OF TRUST", cap(d.wotValue)],
    ["PEAK (UTC)", d.peakUTC || "—"],
    ["BUSIEST DAY", bDay ? fmtISO(bDay * 86400) + "  ·  " + bN : "—"],
  ];
  const gy0 = 648, rowH = 80, colW = (W - PAD * 2) / 3;
  for (let i = 0; i < grid.length; i++) {
    const gx = PAD + (i % 3) * colW, gy = gy0 + Math.floor(i / 3) * rowH;
    c.textAlign = "left"; c.fillStyle = t.sub; c.font = "700 18px 'Hiragino Sans',sans-serif"; c.fillText(grid[i][0], gx, gy);
    c.fillStyle = t.ink; c.font = "700 26px 'Hiragino Sans',sans-serif"; c.fillText(grid[i][1], gx, gy + 32);
    c.strokeStyle = t.gold2; c.globalAlpha = 0.22; c.lineWidth = 1; c.beginPath(); c.moveTo(gx, gy + 46); c.lineTo(gx + colW - 34, gy + 46); c.stroke(); c.globalAlpha = 1;
  }
  // バーコード（npub を Code128 で実エンコード・ラベル無し = スキャンしてからのお楽しみ）
  // npub は63文字と長いので maxW をホロシール左端手前で止める
  drawBarcode(c, d.npub || "", PAD, H - 100, 52, t.ink, { mod: 2.4, maxW: W - PAD * 2 - 160 });
  drawHoloSeal(c, W - 92, H - 74, 40);
}

// ===== 発行フロー（取得元が NIP-07 でも手入力でも完全に同じ処理）=====
async function issueFor(pubkeyHex) {
  try {
    if (getActiveRelays().length === 0) {
      throw new Error("Specify at least one relay");
    }
    const useUserRelays = $("use-user-relays").checked;
    const rangeDays = parseInt($("fetch-range").value, 10);
    const fetchSince = rangeDays > 0 ? Math.floor(Date.now() / 1000) - rangeDays * 86400 : null;
    const data = await fetchProfile(pubkeyHex, { useUserRelays, fetchSince });
    setStatus("Generating avatar / QR…");
    const [avatar, qr] = await Promise.all([
      loadAvatar(data.picture),
      makeQR("https://njump.me/" + data.npub),
    ]);
    data._avatar = avatar;
    data._qr = qr;
    lastData = data;

    await renderCard(data, $("theme-select").value);
    const back = document.getElementById("back-canvas");
    if (back) { back.hidden = false; renderBack(data, $("theme-select").value); }   // 裏面（ACTIVITY RECORD）
    const nip05State = !data.nip05 ? "" :
      data.nip05Verified === true ? " / NIP-05 ✓verified" :
      data.nip05Verified === false ? " / NIP-05 ✗mismatch" : " / NIP-05 unverifiable";
    setStatus(
      `Done: ${data.name} | posts ${data.activity}${data.activityCapped ? "+" : ""} / WoT ${data.wotValue} / vel ${(data.velocity || 0).toFixed(1)}/d / streak ${data.streak}d${data.streakCapped ? "+" : ""} / comm ${data.postsRecent}p+${data.interactions}i (react ${data.reactionsSent}→/←${data.reactionsRecv}, rt ${data.repostSent}→/←${data.repostRecv}, 30d) / peak ${data.peakUTC} / zap[${data.zapSource}] recv ⚡${(data.zapRecvSats || 0).toLocaleString()}(${data.zapRecv}) sent ⚡${(data.zapSentSats || 0).toLocaleString()}(${data.zapSent})${nip05State}`,
      "ok"
    );
    refreshShareCaption();   // 解析数値を下のシェア用テキストボックスへ反映
  } catch (err) {
    console.error(err);
    setStatus("Error: " + (err?.message || err), "error");
  }
}

// npub 文字列/hex を hex pubkey に正規化
function toHexPubkey(raw) {
  raw = raw.trim();
  if (raw.startsWith("npub1")) return nip19.decode(raw).data;
  if (/^[0-9a-f]{64}$/i.test(raw)) return raw.toLowerCase();
  throw new Error("Enter an npub1... or 64-char hex");
}

// ===== イベント =====
// NIP-07：公開鍵を取得して入力欄に入れるだけ（解析はしない）
$("nip07-btn").addEventListener("click", async () => {
  if (!window.nostr) {
    setStatus("No NIP-07 extension found (install nos2x / Alby, etc.)", "error");
    return;
  }
  try {
    setStatus("Getting public key via NIP-07…");
    const pk = await window.nostr.getPublicKey();
    $("npub-input").value = nip19.npubEncode(pk);
    setStatus("Got your npub. Press Issue.", "ok");
  } catch (err) {
    setStatus("Error: " + (err?.message || err), "error");
  }
});

// 発行
$("manual-btn").addEventListener("click", async () => {
  const raw = $("npub-input").value.trim();
  if (!raw) { setStatus("Enter an npub", "error"); return; }
  try {
    await issueFor(toHexPubkey(raw));
  } catch (err) {
    setStatus("Error: " + (err?.message || err), "error");
  }
});

$("theme-select").addEventListener("change", () => {
  if (lastData) {
    const theme = $("theme-select").value;
    renderCard(lastData, theme);
    if (lastData.noteTimes) renderBack(lastData, theme);
  }
});

// ===== アイコン正方形表示トグル =====
try {
  const sq = $("square-avatar");
  if (localStorage.getItem("nl_square") === "1") sq.checked = true;
  sq.addEventListener("change", () => {
    try { localStorage.setItem("nl_square", sq.checked ? "1" : "0"); } catch {}
    if (lastData) renderCard(lastData, $("theme-select").value);
  });
} catch {}

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
  del.title = "Remove this relay";
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
  ctx.fillText("Enter an npub and press Issue", canvas.width / 2, canvas.height / 2);
})();

$("download-btn").addEventListener("click", () => {
  try {
    const back = document.getElementById("back-canvas");
    if (back && !back.hidden) {
      // 表裏を縦に並べて1枚のPNGに結合（表 → 裏）
      const out = document.createElement("canvas");
      out.width = canvas.width; out.height = canvas.height * 2 + 32;
      const oc = out.getContext("2d");
      oc.fillStyle = "#0c1015"; oc.fillRect(0, 0, out.width, out.height);
      oc.drawImage(canvas, 0, 0);
      oc.drawImage(back, 0, canvas.height + 32);
      const a = document.createElement("a"); a.href = out.toDataURL("image/png"); a.download = "nostr-license.png"; a.click();
    } else {
      const a = document.createElement("a"); a.href = canvas.toDataURL("image/png"); a.download = "nostr-license.png"; a.click();
    }
  } catch (err) {
    setStatus("Download failed (possible avatar CORS restriction): " + err.message, "error");
  }
});

// ===== NIP-07 でログイン中なら Nostr へ投稿（NIP-96 アップロード + kind:1）=====
// NIP-96 アップロードAPI（well-known で確認済み）
const UPLOAD_HOSTS = {
  "nostr.build": "https://nostr.build/api/v2/nip96/upload",
  "nostrcheck.me": "https://cdn.nostrcheck.me",
  "share.yabu.me": "https://yabu.me/api/v2/media",
};
const isJa = () => (navigator.language || "").toLowerCase().startsWith("ja");

function shareStatus(msg, kind = "") {
  const el = $("share-status");
  if (el) { el.textContent = msg; el.className = "status" + (kind ? " " + kind : ""); }
}

const SITE_URL = "https://kojira.github.io/nostr-license/";

function defaultCaption() {
  return "I made my Nostr License! #NostrLicense\n" + SITE_URL;
}

// シェア用キャプション（英語固定。カード・UI が英語なので統一）。カードは「★評価」しか
// 描かないので、その裏付けとなる【実数値】を入れる（CLASS/ENDORSEMENT/PEAK/★自体は除く）。
function statsCaption(d) {
  const streak = (d.streak || 0) + "d" + (d.streakCapped ? "+" : "");
  const vel = (d.velocity || 0).toFixed(1);
  const core = `WoT ${d.wotValue} / velocity ${vel}/d / streak ${streak}`;
  const zap = `⚡ zap received ${d.zapRecv} / sent ${d.zapSent}`;
  const social = `👥 ${d.followers} followers / ${d.following} following`;
  const comm = `💬 reactions ${d.reactionsRecv} in / ${d.reactionsSent} out · reposts ${d.repostRecv} in / ${d.repostSent} out (30d)`;
  return `🪪 My Nostr License\n${core}\n${zap}\n${social}\n${comm}\n#NostrLicense\n${SITE_URL}`;
}

// 発行後にキャプションを更新（ユーザーが手編集していたら上書きしない）。
let lastAutoCaption = "";
function refreshShareCaption() {
  const el = document.getElementById("post-caption");
  if (!el) return;
  const cur = el.value.trim();
  if (cur && cur !== lastAutoCaption) return;   // 手編集済みは尊重
  const cap = lastData ? statsCaption(lastData) : defaultCaption();
  el.value = cap;
  lastAutoCaption = cap;
}

function buildHostOptions() {
  const sel = $("upload-host");
  if (!sel) return;
  const order = isJa()
    ? ["share.yabu.me", "nostr.build", "nostrcheck.me"]
    : ["nostr.build", "nostrcheck.me"];
  sel.innerHTML = order.map((h) => `<option value="${h}">${h}</option>`).join("");
}

function canvasToBlob() {
  return new Promise((resolve, reject) =>
    canvas.toBlob((b) => (b ? resolve(b) : reject(new Error("Could not export image (avatar CORS?)"))), "image/png")
  );
}

// NIP-98: HTTP リクエスト認可イベント（kind:27235）を NIP-07 で署名
async function nip98Header(url, method) {
  const evt = {
    kind: 27235,
    created_at: Math.floor(Date.now() / 1000),
    tags: [["u", url], ["method", method]],
    content: "",
  };
  const signed = await window.nostr.signEvent(evt);
  return "Nostr " + btoa(JSON.stringify(signed));
}

// NIP-96 アップロード → 公開URLと NIP-94 タグを返す
async function uploadImage(apiUrl, blob) {
  const auth = await nip98Header(apiUrl, "POST");
  const fd = new FormData();
  fd.append("file", blob, "nostr-license.png");
  fd.append("content_type", "image/png");
  const res = await fetch(apiUrl, { method: "POST", headers: { Authorization: auth }, body: fd });
  const text = await res.text();
  if (!res.ok) throw new Error(`upload failed (HTTP ${res.status}) ${text.slice(0, 160)}`);
  let j; try { j = JSON.parse(text); } catch { throw new Error("upload: invalid JSON response"); }
  const tags = (j && j.nip94_event && j.nip94_event.tags) || [];
  let url = (tags.find((t) => t[0] === "url") || [])[1] || j.url || (j.data && j.data.url);
  if (!url) throw new Error("upload: no URL in response");
  return { url, nip94: tags };
}

function extractHashtags(text) {
  const out = [], seen = new Set();
  const re = /(?:^|\s)#([\p{L}\p{N}_]+)/gu;
  let m;
  while ((m = re.exec(text))) {
    const t = m[1].toLowerCase();
    if (!seen.has(t)) { seen.add(t); out.push(t); }
  }
  return out;
}

function imetaTag(url, nip94) {
  const want = ["m", "x", "ox", "dim", "blurhash"];
  const parts = ["url " + url];
  for (const t of nip94 || []) if (want.includes(t[0]) && t[1]) parts.push(`${t[0]} ${t[1]}`);
  return ["imeta", ...parts];
}

// kind:1 を NIP-07 で署名して選択中リレーへ publish
async function publishNote(content, imageUrl, nip94) {
  const pk = await window.nostr.getPublicKey();
  const tags = extractHashtags(content).map((t) => ["t", t]);
  tags.push(imetaTag(imageUrl, nip94));
  const evt = { kind: 1, created_at: Math.floor(Date.now() / 1000), tags, content, pubkey: pk };
  const signed = await window.nostr.signEvent(evt);
  const relays = getActiveRelays();
  const results = await Promise.all(relays.map((url) => new Promise((resolve) => {
    let done = false, ws;
    const finish = (ok, msg) => { if (done) return; done = true; try { ws.close(); } catch {} resolve({ url, ok, msg }); };
    try { ws = new WebSocket(url); } catch { resolve({ url, ok: false, msg: "ws error" }); return; }
    const timer = setTimeout(() => finish(false, "timeout"), 6000);
    ws.onopen = () => { try { ws.send(JSON.stringify(["EVENT", signed])); } catch (e) { clearTimeout(timer); finish(false, String(e)); } };
    ws.onmessage = (m) => {
      try { const d = JSON.parse(m.data); if (d[0] === "OK" && d[1] === signed.id) { clearTimeout(timer); finish(!!d[2], d[3] || ""); } } catch {}
    };
    ws.onerror = () => { clearTimeout(timer); finish(false, "ws error"); };
  })));
  return { signed, results };
}

async function postToNostr() {
  if (!window.nostr) { shareStatus("No NIP-07 extension found", "error"); return; }
  if (!lastData) { shareStatus("Generate a card first (press Issue)", "error"); return; }
  if (getActiveRelays().length === 0) { shareStatus("Add at least one relay to publish to", "error"); return; }
  const hostName = $("upload-host").value;
  const apiUrl = UPLOAD_HOSTS[hostName];
  const caption = $("post-caption").value.trim();
  const btn = $("post-btn");
  try {
    btn.disabled = true;
    shareStatus("Exporting image…");
    const blob = await canvasToBlob();
    shareStatus(`Uploading to ${hostName}… (approve the NIP-98 signature)`);
    const { url, nip94 } = await uploadImage(apiUrl, blob);
    shareStatus("Signing & publishing the note… (approve the signature)");
    const content = caption ? `${caption}\n${url}` : url;
    const { results } = await publishNote(content, url, nip94);
    const ok = results.filter((r) => r.ok).length;
    shareStatus(`Posted to ${ok}/${results.length} relays. Image: ${url}`, ok ? "ok" : "error");
  } catch (err) {
    console.error(err);
    shareStatus("Error: " + (err?.message || err), "error");
  } finally {
    btn.disabled = false;
  }
}

function initShare() {
  if (!window.nostr) return false;
  const box = $("share-box");
  if (!box || !box.hidden) return true; // already shown
  buildHostOptions();
  refreshShareCaption();
  $("post-btn").addEventListener("click", postToNostr);
  box.hidden = false;
  return true;
}

// 拡張機能が遅れて inject される場合に備えて数回試す
if (!initShare()) {
  let tries = 0;
  const iv = setInterval(() => { if (initShare() || ++tries > 6) clearInterval(iv); }, 500);
}
