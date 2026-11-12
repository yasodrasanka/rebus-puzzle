/* Rebus Challenge: shared config, answer checking and realtime transport. */
(function () {
  const PUZZLES = [
    { img: 'img/p01.jpg', answer: 'Scrambled eggs', accept: ['scrambled eggs', 'scrambled egg', 'scramble eggs', 'scramble egg', 'eggs scrambled', 'egg scramble'] },
    { img: 'img/p02.jpg', answer: 'Microservices', accept: ['microservices', 'microservice'] },
    { img: 'img/p03.jpg', answer: 'Travel overseas', accept: ['travel overseas', 'travel oversea', 'travelling overseas', 'traveling overseas', 'overseas travel', 'travel over the seas', 'travel over the sea'] },
    { img: 'img/p04.jpg', answer: 'Backlog', accept: ['backlog', 'backlogs'] },
    { img: 'img/p05.jpg', answer: 'Upgrade', accept: ['upgrade', 'upgrades', 'upgraded', 'grade up'] },
    { img: 'img/p06.jpg', answer: "Summer (sum of R's)", accept: ['summer', 'sum of r', 'sum of rs', 'sum r'] },
    { img: 'img/p07.jpg', answer: 'Pair programming', accept: ['pair programming', 'pair program', 'pair programmer', 'pair coding', 'peer programming'] },
    { img: 'img/p08.jpg', answer: "Paradise (pair o' dice)", accept: ['paradise', 'pair of dice', 'pair o dice', 'pair a dice', 'paradice', 'pair of dices', 'pairs of dice'] },
    { img: 'img/p09.jpg', answer: 'Stand-up meeting', accept: ['stand up meeting', 'stand up meetings', 'standup', 'stand up', 'standing meeting', 'daily standup', 'daily stand up meeting'] },
    { img: 'img/p10.jpg', answer: 'Cut-off time', accept: ['cut off time', 'cut off times', 'time cut off', 'cut of time'] },
  ];

  const params = new URLSearchParams(location.search);
  const MOCK = params.get('mock') === '1';
  const NTFY = 'https://ntfy.sh';
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

  /* ---------- answers ---------- */
  function norm(s) {
    return String(s || '')
      .toLowerCase()
      .normalize('NFKD').replace(/[̀-ͯ]/g, '')
      .replace(/&/g, 'and')
      .replace(/[^a-z0-9]/g, '');
  }
  // Edit distance that counts a swap of two neighbouring letters ("upgarde") as one typo.
  function lev(a, b) {
    const m = a.length, n = b.length, d = [];
    for (let i = 0; i <= m; i++) { d[i] = [i]; }
    for (let j = 0; j <= n; j++) d[0][j] = j;
    for (let i = 1; i <= m; i++) {
      for (let j = 1; j <= n; j++) {
        const c = a[i - 1] === b[j - 1] ? 0 : 1;
        d[i][j] = Math.min(d[i - 1][j] + 1, d[i][j - 1] + 1, d[i - 1][j - 1] + c);
        if (i > 1 && j > 1 && a[i - 1] === b[j - 2] && a[i - 2] === b[j - 1]) d[i][j] = Math.min(d[i][j], d[i - 2][j - 2] + 1);
      }
    }
    return d[m][n];
  }
  function isCorrect(q, text) {
    const n = norm(text);
    if (!n || !PUZZLES[q]) return false;
    return PUZZLES[q].accept.some((acc) => {
      const m = norm(acc);
      if (n === m) return true;
      const tol = m.length >= 12 ? 2 : m.length >= 5 ? 1 : 0; // forgive small typos
      return tol > 0 && Math.abs(n.length - m.length) <= tol && lev(n, m) <= tol;
    });
  }
  // Kahoot-style: 1000 for an instant correct answer, down to 500 at the buzzer.
  function points(elapsedMs, durSec) {
    const d = durSec * 1000;
    return Math.round(1000 * (1 - Math.min(Math.max(elapsedMs, 0), d) / d / 2));
  }

  /* ---------- ids and names ---------- */
  function rand(chars, n) {
    const a = new Uint32Array(n);
    crypto.getRandomValues(a);
    return Array.from(a, (x) => chars[x % chars.length]).join('');
  }
  const rid = () => rand('abcdefghijkmnpqrstuvwxyz23456789', 8);
  const roomCode = () => rand('ABCDEFGHJKLMNPQRSTUVWXYZ23456789', 5);
  const cleanCode = (s) => String(s || '').toUpperCase().replace(/[^A-Z0-9]/g, '').slice(0, 8);
  const cleanName = (s) => String(s || '').replace(/[\u0000-\u001f<>]/g, '').replace(/\s+/g, ' ').trim().slice(0, 24);
  const topic = (code, kind) => 'rbsgame-' + code.toLowerCase() + '-' + kind + '-x9q';

  /* ---------- transport (ntfy.sh pub/sub, or BroadcastChannel when ?mock=1) ---------- */
  async function publish(t, obj) {
    const body = JSON.stringify(obj);
    if (MOCK) { mockPublish(t, body); return true; }
    for (let i = 0; i < 4; i++) {
      try {
        const r = await fetch(NTFY + '/' + t, { method: 'POST', body });
        if (r.ok) return true;
        if (r.status === 429) { await sleep(2500 + i * 1500); continue; }
      } catch (e) { /* network hiccup: retry */ }
      await sleep(700 * (i + 1));
    }
    return false;
  }

  function subscribe(t, since, onMsg, onStatus) {
    if (MOCK) return mockSubscribe(t, since, onMsg, onStatus);
    let es = null, closed = false, retry = 0, lastId = null;
    const seen = new Set();
    const open = () => {
      if (closed) return;
      const s = lastId || since || 'all';
      es = new EventSource(NTFY + '/' + t + '/sse?since=' + encodeURIComponent(s));
      es.onopen = () => { retry = 0; onStatus && onStatus(true); };
      es.onmessage = (e) => {
        let m;
        try { m = JSON.parse(e.data); } catch (_) { return; }
        if (!m || m.event !== 'message' || seen.has(m.id)) return;
        seen.add(m.id);
        lastId = m.id;
        let body;
        try { body = JSON.parse(m.message); } catch (_) { return; }
        onMsg(body, m);
      };
      es.onerror = () => {
        onStatus && onStatus(false);
        try { es.close(); } catch (_) {}
        retry++;
        setTimeout(open, Math.min(1000 * retry, 5000));
      };
    };
    open();
    return { close() { closed = true; try { es && es.close(); } catch (_) {} } };
  }

  // Local test transport: same-browser tabs talk over BroadcastChannel, with a replay log.
  function mockLog(t) { try { return JSON.parse(localStorage.getItem('mock:' + t) || '[]'); } catch (_) { return []; } }
  function mockPublish(t, body) {
    const m = { id: rid(), time: Math.floor(Date.now() / 1000), event: 'message', topic: t, message: body };
    const log = mockLog(t); log.push(m);
    try { localStorage.setItem('mock:' + t, JSON.stringify(log.slice(-400))); } catch (_) {}
    new BroadcastChannel('mock:' + t).postMessage(m);
  }
  function mockSubscribe(t, since, onMsg, onStatus) {
    const seen = new Set();
    const deliver = (m) => {
      if (seen.has(m.id)) return; seen.add(m.id);
      let body; try { body = JSON.parse(m.message); } catch (_) { return; }
      onMsg(body, m);
    };
    const bc = new BroadcastChannel('mock:' + t);
    bc.onmessage = (e) => deliver(e.data);
    setTimeout(() => {
      onStatus && onStatus(true);
      const log = mockLog(t);
      let start = 0;
      if (since && since !== 'all') { const i = log.findIndex((m) => m.id === since); start = i >= 0 ? i + 1 : log.length; }
      log.slice(start).forEach(deliver);
    }, 50);
    return { close() { bc.close(); } };
  }

  /* ---------- small helpers ---------- */
  const esc = (s) => String(s == null ? '' : s).replace(/[&<>"']/g, (c) => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const fmt = (n) => Number(n || 0).toLocaleString('en-US');
  function store(key, val) { try { localStorage.setItem(key, JSON.stringify(val)); } catch (_) {} }
  function load(key) { try { return JSON.parse(localStorage.getItem(key) || 'null'); } catch (_) { return null; } }

  window.RB = { PUZZLES, MOCK, isCorrect, points, norm, rid, roomCode, cleanCode, cleanName, topic, publish, subscribe, esc, fmt, store, load, sleep };
})();
