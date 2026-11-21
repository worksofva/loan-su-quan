// Máy chủ Loạn Sứ Quân: phục vụ trang game + ghép trận 15 giây + chạy trận 12 tướng (người thật + bot).
// Không cần cài thêm gói nào: chỉ cần Node.js bản 18 trở lên. Chạy:  node server.js
const http = require('http'), fs = require('fs'), path = require('path'), crypto = require('crypto'), vm = require('vm');
const PORT = process.env.PORT || 8080;
const PUB = path.join(__dirname, 'public');
const LOBBY_SEC = 15, MAXP = 12, TICK_MS = 33, MAX_MATCH_SEC = 25 * 60;

/* ---------- Nạp code game để chạy trong máy chủ ---------- */
const HTML = fs.readFileSync(path.join(PUB, 'index.html'), 'utf8');
const GAME_JS = (HTML.match(/<script>([\s\S]*?)<\/script>/g) || []).map(x => x.slice(8, -9)).sort((a, b) => b.length - a.length)[0]
  .replace(/data:image\/[a-z]+;base64,[A-Za-z0-9+\/=]+/g, '');   // máy chủ không cần ảnh nhúng: bỏ để tiết kiệm bộ nhớ
const stub = () => new Proxy(function () {}, { get: (t, k) => { if (k === Symbol.toPrimitive) return () => 0; return k === 'querySelectorAll' ? () => [] : stub(); }, set: () => true, apply: () => stub(), construct: () => stub() });
function makeCtx() {
  const c = { document: { getElementById: () => stub(), querySelectorAll: () => [], querySelector: () => stub(), createElement: () => stub() }, window: {}, innerWidth: 960, innerHeight: 540, devicePixelRatio: 1,
    addEventListener() {}, performance, setTimeout, clearTimeout, setInterval() {}, console, Math, Set, WeakSet, Map };
  vm.createContext(c); vm.runInContext(GAME_JS, c); return c;
}
const probe = makeCtx();
const HEROES = vm.runInContext('Object.keys(CFG.heroes)', probe), WEAPONS = vm.runInContext('Object.keys(CFG.weapons)', probe);
const INDEX_ONLINE = HTML.replace('<head>', '<head><script>window.LSQ_ONLINE=true;</script>');

/* ---------- HTTP: phục vụ file tĩnh ---------- */
const MIME = { '.html': 'text/html; charset=utf-8', '.png': 'image/png', '.jpg': 'image/jpeg', '.js': 'text/javascript', '.json': 'application/json', '.ico': 'image/x-icon' };
const server = http.createServer((req, res) => {
  const u = decodeURIComponent((req.url || '/').split('?')[0]);
  if (u === '/' || u === '/index.html') { res.writeHead(200, { 'Content-Type': MIME['.html'], 'Cache-Control': 'no-cache' }); return res.end(INDEX_ONLINE); }
  if (u === '/health') { res.writeHead(200); return res.end('ok'); }
  const f = path.join(PUB, path.normalize(u).replace(/^(\.\.[\/\\])+/, ''));
  if (!f.startsWith(PUB) || !fs.existsSync(f) || fs.statSync(f).isDirectory()) { res.writeHead(404); return res.end('not found'); }
  res.writeHead(200, { 'Content-Type': MIME[path.extname(f)] || 'application/octet-stream', 'Cache-Control': 'public, max-age=3600' });
  fs.createReadStream(f).pipe(res);
});

/* ---------- WebSocket tối giản (không cần thư viện ngoài) ---------- */
function wsAccept(key) { return crypto.createHash('sha1').update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11').digest('base64'); }
function wsFrame(str) {
  const p = Buffer.from(str, 'utf8'), n = p.length; let h;
  if (n < 126) h = Buffer.from([0x81, n]); else if (n < 65536) { h = Buffer.alloc(4); h[0] = 0x81; h[1] = 126; h.writeUInt16BE(n, 2); } else { h = Buffer.alloc(10); h[0] = 0x81; h[1] = 127; h.writeBigUInt64BE(BigInt(n), 2); }
  return Buffer.concat([h, p]);
}
class Conn {
  constructor(sock) { this.sock = sock; this.buf = Buffer.alloc(0); this.alive = true; this.lastRx = Date.now(); this.onmsg = null; this.onclose = null; this.msgs = 0; this.win = Date.now();
    sock.on('data', d => this.rx(d)); sock.on('close', () => this.close()); sock.on('error', () => this.close()); }
  send(o) { if (!this.alive) return; try { if (this.sock.writableLength > 2e6) return; this.sock.write(wsFrame(typeof o === 'string' ? o : JSON.stringify(o))); } catch (e) { this.close(); } }
  rx(d) {
    this.lastRx = Date.now(); this.buf = Buffer.concat([this.buf, d]); if (this.buf.length > 1e6) return this.close();
    for (;;) {
      const b = this.buf; if (b.length < 2) return;
      const op = b[0] & 15, masked = !!(b[1] & 128); let len = b[1] & 127, off = 2;
      if (len === 126) { if (b.length < 4) return; len = b.readUInt16BE(2); off = 4; } else if (len === 127) { if (b.length < 10) return; len = Number(b.readBigUInt64BE(2)); off = 10; }
      if (len > 65536) return this.close();
      const need = off + (masked ? 4 : 0) + len; if (b.length < need) return;
      let p = b.slice(off + (masked ? 4 : 0), need);
      if (masked) { const k = b.slice(off, off + 4); p = Buffer.from(p); for (let i = 0; i < p.length; i++) p[i] ^= k[i & 3]; }
      this.buf = b.slice(need);
      if (op === 8) return this.close();
      if (op === 9) { try { this.sock.write(Buffer.concat([Buffer.from([0x8a, p.length]), p])); } catch (e) { } continue; }
      if (op === 1) {
        const now = Date.now(); if (now - this.win > 1000) { this.win = now; this.msgs = 0; } if (++this.msgs > 60) continue;   // chống gửi dồn dập
        let m; try { m = JSON.parse(p.toString('utf8')); } catch (e) { continue; } if (this.onmsg) this.onmsg(m);
      }
    }
  }
  close() { if (!this.alive) return; this.alive = false; try { this.sock.destroy(); } catch (e) { } if (this.onclose) this.onclose(); }
}
server.on('upgrade', (req, sock) => {
  if ((req.url || '').split('?')[0] !== '/ws' || !req.headers['sec-websocket-key']) { sock.destroy(); return; }
  sock.write('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ' + wsAccept(req.headers['sec-websocket-key']) + '\r\n\r\n');
  sock.setNoDelay(true); onConnect(new Conn(sock));
});

/* ---------- Sảnh ghép trận ---------- */
const players = new Set(), queue = [];
let qTimer = null, qStart = 0;
const cleanName = v => String(v || '').replace(/[\u0000-\u001f<>&"]/g, '').trim().slice(0, 14) || 'Người chơi';
function lobbyBroadcast() {
  const left = Math.max(0, Math.ceil(LOBBY_SEC - (Date.now() - qStart) / 1000)), names = queue.map(p => p.name);
  queue.forEach((p, i) => p.conn.send({ t: 'lobby', left: queue.length >= MAXP ? 0 : left, names, you: i }));
}
function lobbyTick() {
  if (!queue.length) { clearInterval(qTimer); qTimer = null; return; }
  lobbyBroadcast();
  if (queue.length >= MAXP || Date.now() - qStart >= LOBBY_SEC * 1000) {
    const group = queue.splice(0, MAXP); clearInterval(qTimer); qTimer = null;
    group.forEach(p => p.conn.send({ t: 'lobby', left: 0, names: group.map(x => x.name), you: group.indexOf(p) }));
    setTimeout(() => startMatch(group), 1200);
    if (queue.length) { qStart = Date.now(); qTimer = setInterval(lobbyTick, 1000); }
  }
}
function enqueue(p) {
  if (queue.includes(p)) return; leaveMatch(p);
  queue.push(p);
  if (!qTimer) { qStart = Date.now(); qTimer = setInterval(lobbyTick, 1000); }
  lobbyBroadcast();
}
function dequeue(p) { const i = queue.indexOf(p); if (i >= 0) queue.splice(i, 1); if (!queue.length && qTimer) { clearInterval(qTimer); qTimer = null; } else lobbyBroadcast(); }

/* ---------- Trận đấu ---------- */
const matches = new Set();
function startMatch(group) {
  group = group.filter(p => p.conn.alive); if (!group.length) return;
  const ctx = makeCtx(), names = group.map(p => p.name); for (let i = names.length; i < MAXP; i++) names.push('Bot ' + String.fromCharCode(65 + i - names.length));
  const m = { ctx, group, tick: 0, over: false, startedAt: Date.now() };
  ctx.__roster = group.map(p => ({ hero: p.hero, weapon: p.weapon })); ctx.__names = names;
  ctx.__hook = (o, d) => { const p = group[o]; if (p && p.match === m) p.conn.send({ t: 'over', d }); };
  const init = vm.runInContext('netInit(__roster, __names)', ctx); vm.runInContext('netServerMode(__hook)', ctx);
  group.forEach((p, i) => { p.match = m; p.slot = i; p.conn.send({ t: 'start', me: i, names, init }); });
  m.iv = setInterval(() => stepMatch(m), TICK_MS); matches.add(m);
  console.log('Trận mới:', group.length, 'người thật; đang chạy', matches.size, 'trận');
}
function stepMatch(m) {
  try {
    m.ctx.netTick(); m.tick++;
    if (m.tick % 2 === 0) {   // 15 lần/giây cho chuyển động mượt
      const slow = m.tick % 16 === 0;
      for (const p of m.group) if (p.match === m && p.conn.alive) p.conn.send(JSON.stringify(Object.assign({ t: 'snap' }, m.ctx.netEncode(p.slot, slow))));
    }
    if (m.tick % 30 === 0) {
      const st = m.ctx.netState();
      if (st.humans === 0 || st.left <= 1 || (Date.now() - m.startedAt) / 1000 > MAX_MATCH_SEC) endMatch(m);
    }
  } catch (e) { console.error('Lỗi trận:', e); endMatch(m); }
}
function endMatch(m) { if (m.over) return; m.over = true; clearInterval(m.iv); matches.delete(m); for (const p of m.group) if (p.match === m) { p.conn.send({ t: 'matchend' }); p.match = null; } console.log('Trận kết thúc; còn', matches.size, 'trận'); }
function leaveMatch(p) { const m = p.match; if (!m) return; p.match = null; try { m.ctx.netDrop(p.slot); } catch (e) { } }

/* ---------- Kết nối người chơi ---------- */
function onConnect(conn) {
  const p = { conn, name: 'Người chơi', hero: 'long', weapon: 'dao', match: null, slot: -1 }; players.add(p);
  conn.onmsg = m => {
    if (!m || typeof m !== 'object') return;
    if (m.t === 'join') { p.name = cleanName(m.name); p.hero = HEROES.includes(m.hero) ? m.hero : HEROES[0]; p.weapon = WEAPONS.includes(m.weapon) ? m.weapon : WEAPONS[0]; enqueue(p); }
    else if (m.t === 'leave') { dequeue(p); leaveMatch(p); }
    else if (m.t === 'spec' && p.match) { try { p.match.ctx.netSpec(p.slot, m.on, m.to); } catch (e) { } }
    else if (m.t === 'in' && p.match) { try { p.match.ctx.netSetInput(p.slot, m.x, m.y); } catch (e) { } }
    else if (m.t === 'act' && p.match && typeof m.f === 'string') { try { p.match.ctx.netAct(p.slot, m.f, Array.isArray(m.a) ? m.a : []); } catch (e) { } }
  };
  conn.onclose = () => { dequeue(p); leaveMatch(p); players.delete(p); };
}
setInterval(() => { const now = Date.now(); for (const p of players) if (now - p.conn.lastRx > 60000) p.conn.close(); }, 15000);   // ngắt kết nối im lặng quá 60 giây
setInterval(() => { for (const p of players) { try { p.conn.sock.write(Buffer.from([0x89, 0])); } catch (e) { } } }, 20000);   // ping giữ kết nối

server.listen(PORT, () => console.log('Loạn Sứ Quân chạy tại cổng ' + PORT + ' (mở http://localhost:' + PORT + ')'));
