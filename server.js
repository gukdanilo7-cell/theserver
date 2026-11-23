// ============================================================
//  MONO — co-op relay server (готовий до деплою на Render.com)
//  Протокол точно відповідає клієнту з index.html:
//    client -> {t:'hello', name, code}      server -> {t:'hello', id}
//    client -> {t:'create'}                 server -> {t:'room', room, host, players}
//    client -> {t:'join', room}             server -> {t:'room', ...} | {t:'err', msg}
//    client -> {t:'leave'}
//    client -> {t:'ready', ready}           server -> {t:'room', ...}
//    client -> {t:'start', map, mode}       server -> {t:'start', map, mode} (усім у кімнаті)
//    client -> {t:'pos', x,y,z,yaw,...}     server -> {t:'pos', id, name, ...} (іншим)
//    client -> {t:'shot', x,y,z,dx,dy,dz,w} server -> {t:'shot', id, name, ...} (іншим)
//    server -> {t:'peer', id, name}  |  {t:'left', id}
// ============================================================
const http = require('http');
const fs = require('fs');
const path = require('path');
const { WebSocketServer, WebSocket } = require('ws');

const PORT = process.env.PORT || 8765;
const ROOM_LEN = 6;
const ALPHABET = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'; // без 0/O/1/I

// ---------- HTTP (health check + роздача гри, якщо index.html поруч) ----------
const server = http.createServer((req, res) => {
  if (req.url === '/healthz') {
    res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
    return res.end('ok');
  }
  const gameFile = path.join(__dirname, 'index.html');
  if (req.url === '/' && fs.existsSync(gameFile)) {
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    return fs.createReadStream(gameFile).pipe(res);
  }
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('MONO co-op server is running');
});

const wss = new WebSocketServer({ server });

// ---------- стан ----------
const rooms = new Map();   // code -> { hostId, players: Map<id, {id, ws, name, ready}> }
const clients = new Map(); // ws  -> { id, name, room: code|null }
let nextId = 1;

const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);

function send(ws, obj) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    try { ws.send(JSON.stringify(obj)); } catch (e) {}
  }
}

function roomCode() {
  let code;
  do {
    code = '';
    for (let i = 0; i < ROOM_LEN; i++) code += ALPHABET[Math.floor(Math.random() * ALPHABET.length)];
  } while (rooms.has(code));
  return code;
}

function playersPayload(room) {
  return [...room.players.values()].map(p => ({
    name: p.name,
    host: p.id === room.hostId,
    ready: p.ready
  }));
}

function broadcastRoom(code) {
  const room = rooms.get(code);
  if (!room) return;
  const payload = { t: 'room', room: code, host: room.hostId, players: playersPayload(room) };
  for (const p of room.players.values()) send(p.ws, payload);
}

function relayOthers(code, fromId, obj) {
  const room = rooms.get(code);
  if (!room) return;
  for (const p of room.players.values()) {
    if (p.id !== fromId) send(p.ws, obj);
  }
}

function leaveRoom(info) {
  const code = info.room;
  if (!code) return;
  info.room = null;
  const room = rooms.get(code);
  if (!room) return;
  room.players.delete(info.id);
  relayOthers(code, info.id, { t: 'left', id: info.id });
  if (!room.players.size) {
    rooms.delete(code);
    log(`room ${code} closed (empty)`);
    return;
  }
  if (room.hostId === info.id) {
    room.hostId = room.players.keys().next().value; // хост переїжджає на першого живого
    log(`room ${code}: new host id=${room.hostId}`);
  }
  broadcastRoom(code);
}

// ---------- WebSocket ----------
wss.on('connection', (ws) => {
  const info = { id: nextId++, name: 'Player', room: null };
  clients.set(ws, info);
  ws.isAlive = true;
  ws.on('pong', () => { ws.isAlive = true; });
  log(`connect id=${info.id} (${wss.clients.size} online)`);

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }
    if (!msg || typeof msg.t !== 'string') return;

    switch (msg.t) {
      case 'hello': {
        info.name = String(msg.name || 'Player').slice(0, 16);
        send(ws, { t: 'hello', id: info.id });
        break;
      }

      case 'create': {
        leaveRoom(info);
        const code = roomCode();
        rooms.set(code, { hostId: info.id, players: new Map() });
        const room = rooms.get(code);
        room.players.set(info.id, { id: info.id, ws, name: info.name, ready: false });
        info.room = code;
        log(`room ${code} created by "${info.name}"`);
        broadcastRoom(code);
        break;
      }

      case 'join': {
        const code = String(msg.room || '').trim().toUpperCase();
        const room = rooms.get(code);
        if (!room) { send(ws, { t: 'err', msg: 'Room not found: ' + code }); break; }
        if (room.players.has(info.id)) break;
        leaveRoom(info);
        room.players.set(info.id, { id: info.id, ws, name: info.name, ready: false });
        info.room = code;
        log(`"${info.name}" joined ${code}`);
        relayOthers(code, info.id, { t: 'peer', id: info.id, name: info.name });
        broadcastRoom(code);
        break;
      }

      case 'leave': {
        const code = info.room;
        leaveRoom(info);
        if (code) log(`"${info.name}" left ${code}`);
        break;
      }

      case 'ready': {
        const room = rooms.get(info.room);
        if (!room) break;
        const player = room.players.get(info.id);
        if (player) { player.ready = !!msg.ready; broadcastRoom(info.room); }
        break;
      }

      case 'start': {
        const room = rooms.get(info.room);
        if (!room) break;
        if (room.hostId !== info.id) { send(ws, { t: 'err', msg: 'Only host can start' }); break; }
        log(`room ${info.room}: start map=${msg.map} mode=${msg.mode}`);
        relayOthers(info.room, info.id, { t: 'start', map: msg.map, mode: msg.mode });
        send(ws, { t: 'start', map: msg.map, mode: msg.mode });
        break;
      }

      case 'pos': {
        if (!info.room) break;
        relayOthers(info.room, info.id, {
          t: 'pos', id: info.id, name: info.name,
          x: msg.x, y: msg.y, z: msg.z,
          yaw: msg.yaw, pitch: msg.pitch, hp: msg.hp, w: msg.w
        });
        break;
      }

      case 'shot': {
        if (!info.room) break;
        relayOthers(info.room, info.id, {
          t: 'shot', id: info.id, name: info.name,
          x: msg.x, y: msg.y, z: msg.z,
          dx: msg.dx, dy: msg.dy, dz: msg.dz, w: msg.w
        });
        break;
      }
    }
  });

  ws.on('close', () => {
    clients.delete(ws);
    leaveRoom(info);
    log(`disconnect id=${info.id} (${wss.clients.size} online)`);
  });
  ws.on('error', () => {});
});

// ---------- keep-alive: проксі Render ріже "мертві" зʼєднання ----------
const INTERVAL = 30000;
const pingTimer = setInterval(() => {
  for (const ws of wss.clients) {
    if (!ws.isAlive) { try { ws.terminate(); } catch (e) {} continue; }
    ws.isAlive = false;
    try { ws.ping(); } catch (e) {}
  }
}, INTERVAL);
wss.on('close', () => clearInterval(pingTimer));

server.listen(PORT, '0.0.0.0', () => {
  log(`MONO co-op server listening on port ${PORT}`);
});
