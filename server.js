const WebSocket = require('ws');
const http = require('http');
const { v4: uuidv4 } = require('uuid');

const PORT = process.env.PORT || 3000;
const GRACE_MS = 45000; // keep a dropped player's seat this long for reconnect

const rooms = new Map();

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    status: 'ok',
    rooms: rooms.size,
    players: Array.from(rooms.values()).reduce((n, r) => n + r.players.size, 0),
  }));
});

const wss = new WebSocket.Server({ server });
server.listen(PORT, () => console.log(`PartyHub server on port ${PORT}`));

function generateRoomCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code = '';
  for (let i = 0; i < 4; i++) code += chars[Math.floor(Math.random() * chars.length)];
  return rooms.has(code) ? generateRoomCode() : code;
}

function send(ws, msg) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(msg));
}

function broadcastToRoom(room, msg, excludeId = null) {
  for (const [id, p] of room.players) {
    if (id !== excludeId && p.connected) send(p.ws, msg);
  }
}

function playerList(room) {
  return Array.from(room.players.values()).map(p => ({
    id: p.id, nickname: p.nickname, isHost: p.isHost,
  }));
}

function findRoomByPlayerId(playerId) {
  for (const room of rooms.values()) if (room.players.has(playerId)) return room;
  return null;
}

wss.on('connection', (ws) => {
  ws.playerId = uuidv4();
  console.log(`[Connect] ${ws.playerId}`);
  send(ws, { type: 'connected', playerId: ws.playerId });

  ws.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); } catch (e) { return; }
    handleMessage(ws, msg);
  });

  ws.on('close', () => handleClose(ws));
  ws.on('error', (err) => console.error(`[Error] ${ws.playerId}:`, err.message));
});

function handleClose(ws) {
  const room = findRoomByPlayerId(ws.playerId);
  if (!room) return;
  const player = room.players.get(ws.playerId);
  if (!player) return;

  // Mark disconnected but keep the seat for GRACE_MS for reconnection.
  player.connected = false;
  console.log(`[Room ${room.code}] ${player.nickname} dropped — holding seat ${GRACE_MS}ms`);

  player.graceTimer = setTimeout(() => {
    // If still not reconnected, remove for real.
    const r = findRoomByPlayerId(ws.playerId);
    if (!r) return;
    const p = r.players.get(ws.playerId);
    if (!p || p.connected) return; // reconnected in time

    r.players.delete(ws.playerId);
    console.log(`[Room ${r.code}] ${p.nickname} removed after grace period`);

    broadcastToRoom(r, {
      type: 'playerLeft',
      playerId: ws.playerId,
      nickname: p.nickname,
      players: playerList(r),
    });

    if (p.isHost && r.players.size > 0) {
      const newHost = Array.from(r.players.values()).find(x => x.connected) ||
                      r.players.values().next().value;
      newHost.isHost = true;
      r.hostId = newHost.id;
      broadcastToRoom(r, {
        type: 'hostChanged',
        newHostId: newHost.id,
        newHostNickname: newHost.nickname,
        players: playerList(r),
      });
    }
    if (r.players.size === 0) {
      rooms.delete(r.code);
      console.log(`[Room ${r.code}] empty, removed`);
    }
  }, GRACE_MS);
}

function handleMessage(ws, msg) {
  switch (msg.type) {

    case 'createRoom': {
      const code = generateRoomCode();
      const player = {
        id: ws.playerId, nickname: msg.nickname, ws, isHost: true,
        connected: true, graceTimer: null,
      };
      rooms.set(code, { code, hostId: ws.playerId, players: new Map([[ws.playerId, player]]) });
      console.log(`[Room ${code}] created by ${msg.nickname}`);
      send(ws, { type: 'roomCreated', roomCode: code, playerId: ws.playerId, players: playerList(rooms.get(code)) });
      break;
    }

    case 'joinRoom': {
      const code = (msg.roomCode || '').toUpperCase().trim();
      const room = rooms.get(code);
      if (!room) {
        send(ws, { type: 'error', code: 'ROOM_NOT_FOUND', message: 'Room not found.' });
        return;
      }

      // Reconnection: if a disconnected player with the same nickname is holding
      // a seat, reattach to it instead of creating a new player.
      let reattached = false;
      for (const p of room.players.values()) {
        if (!p.connected && p.nickname === msg.nickname) {
          if (p.graceTimer) { clearTimeout(p.graceTimer); p.graceTimer = null; }
          // move seat to the new socket's playerId
          room.players.delete(p.id);
          p.id = ws.playerId;
          p.ws = ws;
          p.connected = true;
          room.players.set(ws.playerId, p);
          if (p.isHost) room.hostId = ws.playerId;
          reattached = true;
          console.log(`[Room ${code}] ${msg.nickname} reconnected`);
          break;
        }
      }

      if (!reattached) {
        const player = {
          id: ws.playerId, nickname: msg.nickname, ws, isHost: false,
          connected: true, graceTimer: null,
        };
        room.players.set(ws.playerId, player);
        console.log(`[Room ${code}] ${msg.nickname} joined (${room.players.size})`);
      }

      send(ws, {
        type: 'roomJoined', roomCode: code, playerId: ws.playerId,
        hostId: room.hostId, players: playerList(room),
      });
      broadcastToRoom(room, {
        type: 'playerJoined', playerId: ws.playerId,
        nickname: msg.nickname, players: playerList(room),
      }, ws.playerId);
      break;
    }

    case 'startGame': {
      const room = findRoomByPlayerId(ws.playerId);
      if (!room) return;
      const player = room.players.get(ws.playerId);
      if (!player || !player.isHost) {
        send(ws, { type: 'error', code: 'NOT_HOST', message: 'Only the host can start.' });
        return;
      }
      console.log(`[Room ${room.code}] game started`);
      for (const p of room.players.values()) {
        if (p.connected) send(p.ws, { type: 'gameStarted', players: playerList(room) });
      }
      break;
    }

    case 'gameMessage': {
      const room = findRoomByPlayerId(ws.playerId);
      if (!room) return;
      const envelope = { type: 'gameMessage', gameId: msg.gameId, kind: msg.kind, payload: msg.payload };
      if (Array.isArray(msg.to)) {
        for (const targetId of msg.to) {
          const t = room.players.get(targetId);
          if (t && t.connected) send(t.ws, envelope);
        }
      } else {
        broadcastToRoom(room, envelope, ws.playerId);
      }
      break;
    }

    case 'leaveRoom': {
      const room = findRoomByPlayerId(ws.playerId);
      if (!room) break;
      const p = room.players.get(ws.playerId);
      if (!p) break;
      if (p.graceTimer) { clearTimeout(p.graceTimer); p.graceTimer = null; }
      room.players.delete(ws.playerId);
      console.log(`[Room ${room.code}] ${p.nickname} left explicitly`);
      broadcastToRoom(room, {
        type: 'playerLeft', playerId: ws.playerId,
        nickname: p.nickname, players: playerList(room),
      });
      if (p.isHost && room.players.size > 0) {
        const newHost = Array.from(room.players.values()).find(x => x.connected) ||
                        room.players.values().next().value;
        newHost.isHost = true; room.hostId = newHost.id;
        broadcastToRoom(room, {
          type: 'hostChanged', newHostId: newHost.id,
          newHostNickname: newHost.nickname, players: playerList(room),
        });
      }
      if (room.players.size === 0) { rooms.delete(room.code); }
      break;
    }

    case 'ping':
      send(ws, { type: 'pong' });
      break;
  }
}
