const WebSocket = require('ws');
const http = require('http');
const { v4: uuidv4 } = require('uuid');

const PORT = process.env.PORT || 3000;

// ─────────────────────────────────────────────────────────────────────────────
// In-memory state
// ─────────────────────────────────────────────────────────────────────────────

const rooms = new Map();

// ─────────────────────────────────────────────────────────────────────────────
// HTTP server — handles both health checks AND WebSocket upgrades on one port
// ─────────────────────────────────────────────────────────────────────────────

const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({
    status: 'ok',
    rooms: rooms.size,
    players: Array.from(rooms.values()).reduce((n, r) => n + r.players.size, 0),
  }));
});

const wss = new WebSocket.Server({ server });

server.listen(PORT, () => {
  console.log(`PartyHub server running on port ${PORT}`);
});

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

function generateRoomCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code = '';
  for (let i = 0; i < 4; i++) {
    code += chars[Math.floor(Math.random() * chars.length)];
  }
  return rooms.has(code) ? generateRoomCode() : code;
}

function send(ws, msg) {
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(msg));
  }
}

function broadcastToRoom(room, msg, excludePlayerId = null) {
  for (const [id, player] of room.players) {
    if (id !== excludePlayerId) {
      send(player.ws, msg);
    }
  }
}

function getRoomPlayerList(room) {
  return Array.from(room.players.values()).map(p => ({
    id: p.id,
    nickname: p.nickname,
    isHost: p.isHost,
  }));
}

function removePlayerFromRoom(playerId) {
  for (const [code, room] of rooms) {
    if (room.players.has(playerId)) {
      const player = room.players.get(playerId);
      room.players.delete(playerId);

      console.log(`[Room ${code}] ${player.nickname} left. ${room.players.size} remaining.`);

      broadcastToRoom(room, {
        type: 'playerLeft',
        playerId,
        nickname: player.nickname,
        players: getRoomPlayerList(room),
      });

      if (player.isHost && room.players.size > 0) {
        const newHost = room.players.values().next().value;
        newHost.isHost = true;
        room.hostId = newHost.id;
        broadcastToRoom(room, {
          type: 'hostChanged',
          newHostId: newHost.id,
          newHostNickname: newHost.nickname,
          players: getRoomPlayerList(room),
        });
        console.log(`[Room ${code}] New host: ${newHost.nickname}`);
      }

      if (room.players.size === 0) {
        rooms.delete(code);
        console.log(`[Room ${code}] Empty, removed.`);
      }

      break;
    }
  }
}

function findRoomByPlayerId(playerId) {
  for (const room of rooms.values()) {
    if (room.players.has(playerId)) return room;
  }
  return null;
}

// ─────────────────────────────────────────────────────────────────────────────
// WebSocket connections
// ─────────────────────────────────────────────────────────────────────────────

wss.on('connection', (ws) => {
  const playerId = uuidv4();
  ws.playerId = playerId;
  console.log(`[Connect] ${playerId}`);

  send(ws, { type: 'connected', playerId });

  ws.on('message', (data) => {
    let msg;
    try { msg = JSON.parse(data.toString()); }
    catch (e) { return; }
    handleMessage(ws, msg);
  });

  ws.on('close', () => {
    console.log(`[Disconnect] ${playerId}`);
    removePlayerFromRoom(playerId);
  });

  ws.on('error', (err) => {
    console.error(`[Error] ${playerId}:`, err.message);
  });
});

// ─────────────────────────────────────────────────────────────────────────────
// Message handler
// ─────────────────────────────────────────────────────────────────────────────

function handleMessage(ws, msg) {
  const { type } = msg;

  switch (type) {

    case 'createRoom': {
      const { nickname } = msg;
      const code = generateRoomCode();
      const player = { id: ws.playerId, nickname, ws, isHost: true };
      const room = {
        code,
        hostId: ws.playerId,
        players: new Map([[ws.playerId, player]]),
      };
      rooms.set(code, room);
      console.log(`[Room ${code}] Created by ${nickname}`);
      send(ws, {
        type: 'roomCreated',
        roomCode: code,
        playerId: ws.playerId,
        players: getRoomPlayerList(room),
      });
      break;
    }

    case 'joinRoom': {
      const { nickname, roomCode } = msg;
      const code = roomCode.toUpperCase().trim();
      const room = rooms.get(code);
      if (!room) {
        send(ws, { type: 'error', code: 'ROOM_NOT_FOUND', message: 'Room not found. Check the code and try again.' });
        return;
      }
      const player = { id: ws.playerId, nickname, ws, isHost: false };
      room.players.set(ws.playerId, player);
      console.log(`[Room ${code}] ${nickname} joined. ${room.players.size} players.`);
      send(ws, {
        type: 'roomJoined',
        roomCode: code,
        playerId: ws.playerId,
        hostId: room.hostId,
        players: getRoomPlayerList(room),
      });
      broadcastToRoom(room, {
        type: 'playerJoined',
        playerId: ws.playerId,
        nickname,
        players: getRoomPlayerList(room),
      }, ws.playerId);
      break;
    }

    case 'startGame': {
      const room = findRoomByPlayerId(ws.playerId);
      if (!room) return;
      const player = room.players.get(ws.playerId);
      if (!player?.isHost) {
        send(ws, { type: 'error', code: 'NOT_HOST', message: 'Only the host can start the game.' });
        return;
      }
      console.log(`[Room ${room.code}] Game started.`);
      for (const p of room.players.values()) {
        send(p.ws, { type: 'gameStarted', players: getRoomPlayerList(room) });
      }
      break;
    }

    case 'gameMessage': {
      const { gameId, kind, payload, to } = msg;
      const room = findRoomByPlayerId(ws.playerId);
      if (!room) return;
      const envelope = { type: 'gameMessage', gameId, kind, payload };
      if (to && Array.isArray(to)) {
        for (const targetId of to) {
          const target = room.players.get(targetId);
          if (target) send(target.ws, envelope);
        }
      } else {
        broadcastToRoom(room, envelope, ws.playerId);
      }
      break;
    }

    case 'ping':
      send(ws, { type: 'pong' });
      break;

    default:
      console.warn(`[Unknown] type: ${type}`);
  }
}
