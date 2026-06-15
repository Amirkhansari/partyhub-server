const WebSocket = require('ws');
const http = require('http');
const { v4: uuidv4 } = require('uuid');
const Anthropic = require('@anthropic-ai/sdk');

const PORT = process.env.PORT || 3000;
const GRACE_MS = 45000; // keep a dropped player's seat this long for reconnect

const rooms = new Map();

// ── AI rate limiting (sliding window) ────────────────────────────
const AI_RATE_WINDOW_MS = 60 * 60 * 1000; // 1 hour
const AI_RATE_MAX = 100;
const aiRequestTimestamps = [];

function aiRateLimitOk() {
  const now = Date.now();
  while (aiRequestTimestamps.length && aiRequestTimestamps[0] < now - AI_RATE_WINDOW_MS)
    aiRequestTimestamps.shift();
  if (aiRequestTimestamps.length >= AI_RATE_MAX) return false;
  aiRequestTimestamps.push(now);
  return true;
}

// ── Claude client ────────────────────────────────────────────────
const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

// ── System prompts per game type ─────────────────────────────────
function buildSystemPrompt(gameType, theme, language, count, mode) {
  const lang = language === 'fa' ? 'Persian (Farsi)' : 'English';
  const langNote = language === 'fa'
    ? ' Use natural everyday Persian that Iranian audiences understand. No transliterated English words.'
    : '';

  const prompts = {
    hot_seat: `Generate ${count} fun party "hot seat" questions about "${theme}" in ${lang}.${langNote} Return ONLY a JSON array of strings. Example: ["question 1","question 2"]`,

    most_likely_to: `Generate ${count} "Most likely to..." prompts about "${theme}" in ${lang}.${langNote} Each prompt should start with the equivalent of "Most likely to" in the target language. Return ONLY a JSON array of strings.`,

    never_have_i_ever: `Generate ${count} "Never have I ever..." statements about "${theme}" in ${lang}.${langNote} Each should start with the equivalent of "Never have I ever" in the target language. Return ONLY a JSON array of strings.`,

    dare_wheel: `Generate ${count} ${mode || 'mild'} dares about "${theme}" in ${lang}.${langNote} Mild = funny and harmless. Spicy = bold/embarrassing but not dangerous. Group = involves everyone playing. Return ONLY a JSON array of strings.`,

    would_you_rather: `Generate ${count} "Would You Rather" dilemmas about "${theme}" in ${lang}.${langNote} Return ONLY a JSON array of objects: [{"optionA":"...","optionB":"..."}]`,

    pass_the_bomb: `Generate ${count} "Name a..." category prompts about "${theme}" in ${lang}.${langNote} Each should be a category players can rapidly name items from (e.g. "Name a type of..."). Return ONLY a JSON array of strings.`,

    category_rush: `Generate ${count} categories related to "${theme}" in ${lang} with a matching emoji.${langNote} Return ONLY a JSON array of objects: [{"category":"...","emoji":"🍕"}]`,

    trivia_showdown: `Generate ${count} trivia questions about "${theme}" in ${lang}.${langNote} Each question must have exactly 4 options with one correct answer. Return ONLY a JSON array: [{"question":"...","options":["A","B","C","D"],"correctIndex":0,"category":"${theme}"}]`,

    psych: `Generate ${count} trivia questions about "${theme}" in ${lang} where the real answer is surprising or little-known.${langNote} Return ONLY a JSON array: [{"question":"...","answer":"..."}]`,

    charades_timer: `Generate ${count} fun things to act out (charades prompts) related to "${theme}" in ${lang}.${langNote} Mix of actions, characters, movies, and objects. Return ONLY a JSON array of strings.`,

    forehead_guess: `Generate ${count} words/names to guess in a forehead guessing game, related to "${theme}" in ${lang}.${langNote} Mix of famous people, objects, animals, and concepts. Return ONLY a JSON array of strings.`,

    draw_and_guess: `Generate ${count} things to draw related to "${theme}" in ${lang}.${langNote} Each should be something drawable (concrete nouns, characters, scenes). Return ONLY a JSON array of strings.`,

    secret_spy: `Generate ${count} locations related to "${theme}" in ${lang}, each with 6-8 roles that exist at that location.${langNote} Return ONLY a JSON array: [{"name":"...","icon":"building","roles":["role1","role2","role3","role4","role5","role6"],"category":"${theme}"}]`,

    undercover_word: `Generate ${count} word pairs related to "${theme}" in ${lang}.${langNote} Each pair should be two similar but distinct things (hard to tell apart when describing). Return ONLY a JSON array: [{"civilian":"...","undercover":"..."}]`,
  };

  return prompts[gameType] || null;
}

function maxTokensForGame(gameType) {
  const heavy = ['secret_spy', 'trivia_showdown'];
  if (heavy.includes(gameType)) return 4000;
  return 2000;
}

async function handleAIGenerate(req, res) {
  // CORS
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type');
  if (req.method === 'OPTIONS') { res.writeHead(204); res.end(); return; }
  if (req.method !== 'POST') {
    res.writeHead(405, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ success: false, error: 'Method not allowed' }));
    return;
  }

  let body = '';
  for await (const chunk of req) body += chunk;

  let parsed;
  try { parsed = JSON.parse(body); } catch {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ success: false, error: 'Invalid JSON' }));
    return;
  }

  const { gameType, theme, language, count, mode } = parsed;
  if (!gameType || !theme || !language || !count) {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ success: false, error: 'Missing required fields: gameType, theme, language, count' }));
    return;
  }

  if (!aiRateLimitOk()) {
    res.writeHead(429, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ success: false, error: 'Rate limit exceeded. Try again later.' }));
    return;
  }

  const systemPrompt = buildSystemPrompt(gameType, theme, language, count, mode);
  if (!systemPrompt) {
    res.writeHead(400, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ success: false, error: `Unknown game type: ${gameType}` }));
    return;
  }

  try {
    console.log(`[AI] Generating ${count} items for ${gameType} theme="${theme}" lang=${language}`);
    const response = await anthropic.messages.create({
      model: 'claude-haiku-4-5-20251001',
      max_tokens: maxTokensForGame(gameType),
      messages: [{ role: 'user', content: systemPrompt }],
    });

    const text = response.content[0].text.trim();
    // Extract JSON array from response (handle markdown code blocks)
    const jsonMatch = text.match(/\[[\s\S]*\]/);
    if (!jsonMatch) {
      console.error('[AI] No JSON array found in response:', text.substring(0, 200));
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ success: false, error: 'AI returned invalid format' }));
      return;
    }

    const content = JSON.parse(jsonMatch[0]);
    console.log(`[AI] Success: ${content.length} items generated`);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ success: true, content }));
  } catch (err) {
    console.error('[AI] Error:', err.message, err.status || '', err.error?.type || '');
    res.writeHead(500, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ success: false, error: 'AI generation failed', detail: err.message }));
  }
}

// ── HTTP server with routing ─────────────────────────────────────
const server = http.createServer(async (req, res) => {
  if (req.url === '/api/ai/generate') {
    await handleAIGenerate(req, res);
    return;
  }
  // Default health/status endpoint
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
    id: p.id, nickname: p.nickname, isHost: p.isHost, avatar: p.avatar || '',
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

  ws.on('message', (data, isBinary) => {
    if (isBinary) {
      const room = findRoomByPlayerId(ws.playerId);
      if (!room) {
        console.log(`[Voice] binary from ${ws.playerId} — NO ROOM FOUND`);
        return;
      }
      const header = Buffer.from(ws.playerId, 'utf8');
      const tagged = Buffer.concat([header, Buffer.from(data)]);
      let sent = 0;
      for (const [id, p] of room.players) {
        if (id !== ws.playerId && p.connected && p.ws.readyState === WebSocket.OPEN) {
          p.ws.send(tagged, { binary: true });
          sent++;
        }
      }
      if (!ws._voiceLogged) {
        console.log(`[Voice] first audio from ${ws.playerId} in room ${room.code}, relayed to ${sent} peer(s)`);
        ws._voiceLogged = true;
      }
      return;
    }
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
        id: ws.playerId, nickname: msg.nickname, avatar: msg.avatar || '', ws, isHost: true,
        connected: true, graceTimer: null,
      };
      rooms.set(code, { code, hostId: ws.playerId, locked: false, kickedNicknames: new Set(), players: new Map([[ws.playerId, player]]) });
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
          if (msg.avatar) p.avatar = msg.avatar;
          room.players.set(ws.playerId, p);
          if (p.isHost) room.hostId = ws.playerId;
          reattached = true;
          console.log(`[Room ${code}] ${msg.nickname} reconnected`);
          break;
        }
      }

      if (!reattached) {
        if (room.locked) {
          send(ws, { type: 'error', code: 'ROOM_LOCKED', message: 'This room is locked.' });
          return;
        }
        if (room.kickedNicknames && room.kickedNicknames.has(msg.nickname)) {
          send(ws, { type: 'error', code: 'KICKED', message: 'You have been kicked from this room.' });
          return;
        }
        const player = {
          id: ws.playerId, nickname: msg.nickname, avatar: msg.avatar || '', ws, isHost: false,
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

    case 'kickPlayer': {
      const room = findRoomByPlayerId(ws.playerId);
      if (!room) break;
      const me = room.players.get(ws.playerId);
      if (!me || !me.isHost) {
        send(ws, { type: 'error', code: 'NOT_HOST', message: 'Only the host can kick.' });
        break;
      }
      const target = room.players.get(msg.targetId);
      if (!target) break;
      if (target.graceTimer) { clearTimeout(target.graceTimer); target.graceTimer = null; }
      room.players.delete(msg.targetId);
      if (!room.kickedNicknames) room.kickedNicknames = new Set();
      room.kickedNicknames.add(target.nickname);
      console.log(`[Room ${room.code}] ${target.nickname} kicked by host`);
      send(target.ws, { type: 'kicked', message: 'You have been kicked from the room.' });
      broadcastToRoom(room, {
        type: 'playerKicked', playerId: msg.targetId,
        nickname: target.nickname, players: playerList(room),
      });
      break;
    }

    case 'lockRoom': {
      const room = findRoomByPlayerId(ws.playerId);
      if (!room) break;
      const me = room.players.get(ws.playerId);
      if (!me || !me.isHost) break;
      room.locked = true;
      console.log(`[Room ${room.code}] locked by host`);
      broadcastToRoom(room, { type: 'roomLocked', locked: true });
      break;
    }

    case 'unlockRoom': {
      const room = findRoomByPlayerId(ws.playerId);
      if (!room) break;
      const me = room.players.get(ws.playerId);
      if (!me || !me.isHost) break;
      room.locked = false;
      console.log(`[Room ${room.code}] unlocked by host`);
      broadcastToRoom(room, { type: 'roomLocked', locked: false });
      break;
    }

    case 'chatMessage': {
      const room = findRoomByPlayerId(ws.playerId);
      if (!room) break;
      const sender = room.players.get(ws.playerId);
      if (!sender) break;
      const chatMsg = {
        type: 'chatMessage',
        senderId: ws.playerId,
        senderName: sender.nickname,
        text: msg.text,
        timestamp: Date.now(),
      };
      // Send to ALL players including sender (confirmation)
      for (const [, p] of room.players) {
        if (p.connected) send(p.ws, chatMsg);
      }
      break;
    }

    case 'voiceState': {
      const room = findRoomByPlayerId(ws.playerId);
      if (!room) break;
      broadcastToRoom(room, {
        type: 'voiceState',
        playerId: ws.playerId,
        isMuted: msg.isMuted,
        inVoice: msg.inVoice,
      });
      break;
    }

    case 'hostMutePlayer': {
      const room = findRoomByPlayerId(ws.playerId);
      if (!room) break;
      const me = room.players.get(ws.playerId);
      if (!me || !me.isHost) break;
      const target = room.players.get(msg.targetId);
      if (!target) break;
      send(target.ws, { type: 'hostMuted' });
      broadcastToRoom(room, {
        type: 'voiceState',
        playerId: msg.targetId,
        isMuted: true,
        inVoice: true,
      });
      break;
    }

    case 'ping':
      send(ws, { type: 'pong' });
      break;
  }
}
