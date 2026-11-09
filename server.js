const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const crypto = require("crypto");

const app = express();
const server = http.createServer(app);
const allowedOrigins = (process.env.ALLOWED_ORIGIN || "").split(",").map(v => v.trim()).filter(Boolean);
const io = new Server(server, {
  cors: { origin: allowedOrigins.length ? allowedOrigins : true, methods: ["GET", "POST"] },
  pingInterval: 25000,
  pingTimeout: 20000,
  maxHttpBufferSize: 1e5
});

app.get("/health", (_req, res) => res.json({ ok: true, game: "BATTLESHOT", mode: "challenge-arena", players: io.engine.clientsCount }));
app.get("/", (_req, res) => res.json({ ok: true, service: "BATTLESHOT realtime server" }));

const rooms = new Map();
const BEACONS = [[-24,-18],[-12,16],[0,-27],[17,-13],[27,14],[-30,5],[8,28],[0,4]];
const cleanName = value => String(value || "Player").replace(/[<>\u0000-\u001f]/g, "").trim().slice(0, 16) || "Player";
const cleanRoom = value => String(value || "").toUpperCase().replace(/[^A-Z0-9]/g, "").slice(0, 6);
const newRoomCode = () => crypto.randomBytes(3).toString("hex").toUpperCase();
const clamp = (n, min, max) => Math.max(min, Math.min(max, Number.isFinite(Number(n)) ? Number(n) : 0));

function publicRoom(room) {
  return {
    code: room.code,
    players: [...room.players.values()].map(p => ({ id: p.id, name: p.name, x: p.x, z: p.z, rotation: p.rotation, color: p.color, score: p.score })),
    beaconCount: BEACONS.length,
    collected: [...room.collected]
  };
}

function leaveRoom(socket) {
  const code = socket.data.room;
  if (!code) return;
  const room = rooms.get(code);
  if (room) {
    room.players.delete(socket.id);
    socket.to(code).emit("player:left", socket.id);
    if (room.players.size === 0) rooms.delete(code);
    else io.to(code).emit("room:update", publicRoom(room));
  }
  socket.leave(code);
  socket.data.room = null;
}

io.on("connection", socket => {
  socket.data.lastMove = 0;
  socket.data.lastPositionAt = Date.now();

  socket.on("room:create", ({ name } = {}, reply = () => {}) => {
    leaveRoom(socket);
    let code = newRoomCode();
    while (rooms.has(code)) code = newRoomCode();
    const room = { code, players: new Map(), collected: new Set() };
    rooms.set(code, room);
    join(socket, room, name);
    reply({ ok: true, code, state: publicRoom(room) });
  });

  socket.on("room:join", ({ name, code } = {}, reply = () => {}) => {
    const roomCode = cleanRoom(code);
    const room = rooms.get(roomCode);
    if (!room) return reply({ ok: false, error: "לא מצאנו חדר כזה" });
    if (room.players.size >= 12) return reply({ ok: false, error: "החדר מלא" });
    leaveRoom(socket);
    join(socket, room, name);
    reply({ ok: true, code: room.code, state: publicRoom(room) });
  });

  socket.on("player:move", data => {
    const now = Date.now();
    if (now - socket.data.lastMove < 45) return;
    socket.data.lastMove = now;
    const room = rooms.get(socket.data.room);
    const player = room && room.players.get(socket.id);
    if (!player || !data) return;

    const nextX = clamp(data.x, -42, 42);
    const nextZ = clamp(data.z, -42, 42);
    const elapsed = Math.max(0.05, Math.min(0.5, (now - socket.data.lastPositionAt) / 1000));
    const maxDistance = 8.8 * elapsed + 1.0;
    if (Math.hypot(nextX - player.x, nextZ - player.z) > maxDistance) return;

    player.x = nextX;
    player.z = nextZ;
    player.rotation = clamp(data.rotation, -Math.PI * 4, Math.PI * 4);
    socket.data.lastPositionAt = now;
    socket.to(room.code).volatile.emit("player:moved", { id: socket.id, x: player.x, z: player.z, rotation: player.rotation });
  });

  socket.on("beacon:collect", ({ id } = {}, reply = () => {}) => {
    const room = rooms.get(socket.data.room);
    const player = room && room.players.get(socket.id);
    const index = Number(id);
    if (!room || !player || !Number.isInteger(index) || index < 0 || index >= BEACONS.length) {
      if (typeof reply === "function") reply({ ok: false });
      return;
    }
    const key = String(index);
    if (room.collected.has(key)) {
      if (typeof reply === "function") reply({ ok: false, reason: "already-collected" });
      return;
    }
    const [bx, bz] = BEACONS[index];
    if (Math.hypot(player.x - bx, player.z - bz) > 2.8) {
      if (typeof reply === "function") reply({ ok: false, reason: "too-far" });
      return;
    }

    room.collected.add(key);
    player.score += 1;
    io.to(room.code).emit("beacon:collected", { id: index, playerId: socket.id, score: player.score });
    io.to(room.code).emit("room:update", publicRoom(room));
    if (room.collected.size === BEACONS.length) io.to(room.code).emit("round:complete");
    if (typeof reply === "function") reply({ ok: true });
  });

  socket.on("disconnect", () => leaveRoom(socket));
});

function join(socket, room, name) {
  const colors = ["#c6a36a", "#5e9b8b", "#788db8", "#b47d78", "#a4a16b", "#8e83b5"];
  const player = {
    id: socket.id,
    name: cleanName(name),
    x: (Math.random() - 0.5) * 8,
    z: (Math.random() - 0.5) * 8,
    rotation: 0,
    color: colors[room.players.size % colors.length],
    score: 0
  };
  room.players.set(socket.id, player);
  socket.data.room = room.code;
  socket.data.lastPositionAt = Date.now();
  socket.join(room.code);
  socket.emit("room:state", publicRoom(room));
  socket.to(room.code).emit("player:joined", player);
  io.to(room.code).emit("room:update", publicRoom(room));
}

const PORT = process.env.PORT || 3001;
server.listen(PORT, "0.0.0.0", () => console.log("BATTLESHOT realtime server listening on", PORT));
