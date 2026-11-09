const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const crypto = require("crypto");

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: process.env.ALLOWED_ORIGIN ? process.env.ALLOWED_ORIGIN.split(",") : true, methods: ["GET","POST"] },
  pingInterval: 25000,
  pingTimeout: 20000,
  maxHttpBufferSize: 1e5
});

app.get("/health", (_req,res) => res.json({ ok:true, game:"BATTLESHOT", mode:"challenge-arena", players:io.engine.clientsCount }));
app.get("/", (_req,res) => res.json({ ok:true, service:"BATTLESHOT realtime server" }));

const rooms = new Map();
const cleanName = value => String(value || "Player").replace(/[<>\u0000-\u001f]/g,"").trim().slice(0,16) || "Player";
const cleanRoom = value => String(value || "").toUpperCase().replace(/[^A-Z0-9]/g,"").slice(0,6);
const newRoomCode = () => crypto.randomBytes(3).toString("hex").toUpperCase();
const clamp = (n,min,max) => Math.max(min,Math.min(max,Number.isFinite(Number(n)) ? Number(n) : 0));

function publicRoom(room) {
  return { code:room.code, players:[...room.players.values()].map(p => ({id:p.id,name:p.name,x:p.x,z:p.z,rotation:p.rotation,color:p.color,score:p.score})), beaconCount:room.beaconCount };
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
  socket.on("room:create", ({name} = {}, reply = () => {}) => {
    leaveRoom(socket);
    let code = newRoomCode();
    while (rooms.has(code)) code = newRoomCode();
    const room = {code,players:new Map(),beaconCount:8};
    rooms.set(code,room);
    join(socket,room,name);
    reply({ok:true,code,state:publicRoom(room)});
  });
  socket.on("room:join", ({name,code} = {}, reply = () => {}) => {
    const roomCode = cleanRoom(code);
    const room = rooms.get(roomCode);
    if (!room) return reply({ok:false,error:"לא מצאנו חדר כזה"});
    if (room.players.size >= 12) return reply({ok:false,error:"החדר מלא"});
    leaveRoom(socket);
    join(socket,room,name);
    reply({ok:true,code:room.code,state:publicRoom(room)});
  });
  socket.on("player:move", data => {
    const now = Date.now();
    if (now - socket.data.lastMove < 45) return;
    socket.data.lastMove = now;
    const room = rooms.get(socket.data.room);
    const player = room && room.players.get(socket.id);
    if (!player || !data) return;
    player.x = clamp(data.x,-42,42);
    player.z = clamp(data.z,-42,42);
    player.rotation = clamp(data.rotation,-Math.PI*4,Math.PI*4);
    socket.to(room.code).volatile.emit("player:moved", {id:socket.id,x:player.x,z:player.z,rotation:player.rotation});
  });
  socket.on("beacon:collect", ({id} = {}, reply = () => {}) => {
    const room = rooms.get(socket.data.room);
    const player = room && room.players.get(socket.id);
    const index = Number(id);
    if (!room || !player || !Number.isInteger(index) || index < 0 || index >= room.beaconCount) return;
    const key = String(index);
    if (room.collected?.has(key)) return;
    room.collected ||= new Set();
    room.collected.add(key);
    player.score += 1;
    io.to(room.code).emit("beacon:collected",{id:index,playerId:socket.id,score:player.score});
    io.to(room.code).emit("room:update",publicRoom(room));
    if (room.collected.size === room.beaconCount) io.to(room.code).emit("round:complete");
    if (typeof reply === "function") reply({ok:true});
  });
  socket.on("disconnect", () => leaveRoom(socket));
});

function join(socket,room,name) {
  const colors = ["#c6a36a","#5e9b8b","#788db8","#b47d78","#a4a16b","#8e83b5"];
  const player = {id:socket.id,name:cleanName(name),x:(Math.random()-.5)*8,z:(Math.random()-.5)*8,rotation:0,color:colors[room.players.size % colors.length],score:0};
  room.players.set(socket.id,player);
  socket.data.room = room.code;
  socket.join(room.code);
  socket.emit("room:state",publicRoom(room));
  socket.to(room.code).emit("player:joined",player);
  io.to(room.code).emit("room:update",publicRoom(room));
}

const PORT = process.env.PORT || 3001;
server.listen(PORT,"0.0.0.0",() => console.log("BATTLESHOT realtime server listening on",PORT));
