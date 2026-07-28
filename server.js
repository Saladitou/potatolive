const express = require('express');
const app = express();
const http = require('http').createServer(app);
const io = require('socket.io')(http);

app.use(express.static(__dirname));
let streams = {};

io.on('connection', (socket) => {
  socket.emit('update-stream-list', Object.values(streams));

  // Iniciar directo y crear sala
  socket.on('start-stream', (data) => {
    streams[socket.id] = {
      id: socket.id,
      username: data.username,
      category: data.category,
      format: data.format,
      startTime: Date.now(),
      viewers: 0
    };
    socket.join(socket.id); // El streamer se une a su propia sala
    io.emit('update-stream-list', Object.values(streams));
  });

  // Espectador se une al directo y al chat
  socket.on('join-stream', (streamerId) => {
    socket.join(streamerId);
  });

  // SISTEMA DE CHAT
  socket.on('chat-message', (data) => {
    io.to(data.streamerId).emit('chat-message', { username: data.username, text: data.text });
  });

  socket.on('pin-message', (data) => {
    io.to(data.streamerId).emit('pin-message', { username: data.username, text: data.text });
  });

  socket.on('set-slow-mode', (data) => {
    io.to(socket.id).emit('slow-mode-changed', data.seconds);
  });

  // Métricas y WebRTC
  socket.on('update-viewers', (count) => {
    if (streams[socket.id]) {
      streams[socket.id].viewers = count;
      io.emit('update-stream-list', Object.values(streams));
    }
  });

  socket.on('camera-status', (data) => {
    socket.broadcast.emit('peer-camera-status', { streamerId: socket.id, enabled: data.enabled });
  });

  socket.on('signal', (data) => {
    io.to(data.to).emit('signal', { from: socket.id, signal: data.signal });
  });

  socket.on('disconnect', () => {
    if (streams[socket.id]) {
      delete streams[socket.id];
      io.emit('update-stream-list', Object.values(streams));
    }
  });
});

http.listen(process.env.PORT || 3000, () => console.log('Servidor V4 activo'));
