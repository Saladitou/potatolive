const express = require('express');
const app = express();
const http = require('http').createServer(app);
const io = require('socket.io')(http);

app.use(express.static(__dirname));

let streams = {}; // Almacena toda la info de los directos

io.on('connection', (socket) => {
  // Al conectar, enviamos la lista actual
  socket.emit('update-stream-list', Object.values(streams));

  // Crear un nuevo directo con todos los datos
  socket.on('start-stream', (data) => {
    streams[socket.id] = {
      id: socket.id,
      username: data.username,
      category: data.category,
      format: data.format,
      startTime: Date.now(),
      viewers: 0
    };
    io.emit('update-stream-list', Object.values(streams));
  });

  // El streamer avisa de cuántos espectadores tiene conectados
  socket.on('update-viewers', (count) => {
    if (streams[socket.id]) {
      streams[socket.id].viewers = count;
      io.emit('update-stream-list', Object.values(streams));
    }
  });

  // Avisar a los espectadores si el streamer apaga la cámara
  socket.on('camera-status', (data) => {
    socket.broadcast.emit('peer-camera-status', { streamerId: socket.id, enabled: data.enabled });
  });

  // Señales de vídeo WebRTC
  socket.on('signal', (data) => {
    io.to(data.to).emit('signal', { from: socket.id, signal: data.signal });
  });

  // Cuando alguien cierra la página
  socket.on('disconnect', () => {
    if (streams[socket.id]) {
      delete streams[socket.id];
      io.emit('update-stream-list', Object.values(streams));
    }
  });
});

http.listen(process.env.PORT || 3000, () => {
  console.log('Servidor V3 activo');
});
