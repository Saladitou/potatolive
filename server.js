const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);

// AQUÍ ESTÁ LA LÍNEA: Le dice al servidor que entregue el index.html cuando entren
app.get('/', (req, res) => {
  res.sendFile(__dirname + '/index.html');
});

const io = new Server(server, {
  cors: { origin: "*", methods: ["GET", "POST"] }
});

let activeStreams = {};

io.on('connection', (socket) => {
  console.log(`📱 Usuario conectado: ${socket.id}`);

  // Envia los directos que hay activos en ese momento
  socket.emit('update-stream-list', Object.values(activeStreams));

  // Cuando alguien empieza a transmitir
  socket.on('start-stream', (data) => {
    activeStreams[socket.id] = {
      id: socket.id,
      username: data.username || 'PotatoStreamer'
    };
    console.log(`🔴 Directo activo de: ${data.username}`);
    io.emit('update-stream-list', Object.values(activeStreams));
  });

  // Conexión automática por detrás
  socket.on('signal', (data) => {
    io.to(data.to).emit('signal', {
      from: socket.id,
      signal: data.signal
    });
  });

  // Cuando se cierra el directo o el móvil se desconecta
  socket.on('disconnect', () => {
    if (activeStreams[socket.id]) {
      console.log(`⏹️ Directo terminado: ${activeStreams[socket.id].username}`);
      delete activeStreams[socket.id];
      io.emit('update-stream-list', Object.values(activeStreams));
    }
  });
});

const PORT = 3000;
server.listen(PORT, '0.0.0.0', () => {
  console.log(`\n🚀 ¡Servidor PotatoLive listo en el puerto ${PORT}!`);
});