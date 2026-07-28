const express = require('express');
const app = express();
const http = require('http').createServer(app);
const io = require('socket.io')(http);

app.use(express.static(__dirname));

let streams = {};
let newsList = [];
let supportTickets = {}; // Almacena los tickets de soporte
let maintenanceMode = false;
let adminSockets = new Set();

io.on('connection', (socket) => {
  // Enviar estado inicial
  socket.emit('initial-state', { 
    streams: Object.values(streams), 
    news: newsList, 
    maintenance: maintenanceMode 
  });

  // --- PANEL ADMIN (Oculto) ---
  socket.on('admin-login', (pass) => {
    if (pass === '5856') {
      adminSockets.add(socket.id);
      socket.emit('admin-auth-success', { news: newsList, tickets: Object.values(supportTickets) });
    }
  });

  socket.on('toggle-maintenance', (state) => {
    if (!adminSockets.has(socket.id)) return;
    maintenanceMode = state;
    io.emit('maintenance-update', maintenanceMode);
  });

  // --- GESTIÓN DE NOTICIAS ---
  socket.on('save-news', (newsItem) => {
    if (!adminSockets.has(socket.id)) return;
    if (newsItem.id) {
      const idx = newsList.findIndex(n => n.id === newsItem.id);
      if (idx !== -1) newsList[idx] = newsItem;
    } else {
      newsItem.id = Date.now().toString();
      newsList.unshift(newsItem);
    }
    io.emit('news-update', newsList);
    socket.emit('admin-news-list', newsList); // Refrescar lista admin
  });

  socket.on('delete-news', (id) => {
    if (!adminSockets.has(socket.id)) return;
    newsList = newsList.filter(n => n.id !== id);
    io.emit('news-update', newsList);
    socket.emit('admin-news-list', newsList);
  });

  // --- SISTEMA DE SOPORTE E IA ---
  socket.on('create-ticket', (data) => {
    const ticketId = 'tk-' + Math.random().toString(36).substr(2, 5);
    // IA genera título automático basado en el mensaje
    const generatedTitle = data.text.length > 25 ? data.text.substring(0, 25) + '...' : data.text;
    
    const ticket = {
      id: ticketId,
      userId: socket.id,
      title: generatedTitle,
      status: 'PENDIENTE',
      startTime: Date.now(),
      messages: [{ sender: 'user', text: data.text, time: new Date().toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'}) }]
    };
    supportTickets[ticketId] = ticket;
    
    socket.emit('ticket-created', ticket);
    
    // Avisar a los admins
    Array.from(adminSockets).forEach(adminId => {
      io.to(adminId).emit('admin-tickets-update', Object.values(supportTickets));
    });
  });

  socket.on('send-ticket-msg', (data) => {
    const ticket = supportTickets[data.ticketId];
    if (ticket) {
      ticket.messages.push({ sender: data.sender, text: data.text, time: new Date().toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'}) });
      if (data.sender === 'admin') ticket.status = 'RESPONDIDO';
      
      io.to(ticket.userId).emit('ticket-updated', ticket);
      Array.from(adminSockets).forEach(adminId => {
        io.to(adminId).emit('admin-tickets-update', Object.values(supportTickets));
      });
    }
  });

  // Simulación de respuesta IA (Acelerar 3h)
  socket.on('trigger-ai-response', (ticketId) => {
    const ticket = supportTickets[ticketId];
    if (ticket && ticket.status === 'PENDIENTE') {
      ticket.status = 'RESPONDIDO';
      ticket.messages.push({
        sender: 'ai',
        time: new Date().toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'}),
        text: "¡Hola! Soy el Asistente de IA de Soporte de PotatoLive. Como nuestro equipo de administración se encuentra temporalmente ausente, intervengo de manera automática para ayudarte de inmediato.\n\nDime, ¿en qué puedo colaborarte hoy? Te comparto una guía rápida:\n\n* **Verificación de canal:** Necesitas 50 subs y 1000 visualizaciones.\n* **Reportar un Bug:** Usa la opción del menú.\n\nCuéntame detalladamente tu caso."
      });
      io.to(ticket.userId).emit('ticket-updated', ticket);
      Array.from(adminSockets).forEach(adminId => {
        io.to(adminId).emit('admin-tickets-update', Object.values(supportTickets));
      });
    }
  });

  socket.on('get-my-tickets', () => {
    const myTickets = Object.values(supportTickets).filter(t => t.userId === socket.id);
    socket.emit('my-tickets-list', myTickets);
  });

  // --- STREAMING & WEBRTC ---
  socket.on('start-stream', (data) => {
    streams[socket.id] = {
      id: socket.id,
      username: data.username,
      title: data.title,
      category: data.category,
      format: data.format,
      fps: data.fps,
      quality: data.quality,
      startTime: Date.now(),
      viewers: 0,
      likes: 0,
      thumbnail: null
    };
    socket.join(socket.id);
    io.emit('update-stream-list', Object.values(streams));
  });

  socket.on('update-thumbnail', (base64) => {
    if (streams[socket.id]) {
      streams[socket.id].thumbnail = base64;
      io.emit('update-stream-list', Object.values(streams));
    }
  });

  socket.on('add-like', (streamerId) => {
    if (streams[streamerId]) {
      streams[streamerId].likes++;
      io.to(streamerId).emit('update-likes', streams[streamerId].likes);
      io.emit('update-stream-list', Object.values(streams));
    }
  });

  socket.on('update-viewers', (count) => {
    if (streams[socket.id]) {
      streams[socket.id].viewers = count;
      io.emit('update-stream-list', Object.values(streams));
    }
  });

  // Señales
  socket.on('camera-status', (data) => socket.broadcast.emit('peer-camera-status', { streamerId: socket.id, enabled: data.enabled }));
  socket.on('mic-status', (data) => socket.broadcast.emit('peer-mic-status', { streamerId: socket.id, enabled: data.enabled }));
  socket.on('set-slow-mode', (data) => io.to(socket.id).emit('slow-mode-changed', data.seconds));
  socket.on('signal', (data) => io.to(data.to).emit('signal', { from: socket.id, signal: data.signal }));

  socket.on('disconnect', () => {
    adminSockets.delete(socket.id);
    if (streams[socket.id]) {
      delete streams[socket.id];
      io.emit('update-stream-list', Object.values(streams));
    }
  });
});

http.listen(process.env.PORT || 3000, () => console.log('Servidor V6 activo'));
