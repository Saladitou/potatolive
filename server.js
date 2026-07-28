const express = require('express');
const app = express();
const http = require('http').createServer(app);
const io = require('socket.io')(http);

app.use(express.static(__dirname));

let streams = {};
let newsList = [];
let supportTickets = {}; 
let maintenanceMode = false;
let adminSockets = new Set();

// Utilidad para repartir puntos
function resolvePrediction(streamId, winningOption) {
  const stream = streams[streamId];
  if (!stream || !stream.prediction) return;
  const pred = stream.prediction;
  const totalPool = pred.pool.yes + pred.pool.no;
  const winningPool = winningOption === 'yes' ? pred.pool.yes : pred.pool.no;
  if (winningPool > 0) {
    for (const [socketId, bet] of Object.entries(pred.bets)) {
      if (bet.option === winningOption) {
        const percentage = bet.amount / winningPool;
        const winnings = Math.floor(totalPool * percentage);
        if (stream.viewersData[socketId]) {
          stream.viewersData[socketId].points += winnings;
          io.to(socketId).emit('points-update', stream.viewersData[socketId].points);
        }
      }
    }
  }
  stream.prediction = null;
  io.to(streamId).emit('prediction-ended');
}

function getRanking(streamerId) {
  if (!streams[streamerId]) return [];
  return Object.values(streams[streamerId].viewersData).sort((a, b) => b.points - a.points).slice(0, 10);
}

io.on('connection', (socket) => {
  socket.emit('initial-state', { streams: Object.values(streams), news: newsList, maintenance: maintenanceMode });

  // --- PANEL ADMIN ---
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
    socket.emit('admin-news-list', newsList);
  });

  socket.on('delete-news', (id) => {
    if (!adminSockets.has(socket.id)) return;
    newsList = newsList.filter(n => n.id !== id);
    io.emit('news-update', newsList);
    socket.emit('admin-news-list', newsList);
  });

  // --- SOPORTE E IA ---
  socket.on('create-ticket', (data) => {
    const ticketId = 'tk-' + Math.random().toString(36).substr(2, 5);
    const generatedTitle = data.text.length > 25 ? data.text.substring(0, 25) + '...' : data.text;
    const ticket = {
      id: ticketId, userId: socket.id, title: generatedTitle, status: 'PENDIENTE', startTime: Date.now(),
      messages: [{ sender: 'user', text: data.text, time: new Date().toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'}) }]
    };
    supportTickets[ticketId] = ticket;
    socket.emit('ticket-created', ticket);
    Array.from(adminSockets).forEach(adminId => io.to(adminId).emit('admin-tickets-update', Object.values(supportTickets)));
  });

  socket.on('send-ticket-msg', (data) => {
    const ticket = supportTickets[data.ticketId];
    if (ticket) {
      ticket.messages.push({ sender: data.sender, text: data.text, time: new Date().toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'}) });
      if (data.sender === 'admin') ticket.status = 'RESPONDIDO';
      io.to(ticket.userId).emit('ticket-updated', ticket);
      Array.from(adminSockets).forEach(adminId => io.to(adminId).emit('admin-tickets-update', Object.values(supportTickets)));
    }
  });

  socket.on('trigger-ai-response', (ticketId) => {
    const ticket = supportTickets[ticketId];
    if (ticket && ticket.status === 'PENDIENTE') {
      ticket.status = 'RESPONDIDO';
      ticket.messages.push({
        sender: 'ai', time: new Date().toLocaleTimeString([], {hour: '2-digit', minute:'2-digit'}),
        text: "¡Hola! Soy el Asistente de IA de Soporte de PotatoLive. Como nuestro equipo de administración se encuentra temporalmente ausente, intervengo de manera automática para ayudarte de inmediato.\n\nDime, ¿en qué puedo colaborarte hoy? Te comparto una guía rápida:\n\n* **Verificación de canal:** Necesitas 50 subs y 1000 visualizaciones.\n* **Reportar un Bug:** Usa la opción del menú.\n\nCuéntame detalladamente tu caso."
      });
      io.to(ticket.userId).emit('ticket-updated', ticket);
      Array.from(adminSockets).forEach(adminId => io.to(adminId).emit('admin-tickets-update', Object.values(supportTickets)));
    }
  });

  socket.on('get-my-tickets', () => {
    socket.emit('my-tickets-list', Object.values(supportTickets).filter(t => t.userId === socket.id));
  });

  // --- STREAMING, PUNTOS Y CHAT ---
  socket.on('start-stream', (data) => {
    streams[socket.id] = {
      id: socket.id, username: data.username, title: data.title, category: data.category,
      format: data.format, fps: data.fps, startTime: Date.now(), viewers: 0, likes: 0,
      thumbnail: null, viewersData: {}, rewards: [], prediction: null
    };
    socket.join(socket.id);
    io.emit('update-stream-list', Object.values(streams));
  });

  socket.on('join-stream', (streamerId, username) => {
    socket.join(streamerId);
    if (streams[streamerId]) {
      if (!streams[streamerId].viewersData[socket.id]) {
        streams[streamerId].viewersData[socket.id] = { username, points: 0, lastChat: 0 };
      }
      socket.emit('points-update', streams[streamerId].viewersData[socket.id].points);
      if (streams[streamerId].prediction) socket.emit('prediction-started', streams[streamerId].prediction.data);
      if (streams[streamerId].rewards) socket.emit('rewards-update', streams[streamerId].rewards);
    }
  });

  socket.on('chat-message', (data) => {
    const stream = streams[data.streamerId];
    if (stream && stream.viewersData[socket.id]) {
      const now = Date.now();
      if (now - stream.viewersData[socket.id].lastChat > 10000) {
        stream.viewersData[socket.id].points += 5;
        stream.viewersData[socket.id].lastChat = now;
        socket.emit('points-update', stream.viewersData[socket.id].points);
        io.to(data.streamerId).emit('ranking-update', getRanking(data.streamerId));
      }
    }
    io.to(data.streamerId).emit('chat-message', { username: data.username, text: data.text });
  });

  socket.on('claim-watch-points', (streamerId) => {
    if (streams[streamerId] && streams[streamerId].viewersData[socket.id]) {
      streams[streamerId].viewersData[socket.id].points += 10;
      socket.emit('points-update', streams[streamerId].viewersData[socket.id].points);
      io.to(streamerId).emit('ranking-update', getRanking(streamerId));
    }
  });

  socket.on('update-thumbnail', (base64) => { if (streams[socket.id]) { streams[socket.id].thumbnail = base64; io.emit('update-stream-list', Object.values(streams)); } });
  socket.on('add-like', (streamerId) => { if (streams[streamerId]) { streams[streamerId].likes++; io.to(streamerId).emit('update-likes', streams[streamerId].likes); } });
  socket.on('update-viewers', (count) => { if (streams[socket.id]) { streams[socket.id].viewers = count; io.emit('update-stream-list', Object.values(streams)); } });

  // APUESTAS Y RECOMPENSAS
  socket.on('create-reward', (reward) => {
    if (streams[socket.id]) {
      reward.id = Date.now().toString(); streams[socket.id].rewards.push(reward);
      io.to(socket.id).emit('rewards-update', streams[socket.id].rewards);
    }
  });
  socket.on('redeem-reward', (data) => {
    const stream = streams[data.streamerId];
    if (stream && stream.viewersData[socket.id]) {
      const reward = stream.rewards.find(r => r.id === data.rewardId);
      if (reward && stream.viewersData[socket.id].points >= reward.cost) {
        stream.viewersData[socket.id].points -= reward.cost;
        socket.emit('points-update', stream.viewersData[socket.id].points);
        io.to(data.streamerId).emit('reward-redeemed', { username: data.username, action: reward.action });
      }
    }
  });
  socket.on('start-prediction', (data) => {
    if (streams[socket.id]) {
      streams[socket.id].prediction = { data: data, pool: { yes: 0, no: 0 }, bets: {} };
      io.to(socket.id).emit('prediction-started', data);
    }
  });
  socket.on('vote-prediction', (data) => {
    const stream = streams[data.streamerId];
    if (stream && stream.prediction && stream.viewersData[socket.id]) {
      const vData = stream.viewersData[socket.id];
      if (vData.points >= data.amount && !stream.prediction.bets[socket.id]) {
        vData.points -= data.amount;
        stream.prediction.bets[socket.id] = { option: data.option, amount: data.amount };
        stream.prediction.pool[data.option] += data.amount;
        socket.emit('points-update', vData.points);
        io.to(data.streamerId).emit('prediction-stats', stream.prediction.pool);
      }
    }
  });
  socket.on('resolve-prediction', (winningOption) => resolvePrediction(socket.id, winningOption));

  // WEBRTC Y SEÑALES
  socket.on('camera-status', (data) => socket.broadcast.emit('peer-camera-status', { streamerId: socket.id, enabled: data.enabled }));
  socket.on('mic-status', (data) => socket.broadcast.emit('peer-mic-status', { streamerId: socket.id, enabled: data.enabled }));
  socket.on('set-slow-mode', (data) => io.to(socket.id).emit('slow-mode-changed', data.seconds));
  socket.on('signal', (data) => io.to(data.to).emit('signal', { from: socket.id, signal: data.signal }));
  socket.on('pin-message', (data) => io.to(data.streamerId).emit('pin-message', data));

  socket.on('disconnect', () => {
    adminSockets.delete(socket.id);
    if (streams[socket.id]) {
      delete streams[socket.id]; io.emit('update-stream-list', Object.values(streams));
    }
  });
});

http.listen(process.env.PORT || 3000, () => console.log('Servidor Completo Activo'));
