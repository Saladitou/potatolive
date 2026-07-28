const express = require('express');
const app = express();
const http = require('http').createServer(app);
const io = require('socket.io')(http);

app.use(express.static(__dirname));

let streams = {};
let newsList = [];
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
        // Regla de 3: te llevas tu porcentaje del bote total
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

io.on('connection', (socket) => {
  // Enviar estado inicial
  socket.emit('initial-state', { 
    streams: Object.values(streams), 
    news: newsList, 
    maintenance: maintenanceMode 
  });

  // --- PANEL ADMIN ---
  socket.on('admin-login', (pass) => {
    if (pass === '5856') {
      adminSockets.add(socket.id);
      socket.emit('admin-auth-success');
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
  });

  socket.on('delete-news', (id) => {
    if (!adminSockets.has(socket.id)) return;
    newsList = newsList.filter(n => n.id !== id);
    io.emit('news-update', newsList);
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
      startTime: Date.now(),
      viewers: 0,
      likes: 0,
      thumbnail: null,
      viewersData: {}, // { socketId: { username, points, lastChat } }
      rewards: [],
      prediction: null
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

  socket.on('join-stream', (streamerId, username) => {
    socket.join(streamerId);
    if (streams[streamerId]) {
      if (!streams[streamerId].viewersData[socket.id]) {
        streams[streamerId].viewersData[socket.id] = { username, points: 0, lastChat: 0 };
      }
      socket.emit('points-update', streams[streamerId].viewersData[socket.id].points);
      if (streams[streamerId].prediction) {
        socket.emit('prediction-started', streams[streamerId].prediction.data);
      }
      if (streams[streamerId].rewards) {
        socket.emit('rewards-update', streams[streamerId].rewards);
      }
    }
  });

  socket.on('add-like', (streamerId) => {
    if (streams[streamerId]) {
      streams[streamerId].likes++;
      io.to(streamerId).emit('update-likes', streams[streamerId].likes);
    }
  });

  // --- SISTEMA DE PUNTOS Y CHAT ---
  socket.on('chat-message', (data) => {
    const stream = streams[data.streamerId];
    if (stream && stream.viewersData[socket.id]) {
      const now = Date.now();
      const vData = stream.viewersData[socket.id];
      // Cooldown de 10s para ganar puntos por chat
      if (now - vData.lastChat > 10000) {
        vData.points += 5;
        vData.lastChat = now;
        socket.emit('points-update', vData.points);
        io.to(data.streamerId).emit('ranking-update', getRanking(data.streamerId));
      }
    }
    io.to(data.streamerId).emit('chat-message', { username: data.username, text: data.text });
  });

  // Dar puntos por ver (Llamado cada minuto desde el cliente)
  socket.on('claim-watch-points', (streamerId) => {
    const stream = streams[streamerId];
    if (stream && stream.viewersData[socket.id]) {
      stream.viewersData[socket.id].points += 10;
      socket.emit('points-update', stream.viewersData[socket.id].points);
      io.to(streamerId).emit('ranking-update', getRanking(streamerId));
    }
  });

  function getRanking(streamerId) {
    if (!streams[streamerId]) return [];
    const vData = streams[streamerId].viewersData;
    return Object.values(vData)
      .sort((a, b) => b.points - a.points)
      .slice(0, 10);
  }

  // --- RECOMPENSAS & PREDICCIONES ---
  socket.on('create-reward', (reward) => {
    if (streams[socket.id]) {
      reward.id = Date.now().toString();
      streams[socket.id].rewards.push(reward);
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
        // Avisar al streamer que alguien canjeó
        io.to(data.streamerId).emit('reward-redeemed', { username: data.username, action: reward.action });
      }
    }
  });

  socket.on('start-prediction', (data) => {
    if (streams[socket.id]) {
      streams[socket.id].prediction = {
        data: data, // { question, optYes, optNo }
        pool: { yes: 0, no: 0 },
        bets: {} // { socketId: { option, amount } }
      };
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

  socket.on('resolve-prediction', (winningOption) => {
    resolvePrediction(socket.id, winningOption);
  });

  // --- SEÑALES WEBRTC ---
  socket.on('update-viewers', (count) => {
    if (streams[socket.id]) {
      streams[socket.id].viewers = count;
      io.emit('update-stream-list', Object.values(streams));
    }
  });

  socket.on('camera-status', (data) => socket.broadcast.emit('peer-camera-status', { streamerId: socket.id, enabled: data.enabled }));
  socket.on('mic-status', (data) => socket.broadcast.emit('peer-mic-status', { streamerId: socket.id, enabled: data.enabled }));
  socket.on('signal', (data) => io.to(data.to).emit('signal', { from: socket.id, signal: data.signal }));
  socket.on('pin-message', (data) => io.to(data.streamerId).emit('pin-message', data));
  socket.on('set-slow-mode', (data) => io.to(socket.id).emit('slow-mode-changed', data.seconds));

  socket.on('disconnect', () => {
    adminSockets.delete(socket.id);
    if (streams[socket.id]) {
      delete streams[socket.id];
      io.emit('update-stream-list', Object.values(streams));
    }
  });
});

http.listen(process.env.PORT || 3000, () => console.log('Servidor V5 activo'));
