const express = require('express');
const { createServer } = require('http');
const { Server } = require('socket.io');
const path = require('path');

const app = express();
const server = createServer(app);
const io = new Server(server);

app.use(express.static(path.join(__dirname, 'public')));

// ===== Data =====
const rooms = new Map();
const socketRoom = new Map();

const COLORS = ['#e74c3c','#3498db','#2ecc71','#f39c12','#9b59b6','#1abc9c'];
const NAMES = ['入学报到','紫荆园','奖学金','清芬园','缴学费','东门','桃李园','学生会通知','听涛园','芝兰园','挂科补考','六教','校园网','四教','三教','南门','西操','奖学金','东操','综合体育馆','情人坡','图书馆','学生会通知','美术学院','FIT楼','西门','大礼堂','新清华学堂','校园卡','蒙民伟音乐厅','被辅导员约谈','主楼','工字厅','奖学金','近春园','北门','学生会通知','苏世民书院','交书本费','二校门'];
const PRICES = {1:600,3:600,5:2000,6:1000,8:1000,9:1200,11:1400,12:1500,13:1400,14:1600,15:2000,16:1800,18:1800,19:2000,21:2200,23:2200,24:2400,25:2000,26:2600,27:2600,28:1500,29:2800,31:3000,32:3000,34:3200,35:2000,37:3500,39:4000};

function genCode() {
  const c = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code;
  do { code = Array.from({length:6},()=>c[Math.floor(Math.random()*c.length)]).join(''); } while (rooms.has(code));
  return code;
}

function getRoom(socket) {
  const code = socketRoom.get(socket.id);
  return code ? rooms.get(code) : null;
}

function getCode(socket) { return socketRoom.get(socket.id); }

function addLog(room, text) {
  const t = new Date().toLocaleTimeString('zh-CN',{hour:'2-digit',minute:'2-digit'});
  room.log.push(`[${t}] ${text}`);
  if (room.log.length > 200) room.log = room.log.slice(-100);
}

function broadcast(code) {
  const room = rooms.get(code);
  if (room) io.to(code).emit('room-update', room);
}

function advanceTurn(room) {
  let next = room.currentTurn;
  for (let i = 0; i < room.players.length; i++) {
    next = (next + 1) % room.players.length;
    if (!room.players[next].bankrupt && !room.players[next].disconnected) break;
  }
  if (next <= room.currentTurn) room.round++;
  room.currentTurn = next;
}

function leaveRoom(socket) {
  const code = socketRoom.get(socket.id);
  if (!code) return;
  const room = rooms.get(code);
  if (!room) { socketRoom.delete(socket.id); return; }

  socket.leave(code);
  socketRoom.delete(socket.id);
  const idx = room.players.findIndex(p => p.id === socket.id);
  if (idx === -1) return;
  const name = room.players[idx].name;

  if (!room.started) {
    room.players.splice(idx, 1);
    room.players.forEach((p, i) => p.color = COLORS[i % COLORS.length]);
    if (room.hostId === socket.id && room.players.length > 0) room.hostId = room.players[0].id;
  } else {
    room.players[idx].disconnected = true;
    if (room.currentTurn === idx) advanceTurn(room);
  }

  addLog(room, `${name} 离开了房间`);
  if (room.players.length === 0 || room.players.every(p => p.disconnected)) {
    rooms.delete(code);
  } else {
    broadcast(code);
  }
}

// ===== Socket Events =====
io.on('connection', (socket) => {

  socket.on('create-room', ({ name }) => {
    leaveRoom(socket);
    const code = genCode();
    const room = {
      code, hostId: socket.id,
      players: [{ id: socket.id, name: name||'房主', color: COLORS[0], money: 15000, position: 0, inJail: false, bankrupt: false, disconnected: false }],
      started: false, currentTurn: 0, round: 1, properties: {}, log: [], lastDice: [0,0],
    };
    rooms.set(code, room);
    socketRoom.set(socket.id, code);
    socket.join(code);
    addLog(room, `${name} 创建了房间`);
    broadcast(code);
  });

  socket.on('join-room', ({ code, name }) => {
    leaveRoom(socket);
    code = (code||'').toUpperCase().trim();
    const room = rooms.get(code);
    if (!room) return socket.emit('error-msg', '房间不存在，请检查代码');
    if (room.players.length >= 6 && !room.started) return socket.emit('error-msg', '房间已满（最多6人）');

    // Reconnect disconnected player with same name
    if (room.started) {
      const disc = room.players.findIndex(p => p.name === name && p.disconnected);
      if (disc !== -1) {
        room.players[disc].id = socket.id;
        room.players[disc].disconnected = false;
        socketRoom.set(socket.id, code);
        socket.join(code);
        addLog(room, `${name} 重新连接`);
        broadcast(code);
        return;
      }
      return socket.emit('error-msg', '游戏已开始，无法加入');
    }

    room.players.push({
      id: socket.id, name: name||`玩家${room.players.length+1}`, color: COLORS[room.players.length % COLORS.length],
      money: 15000, position: 0, inJail: false, bankrupt: false, disconnected: false,
    });
    socketRoom.set(socket.id, code);
    socket.join(code);
    addLog(room, `${name} 加入了房间`);
    broadcast(code);
  });

  socket.on('leave-room', () => leaveRoom(socket));

  socket.on('start-game', () => {
    const code = getCode(socket);
    const room = rooms.get(code);
    if (!room || room.hostId !== socket.id || room.started) return;
    if (room.players.length < 2) return socket.emit('error-msg', '至少需要2名玩家');
    room.started = true;
    addLog(room, '游戏开始！每人 ¥15,000');
    broadcast(code);
  });

  socket.on('roll-dice', () => {
    const code = getCode(socket);
    const room = rooms.get(code);
    if (!room || !room.started) return;
    const ci = room.currentTurn;
    if (room.players[ci].id !== socket.id) return;
    if (room.rolled) return;
    room.rolled = true;
    const d1 = Math.floor(Math.random()*6)+1;
    const d2 = Math.floor(Math.random()*6)+1;
    room.lastDice = [d1, d2];
    const p = room.players[ci];
    const total = d1 + d2;
    const oldPos = p.position;
    p.position = (p.position + total) % 40;
    const sn = NAMES[p.position]||'?';
    const dbl = d1===d2 ? ' (双数！)' : '';
    addLog(room, `${p.name} 掷出 ${d1}+${d2}=${total}${dbl}，到达「${sn}」`);

    // Pass go (crossed position 0)
    if (p.position < oldPos && p.position !== 0) {
      p.money += 2000;
      addLog(room, `${p.name} 经过起点 +¥2,000`);
    }

    // Landing effects
    if (p.position === 4) { p.money -= 2000; addLog(room, `${p.name} 缴学费 -¥2,000`); }
    else if (p.position === 38) { p.money -= 1000; addLog(room, `${p.name} 交书本费 -¥1,000`); }
    else if (p.position === 30) { p.position = 10; p.inJail = true; addLog(room, `${p.name} 被辅导员约谈，进入补考！`); }

    // Rent: if landing on owned property
    const prop = room.properties[p.position];
    if (prop && prop.owner !== ci) {
      const sp = {1:1,3:3,6:6,8:8,9:9,11:11,13:13,14:14,16:16,18:18,19:19,21:21,23:23,24:24,26:26,27:27,29:29,31:31,32:32,34:34,37:37,39:39};
      const RENTS = {
        1:[20,100,300,900,1600,2500],3:[40,200,600,1800,3200,4500],
        6:[60,300,900,2700,4000,5500],8:[60,300,900,2700,4000,5500],9:[80,400,1000,3000,4500,6000],
        11:[100,500,1500,4500,6250,7500],13:[100,500,1500,4500,6250,7500],14:[120,600,1800,5000,7000,9000],
        16:[140,700,2000,5500,7500,9500],18:[140,700,2000,5500,7500,9500],19:[160,800,2200,6000,8000,10000],
        21:[180,900,2500,7000,8750,10500],23:[180,900,2500,7000,8750,10500],24:[200,1000,3000,7500,9250,11000],
        26:[220,1100,3300,8000,9750,11500],27:[220,1100,3300,8000,9750,11500],29:[240,1200,3600,8500,10250,12000],
        31:[260,1300,3900,9000,11000,12750],32:[260,1300,3900,9000,11000,12750],34:[280,1500,4500,10000,12000,14000],
        37:[350,1750,5000,11000,13000,15000],39:[500,2000,6000,14000,17000,20000]
      };
      const owner = room.players[prop.owner];
      let rent = 0;
      if (RENTS[p.position]) {
        rent = RENTS[p.position][prop.level];
      } else if ([5,15,25,35].includes(p.position)) {
        // Railroad
        const rrCount = [5,15,25,35].filter(id=>room.properties[id]&&room.properties[id].owner===prop.owner).length;
        rent = [0,250,500,1000,2000][rrCount];
      } else if ([12,28].includes(p.position)) {
        // Utility
        const uCount = [12,28].filter(id=>room.properties[id]&&room.properties[id].owner===prop.owner).length;
        rent = total * (uCount===2?10:4);
      }
      if (rent > 0) {
        p.money -= rent;
        owner.money += rent;
        addLog(room, `${p.name} 向 ${owner.name} 支付租金 ¥${rent.toLocaleString()}`);
      }
    }

    io.to(code).emit('dice-rolled', { dice:[d1,d2], playerIndex:ci, from:oldPos, to:p.position });
    broadcast(code);
  });

  socket.on('end-turn', () => {
    const code = getCode(socket);
    const room = rooms.get(code);
    if (!room || !room.started) return;
    room.rolled = false;
    advanceTurn(room);
    broadcast(code);
  });

  socket.on('buy-property', ({ spaceId, playerIndex }) => {
    const code = getCode(socket);
    const room = rooms.get(code);
    if (!room || !room.started) return;
    const price = PRICES[spaceId];
    if (price === undefined || room.properties[spaceId]) return;
    const pi = playerIndex ?? room.currentTurn;
    if (pi < 0 || pi >= room.players.length) return;
    room.players[pi].money -= price;
    room.properties[spaceId] = { owner: pi, level: 0 };
    addLog(room, `${room.players[pi].name} 购买了「${NAMES[spaceId]}」（¥${price}）`);
    broadcast(code);
  });

  socket.on('upgrade-property', ({ spaceId }) => {
    const code = getCode(socket);
    const room = rooms.get(code);
    if (!room || !room.started) return;
    const prop = room.properties[spaceId];
    if (!prop || prop.level >= 5) return;
    prop.level++;
    addLog(room, `「${NAMES[spaceId]}」升级到 ${prop.level} 级`);
    broadcast(code);
  });

  socket.on('transfer', ({ fromIndex, toIndex, amount }) => {
    const code = getCode(socket);
    const room = rooms.get(code);
    if (!room || !room.started) return;
    if (fromIndex===toIndex || !amount || amount<=0) return;
    if (fromIndex<0||fromIndex>=room.players.length||toIndex<0||toIndex>=room.players.length) return;
    room.players[fromIndex].money -= amount;
    room.players[toIndex].money += amount;
    addLog(room, `${room.players[fromIndex].name} → ${room.players[toIndex].name}：¥${amount.toLocaleString()}`);
    broadcast(code);
  });

  socket.on('adjust-money', ({ playerIndex, amount, note }) => {
    const code = getCode(socket);
    const room = rooms.get(code);
    if (!room || !room.started) return;
    if (playerIndex<0||playerIndex>=room.players.length) return;
    room.players[playerIndex].money += amount;
    addLog(room, `${room.players[playerIndex].name} ${note||(amount>0?'收入':'支出')} ¥${Math.abs(amount).toLocaleString()}`);
    broadcast(code);
  });

  socket.on('quick-action', ({ action }) => {
    const code = getCode(socket);
    const room = rooms.get(code);
    if (!room || !room.started) return;
    const p = room.players[room.currentTurn];
    switch(action) {
      case 'passgo': p.money+=2000; addLog(room,`${p.name} 经过起点 +¥2,000`); break;
      case 'tax2000': p.money-=2000; addLog(room,`${p.name} 缴学费 -¥2,000`); break;
      case 'tax1000': p.money-=1000; addLog(room,`${p.name} 交书本费 -¥1,000`); break;
    }
    broadcast(code);
  });

  socket.on('send-chat', ({ text }) => {
    const code = getCode(socket);
    const room = rooms.get(code);
    if (!room || !text) return;
    const player = room.players.find(p=>p.id===socket.id);
    io.to(code).emit('chat-msg', { from: player?.name||'???', text, color: player?.color||'#999' });
  });

  socket.on('disconnect', () => leaveRoom(socket));
});

// Cleanup stale rooms every 10 min
setInterval(() => {
  for (const [code, room] of rooms) {
    if (room.players.length === 0 || room.players.every(p => p.disconnected)) rooms.delete(code);
  }
}, 600000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`\n  清华大富翁 服务器已启动`);
  console.log(`  本机访问: http://localhost:${PORT}`);
  console.log(`  局域网: http://<你的IP>:${PORT}\n`);
});
