const express = require('express');
const { createServer } = require('http');
const { Server } = require('socket.io');
const path = require('path');
const fs = require('fs');

const app = express();
const server = createServer(app);
const io = new Server(server);

app.use(express.static(path.join(__dirname, 'public'), {
  etag: true,
  lastModified: true,
  setHeaders: (res, filePath) => {
    if (filePath.endsWith('.html')) {
      res.setHeader('Cache-Control', 'no-cache, no-store, must-revalidate');
    }
  }
}));

// ===== Stats (persistent) =====
const DATA_DIR = process.env.FLY_APP_NAME ? '/data' : path.join(__dirname, 'data');
const STATS_FILE = path.join(DATA_DIR, 'stats.json');
const ADMIN_PASSWORD = process.env.ADMIN_PASSWORD || 'monopoly2024';

function loadStats() {
  try {
    if (fs.existsSync(STATS_FILE)) return JSON.parse(fs.readFileSync(STATS_FILE, 'utf8'));
  } catch(e) { console.error('Failed to load stats:', e.message); }
  return { totalRooms: 0, totalGames: 0, totalConnections: 0, gameHistory: [], roomHistory: [], connectionLog: [], roomLog: [], gameStartLog: [] };
}

function saveStats() {
  try {
    const dir = path.dirname(STATS_FILE);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    fs.writeFileSync(STATS_FILE, JSON.stringify(stats, null, 2));
  } catch(e) { console.error('Failed to save stats:', e.message); }
}

const stats = loadStats();
const serverStartTime = Date.now();

// Save stats periodically (every 60s) and on exit
// Trim logs older than 90 days every hour
setInterval(() => {
  const cutoff = Date.now() - 90*86400000;
  ['connectionLog','roomLog','gameStartLog'].forEach(k => {
    if (stats[k] && stats[k].length > 0 && stats[k][0] < cutoff) {
      stats[k] = stats[k].filter(t => t > cutoff);
    }
  });
}, 3600000);
setInterval(saveStats, 60000);
process.on('SIGINT', () => { saveStats(); process.exit(); });
process.on('SIGTERM', () => { saveStats(); process.exit(); });

function adminAuth(req, res, next) {
  const pw = req.query.pw || req.headers['x-admin-password'];
  if (pw !== ADMIN_PASSWORD) return res.status(401).json({ error: '密码错误' });
  next();
}

app.get('/api/stats', adminAuth, (req, res) => {
  const roomList = [];
  for (const [code, room] of rooms) {
    roomList.push({
      code,
      players: room.players.map(p => ({ name: p.name, disconnected: p.disconnected, role: p.role, roleName: ROLES[p.role]?.name || '新生', roleEmoji: ROLES[p.role]?.emoji || '🎒' })),
      started: room.started,
      round: room.round,
    });
  }
  const now = Date.now();
  const cLog = stats.connectionLog || [];
  const rLog = stats.roomLog || [];
  function count(arr, ms) { return arr.filter(t => t > now - ms).length; }
  const _1d = 86400000, _7d = 7*_1d, _30d = 30*_1d;
  // Count all games that were actually started (gameHistory only contains started games)
  const playedGames = stats.gameHistory || [];
  function countGames(arr, ms) { return arr.filter(g => g.startTime && new Date(g.startTime).getTime() > now - ms).length; }

  const inRoom = socketRoom.size;
  res.json({
    online: io.engine.clientsCount,
    inRoom,
    idle: Math.max(0, io.engine.clientsCount - inRoom),
    activeRooms: rooms.size,
    roomList,
    totalRooms: stats.totalRooms,
    totalGames: playedGames.length,
    totalConnections: stats.totalConnections,
    visitDay: count(cLog, _1d), visitWeek: count(cLog, _7d), visitMonth: count(cLog, _30d),
    roomDay: count(rLog, _1d), roomWeek: count(rLog, _7d), roomMonth: count(rLog, _30d),
    gameDay: countGames(playedGames, _1d), gameWeek: countGames(playedGames, _7d), gameMonth: countGames(playedGames, _30d),
    // Players (人次 = total participations, 人 = unique names) — only count actually played games
    playerDay: playedGames.filter(g=>g.startTime&&new Date(g.startTime).getTime()>now-_1d).reduce((s,g)=>s+(g.playerNames||g.players||[]).length,0),
    playerWeek: playedGames.filter(g=>g.startTime&&new Date(g.startTime).getTime()>now-_7d).reduce((s,g)=>s+(g.playerNames||g.players||[]).length,0),
    playerMonth: playedGames.filter(g=>g.startTime&&new Date(g.startTime).getTime()>now-_30d).reduce((s,g)=>s+(g.playerNames||g.players||[]).length,0),
    playerTotal: playedGames.reduce((s,g)=>s+(g.playerNames||g.players||[]).length,0),
    uniquePlayerDay: new Set(playedGames.filter(g=>g.startTime&&new Date(g.startTime).getTime()>now-_1d).flatMap(g=>{const p=g.playerNames||g.players||[];return p.map(x=>typeof x==='string'?x:x.name);})).size,
    uniquePlayerWeek: new Set(playedGames.filter(g=>g.startTime&&new Date(g.startTime).getTime()>now-_7d).flatMap(g=>{const p=g.playerNames||g.players||[];return p.map(x=>typeof x==='string'?x:x.name);})).size,
    uniquePlayerMonth: new Set(playedGames.filter(g=>g.startTime&&new Date(g.startTime).getTime()>now-_30d).flatMap(g=>{const p=g.playerNames||g.players||[];return p.map(x=>typeof x==='string'?x:x.name);})).size,
    uniquePlayerTotal: new Set(playedGames.flatMap(g=>{const p=g.playerNames||g.players||[];return p.map(x=>typeof x==='string'?x:x.name);})).size,
    connLastHour: count(cLog, 3600000),
    roomHistory: (stats.roomHistory || []).slice(-100),
    // Mark truly active games
    gameHistory: stats.gameHistory.slice(-100).map(g => {
      if (!g.endTime) {
        const activeRoom = rooms.get(g.code);
        return { ...g, _active: !!(activeRoom && activeRoom.started && !activeRoom.finished) };
      }
      return g;
    }),
    uptimeMs: Date.now() - serverStartTime,
  });
});

// ===== Rate Limiting =====
// Per-socket event rate limiter: prevents any single connection from spamming
function rateLimit(socket, event, maxPerWindow, windowMs) {
  const key = `${socket.id}:${event}`;
  const now = Date.now();
  if (!socket._rl) socket._rl = {};
  const rl = socket._rl[key] || (socket._rl[key] = { count: 0, reset: now + windowMs });
  if (now > rl.reset) { rl.count = 0; rl.reset = now + windowMs; }
  rl.count++;
  if (rl.count > maxPerWindow) return true; // blocked
  return false;
}

function guarded(socket, event, limit, windowMs, handler) {
  socket.on(event, (...args) => {
    if (rateLimit(socket, event, limit, windowMs)) return;
    handler(...args);
  });
}

// ===== Data =====
const rooms = new Map();
const socketRoom = new Map();

const COLORS = ['#e74c3c','#3498db','#2ecc71','#f39c12','#9b59b6','#1abc9c'];
const NAMES = ['入学报到','紫荆园','奖学金','清芬园','缴学费','东门','桃李园','学生会通知','听涛园','芝兰园','挂科补考','六教','校园网','四教','三教','南门','西操','奖学金','东操','综合体育馆','情人坡','李文正馆','学生会通知','美术学院','FIT楼','西门','大礼堂','新清华学堂','校园卡','蒙民伟音乐厅','被辅导员约谈','主楼','工字厅','奖学金','近春园','北门','学生会通知','苏世民书院','交书本费','二校门'];
const PRICES = {1:600,3:600,5:2000,6:1000,8:1000,9:1200,11:1400,12:1500,13:1400,14:1600,15:2000,16:1800,18:1800,19:2000,21:2200,23:2200,24:2400,25:2000,26:2600,27:2600,28:1500,29:2800,31:3000,32:3000,34:3200,35:2000,37:3500,39:4000};
const BUILDING_COSTS = {1:500,3:500,6:500,8:500,9:500,11:1000,13:1000,14:1000,16:1000,18:1000,19:1000,21:1500,23:1500,24:1500,26:1500,27:1500,29:1500,31:2000,32:2000,34:2000,37:2000,39:2000};
const COLOR_GROUPS = {brown:[1,3],lightblue:[6,8,9],pink:[11,13,14],orange:[16,18,19],red:[21,23,24],yellow:[26,27,29],green:[31,32,34],darkblue:[37,39]};
const SPACE_GROUP = {}; for(const[g,ids] of Object.entries(COLOR_GROUPS)) ids.forEach(id=>SPACE_GROUP[id]=g);
const ROLES = {
  freshman:      { name:'新生',   emoji:'🎒', startMoney:0, diceBonus:0 },
  athlete:       { name:'体育生', emoji:'🏃', startMoney:0, diceBonus:2 },
  competitor:    { name:'竞赛生', emoji:'🏆', startMoney:-3000, diceBonus:0 },
  alumni:        { name:'校友',   emoji:'🎓', startMoney:0, diceBonus:0 },
  international: { name:'国际生', emoji:'🌍', startMoney:5000, diceBonus:0 },
  talent:        { name:'特长生', emoji:'🎨', startMoney:0, diceBonus:0 },
  faculty:       { name:'教职工', emoji:'👔', startMoney:0, diceBonus:-1 },
};
const DROP_REASONS = [
  '沉迷打剧本杀无法自拔',
  '连续三学期GPA不到2.0',
  '在主楼天台表白被全校直播',
  '把导师的实验数据删了',
  '论文查重率99.9%',
  '考试带小抄被当场抓获',
  '在荷塘里游泳被保安抓了',
  '把校长的车位占了一学期',
  '体育课挂科四次',
  '在李文正馆吃螺蛳粉被举报',
  '用校园网挖矿导致全校断网',
  '毕设答辩PPT打不开',
  '替室友签到被监控拍到',
  '在宿舍养了一窝猫',
  '骑车逆行撞了辅导员',
];

const CHANCE_CARDS = [
  { text: '获得"国家奖学金"！收取 ¥8,000', amount: 8000 },
  { text: '在"挑战杯"中获奖，收取 ¥3,000', amount: 3000 },
  { text: '自行车被偷了，支付 ¥500 买新车', amount: -500 },
  { text: '论文发表在 Nature 上！收取 ¥5,000', amount: 5000 },
  { text: '被选为学生会主席，每位玩家向你支付 ¥500', collect: 500 },
  { text: '食堂饭卡充值故障，支付 ¥200', amount: -200 },
  { text: '获得出国交换机会，前进到起点收取 ¥1,500', amount: 1500, moveTo: 0 },
  { text: '宿舍违规用电被抓，支付 ¥500', amount: -500 },
  { text: '获得创业大赛奖金，收取 ¥4,000', amount: 4000 },
  { text: '期末复习太累进了校医院，支付 ¥400', amount: -400 },
  { text: '被选中参加军训方阵表演，获得补贴 ¥1,000', amount: 1000 },
  { text: '清华110周年校庆捐款，支付 ¥800', amount: -800 },
  { text: '在SRT项目中表现优异，收取 ¥2,500', amount: 2500 },
  { text: '深夜在紫操跑步被表白，对方请你吃大餐，收取 ¥500', amount: 500 },
  { text: '选课系统崩溃，重选的课要买新教材，支付 ¥300', amount: -300 },
  { text: '期末在C楼通宵赶DDL，咖啡外卖费 ¥300', amount: -300 },
];
const CHEST_CARDS = [
  { text: '获得"一二·九"奖学金，收取 ¥2,000', amount: 2000 },
  { text: '助教工资到账，收取 ¥1,500', amount: 1500 },
  { text: '获得"蒋南翔"奖学金，收取 ¥2,000', amount: 2000 },
  { text: '银行转账错误，多收到 ¥1,000', amount: 1000 },
  { text: '生日快乐！每位玩家向你支付 ¥100', collect: 100 },
  { text: '获得"好读书"奖学金，收取 ¥500', amount: 500 },
  { text: '科研项目经费到账，收取 ¥3,000', amount: 3000 },
  { text: '获得社会实践优秀奖，收取 ¥800', amount: 800 },
  { text: '获得企业赞助，收取 ¥2,500', amount: 2500 },
  { text: '前往起点，收取 ¥1,500', amount: 1500, moveTo: 0 },
];

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

function checkGameOver(room) {
  if (!room.started || room.finished) return;
  const active = room.players.filter(p => !p.bankrupt && !p.disconnected);
  if (active.length <= 1) {
    if (active.length === 1) {
      addLog(room, `🎓 游戏结束！${active[0].name} 顺利毕业，荣获学霸称号！`);
      room.winner = active[0].name;
    } else {
      addLog(room, '游戏结束！');
    }
    // Generate settlement so client can show end-game popup
    room.settlement = room.players.map((pl, i) => {
      if (pl.bankrupt) {
        return { name: pl.name, money: 0, propValue: pl.lastPropertyValue || 0, total: 0, bankrupt: true, props: pl.lastPropertyDetails || [] };
      }
      let propValue = 0;
      const props = [];
      for (const [sid, prop] of Object.entries(room.properties)) {
        if (prop.owner === i) {
          const val = (PRICES[+sid] || 0) + (BUILDING_COSTS[+sid] || 0) * prop.level;
          propValue += val;
          props.push({ name: NAMES[+sid], level: prop.level, value: val });
        }
      }
      return { name: pl.name, money: pl.money, propValue, total: pl.money + propValue, bankrupt: false, props };
    });
    room.finished = true;
    finalizeGame(room);
  }
}

function checkBankruptcy(room, code) {
  let changed = false;
  for (const p of room.players) {
    if (!p.bankrupt && !p.disconnected && p.money <= 0) {
      p.bankrupt = true;
      p.money = 0;
      changed = true;
      // Save property details before releasing
      const pi = room.players.indexOf(p);
      const released = [];
      const releasedDetails = [];
      let releasedValue = 0;
      for (const [sid, prop] of Object.entries(room.properties)) {
        if (prop.owner === pi) {
          released.push(NAMES[+sid]);
          const val = (PRICES[+sid] || 0) + (BUILDING_COSTS[+sid] || 0) * prop.level;
          releasedDetails.push({ name: NAMES[+sid], level: prop.level, value: val });
          releasedValue += val;
          delete room.properties[sid];
        }
      }
      p.lastPropertyNames = released;
      p.lastPropertyDetails = releasedDetails;
      p.lastPropertyValue = releasedValue;
      const reason = DROP_REASONS[Math.floor(Math.random() * DROP_REASONS.length)];
      addLog(room, `💀 ${p.name} 因「${reason}」退学了！`);
      if (released.length > 0) {
        addLog(room, `📢 ${p.name} 的地产已释放：${released.join('、')}，可重新购买`);
      }
      io.to(code).emit('player-dropped', { name: p.name, reason });
    }
  }
  if (!changed) return;
  checkGameOver(room);
  // If current player went bankrupt, advance turn
  const cur = room.players[room.currentTurn];
  if (cur && cur.bankrupt && !room.finished) {
    advanceTurn(room);
  }
}

function finalizeGame(room) {
  if (room._finalized || !room.started) return;
  room._finalized = true;
  const entry = (room.histIndex !== undefined) ? stats.gameHistory[room.histIndex] : null;
  if (!entry) return;
  entry.endTime = new Date().toISOString();
  entry.durationMs = room.startTime ? Date.now() - room.startTime : 0;
  entry.rounds = room.round;
  entry.finished = !!room.finished;
  entry.settled = !!room.settled;
  entry.winner = room.winner || null;
  entry.players = room.players.map((p, i) => {
    if (p.bankrupt) {
      return { name: p.name, money: p.money, properties: (p.lastPropertyNames || []).length, propertyNames: p.lastPropertyNames || [], bankrupt: true, role: p.role, roleName: ROLES[p.role]?.name || '新生', roleEmoji: ROLES[p.role]?.emoji || '🎒' };
    }
    const propCount = Object.values(room.properties).filter(pr => pr.owner === i).length;
    const propNames = Object.entries(room.properties)
      .filter(([_, pr]) => pr.owner === i)
      .map(([sid]) => NAMES[+sid]);
    return { name: p.name, money: p.money, properties: propCount, propertyNames: propNames, bankrupt: p.bankrupt, role: p.role, roleName: ROLES[p.role]?.name || '新生', roleEmoji: ROLES[p.role]?.emoji || '🎒' };
  });
  saveStats();
}

const ROOM_KEEP_ALIVE = 3 * 60 * 1000; // 3 minutes

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

  room.players[idx].disconnected = true;
  if (room.started && room.currentTurn === idx) advanceTurn(room);

  addLog(room, `${name} 离开了房间`);
  if (room.players.every(p => p.disconnected)) {
    // All gone — schedule deletion after grace period
    if (room._deleteTimer) clearTimeout(room._deleteTimer);
    room._deleteTimer = setTimeout(() => {
      if (rooms.has(code) && rooms.get(code).players.every(p => p.disconnected)) {
        finalizeGame(room);
        rooms.delete(code);
      }
    }, ROOM_KEEP_ALIVE);
  } else {
    broadcast(code);
  }
}

// ===== Socket Events =====
io.on('connection', (socket) => {
  stats.totalConnections++;
  if (!stats.connectionLog) stats.connectionLog = [];
  if (!stats.roomLog) stats.roomLog = [];
  if (!stats.gameStartLog) stats.gameStartLog = [];
  stats.connectionLog.push(Date.now());

  // create-room: max 5 per 30s
  guarded(socket, 'create-room', 5, 30000, ({ name }) => {
    leaveRoom(socket);
    const code = genCode();
    const room = {
      code, hostId: socket.id,
      players: [{ id: socket.id, name: name||'房主', color: COLORS[0], money: 15000, position: 0, inJail: false, bankrupt: false, disconnected: false, role: 'freshman' }],
      started: false, currentTurn: 0, round: 1, properties: {}, log: [], lastDice: [0,0],
    };
    rooms.set(code, room);
    socketRoom.set(socket.id, code);
    socket.join(code);
    stats.totalRooms++;
    stats.roomLog.push(Date.now());
    // Record room history
    if (!stats.roomHistory) stats.roomHistory = [];
    const rh = { code, createTime: new Date().toISOString(), playerNames: [name||'房主'], gameStarted: false };
    stats.roomHistory.push(rh);
    if (stats.roomHistory.length > 500) stats.roomHistory = stats.roomHistory.slice(-500);
    room._roomHistIndex = stats.roomHistory.length - 1;
    addLog(room, `${name} 创建了房间`);
    broadcast(code);
  });

  // join-room: max 5 per 15s
  guarded(socket, 'join-room', 5, 15000, ({ code, name }) => {
    leaveRoom(socket);
    code = (code||'').toUpperCase().trim();
    const room = rooms.get(code);
    if (!room) return socket.emit('error-msg', '房间不存在，请检查代码');

    // Reconnect disconnected player with same name (works for both started and waiting rooms)
    const disc = room.players.findIndex(p => p.name === name && p.disconnected);
    if (disc !== -1) {
      room.players[disc].id = socket.id;
      room.players[disc].disconnected = false;
      socketRoom.set(socket.id, code);
      socket.join(code);
      if (room._deleteTimer) { clearTimeout(room._deleteTimer); room._deleteTimer = null; }
      addLog(room, `${name} 重新连接`);
      broadcast(code);
      return;
    }

    if (room.started) return socket.emit('error-msg', '游戏已开始，无法加入');
    const activePlayers = room.players.filter(p => !p.disconnected).length;
    if (activePlayers >= 6) return socket.emit('error-msg', '房间已满（最多6人）');

    room.players.push({
      id: socket.id, name: name||`玩家${room.players.length+1}`, color: COLORS[room.players.length % COLORS.length],
      money: 15000, position: 0, inJail: false, bankrupt: false, disconnected: false, role: 'freshman',
    });
    socketRoom.set(socket.id, code);
    socket.join(code);
    if (room._deleteTimer) { clearTimeout(room._deleteTimer); room._deleteTimer = null; }
    stats.roomLog.push(Date.now());
    // Update room history
    if (room._roomHistIndex !== undefined && stats.roomHistory[room._roomHistIndex]) {
      stats.roomHistory[room._roomHistIndex].playerNames.push(name||`玩家${room.players.length}`);
    }
    addLog(room, `${name} 加入了房间`);
    broadcast(code);
  });

  guarded(socket, 'leave-room', 5, 10000, () => leaveRoom(socket));

  socket.on('select-role', ({ role }) => {
    if (!role || !ROLES[role]) return;
    const code = getCode(socket);
    const room = rooms.get(code);
    if (!room || room.started) return;
    const p = room.players.find(pl => pl.id === socket.id);
    if (p) { p.role = role; broadcast(code); }
  });

  // start-game: max 10 per 30s
  guarded(socket, 'start-game', 10, 30000, () => {
    const code = getCode(socket);
    const room = rooms.get(code);
    if (!room || room.hostId !== socket.id || room.started) return;
    // Remove disconnected players before starting
    room.players = room.players.filter(p => !p.disconnected);
    room.players.forEach((p, i) => p.color = COLORS[i % COLORS.length]);
    if (room.players.length < 2) return socket.emit('error-msg', '至少需要2名玩家');
    room.started = true;
    room.startTime = Date.now();
    stats.totalGames++;
    stats.gameStartLog.push(Date.now());
    // Mark room history as game started
    if (room._roomHistIndex !== undefined && stats.roomHistory[room._roomHistIndex]) {
      stats.roomHistory[room._roomHistIndex].gameStarted = true;
    }
    const histEntry = {
      code, startTime: new Date().toISOString(),
      playerNames: room.players.map(p => p.name),
      playerRoles: room.players.map(p => ({ name: p.name, role: p.role, roleName: ROLES[p.role]?.name || '新生', roleEmoji: ROLES[p.role]?.emoji || '🎒' })),
    };
    stats.gameHistory.push(histEntry);
    if (stats.gameHistory.length > 500) stats.gameHistory = stats.gameHistory.slice(-500);
    room.histIndex = stats.gameHistory.length - 1;
    saveStats();
    // Apply role starting money adjustments
    room.players.forEach(p => {
      const r = ROLES[p.role];
      if (r && r.startMoney) p.money += r.startMoney;
    });
    const roleInfo = room.players.map(p => `${ROLES[p.role]?.emoji||'🎒'} ${p.name}(${ROLES[p.role]?.name||'新生'})`).join('、');
    addLog(room, `游戏开始！${roleInfo}`);
    broadcast(code);
  });

  // roll-dice: max 5 per 5s
  guarded(socket, 'roll-dice', 5, 5000, () => {
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
    const roleBonus = ROLES[p.role]?.diceBonus || 0;
    const total = Math.max(d1 + d2 + roleBonus, 2);
    const oldPos = p.position;
    p.position = (p.position + total) % 40;
    const sn = NAMES[p.position]||'?';
    const dbl = d1===d2 ? ' (双数！)' : '';
    const bonusStr = roleBonus > 0 ? `+${roleBonus}` : roleBonus < 0 ? `${roleBonus}` : '';
    addLog(room, `${p.name} 掷出 ${d1}+${d2}${bonusStr}=${total}${dbl}，到达「${sn}」`);

    // Pass go (crossed or landed on position 0)
    if (p.position < oldPos) {
      const goMoney = p.role === 'alumni' ? 4000 : 2000;
      p.money += goMoney;
      addLog(room, `${p.name} 经过起点 +¥${goMoney.toLocaleString()}`);
    }

    // Landing effects
    room.lastCard = null;
    if (p.position === 4) { p.money -= 2000; addLog(room, `${p.name} 缴学费 -¥2,000`); }
    else if (p.position === 38) { p.money -= 1000; addLog(room, `${p.name} 交书本费 -¥1,000`); }
    else if (p.position === 30) { p.position = 10; p.inJail = true; addLog(room, `${p.name} 被辅导员约谈，进入补考！`); }
    else if (p.position === 20) {
      // 情人坡：如果有其他人也在这里，两人都扣钱
      const dates = room.players.filter(o => o !== p && !o.bankrupt && !o.disconnected && o.position === 20);
      if (dates.length > 0) {
        const cost = 1000;
        p.money -= cost;
        addLog(room, `💕 ${p.name} 在情人坡约会，花了 ¥${cost.toLocaleString()}`);
        for (const d of dates) {
          d.money -= cost;
          addLog(room, `💕 ${d.name} 也在情人坡约会，花了 ¥${cost.toLocaleString()}`);
        }
      }
    }

    // Chance / Chest cards
    const isChance = [7,22,36].includes(p.position);
    const isChest = [2,17,33].includes(p.position);
    if (isChance || isChest) {
      const deck = isChance ? CHANCE_CARDS : CHEST_CARDS;
      const card = deck[Math.floor(Math.random() * deck.length)];
      if (card.collect) {
        for (const other of room.players) {
          if (other !== p && !other.bankrupt && !other.disconnected) {
            other.money -= card.collect;
            p.money += card.collect;
          }
        }
      } else {
        if (card.amount) p.money += card.amount;
        if (card.moveTo !== undefined) p.position = card.moveTo;
      }
      addLog(room, `${p.name} 抽到：${card.text}`);
      room.lastCard = { type: isChance ? 'chance' : 'chest', text: card.text };
    }

    // Rent: if landing on owned property
    const prop = room.properties[p.position];
    if (prop && prop.owner !== ci) {
      const sp = {1:1,3:3,6:6,8:8,9:9,11:11,13:13,14:14,16:16,18:18,19:19,21:21,23:23,24:24,26:26,27:27,29:29,31:31,32:32,34:34,37:37,39:39};
      const RENTS = {
        1:[30,60,150,450,1350,2400],3:[60,120,300,900,2700,4800],
        6:[90,180,450,1350,4050,6000],8:[90,180,450,1350,4050,6000],9:[120,240,600,1500,4500,6750],
        11:[150,300,750,2250,6750,9375],13:[150,300,750,2250,6750,9375],14:[180,360,900,2700,7500,10500],
        16:[210,420,1050,3000,8250,11250],18:[210,420,1050,3000,8250,11250],19:[240,480,1200,3300,9000,12000],
        21:[270,540,1350,3750,10500,13125],23:[270,540,1350,3750,10500,13125],24:[300,600,1500,4500,11250,13875],
        26:[330,660,1650,4950,12000,14625],27:[330,660,1650,4950,12000,14625],29:[360,720,1800,5400,12750,15375],
        31:[390,780,1950,5850,13500,16500],32:[390,780,1950,5850,13500,16500],34:[420,840,2250,6750,15000,18000],
        37:[525,1050,2625,7500,16500,19500],39:[750,1500,3000,9000,21000,25500]
      };
      const owner = room.players[prop.owner];
      let rent = 0;
      if (RENTS[p.position]) {
        rent = RENTS[p.position][prop.level];
        // Same-color group bonus: double rent if owner has all properties in the group
        const group = SPACE_GROUP[p.position];
        if (group) {
          const groupIds = COLOR_GROUPS[group];
          const ownsAll = groupIds.every(id => room.properties[id] && room.properties[id].owner === prop.owner);
          if (ownsAll) rent *= 2;
        }
      } else if ([5,15,25,35].includes(p.position)) {
        // Railroad
        const rrCount = [5,15,25,35].filter(id=>room.properties[id]&&room.properties[id].owner===prop.owner).length;
        rent = [0,375,750,1500,3000][rrCount];
      } else if ([12,28].includes(p.position)) {
        // Utility
        const uCount = [12,28].filter(id=>room.properties[id]&&room.properties[id].owner===prop.owner).length;
        rent = total * (uCount===2?15:6);
      }
      if (rent > 0) {
        // Role bonuses for rent
        const posGroup = SPACE_GROUP[p.position];
        // Owner bonus: talent doubles yellow rent income
        if (owner.role === 'talent' && posGroup === 'yellow') rent *= 2;
        // Payer penalties
        if (p.role === 'athlete' && (posGroup === 'brown' || posGroup === 'lightblue')) rent *= 2;
        if (p.role === 'talent' && posGroup === 'orange') rent *= 2;
        if (p.role === 'international') rent = Math.ceil(rent * 1.3);
        rent = Math.round(rent);
        p.money -= rent;
        owner.money += rent;
        addLog(room, `${p.name} 向 ${owner.name} 支付租金 ¥${rent.toLocaleString()}`);
      }
    }

    checkBankruptcy(room, code);
    io.to(code).emit('dice-rolled', { dice:[d1,d2], playerIndex:ci, from:oldPos, to:p.position });
    broadcast(code);
  });

  // end-turn: max 5 per 5s
  guarded(socket, 'end-turn', 5, 5000, () => {
    const code = getCode(socket);
    const room = rooms.get(code);
    if (!room || !room.started) return;
    if (room.players[room.currentTurn]?.id !== socket.id) return;
    room.rolled = false;
    advanceTurn(room);
    broadcast(code);
  });

  // buy-property: max 5 per 5s
  guarded(socket, 'buy-property', 5, 5000, ({ spaceId, playerIndex }) => {
    const code = getCode(socket);
    const room = rooms.get(code);
    if (!room || !room.started) return;
    const basePrice = PRICES[spaceId];
    if (basePrice === undefined || room.properties[spaceId]) return;
    const pi = playerIndex ?? room.currentTurn;
    if (pi < 0 || pi >= room.players.length) return;
    const role = room.players[pi].role;
    const price = role === 'alumni' ? Math.ceil(basePrice * 1.25) : role === 'faculty' ? Math.floor(basePrice * 0.7) : basePrice;
    if (room.players[pi].money < price) return;
    room.players[pi].money -= price;
    room.properties[spaceId] = { owner: pi, level: 0 };
    addLog(room, `${room.players[pi].name} 购买了「${NAMES[spaceId]}」（¥${price.toLocaleString()}）`);
    checkBankruptcy(room, code);
    broadcast(code);
  });

  guarded(socket, 'upgrade-property', 5, 5000, ({ spaceId }) => {
    const code = getCode(socket);
    const room = rooms.get(code);
    if (!room || !room.started || room.finished) return;
    const ci = room.currentTurn;
    if (room.players[ci].id !== socket.id) return;
    const prop = room.properties[spaceId];
    if (!prop || prop.owner !== ci || prop.level >= 5) return;
    const baseCost = BUILDING_COSTS[spaceId];
    if (!baseCost) return;
    const cost = room.players[ci].role === 'competitor' ? Math.floor(baseCost * 0.5) : baseCost;
    if (room.players[ci].money < cost) return socket.emit('error-msg', '余额不足');
    room.players[ci].money -= cost;
    prop.level++;
    addLog(room, `${room.players[ci].name} 在「${NAMES[spaceId]}」盖了房子（等级${prop.level}，¥${cost.toLocaleString()}）`);
    broadcast(code);
  });

  guarded(socket, 'transfer', 5, 5000, ({ fromIndex, toIndex, amount }) => {
    const code = getCode(socket);
    const room = rooms.get(code);
    if (!room || !room.started) return;
    if (fromIndex===toIndex || !amount || amount<=0) return;
    if (fromIndex<0||fromIndex>=room.players.length||toIndex<0||toIndex>=room.players.length) return;
    room.players[fromIndex].money -= amount;
    room.players[toIndex].money += amount;
    addLog(room, `${room.players[fromIndex].name} → ${room.players[toIndex].name}：¥${amount.toLocaleString()}`);
    checkBankruptcy(room, code);
    broadcast(code);
  });

  guarded(socket, 'adjust-money', 5, 5000, ({ playerIndex, amount, note }) => {
    const code = getCode(socket);
    const room = rooms.get(code);
    if (!room || !room.started) return;
    if (playerIndex<0||playerIndex>=room.players.length) return;
    room.players[playerIndex].money += amount;
    addLog(room, `${room.players[playerIndex].name} ${note||(amount>0?'收入':'支出')} ¥${Math.abs(amount).toLocaleString()}`);
    checkBankruptcy(room, code);
    broadcast(code);
  });

  guarded(socket, 'quick-action', 5, 5000, ({ action }) => {
    const code = getCode(socket);
    const room = rooms.get(code);
    if (!room || !room.started) return;
    const p = room.players[room.currentTurn];
    switch(action) {
      case 'passgo': { const gm=p.role==='alumni'?4000:2000; p.money+=gm; addLog(room,`${p.name} 经过起点 +¥${gm.toLocaleString()}`); break; }
      case 'tax2000': p.money-=2000; addLog(room,`${p.name} 缴学费 -¥2,000`); break;
      case 'tax1000': p.money-=1000; addLog(room,`${p.name} 交书本费 -¥1,000`); break;
    }
    checkBankruptcy(room, code);
    broadcast(code);
  });

  // end-game vote: initiate or confirm
  guarded(socket, 'end-game-vote', 5, 5000, ({ confirm }) => {
    const code = getCode(socket);
    const room = rooms.get(code);
    if (!room || !room.started || room.finished) return;
    const pi = room.players.findIndex(p => p.id === socket.id);
    if (pi === -1) return;
    const p = room.players[pi];
    if (p.bankrupt || p.disconnected) return;

    if (confirm === false) {
      // Someone rejected — cancel the vote
      if (room.endVote) {
        addLog(room, `${p.name} 拒绝了结算请求`);
        room.endVote = null;
        broadcast(code);
      }
      return;
    }

    // Start or join the vote
    if (!room.endVote) {
      room.endVote = { voters: [pi], initiator: p.name };
      addLog(room, `${p.name} 发起了结算投票，等待所有玩家确认...`);
    } else if (!room.endVote.voters.includes(pi)) {
      room.endVote.voters.push(pi);
      addLog(room, `${p.name} 同意结算`);
    }

    // Check if all active players have confirmed
    const activePlayers = room.players.filter((pl) => !pl.bankrupt && !pl.disconnected);
    const allConfirmed = activePlayers.every((pl) => {
      const idx = room.players.indexOf(pl);
      return room.endVote.voters.includes(idx);
    });

    if (allConfirmed) {
      // Settle the game
      const results = room.players.map((pl, i) => {
        if (pl.bankrupt) return { name: pl.name, money: 0, propValue: pl.lastPropertyValue || 0, total: 0, bankrupt: true, props: pl.lastPropertyDetails || [] };
        let propValue = 0;
        const props = [];
        for (const [sid, prop] of Object.entries(room.properties)) {
          if (prop.owner === i) {
            const val = (PRICES[+sid] || 0) + (BUILDING_COSTS[+sid] || 0) * prop.level;
            propValue += val;
            props.push({ name: NAMES[+sid], level: prop.level, value: val });
          }
        }
        return { name: pl.name, money: pl.money, propValue, total: pl.money + propValue, bankrupt: false, props };
      });
      const active = results.filter(r => !r.bankrupt);
      active.sort((a, b) => b.total - a.total);
      const winner = active.length > 0 ? active[0] : null;
      addLog(room, '📊 游戏结算！');
      active.forEach((r, i) => addLog(room, `${i+1}. ${r.name}：总资产 ¥${r.total.toLocaleString()}（现金 ¥${r.money.toLocaleString()} + 地产 ¥${r.propValue.toLocaleString()}）`));
      if (winner) {
        room.winner = winner.name;
        addLog(room, `🎓 ${winner.name} 以总资产 ¥${winner.total.toLocaleString()} 顺利毕业，荣获学霸称号！`);
      }
      room.finished = true;
      room.settled = true;
      room.settlement = results;
      room.endVote = null;
      finalizeGame(room);
    }
    broadcast(code);
  });

  // chat: max 5 per 10s (stricter)
  guarded(socket, 'send-chat', 5, 10000, ({ text }) => {
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
    if (room.players.length === 0 || room.players.every(p => p.disconnected)) { finalizeGame(room); rooms.delete(code); }
  }
}, 600000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`\n  清华大富翁 服务器已启动`);
  console.log(`  本机访问: http://localhost:${PORT}`);
  console.log(`  局域网: http://<你的IP>:${PORT}\n`);
});
