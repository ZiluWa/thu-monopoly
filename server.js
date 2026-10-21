const express = require('express');
const { createServer } = require('http');
const { Server } = require('socket.io');
const path = require('path');
const fs = require('fs');

const app = express();
const server = createServer(app);
// 不开 socket.io 自带的 connectionStateRecovery：它恢复后不删旧会话，手机切网络时新旧两条连接会共用同一个 id，
// 旧连接一断就把新连接从房间里摘掉（页面卡死、被当成掉线）。重连改由页面在握手里带上身份，按令牌接回座位。
// 心跳间隔 10 秒：页面在前台 15 秒收不到任何消息就当连接卡死、主动重连（index.html 的 STALL_MS），改这里要一起改
const io = new Server(server, {
  pingTimeout: 30000,
  pingInterval: 10000,
});

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
// DATA_DIR 可用环境变量指定（测试用临时目录，免得改到本地 data/stats.json）
const DATA_DIR = process.env.DATA_DIR || (process.env.FLY_APP_NAME ? '/data' : path.join(__dirname, 'data'));
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

// ===== 连接日志：每次连上/断开记一行（断开原因、连了多久、从哪个 Fly 节点进来、什么浏览器），用来排查掉线 =====
// 存在卷上 /data/netlog.jsonl，超过 5MB 轮换一次；后台 /api/netlog 可以查看
// 每行带玩家 IP（ip）：用来判断是不是开着代理、走的哪条线路
const NETLOG_FILE = path.join(DATA_DIR, 'netlog.jsonl');
function uaFamily(ua) {
  const s = String(ua || '');
  const os = /iPhone|iPad|iPod/i.test(s) ? 'ios' : /Android/i.test(s) ? 'android' : /Mac OS X/i.test(s) ? 'mac' : /Windows/i.test(s) ? 'win' : 'other';
  const app = /MicroMessenger/i.test(s) ? 'wechat' : /\bQQ\//.test(s) ? 'qq' : /DingTalk/i.test(s) ? 'dingtalk' : /Lark|Feishu/i.test(s) ? 'feishu'
    : /Edg\//.test(s) ? 'edge' : /Chrome\/|CriOS/.test(s) ? 'chrome' : /Firefox\/|FxiOS/.test(s) ? 'firefox' : /Safari\//.test(s) ? 'safari' : 'other';
  return `${os}-${app}`;
}
function netlog(obj, quiet) {
  const line = JSON.stringify({ t: new Date().toISOString(), ...obj });
  if (!quiet) console.log('[net]', line);
  try {
    if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
    fs.appendFileSync(NETLOG_FILE, line + '\n');
    if (Math.random() < 0.02 && fs.statSync(NETLOG_FILE).size > 5e6) fs.renameSync(NETLOG_FILE, NETLOG_FILE + '.1');
  } catch (e) {}
}

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
      players: room.players.map(p => ({ name: p.name, disconnected: p.disconnected, offline: !!p.offline, role: p.role, roleName: ROLES[p.role]?.name || '新生', roleEmoji: ROLES[p.role]?.emoji || '🎒' })),
      started: room.started,
      round: room.round,
    });
  }
  const now = Date.now();
  const cLog = stats.connectionLog || [];
  const rLog = stats.roomLog || [];
  function count(arr, ms) { return arr.filter(t => t > now - ms).length; }
  const _1d = 86400000, _7d = 7*_1d, _30d = 30*_1d;
  // Only count games that were finalized (endTime set) or currently in progress
  const playedGames = (stats.gameHistory || []).filter(g => {
    if (g.endTime) return true;
    const activeRoom = rooms.get(g.code);
    return !!(activeRoom && activeRoom.started && !activeRoom.finished);
  });
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

// 连接日志（最近 n 行，默认 500）
app.get('/api/netlog', adminAuth, (req, res) => {
  const n = Math.min(Math.max(parseInt(req.query.n, 10) || 500, 1), 20000);
  let lines = [];
  try { lines = fs.readFileSync(NETLOG_FILE, 'utf8').trim().split('\n'); } catch (e) {}
  res.type('text/plain; charset=utf-8').send(lines.slice(-n).join('\n'));
});

// 页面自动上传的诊断记录（断开原因、是否在后台、有没有网、重连用了多久、页面报错、看到了什么提示）
// 每条存成连接日志里的一行，ev 以 "c." 开头；每个 IP 每分钟最多 60 次，单次最多 200 条
const clientLogHits = new Map();
const clip = (v, n) => (v === undefined || v === null) ? undefined : String(v).slice(0, n);
app.post('/api/clientlog', express.json({ limit: '64kb' }), (req, res) => {
  try {
    const ip = req.headers['fly-client-ip'] || (req.socket && req.socket.remoteAddress) || '';
    const now = Date.now();
    if (clientLogHits.size > 5000) clientLogHits.clear();
    const hit = clientLogHits.get(ip) || { n: 0, reset: now + 60000 };
    if (now > hit.reset) { hit.n = 0; hit.reset = now + 60000; }
    if (++hit.n > 60) { clientLogHits.set(ip, hit); return res.status(429).end(); }
    clientLogHits.set(ip, hit);
    const b = req.body || {};
    const base = { cid: clip(b.cid, 40), name: clip(b.name, 20), tz: clip(b.tz, 40), lang: clip(b.lang, 20), ua: uaFamily(req.headers['user-agent']), edge: req.headers['fly-region'], ip };
    for (const e of (Array.isArray(b.events) ? b.events.slice(0, 200) : [])) {
      if (!e || typeof e !== 'object') continue;
      const ev = {};
      for (const [k, v] of Object.entries(e).slice(0, 20)) ev[clip(k, 20)] = (typeof v === 'number' || typeof v === 'boolean') ? v : clip(v, 200);
      netlog({ ...base, ...ev, ev: 'c.' + clip(e.type, 30) }, true);
    }
    res.status(204).end();
  } catch (err) { res.status(400).end(); }
});

// 没连上的尝试（握手失败、会话不存在等），以前完全看不到
io.engine.on('connection_error', (err) => {
  const h = (err && err.req && err.req.headers) || {};
  netlog({ ev: 'conn_err', code: err && err.code, msg: clip(err && err.message, 120), edge: h['fly-region'], ip: h['fly-client-ip'], ua: uaFamily(h['user-agent']), uaFull: clip(h['user-agent'], 300) });
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

// 每个事件处理都包一层 try/catch：某个客户端发来异常数据时只记日志，不让整个服务器崩掉（一崩所有房间都会丢）
function safe(event, handler) {
  return (...args) => {
    try { handler(...args); }
    catch (e) { console.error(`[err] ${event}:`, (e && e.stack) || e); }
  };
}

function guarded(socket, event, limit, windowMs, handler) {
  const run = safe(event, handler);
  socket.on(event, (...args) => {
    if (rateLimit(socket, event, limit, windowMs)) return;
    run(...args);
  });
}

// ===== Data =====
const rooms = new Map();
const socketRoom = new Map();

const COLORS = ['#e74c3c','#3498db','#2ecc71','#f39c12','#9b59b6','#1abc9c'];
const NAMES = ['入学报到','紫荆园','奖学金','清芬园','观畴园','缴学费','东门','桃李园','学生会通知','听涛园','芝兰园','奖学金','挂科补考','六教','校园网','四教','法学院','南门','紫荆操场','奖学金','北体育馆','综合体育馆','紫荆公寓','南区宿舍','情人坡','李文正馆','学生会通知','美术学院','经管学院','西门','大礼堂','新清华学堂','快递站','校园卡','蒙民伟音乐厅','学生会通知','被辅导员约谈','主楼','工字厅','奖学金','C楼','北门','西北门','学生会通知','水木清华','苏世民书院','交书本费','二校门'];
const PRICES = {1:500,3:500,4:650,6:1750,7:850,9:850,10:1000,13:1200,14:1250,15:1200,16:1350,17:1750,18:1550,20:1550,21:1700,22:1750,23:1750,25:1800,27:1800,28:2050,29:1750,30:2150,31:2150,32:1250,33:1250,34:2400,37:2500,38:2500,40:2650,41:1750,42:1750,44:3150,45:2950,47:3350};
const BUILDING_COSTS = {1:350,3:350,4:350,7:350,9:350,10:350,13:700,15:700,16:700,18:700,20:700,21:700,22:1050,23:1050,25:1050,27:1050,28:1050,30:1050,31:1050,34:1050,37:1400,38:1400,40:1400,44:1400,45:1400,47:1400};
// 卖出价：地价 50% + 建筑 50%（即契约卡上的“抵押”价）
function sellValue(sid, level) { return Math.floor((PRICES[sid] || 0) / 2) + Math.floor(((BUILDING_COSTS[sid] || 0) * (level || 0)) / 2); }
function sellableTotal(room, pi) { return Object.entries(room.properties).filter(([_, pr]) => pr.owner === pi).reduce((sum, [sid, pr]) => sum + sellValue(+sid, pr.level), 0); }
const COLOR_GROUPS = {brown:[1,3,4],lightblue:[7,9,10],pink:[13,15,16],orange:[18,20,21],purple:[22,23],red:[25,27,28],yellow:[30,31,34],green:[37,38,40],darkblue:[44,45,47]};
const SPACE_GROUP = {}; for(const[g,ids] of Object.entries(COLOR_GROUPS)) ids.forEach(id=>SPACE_GROUP[id]=g);
// 48 格棋盘（每边 12 格，四角为 0/12/24/36）的关键格子
const BOARD_SIZE = 48;
const JAIL_POS = 12;                              // 挂科补考
const GATES = [6,17,29,41,42];                    // 五个校门
const UTILITIES = [14,32,33];                     // 校园网、快递站、校园卡
const CHANCE_TILES = [8,26,35,43];                // 学生会通知（抽卡）
const CHEST_TILES = [2,11,19,39];                 // 奖学金（抽卡）
const ROLES = {
  freshman:      { name:'新生',   emoji:'🎒', startMoney:0, diceBonus:0 },
  athlete:       { name:'体育生', emoji:'🏃', startMoney:0, diceBonus:2 },
  competitor:    { name:'竞赛生', emoji:'🏆', startMoney:-2000, diceBonus:0 },
  alumni:        { name:'校友',   emoji:'🎓', startMoney:0, diceBonus:0 },
  international: { name:'国际生', emoji:'🌍', startMoney:3000, diceBonus:0 },
  talent:        { name:'特长生', emoji:'🎨', startMoney:0, diceBonus:0 },
  faculty:       { name:'教职工', emoji:'👔', startMoney:0, diceBonus:-1 },
};
// ===== 房间玩法设置（开局前可改）=====
const SETTING_DEFS = {
  sellProperty: { label: '可以卖地产还债', default: true },
  passGoMoney:  { label: '经过起点 +¥1,500', default: true },
};
function normalizeSettings(input) {
  const out = {};
  for (const [k, d] of Object.entries(SETTING_DEFS)) out[k] = (input && typeof input[k] === 'boolean') ? input[k] : d.default;
  return out;
}
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
  { text: '获得"国家奖学金"！收取 ¥5,000', amount: 5000 },
  { text: '在"挑战杯"中获奖，收取 ¥2,000', amount: 2000 },
  { text: '自行车被偷了，支付 ¥700 买新车', amount: -700 },
  { text: '论文发表在 Nature 上！收取 ¥3,000', amount: 3000 },
  { text: '被选为学生会主席，请全体同学喝奶茶，向每位玩家支付 ¥300', pay: 300 },
  { text: '食堂饭卡充值故障，支付 ¥300', amount: -300 },
  { text: '获得出国交换机会，前进到起点收取 ¥1,500', amount: 1500, moveTo: 0 },
  { text: '宿舍违规用电被抓，支付 ¥700', amount: -700 },
  { text: '获得创业大赛奖金，收取 ¥2,500', amount: 2500 },
  { text: '期末复习太累进了校医院，支付 ¥500', amount: -500 },
  { text: '被选中参加军训方阵表演，获得补贴 ¥600', amount: 600 },
  { text: '清华110周年校庆捐款，支付 ¥1,000', amount: -1000 },
  { text: '在SRT项目中表现优异，收取 ¥1,500', amount: 1500 },
  { text: '深夜在紫操跑步被表白，对方请你吃大餐，收取 ¥300', amount: 300 },
  { text: '选课系统崩溃，重选的课要买新教材，支付 ¥400', amount: -400 },
  { text: '期末在C楼通宵赶DDL，咖啡外卖费 ¥400', amount: -400 },
  // ---- 2026-09 新增 ----
  { text: '校庆志愿者被抓壮丁，前往「大礼堂」（无主可买，有主付租）', goTo: 30 },
  { text: '自行车没气了，退后 3 格', moveBack: 3 },
  { text: '期中考试周到了，直接进入补考', goToJail: true },
  { text: '宿舍查寝违规，每处地产交 ¥200 整改费，每级建筑再加 ¥100', repairs: { perProperty: 200, perLevel: 100 } },
  { text: '拿到大厂实习 offer，收取 ¥2,000', amount: 2000 },
  { text: '校园卡丢了，补办支付 ¥200', amount: -200 },
  { text: '抢到了陈明游泳馆的课，前往「综合体育馆」', goTo: 21 },
  { text: '被拉去学生节当演员，每位玩家给你 ¥200 打赏', collect: 200 },
  { text: '校车坐过站，前往最近的校门；若有主，付双倍租金', nearestRailroad: true },
  { text: '登上清华官微，收取 ¥1,000', amount: 1000 },
];
const CHEST_CARDS = [
  { text: '获得"一二·九"奖学金，收取 ¥1,200', amount: 1200 },
  { text: '助教工资到账，收取 ¥900', amount: 900 },
  { text: '获得"蒋南翔"奖学金，收取 ¥1,200', amount: 1200 },
  { text: '银行转账错误，多收到 ¥600', amount: 600 },
  { text: '生日快乐！每位玩家向你支付 ¥100', collect: 100 },
  { text: '获得"好读书"奖学金，收取 ¥300', amount: 300 },
  { text: '科研项目经费到账，收取 ¥1,800', amount: 1800 },
  { text: '获得社会实践优秀奖，收取 ¥500', amount: 500 },
  { text: '获得企业赞助，收取 ¥1,500', amount: 1500 },
  { text: '前往起点，收取 ¥1,500', amount: 1500, moveTo: 0 },
  // ---- 2026-09 新增 ----
  { text: '获得"综合优秀奖学金"，收取 ¥2,000', amount: 2000 },
  { text: '获得"国家励志奖学金"，收取 ¥2,500', amount: 2500 },
  { text: '课程助教补贴到账，收取 ¥700', amount: 700 },
  { text: '校医院报销到账，收取 ¥400', amount: 400 },
  { text: '校友捐赠基金分红，每位玩家向你支付 ¥150', collect: 150 },
  { text: '奖学金重复申报被追回，支付 ¥500', amount: -500 },
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
  if (!room) return;
  // 不下发内部字段（下划线开头）和玩家令牌（令牌泄露会被用来顶号）
  const pub = {};
  for (const [k, v] of Object.entries(room)) if (!k.startsWith('_')) pub[k] = v;
  pub.players = room.players.map(p => { const { token, ...rest } = p; return rest; });
  pub.serverNow = Date.now();   // 客户端据此算倒计时（不依赖手机本地时钟）
  pub.timing = { notice: OFFLINE_NOTICE_MS, autoSkip: AUTO_SKIP_MS, idle: IDLE_SKIP_MS };
  io.to(code).emit('room-update', pub);
}

function advanceTurn(room) {
  room.rolled = false;
  let next = room.currentTurn;
  for (let i = 0; i < room.players.length; i++) {
    next = (next + 1) % room.players.length;
    if (!room.players[next].bankrupt && !room.players[next].disconnected) break;
  }
  if (next <= room.currentTurn) room.round++;
  room.currentTurn = next;
  room.turnSeq = (room.turnSeq || 0) + 1;
  room.turnStartedAt = room.turnActiveAt = Date.now();
}

function checkGameOver(room) {
  if (!room.started || room.finished) return;
  // 掉线/暂时离开的人仍在局里（回来可以接着玩），只有退学的人出局
  const active = room.players.filter(p => !p.bankrupt);
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

// ===== 落地效果 =====
// 顺序：税/补考/情人坡 → 抽卡（卡牌可能再次移动，递归落地，最多 3 层）→ 租金
function applyLanding(room, code, ci, ctx) {
  const p = room.players[ci];
  const total = ctx.total;
  if ((ctx.depth || 0) > 3) return;
  if (p.position === 5) { p.money -= 2000; addLog(room, `${p.name} 缴学费 -¥2,000`); }
  else if (p.position === 46) { p.money -= 1000; addLog(room, `${p.name} 交书本费 -¥1,000`); }
  else if (p.position === 36) { p.position = JAIL_POS; p.inJail = true; addLog(room, `${p.name} 被辅导员约谈，进入补考！`); return; }
  else if (p.position === 24) { addLog(room, `💕 ${p.name} 在情人坡休息`); }

  // Chance / Chest cards
  const isChance = CHANCE_TILES.includes(p.position);
  const isChest = CHEST_TILES.includes(p.position);
  if (isChance || isChest) {
    const deck = isChance ? CHANCE_CARDS : CHEST_CARDS;
    const card = deck[Math.floor(Math.random() * deck.length)];
    addLog(room, `${p.name} 抽到：${card.text}`);
    const type = isChance ? 'chance' : 'chest';
    room.lastCard = room.lastCard ? { type, text: room.lastCard.text + '  ➜  ' + card.text } : { type, text: card.text };
    if (applyCard(room, code, ci, card, ctx)) return;   // 卡牌已把玩家移走并处理了新位置的落地
  }

  // Rent: if landing on owned property
  const prop = room.properties[p.position];
  if (prop && prop.owner !== ci) {
    const RENTS = {
      1:[40,70,180,490,1400,2520],3:[70,140,320,980,2800,5040],4:[80,170,390,1120,3150,5600],
      7:[110,210,490,1400,4200,6300],9:[110,210,490,1400,4200,6300],10:[140,280,630,1580,4730,7000],
      13:[180,350,770,2380,7000,9800],15:[180,350,770,2380,7000,9800],16:[210,390,940,2800,7700,10850],
      18:[240,450,1120,3150,8750,11900],20:[240,450,1120,3150,8750,11900],21:[280,530,1260,3500,9450,12600],
      22:[290,560,1330,3710,9800,12950],23:[290,560,1330,3710,9800,12950],
      25:[280,560,1400,3920,10850,13650],27:[280,560,1400,3920,10850,13650],28:[320,630,1580,4730,11900,14700],
      30:[350,700,1750,5250,12600,15400],31:[350,700,1750,5250,12600,15400],34:[390,770,1890,5600,13300,16100],
      37:[420,840,2030,6160,14000,17500],38:[420,840,2030,6160,14000,17500],40:[450,910,2380,7000,15750,18900],
      44:[670,1330,2940,8400,19600,23100],45:[560,1120,2800,7700,17500,20300],47:[770,1580,3150,9450,22050,26600]
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
    } else if (GATES.includes(p.position)) {
      // 校门：拥有 1-5 个的阶梯租金，5 个全有 ¥4,200（2026-10-20 地价/盖房/租金统一打七折）
      const rrCount = GATES.filter(id=>room.properties[id]&&room.properties[id].owner===prop.owner).length;
      rent = [0,350,700,1400,2800,4200][rrCount] * (ctx.rentMult || 1);   // 校车坐过站：双倍
    } else if (UTILITIES.includes(p.position)) {
      // 公用设施：拥有 1 个骰子×30，2 个×70，3 个全有×110
      const uCount = UTILITIES.filter(id=>room.properties[id]&&room.properties[id].owner===prop.owner).length;
      rent = total * (uCount===3?110:uCount===2?70:30);
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
}

// 执行卡牌效果。返回 true 表示卡牌移动了玩家并已递归处理落地（调用方不再结算当前格租金）
function applyCard(room, code, ci, card, ctx) {
  const p = room.players[ci];
  const others = room.players.filter(o => o !== p && !o.bankrupt && !o.disconnected);
  if (card.collect) { for (const o of others) { o.money -= card.collect; p.money += card.collect; } return false; }
  if (card.pay) { for (const o of others) { p.money -= card.pay; o.money += card.pay; } return false; }
  if (card.amount) p.money += card.amount;
  if (card.moveTo !== undefined) { p.position = card.moveTo; return false; }   // 旧卡“前往起点”：只移动，不触发落地
  if (card.goToJail) { p.position = JAIL_POS; p.inJail = true; addLog(room, `${p.name} 进入补考！`); return true; }
  if (card.repairs) {
    let fee = 0;
    for (const pr of Object.values(room.properties)) if (pr.owner === ci) fee += card.repairs.perProperty + card.repairs.perLevel * (pr.level || 0);
    p.money -= fee;
    addLog(room, `${p.name} 支付整改费 ¥${fee.toLocaleString()}`);
    return false;
  }
  let target;
  if (card.goTo !== undefined) target = card.goTo;
  else if (card.moveBack) target = (p.position - card.moveBack + BOARD_SIZE) % BOARD_SIZE;
  else if (card.nearestRailroad) { target = GATES.find(id => id > p.position) ?? GATES[0]; }
  if (target === undefined) return false;
  const from = p.position;
  p.position = target;
  if (!card.moveBack && target < from && room.settings?.passGoMoney) {   // 向前越过起点
    const goMoney = p.role === 'alumni' ? 2000 : 1500;
    p.money += goMoney;
    addLog(room, `${p.name} 经过起点 +¥${goMoney.toLocaleString()}`);
  }
  addLog(room, `${p.name} ${card.moveBack ? '退到' : '前往'}「${NAMES[target]}」`);
  applyLanding(room, code, ci, { ...ctx, depth: (ctx.depth || 0) + 1, rentMult: card.nearestRailroad ? 2 : 1 });
  return true;
}

function checkBankruptcy(room, code) {
  let changed = false;
  for (const p of room.players) {
    if (p.bankrupt || p.disconnected) continue;
    if (p.money > 0) { if (p.inDebt) { p.inDebt = false; addLog(room, `✅ ${p.name} 已还清欠款`); } continue; }
    const sellable = room.settings?.sellProperty === false ? 0 : sellableTotal(room, room.players.indexOf(p));
    if (p.money + sellable > 0) {
      // 还有地产可卖：先给机会卖地产还债，卖光仍还不清才退学
      if (!p.inDebt) {
        p.inDebt = true;
        addLog(room, `⚠️ ${p.name} 资金 ¥${p.money.toLocaleString()}，需卖出地产还债（可卖总额 ¥${sellable.toLocaleString()}）`);
        io.to(p.id).emit('need-sell', { money: p.money, sellable });
      }
      continue;
    }
    {
      p.bankrupt = true;
      p.inDebt = false;
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
  // 按唯一 id 找记录：gameHistory 满 500 条后会整体前移，用下标会写到别的局上
  const entry = room._gid ? findLast(stats.gameHistory, g => g.gid === room._gid) : null;
  if (!entry) return;
  entry.endTime = new Date().toISOString();
  entry.durationMs = room.startTime ? Date.now() - room.startTime : 0;
  entry.rounds = room.round;
  entry.finished = !!room.finished;
  entry.settled = !!room.settled;
  entry.winner = room.winner || null;
  entry.players = room.players.map((p, i) => {
    if (p.bankrupt) {
      return { name: p.name, money: p.money, properties: (p.lastPropertyNames || []).length, propertyNames: p.lastPropertyNames || [], propertyDetails: p.lastPropertyDetails || [], propValue: p.lastPropertyValue || 0, bankrupt: true, role: p.role, roleName: ROLES[p.role]?.name || '新生', roleEmoji: ROLES[p.role]?.emoji || '🎒' };
    }
    const propCount = Object.values(room.properties).filter(pr => pr.owner === i).length;
    const propNames = Object.entries(room.properties)
      .filter(([_, pr]) => pr.owner === i)
      .map(([sid]) => NAMES[+sid]);
    return { name: p.name, money: p.money, properties: propCount, propertyNames: propNames, bankrupt: p.bankrupt, role: p.role, roleName: ROLES[p.role]?.name || '新生', roleEmoji: ROLES[p.role]?.emoji || '🎒' };
  });
  saveStats();
}

function findLast(arr, fn) {
  for (let i = (arr || []).length - 1; i >= 0; i--) if (fn(arr[i])) return arr[i];
  return null;
}
function newId() { return Date.now().toString(36) + Math.random().toString(36).slice(2, 8); }

// ===== 断线容忍：掉线不等于离开 =====
// 玩家状态：
//   offline       连接断了（多半只是闪断，客户端正在自动重连）。轮到他时大家等他，5 秒后显示提示和「跳过」按钮
//   disconnected  暂时离开：掉线太久 / 掉线时被跳过 / 主动点了离开。轮到他时直接跳过，连回来自动恢复
// 所有时长都可以用环境变量改（测试里调小）
const envMs = (name, def) => { const v = Number(process.env[name]); return Number.isFinite(v) && v > 0 ? v : def; };
const OFFLINE_NOTICE_MS = envMs('OFFLINE_NOTICE_MS', 5000);       // 掉线 5 秒后才提示其他人（闪断别人看不到）
const AUTO_SKIP_MS      = envMs('AUTO_SKIP_MS', 30000);           // 提示出现后再等 30 秒，自动跳过他这回合
const IDLE_SKIP_MS      = envMs('IDLE_SKIP_MS', 60000);           // 在线但 60 秒没操作，其他人也可以跳过他
const AWAY_MS           = envMs('AWAY_MS', 2 * 60000);            // 对局中掉线超过 2 分钟算暂时离开
const LOBBY_AWAY_MS     = envMs('LOBBY_AWAY_MS', 15000);          // 等待开局时掉线超过 15 秒就不再挡着大家开局
const KEEP_STARTED_MS   = envMs('KEEP_STARTED_MS', 30 * 60000);   // 进行中的对局：全员离线后保留 30 分钟
const KEEP_IDLE_MS      = envMs('KEEP_IDLE_MS', 15 * 60000);      // 没开局 / 已结束：全员离线后保留 15 分钟

// 从什么时候开始等这位玩家：他掉线的时刻和轮到他的时刻，取较晚的
function waitStart(room, p) { return Math.max(p.offlineSince || 0, room.turnStartedAt || 0); }

// 当前回合能不能被别人跳过：掉线满 5 秒，或在线但 60 秒没操作
function canSkip(room, now) {
  const cur = room.players[room.currentTurn];
  if (!cur || cur.bankrupt) return false;
  if (cur.offline) return now - waitStart(room, cur) >= OFFLINE_NOTICE_MS;
  return now - (room.turnActiveAt || room.turnStartedAt || now) >= IDLE_SKIP_MS;
}

// 除了当前玩家，还有没有能接着玩的人
function hasOtherActive(room) {
  return room.players.some((p, i) => i !== room.currentTurn && !p.bankrupt && !p.disconnected);
}
// 有没有别人在线等着（大家都掉线时不自动跳过，免得回来时每人都莫名丢一回合）
function someoneWaiting(room) {
  return room.players.some((p, i) => i !== room.currentTurn && !p.bankrupt && !p.offline);
}

function skipTurn(room, byName) {
  const cur = room.players[room.currentTurn];
  if (!cur) return;
  const wasOffline = !!cur.offline;
  if (wasOffline) cur.disconnected = true;   // 掉线中被跳过：之后轮到他直接跳过，连回来自动恢复
  if (byName) addLog(room, `⏭️ ${byName} 跳过了 ${cur.name} 的回合（${wasOffline ? '掉线中' : '长时间没操作'}）`);
  else addLog(room, `⏭️ ${cur.name} 掉线没回来，自动跳过本回合`);
  advanceTurn(room);
}

// 当前玩家有操作（掷骰、买地等）时刷新"最近操作时间"，用于判断是否长时间没操作
function touchTurn(room, socket) {
  if (room && room.started && room.players[room.currentTurn]?.id === socket.id) room.turnActiveAt = Date.now();
}

// 座位还标着在线、但对应的连接已经不存在：补记为掉线（防止漏掉 disconnect 事件留下"幽灵"）
function markGhosts(room, now = Date.now()) {
  let changed = false;
  for (const p of room.players) {
    if (!p.offline && !io.sockets.sockets.has(p.id)) { p.offline = true; p.offlineSince = now; changed = true; }
  }
  return changed;
}

// 把一个连接接到某个座位上（重连、换页面、按名字找回都走这里）
function attachSocket(room, code, p, socket) {
  const wasAway = !!p.disconnected;
  const oldId = p.id;
  if (oldId !== socket.id) {
    socketRoom.delete(oldId);
    leaveVoice(room, code, oldId);
    const old = io.sockets.sockets.get(oldId);
    if (old) { old.emit('seat-taken', { code }); old.leave(code); }   // 旧页面还开着：告诉它这局已在别处继续
    p.id = socket.id;
  }
  p.offline = false;
  p.offlineSince = null;
  p.disconnected = false;
  socketRoom.set(socket.id, code);
  socket.join(code);
  room._emptySince = null;
  if (wasAway) addLog(room, `${p.name} 回来了`);
}

// 连接断开（不是主动离开）：只标记掉线，不跳回合、不写日志
function markOffline(socket) {
  const code = socketRoom.get(socket.id);
  if (!code) return null;
  socketRoom.delete(socket.id);
  const room = rooms.get(code);
  if (!room) return null;
  leaveVoice(room, code, socket.id);
  const p = room.players.find(pl => pl.id === socket.id);
  if (!p) return null;
  if (!p.offline) { p.offline = true; p.offlineSince = Date.now(); }
  broadcast(code);
  return { code, name: p.name };
}

// 结算投票：所有在局里、没退学、没暂时离开的人都同意就结算
function checkEndVote(room) {
  if (!room.endVote || room.finished) return false;
  const activePlayers = room.players.filter(pl => !pl.bankrupt && !pl.disconnected);
  const allConfirmed = activePlayers.every(pl => room.endVote.voters.includes(room.players.indexOf(pl)));
  if (!allConfirmed) return false;
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
  return true;
}

// 开局前：在场（没暂时离开）的人都准备好了、且至少 2 人，就自动开局
function readyToStart(room) {
  if (room.started) return false;
  const present = room.players.filter(pl => !pl.disconnected);
  return present.length >= 2 && present.every(pl => pl.ready);
}

// 将某个 socket 移出语音成员列表并通知房间内其他人
function leaveVoice(room, code, socketId) {
  if (!room || !Array.isArray(room.voice)) return;
  const i = room.voice.indexOf(socketId);
  if (i !== -1) {
    room.voice.splice(i, 1);
    io.to(code).emit('voice-peer-left', { id: socketId });
  }
}

// 主动离开（点了"离开"、或去建/进别的房间）：座位标为暂时离开，轮到他时直接跳过
function leaveRoom(socket) {
  const code = socketRoom.get(socket.id);
  if (!code) return;
  socketRoom.delete(socket.id);
  socket.leave(code);
  const room = rooms.get(code);
  if (!room) return;

  leaveVoice(room, code, socket.id);
  const idx = room.players.findIndex(p => p.id === socket.id);
  if (idx === -1) return;
  const p = room.players[idx];
  p.offline = true;
  p.offlineSince = p.offlineSince || Date.now();
  p.disconnected = true;
  if (room.started && !room.finished && room.currentTurn === idx) advanceTurn(room);

  addLog(room, `${p.name} 离开了房间`);
  broadcast(code);
  checkEndVote(room) && broadcast(code);
  if (readyToStart(room)) startGame(room, code);
}

// ===== Socket Events =====
function startGame(room, code) {
  if (room.started) return;
  // 暂时离开（掉线太久 / 主动离开）的人不进这局；刚闪断、还在缓冲期里的人照样带上
  room.players = room.players.filter(p => !p.disconnected);
  room.players.forEach((p, i) => p.color = COLORS[i % COLORS.length]);
  if (room.players.length < 2) return;
  room.started = true;
  room.startTime = Date.now();
  room.currentTurn = 0;
  room.turnSeq = 1;
  room.turnStartedAt = room.turnActiveAt = Date.now();
  stats.totalGames++;
  stats.gameStartLog.push(Date.now());
  const rh = room._rid ? findLast(stats.roomHistory, r => r.rid === room._rid) : null;
  if (rh) rh.gameStarted = true;
  room._gid = newId();
  const histEntry = {
    gid: room._gid, code, startTime: new Date().toISOString(),
    playerNames: room.players.map(p => p.name),
    playerRoles: room.players.map(p => ({ name: p.name, role: p.role, roleName: ROLES[p.role]?.name || '新生', roleEmoji: ROLES[p.role]?.emoji || '🎒' })),
  };
  stats.gameHistory.push(histEntry);
  if (stats.gameHistory.length > 500) stats.gameHistory = stats.gameHistory.slice(-500);
  saveStats();
  // Apply role starting money adjustments
  room.players.forEach(p => {
    const r = ROLES[p.role];
    if (r && r.startMoney) p.money += r.startMoney;
  });
  const roleInfo = room.players.map(p => `${ROLES[p.role]?.emoji||'🎒'} ${p.name}(${ROLES[p.role]?.name||'新生'})`).join('、');
  addLog(room, `游戏开始！${roleInfo}`);
  broadcast(code);
}

io.on('connection', (socket) => {
  stats.totalConnections++;
  if (!stats.connectionLog) stats.connectionLog = [];
  if (!stats.roomLog) stats.roomLog = [];
  if (!stats.gameStartLog) stats.gameStartLog = [];
  stats.connectionLog.push(Date.now());

  const connectedAt = Date.now();
  const hdrs = (socket.handshake && socket.handshake.headers) || {};
  const ip = hdrs['fly-client-ip'] || (socket.handshake && socket.handshake.address) || '';
  netlog({ ev: 'conn', sid: socket.id, rec: !!socket.recovered, tr: socket.conn?.transport?.name, edge: hdrs['fly-region'], ip, ua: uaFamily(hdrs['user-agent']), uaFull: clip(hdrs['user-agent'], 300) });

  // 页面重连时在握手里带上 房间号/名字/令牌：连上的同一刻就接回自己的座位
  // （只认这一个房间里令牌和名字都对上的座位；断线期间页面缓存的操作会在 join-room 之前到达，也能正常生效）
  const auth = (socket.handshake && socket.handshake.auth) || {};
  if (auth.code && auth.token && typeof auth.name === 'string') {
    const code = String(auth.code).toUpperCase().trim();
    const room = rooms.get(code);
    const seat = room && room.players.find(p => p.token === auth.token && p.name === auth.name);
    if (seat) { attachSocket(room, code, seat, socket); broadcast(code); }
  }

  // create-room: max 5 per 30s
  guarded(socket, 'create-room', 5, 30000, (payload) => {
    const { name, token, settings } = payload || {};
    leaveRoom(socket);
    const code = genCode();
    const room = {
      code, hostId: socket.id,
      players: [{ id: socket.id, token: token||null, name: name||'房主', color: COLORS[0], money: 10000, position: 0, inJail: false, jailTurns: 0, bankrupt: false, disconnected: false, offline: false, offlineSince: null, role: 'freshman', ready: false }],
      started: false, currentTurn: 0, round: 1, properties: {}, log: [], lastDice: [0,0], voice: [],
      settings: normalizeSettings(settings),
    };
    rooms.set(code, room);
    socketRoom.set(socket.id, code);
    socket.join(code);
    stats.totalRooms++;
    stats.roomLog.push(Date.now());
    // Record room history
    if (!stats.roomHistory) stats.roomHistory = [];
    const rh = { rid: newId(), code, createTime: new Date().toISOString(), playerNames: [name||'房主'], gameStarted: false };
    stats.roomHistory.push(rh);
    if (stats.roomHistory.length > 500) stats.roomHistory = stats.roomHistory.slice(-500);
    room._rid = rh.rid;
    addLog(room, `${name} 创建了房间`);
    broadcast(code);
  });

  // join-room: max 5 per 15s
  // 参数：code、name、token（这个页面的身份令牌），以及
  //   prevToken  新开的页面在本机记录里找到的上一个令牌（同一浏览器换了标签页 / 微信里重新点开链接）
  //   takeover   旧页面还连着时，用户确认"在这里继续"
  //   auto       页面自动发出的重进请求：失败时回 rejoin-failed，由页面安静地回到大厅，而不是弹窗
  guarded(socket, 'join-room', 5, 15000, (payload) => {
    const { token, prevToken, takeover, auto } = payload || {};
    const code = String((payload && payload.code) || '').toUpperCase().trim();
    const name = typeof (payload && payload.name) === 'string' ? payload.name : '';
    const fail = (reason, msg) => auto ? socket.emit('rejoin-failed', { reason, message: msg }) : socket.emit('error-msg', msg);
    const room = rooms.get(code);
    if (!room) return fail('room-gone', '房间不存在，请检查代码');

    // 1) 这个连接本来就坐在这个房间里（握手时已经接回座位 / 重复发送）
    const own = room.players.find(p => p.id === socket.id);
    if (own) { attachSocket(room, code, own, socket); broadcast(code); return; }

    leaveRoom(socket); // 从别的房间过来：先离开那边
    markGhosts(room);

    // 2) 同一个页面重连：令牌和名字都对上
    let seat = token ? room.players.find(p => p.token === token && p.name === name) : null;
    // 3) 同一个浏览器新开的页面：用本机记录里的上一个令牌找回；旧页面还连着就先问用户
    if (!seat && prevToken) {
      const prev = room.players.find(p => p.token === prevToken && p.name === name);
      if (prev) {
        if (!prev.offline && !takeover) return socket.emit('resume-conflict', { code, name });
        seat = prev;
      }
    }
    // 4) 按名字找回：只能接回掉线中 / 暂时离开的座位，在线的人不会被同名顶掉
    if (!seat) {
      const byName = room.players.find(p => p.name === name);
      if (byName) {
        if (byName.offline || byName.disconnected) seat = byName;
        else return fail('name-taken', '该昵称已被房间内玩家使用。如果是你换了手机或页面，等半分钟再试');
      }
    }
    if (seat) {
      if (token) seat.token = token;   // 以后以最新的页面为准
      attachSocket(room, code, seat, socket);
      broadcast(code);
      return;
    }

    if (room.started) return fail('started', '游戏已开始，无法加入');
    const activePlayers = room.players.filter(p => !p.disconnected).length;
    if (activePlayers >= 6) return fail('full', '房间已满（最多6人）');

    room.players.push({
      id: socket.id, token: token||null, name: name||`玩家${room.players.length+1}`, color: COLORS[room.players.length % COLORS.length],
      money: 10000, position: 0, inJail: false, jailTurns: 0, bankrupt: false, disconnected: false, offline: false, offlineSince: null, role: 'freshman', ready: false,
    });
    socketRoom.set(socket.id, code);
    socket.join(code);
    room._emptySince = null;
    stats.roomLog.push(Date.now());
    // Update room history
    const rh = room._rid ? findLast(stats.roomHistory, r => r.rid === room._rid) : null;
    if (rh) rh.playerNames.push(name||`玩家${room.players.length}`);
    addLog(room, `${name} 加入了房间`);
    broadcast(code);
  });

  guarded(socket, 'leave-room', 5, 10000, () => leaveRoom(socket));

  guarded(socket, 'select-role', 10, 5000, (payload) => {
    const role = payload && payload.role;
    if (!role || !ROLES[role]) return;
    const code = getCode(socket);
    const room = rooms.get(code);
    if (!room || room.started) return;
    const p = room.players.find(pl => pl.id === socket.id);
    if (p) { p.role = role; p.ready = false; broadcast(code); }
  });

  // toggle-ready
  // update-settings: 开局前房间内任何玩家都可以改玩法设置
  guarded(socket, 'update-settings', 10, 5000, (input) => {
    const code = getCode(socket);
    const room = rooms.get(code);
    if (!room) return;
    if (room.started) return socket.emit('error-msg', '游戏已开始，设置不能再修改');
    const p = room.players.find(pl => pl.id === socket.id);
    if (!p) return;
    const next = normalizeSettings({ ...(room.settings || {}), ...(input || {}) });
    const changes = Object.keys(SETTING_DEFS).filter(k => (room.settings || {})[k] !== next[k]);
    room.settings = next;
    if (changes.length) addLog(room, `⚙️ ${p.name} 修改了设置：${changes.map(k => `${SETTING_DEFS[k].label} → ${next[k] ? '开' : '关'}`).join('，')}`);
    broadcast(code);
  });

  guarded(socket, 'toggle-ready', 10, 5000, () => {
    const code = getCode(socket);
    const room = rooms.get(code);
    if (!room || room.started) return;
    const p = room.players.find(pl => pl.id === socket.id);
    if (!p) return;
    p.ready = !p.ready;
    markGhosts(room);
    addLog(room, `${p.name} ${p.ready ? '已准备' : '取消准备'}`);
    broadcast(code);
    // 在场的人都准备好了（≥2 人）就自动开局；刚闪断、已准备的人也会被带上
    if (readyToStart(room)) startGame(room, code);
  });

  // jail-action: pay bail or roll for doubles
  guarded(socket, 'jail-action', 5, 5000, ({ action }) => {
    const code = getCode(socket);
    const room = rooms.get(code);
    if (!room || !room.started) return;
    const ci = room.currentTurn;
    if (room.players[ci].id !== socket.id) return;
    touchTurn(room, socket);
    const p = room.players[ci];
    if (!p.inJail) return;
    if (p.inDebt) return socket.emit('error-msg', '资金为负，请先卖出地产还债');

    if (action === 'pay') {
      p.money -= 500;
      p.inJail = false;
      p.jailTurns = 0;
      addLog(room, `${p.name} 交了 ¥500 补考费，通过补考！`);
      checkBankruptcy(room, code);
      broadcast(code);
    } else if (action === 'roll') {
      const d1 = Math.floor(Math.random()*6)+1;
      const d2 = Math.floor(Math.random()*6)+1;
      room.lastDice = [d1, d2];
      if (d1 === d2) {
        p.inJail = false;
        p.jailTurns = 0;
        const roleBonus = ROLES[p.role]?.diceBonus || 0;
        const total = Math.max(d1 + d2 + roleBonus, 1);
        const bonusStr = roleBonus > 0 ? `+${roleBonus}` : roleBonus < 0 ? `${roleBonus}` : '';
        const oldPos = p.position;
        p.position = (p.position + total) % BOARD_SIZE;
        addLog(room, `${p.name} 掷出 ${d1}+${d2}${bonusStr}=${total} (双数！)，补考通过！前进到「${NAMES[p.position]}」`);
        if (room.settings?.passGoMoney ? p.position < oldPos : p.position === 0) {
          const goMoney = p.role === 'alumni' ? 2000 : 1500;
          p.money += goMoney;
          addLog(room, `${p.name} ${room.settings?.passGoMoney ? '经过起点' : '停在起点'} +¥${goMoney.toLocaleString()}`);
        }
        room.rolled = true;
        io.to(code).emit('dice-rolled', { dice:[d1,d2], playerIndex:ci, from:oldPos, to:p.position, jailBreak: true });
      } else {
        p.jailTurns++;
        const attempt = p.jailTurns;
        const forced = attempt >= 3;
        if (forced) {
          p.money -= 500;
          p.inJail = false;
          p.jailTurns = 0;
          addLog(room, `${p.name} 掷出 ${d1}+${d2}，第三次未掷出双数，强制交 ¥500 补考费`);
          checkBankruptcy(room, code);
        } else {
          addLog(room, `${p.name} 掷出 ${d1}+${d2}，未掷出双数，继续补考...`);
        }
        room.rolled = true;
        io.to(code).emit('jail-roll-failed', { dice:[d1,d2], playerIndex:ci, jailTurns: attempt, forced });
        advanceTurn(room);
      }
      broadcast(code);
    }
  });

  // roll-dice: max 5 per 5s
  guarded(socket, 'roll-dice', 5, 5000, () => {
    const code = getCode(socket);
    const room = rooms.get(code);
    if (!room || !room.started) return;
    const ci = room.currentTurn;
    if (room.players[ci].id !== socket.id) return;
    touchTurn(room, socket);
    if (room.rolled) return;
    if (room.players[ci].inJail) return;
    if (room.players[ci].inDebt) return socket.emit('error-msg', '资金为负，请先卖出地产还债');
    room.rolled = true;
    const d1 = Math.floor(Math.random()*6)+1;
    const d2 = Math.floor(Math.random()*6)+1;
    room.lastDice = [d1, d2];
    const p = room.players[ci];
    const roleBonus = ROLES[p.role]?.diceBonus || 0;
    const total = Math.max(d1 + d2 + roleBonus, 1);
    const oldPos = p.position;
    p.position = (p.position + total) % BOARD_SIZE;
    const sn = NAMES[p.position]||'?';
    const dbl = d1===d2 ? ' (双数！)' : '';
    const bonusStr = roleBonus > 0 ? `+${roleBonus}` : roleBonus < 0 ? `${roleBonus}` : '';
    addLog(room, `${p.name} 掷出 ${d1}+${d2}${bonusStr}=${total}${dbl}，到达「${sn}」`);

    // Go money: setting passGoMoney=true → 经过即给；false → 只有正好停在起点才给
    if (room.settings?.passGoMoney ? p.position < oldPos : p.position === 0) {
      const goMoney = p.role === 'alumni' ? 2000 : 1500;
      p.money += goMoney;
      addLog(room, `${p.name} ${room.settings?.passGoMoney ? '经过起点' : '停在起点'} +¥${goMoney.toLocaleString()}`);
    }

    // Landing effects：税/补考/情人坡 → 抽卡（卡牌可能再次移动并递归落地）→ 租金
    room.lastCard = null;
    const landed = p.position;
    applyLanding(room, code, ci, { total, depth: 0, rentMult: 1 });

    checkBankruptcy(room, code);
    io.to(code).emit('dice-rolled', { dice:[d1,d2], playerIndex:ci, from:oldPos, to:p.position, landed });
    broadcast(code);
  });

  // end-turn: max 5 per 5s
  guarded(socket, 'end-turn', 5, 5000, () => {
    const code = getCode(socket);
    const room = rooms.get(code);
    if (!room || !room.started) return;
    if (room.players[room.currentTurn]?.id !== socket.id) return;
    if (room.players[room.currentTurn].inDebt) return socket.emit('error-msg', '资金为负，请先卖出地产还债再结束回合');
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
    touchTurn(room, socket);
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
    touchTurn(room, socket);
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

  // sell-property: 卖出自己的地产（地价 50% + 建筑 50%），任何时候都可以，主要用于还债
  guarded(socket, 'sell-property', 10, 5000, ({ spaceId }) => {
    const code = getCode(socket);
    const room = rooms.get(code);
    if (!room || !room.started || room.finished) return;
    if (room.settings?.sellProperty === false) return socket.emit('error-msg', '本局设置不允许卖地产');
    const pi = room.players.findIndex(pl => pl.id === socket.id);
    if (pi === -1) return;
    touchTurn(room, socket);
    const p = room.players[pi];
    const prop = room.properties[spaceId];
    if (!prop || prop.owner !== pi || p.bankrupt) return;
    const value = sellValue(spaceId, prop.level);
    delete room.properties[spaceId];
    p.money += value;
    addLog(room, `${p.name} 卖出了「${NAMES[spaceId]}」${prop.level > 0 ? `（等级${prop.level}）` : ''}，收回 ¥${value.toLocaleString()}`);
    if (p.inDebt && p.money > 0) { p.inDebt = false; addLog(room, `✅ ${p.name} 已还清欠款`); }
    broadcast(code);
  });

  guarded(socket, 'transfer', 5, 5000, ({ fromIndex, toIndex, amount }) => {
    const code = getCode(socket);
    const room = rooms.get(code);
    if (!room || !room.started) return;
    if (fromIndex===toIndex || !amount || amount<=0) return;
    touchTurn(room, socket);
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
    touchTurn(room, socket);
    room.players[playerIndex].money += amount;
    addLog(room, `${room.players[playerIndex].name} ${note||(amount>0?'收入':'支出')} ¥${Math.abs(amount).toLocaleString()}`);
    checkBankruptcy(room, code);
    broadcast(code);
  });

  guarded(socket, 'quick-action', 5, 5000, ({ action }) => {
    const code = getCode(socket);
    const room = rooms.get(code);
    if (!room || !room.started) return;
    touchTurn(room, socket);
    const p = room.players[room.currentTurn];
    switch(action) {
      case 'passgo': { const gm=p.role==='alumni'?2000:1500; p.money+=gm; addLog(room,`${p.name} 起点奖励 +¥${gm.toLocaleString()}`); break; }
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

    // 在局里的人（掉线中的也算，暂时离开的不算）都同意了就结算
    checkEndVote(room);
    broadcast(code);
  });

  // 跳过当前玩家的回合：他掉线满 5 秒，或在线但 60 秒没操作时，其他任何人都可以点
  guarded(socket, 'skip-turn', 5, 5000, (payload) => {
    const code = getCode(socket);
    const room = rooms.get(code);
    if (!room || !room.started || room.finished) return;
    const me = room.players.find(p => p.id === socket.id);
    if (!me || me.bankrupt) return;
    if (payload && payload.seq !== undefined && payload.seq !== room.turnSeq) return;   // 回合已经变了（比如别人刚点过）
    const cur = room.players[room.currentTurn];
    if (!cur || cur === me) return;
    if (!canSkip(room, Date.now())) return socket.emit('error-msg', '他刚刚还在操作，再等一下');
    skipTurn(room, me.name);
    broadcast(code);
  });

  // ===== 语音聊天信令 =====
  guarded(socket, 'voice-join', 10, 10000, () => {
    const code = getCode(socket);
    const room = rooms.get(code);
    if (!room) return;
    if (!Array.isArray(room.voice)) room.voice = [];
    // 清掉 socket 已不存在的成员
    room.voice = room.voice.filter(id => io.sockets.sockets.has(id));
    const existing = room.voice.filter(id => id !== socket.id);
    if (!room.voice.includes(socket.id)) room.voice.push(socket.id);
    // 告诉新成员当前已有哪些人，由新成员向他们发起连接
    socket.emit('voice-members', { ids: existing });
    broadcast(code);
  });

  guarded(socket, 'voice-leave', 10, 10000, () => {
    const code = getCode(socket);
    const room = rooms.get(code);
    if (!room) return;
    leaveVoice(room, code, socket.id);
    broadcast(code);
  });

  // WebRTC 信令转发（offer/answer/ICE，频率较高）
  guarded(socket, 'voice-signal', 300, 10000, (payload) => {
    const { to, data } = payload || {};
    const code = getCode(socket);
    if (!code || !to || !data) return;
    if (socketRoom.get(to) !== code) return; // 只允许同房间内转发
    io.to(to).emit('voice-signal', { from: socket.id, data });
  });

  // chat: max 5 per 10s (stricter)
  guarded(socket, 'send-chat', 5, 10000, ({ text }) => {
    const code = getCode(socket);
    const room = rooms.get(code);
    if (!room || !text) return;
    const player = room.players.find(p=>p.id===socket.id);
    io.to(code).emit('chat-msg', { from: player?.name||'???', text, color: player?.color||'#999' });
  });

  // 连接断开（网络闪断、锁屏、关页面都会走这里）：只标记掉线，等他回来
  socket.on('disconnect', safe('disconnect', (reason) => {
    const info = markOffline(socket);
    netlog({ ev: 'disc', sid: socket.id, reason, sec: Math.round((Date.now() - connectedAt) / 1000), room: info && info.code, name: info && info.name, ip });
  }));
});

// 每秒检查所有房间：掉线太久的标为暂时离开、该自动跳过的回合跳过、全员离线够久的房间删除
setInterval(safe('tick', () => {
  const now = Date.now();
  for (const [code, room] of rooms) {
    let changed = markGhosts(room, now);
    const awayAfter = room.started ? AWAY_MS : LOBBY_AWAY_MS;
    let someoneLeft = false;
    for (const p of room.players) {
      if (p.offline && !p.disconnected && now - p.offlineSince >= awayAfter) {
        p.disconnected = true; changed = true; someoneLeft = true;
        if (room.started && !room.finished && !p.bankrupt) addLog(room, `📴 ${p.name} 掉线超过 ${Math.round(AWAY_MS / 60000)} 分钟，先跳过他的回合（回来后可以接着玩）`);
      }
    }
    if (room.started && !room.finished) {
      const cur = room.players[room.currentTurn];
      if (cur && (cur.bankrupt || cur.disconnected) && hasOtherActive(room)) {
        advanceTurn(room); changed = true;
      } else if (cur && cur.offline && someoneWaiting(room) && now - waitStart(room, cur) >= OFFLINE_NOTICE_MS + AUTO_SKIP_MS) {
        skipTurn(room, null); changed = true;
      }
      if (someoneLeft && checkEndVote(room)) changed = true;
    } else if (!room.started && someoneLeft && readyToStart(room)) {
      startGame(room, code);   // 剩下的人都准备好了：不用再等掉线的人
    }
    // 全员离线：进行中的对局保留 30 分钟，其他保留 15 分钟，期间有人回来就继续
    const empty = room.players.length === 0 || room.players.every(p => p.offline);
    if (!empty) room._emptySince = null;
    else {
      if (!room._emptySince) room._emptySince = now;
      const keep = (room.started && !room.finished) ? KEEP_STARTED_MS : KEEP_IDLE_MS;
      if (now - room._emptySince >= keep) { finalizeGame(room); rooms.delete(code); continue; }
    }
    if (changed) broadcast(code);
  }
}), 1000);

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`\n  清华大富翁 服务器已启动`);
  console.log(`  本机访问: http://localhost:${PORT}`);
  console.log(`  局域网: http://<你的IP>:${PORT}\n`);
});
