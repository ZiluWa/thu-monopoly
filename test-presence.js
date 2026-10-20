// 回归测试：掉线容忍（2026-10 掉线排查后加的规则）
// 用缩短的时长跑：提示 1s、自动跳过再等 2s、没操作 3s、对局中掉线 6s 算暂时离开、开局前 2s、全员离线保留 4s / 3s
// 运行: node test-presence.js  (需要 devDependency socket.io-client)
const { spawn } = require('child_process');
const path = require('path');
const fs = require('fs');
const os = require('os');
const { io } = require('socket.io-client');

const PORT = 3124;
const URL = `http://localhost:${PORT}`;
const T = { OFFLINE_NOTICE_MS: 1000, AUTO_SKIP_MS: 2000, IDLE_SKIP_MS: 3000, AWAY_MS: 6000, LOBBY_AWAY_MS: 2000, KEEP_STARTED_MS: 4000, KEEP_IDLE_MS: 3000 };
const sleep = ms => new Promise(r => setTimeout(r, ms));
let failures = 0, passes = 0;
function check(cond, msg, extra) {
  if (cond) { passes++; console.log(`  ✅ ${msg}`); }
  else { failures++; console.log(`  ❌ ${msg}${extra !== undefined ? '  →  ' + JSON.stringify(extra) : ''}`); }
}
async function waitFor(cond, ms = 5000) { const t0 = Date.now(); while (Date.now() - t0 < ms) { if (cond()) return true; await sleep(30); } return false; }

// 模拟页面：每次连上都带 auto:true 重进房间；记录收到的各种事件
const clients = [];
function client(name, token, opts = {}) {
  const s = io(URL, { transports: ['websocket', 'polling'], reconnectionDelay: 200, reconnectionDelayMax: 400, forceNew: true });
  const c = { s, name, token, code: null, room: null, errors: [], failed: [], conflicts: [], taken: 0, rejoin: opts.rejoin !== false };
  s.on('connect', () => { if (c.code && c.rejoin) s.emit('join-room', { code: c.code, name: c.name, token: c.token, auto: true }); });
  s.on('room-update', r => { c.room = r; if (r && r.code) c.code = r.code; });
  s.on('error-msg', m => c.errors.push(m));
  s.on('rejoin-failed', x => c.failed.push(x));
  s.on('resume-conflict', x => c.conflicts.push(x));
  s.on('seat-taken', () => { c.taken++; c.code = null; });
  clients.push(c);
  return c;
}
const me = (c) => c.room && c.room.players.find(p => p.name === c.name);
const turnName = (c) => c.room.players[c.room.currentTurn].name;
const logHas = (c, re) => (c.room.log || []).some(l => re.test(l));
// 断网且不自动重连（模拟锁屏 / 关页面）
async function goOffline(c) { c.s.io.reconnection(false); c.s.io.engine.close(); await waitFor(() => !c.s.connected); }
async function comeBack(c) { c.s.io.reconnection(true); c.s.connect(); await waitFor(() => c.s.connected); await sleep(300); }

async function newRoom(names, { start = true } = {}) {
  const cs = names.map(n => client(n, 'tok-' + n + '-' + Math.random().toString(36).slice(2, 6)));
  await waitFor(() => cs.every(c => c.s.connected));
  cs[0].s.emit('create-room', { name: cs[0].name, token: cs[0].token });
  await waitFor(() => cs[0].code);
  for (const c of cs.slice(1)) { c.s.emit('join-room', { code: cs[0].code, name: c.name, token: c.token }); }
  await waitFor(() => cs.every(c => c.room && c.room.players.length === names.length));
  if (start) {
    for (const c of cs) { c.s.emit('toggle-ready'); await sleep(60); }
    await waitFor(() => cs.every(c => c.room.started));
  }
  return cs;
}

async function main() {
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'monopoly-test-'));
  // 预先放 500 条对局记录，让记录数组处在上限，验证"满 500 条后结算写错局"的问题已修
  fs.writeFileSync(path.join(dataDir, 'stats.json'), JSON.stringify({
    totalRooms: 0, totalGames: 0, totalConnections: 0, connectionLog: [], roomLog: [], gameStartLog: [], roomHistory: [],
    gameHistory: Array.from({ length: 500 }, (_, i) => ({ code: 'OLD' + i, startTime: new Date(Date.now() - 1e9).toISOString(), endTime: new Date(Date.now() - 1e9).toISOString(), playerNames: [] })),
  }));
  const env = { ...process.env, PORT: String(PORT), DATA_DIR: dataDir };
  for (const [k, v] of Object.entries(T)) env[k] = String(v);
  const server = spawn(process.execPath, [path.join(__dirname, 'server.js')], { env, stdio: ['ignore', 'pipe', 'pipe'] });
  let serverErr = '';
  server.stderr.on('data', d => { serverErr += d; });
  try {
    await sleep(1200);

    console.log('\n[1] 轮到 A 时网络闪断 1 秒：不跳回合、不写日志');
    {
      const [a, b] = await newRoom(['A1', 'B1']);
      check(turnName(b) === 'A1', '开局轮到 A');
      a.s.io.engine.close();
      await waitFor(() => !a.s.connected, 2000); await waitFor(() => a.s.connected, 5000); await sleep(400);
      check(turnName(b) === 'A1', '闪断恢复后仍是 A 的回合', turnName(b));
      check(!logHas(b, /离开|掉线|跳过/), '日志里没有"离开/掉线/跳过"', b.room.log.slice(-3));
      check(!me(a).offline && !me(a).disconnected, 'A 状态正常');
      // 正常玩法：非当前玩家掷骰无效，当前玩家掷骰后结束回合轮到 B
      b.s.emit('roll-dice'); await sleep(300);
      check(!b.room.rolled && turnName(b) === 'A1', '不是自己回合掷骰无效');
      a.s.emit('roll-dice'); await waitFor(() => b.room.rolled);
      a.s.emit('end-turn'); await waitFor(() => turnName(b) === 'B1');
      check(turnName(b) === 'B1', 'A 掷骰并结束回合后轮到 B');
    }

    console.log('\n[2] 轮到 A 时 A 掉线：1 秒内不能跳过，之后 B 可以点跳过');
    {
      const [a, b] = await newRoom(['A2', 'B2']);
      await goOffline(a);
      b.s.emit('skip-turn', { seq: b.room.turnSeq }); await sleep(300);
      check(turnName(b) === 'A2' && b.errors.length === 1, '掉线不到提示时间，跳过被拒绝', { turn: turnName(b), errors: b.errors });
      await sleep(T.OFFLINE_NOTICE_MS);
      b.s.emit('skip-turn', { seq: b.room.turnSeq }); await waitFor(() => turnName(b) === 'B2', 2000);
      check(turnName(b) === 'B2', '提示出现后 B 跳过成功，轮到 B');
      check(logHas(b, /B2 跳过了 A2 的回合（掉线中）/), '日志写明谁跳过了谁');
      check(me(b) && b.room.players.find(p => p.name === 'A2').disconnected, 'A 被标为暂时离开');
      const seq = b.room.turnSeq;
      b.s.emit('skip-turn', { seq: seq - 1 }); await sleep(300);
      check(b.room.turnSeq === seq, '用过期的回合号再点跳过无效（防止两人同时点跳过两回合）');
      b.s.emit('roll-dice'); await waitFor(() => b.room.rolled); b.s.emit('end-turn'); await sleep(400);
      check(turnName(b) === 'B2', 'A 暂时离开期间，B 结束回合后直接又轮到 B（不再等 A）');
      await comeBack(a);
      check(logHas(b, /A2 回来了/) && !b.room.players.find(p => p.name === 'A2').disconnected, 'A 连回来后恢复，日志"回来了"');
      b.s.emit('roll-dice'); await waitFor(() => b.room.rolled); b.s.emit('end-turn'); await waitFor(() => turnName(b) === 'A2', 2000);
      check(turnName(b) === 'A2', '之后正常轮到 A');
    }

    console.log('\n[3] A 掉线不回来：提示时间 + 倒计时到了自动跳过');
    {
      const [a, b] = await newRoom(['A3', 'B3']);
      await goOffline(a);
      await sleep(T.OFFLINE_NOTICE_MS + T.AUTO_SKIP_MS - 600);
      check(turnName(b) === 'A3', '倒计时结束前还在等 A');
      await waitFor(() => turnName(b) === 'B3', 3000);
      check(turnName(b) === 'B3', '倒计时到了自动跳过，轮到 B');
      check(logHas(b, /A3 掉线没回来，自动跳过本回合/), '日志写明自动跳过');
    }

    console.log('\n[3b] 两个人都掉线：没人在等，不自动跳过');
    {
      const [a, b] = await newRoom(['A3b', 'B3b']);
      await goOffline(a); await goOffline(b);
      await sleep(T.OFFLINE_NOTICE_MS + T.AUTO_SKIP_MS + 1000);
      await comeBack(a);
      check(turnName(a) === 'A3b' && !logHas(a, /跳过/), '大家都不在时不自动跳过，A 回来还是自己的回合', a.room.log.slice(-2));
    }

    console.log('\n[4] A 在线但一直不操作：没到时间不能跳过，到了可以跳过；A 不算离开');
    {
      const [a, b] = await newRoom(['A4', 'B4']);
      b.s.emit('skip-turn', { seq: b.room.turnSeq }); await sleep(300);
      check(turnName(b) === 'A4', '刚轮到 A 时不能跳过');
      await sleep(T.IDLE_SKIP_MS);
      b.s.emit('skip-turn', { seq: b.room.turnSeq }); await waitFor(() => turnName(b) === 'B4', 2000);
      check(turnName(b) === 'B4', '没操作满时间后 B 跳过成功');
      check(logHas(b, /B4 跳过了 A4 的回合（长时间没操作）/), '日志写明原因是没操作');
      check(!b.room.players.find(p => p.name === 'A4').disconnected, 'A 没有被标为离开');
      b.s.emit('roll-dice'); await waitFor(() => b.room.rolled); b.s.emit('end-turn'); await waitFor(() => turnName(b) === 'A4', 2000);
      check(turnName(b) === 'A4', '下一轮照常轮到 A');
    }

    console.log('\n[5] 开局前：已准备的 C 刚好掉线，A、B 点准备 → C 照样进这局');
    {
      const [a, b, c] = await newRoom(['A5', 'B5', 'C5'], { start: false });
      c.s.emit('toggle-ready'); await sleep(200);
      await goOffline(c); await sleep(200);
      a.s.emit('toggle-ready'); await sleep(100); b.s.emit('toggle-ready');
      await waitFor(() => a.room.started, 2000);
      check(a.room.started && a.room.players.map(p => p.name).join() === 'A5,B5,C5', '开局了，C 在这局里', a.room.players.map(p => p.name));
      await comeBack(c);
      check(c.room && c.room.started && me(c) && !me(c).offline, 'C 连回来直接回到对局', c.failed);
    }

    console.log('\n[6] 开局前：没准备的 C 掉线（关了页面）→ 2 秒后不再挡着，A、B 自动开局');
    {
      const [a, b, c] = await newRoom(['A6', 'B6', 'C6'], { start: false });
      a.s.emit('toggle-ready'); await sleep(100); b.s.emit('toggle-ready'); await sleep(300);
      check(!a.room.started, 'C 还在、没准备时不开局');
      await goOffline(c);
      await waitFor(() => a.room.started, T.LOBBY_AWAY_MS + 2500);
      check(a.room.started && a.room.players.map(p => p.name).join() === 'A6,B6', 'C 掉线超过 2 秒后自动开局（A、B 两人）', a.room.players.map(p => p.name));
    }

    console.log('\n[7] 新开页面（换了标签页 / 微信重新点开）：凭本机记录接回座位；旧页面还连着时先问');
    {
      const [a, b] = await newRoom(['A7', 'B7']);
      const code = a.code, oldToken = a.token;
      a.rejoin = false; a.s.close(); await sleep(300);
      const a2 = client('A7', 'tok-new-page-1');
      await waitFor(() => a2.s.connected);
      a2.s.emit('join-room', { code, name: 'A7', token: a2.token, prevToken: oldToken, auto: true });
      await waitFor(() => a2.room, 2000);
      check(a2.room && me(a2) && me(a2).id === a2.s.id && a2.room.players.length === 2, '旧页面已关：新页面直接接回原座位（没有多出一个玩家）');
      const a3 = client('A7', 'tok-new-page-2');
      await waitFor(() => a3.s.connected);
      a3.s.emit('join-room', { code, name: 'A7', token: a3.token, prevToken: a2.token, auto: true });
      await waitFor(() => a3.conflicts.length, 2000);
      check(a3.conflicts.length === 1 && !a3.room, '旧页面还连着：新页面收到"在这里继续？"的询问，不直接抢座位');
      a3.s.emit('join-room', { code, name: 'A7', token: a3.token, prevToken: a2.token, takeover: true, auto: true });
      await waitFor(() => a3.room && a2.taken, 2000);
      check(a3.room && me(a3).id === a3.s.id && a2.taken === 1, '确认后新页面接管，旧页面收到"已在别处继续"');
      b.s.emit('roll-dice'); await sleep(200);
      a2.s.emit('roll-dice'); await sleep(300);
      check(!b.room.rolled, '被接管的旧页面发的操作无效');
    }

    console.log('\n[8] 按名字找回：座位掉线时可以；对方在线时不行');
    {
      const [a, b] = await newRoom(['A8', 'B8']);
      const x = client('B8', 'tok-other-device');
      await waitFor(() => x.s.connected);
      x.s.emit('join-room', { code: a.code, name: 'B8', token: x.token });
      await sleep(400);
      check(x.errors.length === 1 && !x.room, 'B 在线时，别人用 B 的名字加入被拒绝', x.errors);
      await goOffline(b);
      await waitFor(() => a.room.players.find(p => p.name === 'B8').offline, 2000);   // 等服务器也知道 B 掉线了
      x.s.emit('join-room', { code: a.code, name: 'B8', token: x.token });
      await waitFor(() => x.room, 2000);
      check(x.room && me(x).id === x.s.id && x.room.players.length === 2, 'B 掉线后，换设备用同名可以接回座位', { errors: x.errors, hasRoom: !!x.room });
    }

    console.log('\n[9] 全员离线：保留期内回来能接着玩，过了保留期房间才删除');
    {
      const [a, b] = await newRoom(['A9', 'B9']);
      const code = a.code;
      await goOffline(a); await goOffline(b);
      await sleep(T.KEEP_STARTED_MS - 1500);
      await comeBack(a);
      check(a.room && a.room.started && a.failed.length === 0, `离线 ${(T.KEEP_STARTED_MS - 1500) / 1000}s 后回来，房间还在`);
      await goOffline(a);
      await sleep(T.KEEP_STARTED_MS + 1500);
      const z = client('A9', a.token);
      await waitFor(() => z.s.connected);
      z.s.emit('join-room', { code, name: 'A9', token: a.token, auto: true });
      await waitFor(() => z.failed.length, 2000);
      check(z.failed[0] && z.failed[0].reason === 'room-gone' && z.errors.length === 0, '过了保留期房间删除；自动重进收到"房间没了"（不弹报错）', z.failed);
    }

    console.log('\n[10] 结算投票：掉线中的人也要投；他暂时离开后，其余人同意即结算');
    {
      const [a, b, c] = await newRoom(['A10', 'B10', 'C10']);
      await goOffline(c);
      a.s.emit('end-game-vote', { confirm: true }); await sleep(150);
      b.s.emit('end-game-vote', { confirm: true }); await sleep(400);
      check(!a.room.finished, 'C 只是掉线（还没算离开）时不结算');
      await waitFor(() => a.room.finished, T.AWAY_MS + 2500);
      check(a.room.finished && a.room.settled, 'C 掉线超过时限算暂时离开后，自动完成结算');
    }

    console.log('\n[11] 异常数据不会让服务器崩溃');
    {
      const [a] = await newRoom(['A11', 'B11']);
      for (const ev of ['jail-action', 'select-role', 'buy-property', 'upgrade-property', 'sell-property', 'end-game-vote', 'send-chat', 'join-room', 'create-room', 'skip-turn', 'transfer', 'adjust-money', 'quick-action']) a.s.emit(ev);
      a.s.emit('adjust-money', { amount: 5 });
      await sleep(500);
      const y = client('Y11', 'tok-y');
      await waitFor(() => y.s.connected);
      y.s.emit('create-room', { name: 'Y11', token: y.token });
      await waitFor(() => y.room, 2000);
      check(!!y.room && server.exitCode === null, '发了一堆缺参数的事件后，服务器照常工作');
    }

    console.log('\n[12] 对局记录：记录满 500 条时，两局交错进行，各自的结算写到自己的记录上');
    {
      const [a1, b1] = await newRoom(['G1A', 'G1B']);
      const [a2, b2] = await newRoom(['G2A', 'G2B']);
      a1.s.emit('end-game-vote', { confirm: true }); await sleep(100); b1.s.emit('end-game-vote', { confirm: true });
      await waitFor(() => a1.room.finished, 2000);
      await sleep(300);
      const res = await fetch(`${URL}/api/stats?pw=monopoly2024`).then(r => r.json());
      const g1 = res.gameHistory.find(g => g.code === a1.code), g2 = res.gameHistory.find(g => g.code === a2.code);
      check(g1 && g1.endTime && g1.settled, '先结束的那局有结束时间', g1);
      check(g2 && !g2.endTime, '还在进行的那局没有被误写', g2);
    }

    console.log('\n[14] 换过房间的人网络闪断：只回到现在的房间，不会被拉回旧房间');
    {
      const [a, b] = await newRoom(['A14', 'B14'], { start: false });
      const oldCode = a.code;
      a.s.emit('create-room', { name: 'A14', token: a.token });          // A 离开旧房间、新建一个
      await waitFor(() => a.code && a.code !== oldCode);
      const newCode = a.code;
      const c = client('C14', 'tok-c14'); await waitFor(() => c.s.connected);
      c.s.emit('join-room', { code: newCode, name: 'C14', token: c.token });
      await waitFor(() => c.room && c.room.players.length === 2);
      const seen = [];
      a.s.on('room-update', r => seen.push(r.code));
      a.s.io.engine.close();                                              // 闪断，自动重连
      await waitFor(() => !a.s.connected, 2000); await waitFor(() => a.s.connected, 5000); await sleep(800);
      b.s.emit('toggle-ready'); await sleep(600);                         // 旧房间里有动静
      check(!seen.includes(oldCode) && a.code === newCode, '重连后没有收到旧房间的画面，仍在新房间', { seen, now: a.code, oldCode, newCode });
      check(b.room.players.find(p => p.name === 'A14').disconnected, '旧房间里 A 仍是"已离开"，没被错误地拉回去');
    }

    console.log('\n[15] 手机切网络时新旧两条连接重叠：旧连接断开后，新连接照常能收能发');
    {
      const [a, b] = await newRoom(['A15', 'B15'], { start: false });
      const code = a.code;
      a.s.io.engine.close();                                              // 先闪断一次（服务器留下可恢复的会话）
      await waitFor(() => !a.s.connected, 2000); await waitFor(() => a.s.connected, 5000); await sleep(500);
      // 模拟手机换了网络：新连接带着旧会话号连上来，此时服务器上的旧连接还没断
      const a2 = io(URL, { transports: ['websocket'], forceNew: true, autoConnect: false, reconnection: false,
                           auth: { code, name: 'A15', token: a.token } });
      a2._pid = a.s._pid; a2._lastOffset = a.s._lastOffset;
      let a2room = null; a2.on('room-update', r => a2room = r);
      a2.connect(); await waitFor(() => a2.connected, 3000);
      a2.emit('join-room', { code, name: 'A15', token: a.token, auto: true });
      await sleep(500);
      a.rejoin = false; a.s.close(); await sleep(600);                    // 旧连接这时才断
      a2room = null;
      b.s.emit('toggle-ready'); await sleep(600);
      check(!!a2room, '新连接还能收到房间更新（不是"僵尸"连接）');
      a2.emit('toggle-ready'); await sleep(600);
      const meA = b.room.players.find(p => p.name === 'A15');
      check(meA && !meA.offline && meA.ready, '新连接的操作服务器照收，别人看到他在线', meA && { offline: meA.offline, ready: meA.ready });
      a2.close();
    }

    console.log('\n[13] 连接日志');
    {
      const log = await fetch(`${URL}/api/netlog?pw=monopoly2024&n=5000`).then(r => r.text());
      const lines = log.split('\n').filter(Boolean).map(l => JSON.parse(l));
      check(lines.some(l => l.ev === 'conn' && l.ua) && lines.some(l => l.ev === 'disc' && l.reason && l.room), '记录了连上/断开（含断开原因、房间、浏览器类型）', lines.slice(-2));
      const denied = await fetch(`${URL}/api/netlog`).then(r => r.status);
      check(denied === 401, '没有管理密码看不了连接日志');
    }
  } finally {
    clients.forEach(c => c.s.close());
    server.kill();
  }
  if (serverErr.trim()) console.log('\n服务器错误输出（异常数据测试会触发几条，属正常）:\n' + serverErr.split('\n').filter(l => l.startsWith('[err]')).slice(0, 5).join('\n'));
  console.log(`\n${failures ? '❌' : '✅'} 通过 ${passes} 项，失败 ${failures} 项`);
  process.exitCode = failures ? 1 : 0;
}

main().catch(e => { console.error('❌ 测试出错:', e); process.exitCode = 1; });
