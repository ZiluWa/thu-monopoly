// 回归测试：断线重连不应产生重复玩家
// 模拟手机端网络闪断（connectionStateRecovery 恢复同一 socket.id 的场景）：
// 强制关闭底层 engine → socket.io-client 自动重连 → 页面逻辑在 connect 时重发 join-room。
// 运行: node test-reconnect.js  (需要 devDependency socket.io-client)
const { spawn } = require('child_process');
const path = require('path');
const { io } = require('socket.io-client');

const PORT = 3123;
const URL = `http://localhost:${PORT}`;
const sleep = ms => new Promise(r => setTimeout(r, ms));

function makeClient(name, token) {
  const socket = io(URL, {
    reconnection: true,
    reconnectionDelay: 300,
    reconnectionDelayMax: 500,
    transports: ['websocket', 'polling'],
  });
  const c = { socket, name, token: token || null, code: null, lastRoom: null, connects: 0 };
  socket.on('connect', () => {
    c.connects++;
    // 模拟 public/index.html 的自动重进房逻辑
    if (c.connects > 1 && c.code) socket.emit('join-room', { code: c.code, name, token: c.token });
  });
  socket.on('room-update', r => { c.lastRoom = r; if (r && r.code) c.code = r.code; });
  socket.on('error-msg', m => { c.lastError = m; });
  return c;
}

async function waitFor(cond, timeoutMs, what) {
  const t0 = Date.now();
  while (!cond()) {
    if (Date.now() - t0 > timeoutMs) throw new Error(`超时等待: ${what}`);
    await sleep(50);
  }
}

async function main() {
  const server = spawn(process.execPath, [path.join(__dirname, 'server.js')], {
    env: { ...process.env, PORT: String(PORT) },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  server.stderr.on('data', d => process.stderr.write(`[server] ${d}`));
  try {
    await sleep(1200); // 等服务器启动

    // A 创建房间，B 加入 —— 两名真实玩家
    const A = makeClient('美女');
    await waitFor(() => A.socket.connected, 5000, 'A 连接');
    A.socket.emit('create-room', { name: '美女' });
    await waitFor(() => A.code, 5000, 'A 拿到房间码');

    const B = makeClient('总统');
    await waitFor(() => B.socket.connected, 5000, 'B 连接');
    B.code = A.code;
    B.socket.emit('join-room', { code: A.code, name: '总统' });
    await waitFor(() => B.lastRoom && B.lastRoom.players.length === 2, 5000, 'B 加入房间');
    console.log('初始状态: 2 名玩家 ✓');

    // 模拟 A 的手机网络闪断 4 次（每次底层传输断开，客户端自动重连）
    for (let i = 1; i <= 4; i++) {
      const prev = A.connects;
      A.socket.io.engine.close();
      await waitFor(() => A.connects > prev && A.socket.connected, 8000, `A 第${i}次重连`);
      await sleep(600); // 等 join-room 往返 + 广播
      const players = (B.lastRoom || {}).players || [];
      console.log(`闪断 #${i} 后: 共 ${players.length} 条玩家记录 [${players.map(p => `${p.name}${p.disconnected ? '(离线)' : ''}`).join(', ')}]`);
    }

    await sleep(500);
    let ok = true;
    const players = B.lastRoom.players;
    const active = players.filter(p => !p.disconnected);
    console.log(`\n闪断测试: 总记录 ${players.length}, 在线 ${active.length}`);
    if (players.length !== 2 || active.length !== 2) {
      console.error(`❌ 失败: 应为 2 名在线玩家，实际 ${players.length} 条记录 / ${active.length} 在线`);
      ok = false;
    } else {
      console.log('✅ 反复闪断重连后仍然只有 2 名玩家');
    }

    // 场景2: 整页刷新（全新 socket.id，同名重连）
    A.socket.close();
    await sleep(400);
    const A2 = makeClient('美女');
    await waitFor(() => A2.socket.connected, 5000, 'A2 连接');
    A2.socket.emit('join-room', { code: A.code, name: '美女' });
    await sleep(600);
    const p2 = B.lastRoom.players, a2 = p2.filter(p => !p.disconnected);
    if (p2.length !== 2 || a2.length !== 2) {
      console.error(`❌ 刷新页面重连失败: ${p2.length} 条记录 / ${a2.length} 在线`);
      ok = false;
    } else {
      console.log('✅ 刷新页面（新socket）同名重连正常，仍为 2 名玩家');
    }

    // 场景3: 第三人用已占用的昵称加入 → 应被拒绝
    const C = makeClient('总统');
    await waitFor(() => C.socket.connected, 5000, 'C 连接');
    C.socket.emit('join-room', { code: A.code, name: '总统' });
    await sleep(600);
    const p3 = B.lastRoom.players;
    if (p3.length !== 2 || !C.lastError) {
      console.error(`❌ 重名保护失败: ${p3.length} 条记录, C 收到错误: ${C.lastError || '无'}`);
      ok = false;
    } else {
      console.log(`✅ 重名加入被拒绝（提示: ${C.lastError}），玩家数不变`);
    }

    // 场景4: 换个名字正常加入
    C.socket.emit('join-room', { code: A.code, name: '路人' });
    await sleep(600);
    const p4 = B.lastRoom.players.filter(p => !p.disconnected);
    if (p4.length !== 3) {
      console.error(`❌ 新玩家正常加入失败: 在线 ${p4.length}`);
      ok = false;
    } else {
      console.log('✅ 新玩家换名字后正常加入，3 名在线');
    }

    // 场景5: 非正常刷新——旧 socket 还活着，新 socket 凭 token 找回座位
    const D = makeClient('刷新哥', 'tok-refresh-123');
    await waitFor(() => D.socket.connected, 5000, 'D 连接');
    D.socket.emit('join-room', { code: A.code, name: '刷新哥', token: D.token });
    await sleep(600);
    const D2 = makeClient('刷新哥', 'tok-refresh-123'); // 旧 socket 不关闭，模拟僵尸连接
    await waitFor(() => D2.socket.connected, 5000, 'D2 连接');
    D2.socket.emit('join-room', { code: A.code, name: '刷新哥', token: D2.token });
    await sleep(600);
    const p5 = B.lastRoom.players;
    const refreshers = p5.filter(p => p.name === '刷新哥');
    if (refreshers.length !== 1 || refreshers[0].disconnected || refreshers[0].id !== D2.socket.id) {
      console.error(`❌ token 找回座位失败: ${refreshers.length} 条刷新哥记录, id匹配=${refreshers[0]?.id === D2.socket.id}`);
      ok = false;
    } else {
      console.log('✅ 旧连接未断开时，凭 token 找回座位且无重复记录');
    }

    // 场景6: 广播不应泄露玩家 token
    if (p5.some(p => 'token' in p)) {
      console.error('❌ room-update 泄露了玩家 token');
      ok = false;
    } else {
      console.log('✅ room-update 不包含玩家 token');
    }

    // 场景7: 全员准备 → 自动开局（僵尸旧连接不能干扰）
    D.socket.emit('toggle-ready'); // 被顶掉的旧连接，应被忽略
    for (const cl of [A2, B, C, D2]) cl.socket.emit('toggle-ready');
    await waitFor(() => B.lastRoom && B.lastRoom.started, 4000, '自动开局');
    const started = B.lastRoom;
    if (started.players.length !== 4 || started.players.some(p => p.disconnected)) {
      console.error(`❌ 开局后玩家异常: ${started.players.length} 人`);
      ok = false;
    } else {
      console.log('✅ 全员准备后自动开局，4 名玩家整齐');
    }

    if (!ok) process.exitCode = 1;
    else console.log('\n✅ 全部通过');
    A2.socket.close(); B.socket.close(); C.socket.close(); D.socket.close(); D2.socket.close();
  } finally {
    server.kill();
  }
}

main().catch(e => { console.error('❌ 测试出错:', e.message); process.exitCode = 1; process.exit(); });
