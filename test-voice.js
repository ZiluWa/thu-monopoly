// 语音信令测试：voice-join / voice-members / voice-signal 转发 / voice-peer-left
// 运行: node test-voice.js
const { spawn } = require('child_process');
const path = require('path');
const { io } = require('socket.io-client');

const PORT = 3124;
const URL = `http://localhost:${PORT}`;
const sleep = ms => new Promise(r => setTimeout(r, ms));

function makeClient(name) {
  const socket = io(URL, { transports: ['websocket'] });
  const c = { socket, name, code: null, lastRoom: null, members: null, signals: [], peerLeft: [] };
  socket.on('room-update', r => { c.lastRoom = r; if (r && r.code) c.code = r.code; });
  socket.on('voice-members', d => { c.members = d.ids; });
  socket.on('voice-signal', d => { c.signals.push(d); });
  socket.on('voice-peer-left', d => { c.peerLeft.push(d.id); });
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
    env: { ...process.env, PORT: String(PORT) }, stdio: ['ignore', 'pipe', 'pipe'],
  });
  try {
    await sleep(1200);
    let ok = true;
    const A = makeClient('A'), B = makeClient('B');
    await waitFor(() => A.socket.connected && B.socket.connected, 5000, '连接');
    A.socket.emit('create-room', { name: 'A' });
    await waitFor(() => A.code, 5000, '房间码');
    B.socket.emit('join-room', { code: A.code, name: 'B' });
    await waitFor(() => B.lastRoom && B.lastRoom.players.length === 2, 5000, 'B 加入');

    // A 加入语音 → 应收到空成员列表；房间状态 voice=[A]
    A.socket.emit('voice-join');
    await waitFor(() => A.members !== null, 3000, 'A voice-members');
    if (A.members.length !== 0) { console.error('❌ A 应收到空成员列表'); ok = false; }
    else console.log('✅ A 加入语音，成员列表为空');
    await waitFor(() => (B.lastRoom.voice || []).length === 1, 3000, 'voice 广播');
    if (B.lastRoom.voice[0] !== A.socket.id) { console.error('❌ room.voice 应包含 A'); ok = false; }
    else console.log('✅ room-update 携带语音成员 [A]');

    // B 加入语音 → 应收到 [A]
    B.socket.emit('voice-join');
    await waitFor(() => B.members !== null, 3000, 'B voice-members');
    if (B.members.length !== 1 || B.members[0] !== A.socket.id) { console.error('❌ B 应收到成员 [A]，实际', B.members); ok = false; }
    else console.log('✅ B 加入语音，收到已有成员 [A]');

    // B → A 信令转发
    B.socket.emit('voice-signal', { to: A.socket.id, data: { sdp: { type: 'offer', sdp: 'x' } } });
    await waitFor(() => A.signals.length > 0, 3000, 'A 收到信令');
    const sig = A.signals[0];
    if (sig.from !== B.socket.id || sig.data.sdp.type !== 'offer') { console.error('❌ 信令内容不对', sig); ok = false; }
    else console.log('✅ 信令 B→A 转发成功');

    // 房间外的人不能给房间内的人发信令
    const X = makeClient('X');
    await waitFor(() => X.socket.connected, 3000, 'X 连接');
    X.socket.emit('voice-signal', { to: A.socket.id, data: { sdp: { type: 'offer' } } });
    await sleep(500);
    if (A.signals.length > 1) { console.error('❌ 房间外信令不应被转发'); ok = false; }
    else console.log('✅ 房间外的信令被拦截');

    // B 断开 → A 收到 voice-peer-left
    const bId = B.socket.id;
    B.socket.close();
    await waitFor(() => A.peerLeft.includes(bId), 4000, 'voice-peer-left');
    console.log('✅ B 断开后 A 收到 voice-peer-left');
    await waitFor(() => (A.lastRoom.voice || []).length === 1 && A.lastRoom.voice[0] === A.socket.id, 3000, 'voice 列表更新');
    console.log('✅ 语音成员列表已更新为 [A]');

    // A 主动退出语音
    A.socket.emit('voice-leave');
    await waitFor(() => (A.lastRoom.voice || []).length === 0, 3000, 'voice-leave');
    console.log('✅ A 退出语音后列表为空');

    console.log(ok ? '\n✅ 语音信令全部通过' : '\n❌ 存在失败项');
    if (!ok) process.exitCode = 1;
    A.socket.close(); X.socket.close();
  } finally {
    server.kill();
  }
}
main().catch(e => { console.error('❌ 测试出错:', e.message); process.exitCode = 1; process.exit(); });
