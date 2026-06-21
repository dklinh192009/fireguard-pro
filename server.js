const express   = require('express');
const http      = require('http');
const WebSocket = require('ws');
const path      = require('path');
const { v4: uuidv4 } = require('uuid');
const fetch     = require('node-fetch');

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server });

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ─── FPT.AI TTS ───────────────────────────────────────────────────────────────
const FPT_TTS_KEY = process.env.FPT_TTS_KEY || '';
const FPT_TTS_VOICE = process.env.FPT_TTS_VOICE || 'banmai'; // banmai = nữ Bắc, có thể đổi minhquang, ngoclam, ...

// ─── In-memory "database" (thay SQLite để tránh lỗi compile trên Render) ──────
// alertLog: lưu tối đa 500 bản ghi, tự xoá cũ nhất khi đầy
const alertLog   = [];          // [{ id, type, nodeId, label, location, message, source, timestamp }]
const nodesConfig = new Map();  // nodeId → { label, location, nodeType }

function dbLogAlert(entry) {
  alertLog.unshift(entry);
  if (alertLog.length > 500) alertLog.pop();
  return entry;
}
function dbGetAlerts(limit = 100) { return alertLog.slice(0, limit); }
function dbSaveNodeConfig(nodeId, cfg) { nodesConfig.set(nodeId, { ...nodesConfig.get(nodeId), ...cfg }); }
function dbGetNodeConfig(nodeId) { return nodesConfig.get(nodeId) || null; }

// ─── In-memory state (kết nối sống) ──────────────────────────────────────────
const nodes          = new Map();  // nodeId → { ws, info }
const browserClients = new Set();
const NODE_TYPE = { CENTER: 'center', SENSOR: 'sensor' };

// ─── Helpers ──────────────────────────────────────────────────────────────────
function broadcastToBrowsers(data) {
  const msg = JSON.stringify(data);
  for (const c of browserClients)
    if (c.readyState === WebSocket.OPEN) c.send(msg);
}

function sendToNode(nodeId, data) {
  const node = nodes.get(nodeId);
  if (node && node.ws.readyState === WebSocket.OPEN) {
    node.ws.send(JSON.stringify(data));
    return true;
  }
  return false;
}

function getNodeList() {
  return [...nodes.entries()].map(([id, n]) => ({ id, ...n.info, connected: true }));
}

function getCenterId() {
  return [...nodes.entries()].find(([, n]) => n.info.nodeType === NODE_TYPE.CENTER)?.[0];
}

// ─── FPT.AI TTS ───────────────────────────────────────────────────────────────
// FPT.AI trả về 1 LINK (không phải audio trực tiếp), và file cần vài giây để xử lý
// xong trên server của họ -> phải đợi rồi mới tải link đó về.
//
// LƯU Ý QUAN TRỌNG: API yêu cầu đầy đủ các header sau, kể cả "speed" —
// nếu thiếu hoặc để giá trị rỗng sẽ bị lỗi "is not a legal HTTP header value".
async function fetchFptTTS(text) {
  if (!FPT_TTS_KEY) return null;
  try {
    // Bước 1: gọi API để lấy link async
    const res = await fetch('https://api.fpt.ai/hmi/tts/v5', {
      method: 'POST',
      headers: {
        'api-key': FPT_TTS_KEY,        // FPT.AI dùng "api-key" (gạch ngang), không phải "api_key"
        'voice': FPT_TTS_VOICE,
        'speed': '0',                   // bắt buộc phải có giá trị, "0" = tốc độ mặc định
        'Cache-Control': 'no-cache',
        'Content-Type': 'text/plain; charset=utf-8',
      },
      body: text,
    });
    const data = await res.json();
    if (data.error !== 0 || !data.async) {
      console.error('[TTS] FPT.AI loi:', JSON.stringify(data));
      return null;
    }

    // Bước 2: đợi vài giây để FPT xử lý xong file (đợi theo độ dài văn bản,
    // tối thiểu 4s, tối đa 15s để tránh treo quá lâu)
    const waitMs = Math.min(15000, Math.max(4000, text.length * 60));
    await new Promise((resolve) => setTimeout(resolve, waitMs));

    // Bước 3: tải file mp3 thật từ link async
    const audioRes = await fetch(data.async);
    if (!audioRes.ok) {
      console.error('[TTS] Khong tai duoc file audio tu FPT.AI, status:', audioRes.status);
      return null;
    }
    const arrayBuf = await audioRes.arrayBuffer();
    return Buffer.from(arrayBuf);
  } catch (e) {
    console.error('[TTS]', e.message);
    return null;
  }
}

async function sendTTSToNode(nodeId, text) {
  const node = nodes.get(nodeId);
  if (!node || node.ws.readyState !== WebSocket.OPEN) return;
  // Luôn gửi text trước (ESP32 dùng làm fallback)
  sendToNode(nodeId, { type: 'play_tts', text });
  // Nếu có key → gửi thêm audio binary
  if (FPT_TTS_KEY) {
    const buf = await fetchFptTTS(text);
    if (buf && node.ws.readyState === WebSocket.OPEN) {
      sendToNode(nodeId, { type: 'tts_audio_start', size: buf.length });
      node.ws.send(buf);
    }
  }
}

function buildTTSText(receiverId, fireNodeInfo) {
  const fireLoc = fireNodeInfo.location || 'khu vực không xác định';
  const recvInfo = nodes.get(receiverId)?.info;
  const recvLoc  = recvInfo?.location || 'khu vực của bạn';
  if (receiverId === fireNodeInfo.nodeId) {
    return `Cảnh báo cháy! Khu vực ${fireLoc} đang có cháy. Mọi người hãy nhanh chóng sơ tán!`;
  }
  return `Cảnh báo cháy! Bạn đang ở khu vực ${recvLoc}. Khu vực ${fireLoc} đang có cháy. Mọi người hãy nhanh chóng sơ tán!`;
}

// ─── Core: xử lý sự kiện cháy ────────────────────────────────────────────────
async function handleFireEvent(fireNodeId, source = 'auto') {
  const fireNode = nodes.get(fireNodeId);
  if (!fireNode) { console.warn('[FIRE] Unknown node:', fireNodeId); return; }

  fireNode.info.status = 'fire';

  const entry = {
    id: uuidv4(), type: 'fire',
    nodeId: fireNodeId,
    label: fireNode.info.label,
    location: fireNode.info.location,
    message: `Phát hiện cháy tại ${fireNode.info.location}`,
    source, timestamp: new Date().toISOString(),
  };
  dbLogAlert(entry);

  // 1. Cập nhật dashboard
  broadcastToBrowsers({ type: 'fire_alert', ...entry, nodes: getNodeList() });

  // 2. Yêu cầu node trung tâm broadcast BUZZER_ON qua LoRa
  const centerId = getCenterId();
  if (centerId) {
    sendToNode(centerId, {
      type: 'lora_broadcast', command: 'BUZZER_ON',
      fireNodeId, location: fireNode.info.location,
    });
  }

  // 3. Gửi TTS về tất cả sensor node qua WiFi
  for (const [id, node] of nodes) {
    if (node.info.nodeType === NODE_TYPE.CENTER) continue;
    const text = buildTTSText(id, { ...fireNode.info, nodeId: fireNodeId });
    await sendTTSToNode(id, text);
  }

  console.log(`[FIRE] ${source} — ${fireNode.info.label} @ ${fireNode.info.location}`);
}

// ─── WebSocket ────────────────────────────────────────────────────────────────
wss.on('connection', (ws) => {
  let clientType = null;
  let nodeId     = null;

  ws.on('message', async (raw) => {
    if (Buffer.isBuffer(raw) && raw[0] !== 0x7b) return; // bỏ qua binary không phải JSON
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    switch (msg.type) {

      case 'register': {
        clientType = 'node';
        nodeId     = msg.nodeId || uuidv4();
        const saved = dbGetNodeConfig(nodeId);
        nodes.set(nodeId, {
          ws,
          info: {
            nodeId,
            nodeType: saved?.nodeType || msg.nodeType || NODE_TYPE.SENSOR,
            label:    saved?.label    || msg.label    || `Node-${nodeId.slice(0,4)}`,
            location: saved?.location || msg.location || 'Chưa cấu hình',
            status: 'normal', lastSeen: new Date().toISOString(),
            smoke: 0, temp: 0,
          },
        });
        const info = nodes.get(nodeId).info;
        ws.send(JSON.stringify({ type: 'registered', nodeId, label: info.label, location: info.location, nodeType: info.nodeType }));
        broadcastToBrowsers({ type: 'nodes_update', nodes: getNodeList() });
        console.log(`[REG] ${nodeId} (${info.label}) ${info.nodeType}`);
        break;
      }

      case 'browser_connect': {
        clientType = 'browser';
        browserClients.add(ws);
        ws.send(JSON.stringify({ type: 'nodes_update', nodes: getNodeList() }));
        ws.send(JSON.stringify({ type: 'alert_log', log: dbGetAlerts(50) }));
        ws.send(JSON.stringify({ type: 'tts_configured', configured: !!FPT_TTS_KEY }));
        break;
      }

      case 'sensor_data': {
        const node = nodes.get(msg.nodeId);
        if (!node) break;
        node.info.smoke   = msg.smoke  ?? node.info.smoke;
        node.info.temp    = msg.temp   ?? node.info.temp;
        node.info.lastSeen = new Date().toISOString();
        broadcastToBrowsers({ type: 'sensor_update', nodeId: msg.nodeId, smoke: node.info.smoke, temp: node.info.temp });
        break;
      }

      case 'fire_detected': {
        await handleFireEvent(msg.fireNodeId || msg.nodeId, 'auto');
        break;
      }

      case 'fire_clear': {
        const node = nodes.get(msg.nodeId);
        if (!node) break;
        node.info.status = 'normal';
        const entry = { id: uuidv4(), type: 'clear', nodeId: msg.nodeId, label: node.info.label, location: node.info.location, source: 'auto', timestamp: new Date().toISOString() };
        dbLogAlert(entry);
        const cId = getCenterId();
        if (cId) sendToNode(cId, { type: 'lora_broadcast', command: 'BUZZER_OFF' });
        broadcastToBrowsers({ type: 'fire_clear', ...entry, nodes: getNodeList() });
        break;
      }

      case 'manual_alert': {
        await handleFireEvent(msg.targetNodeId, msg.source || 'manual_web');
        break;
      }

      case 'send_tts': {
        if (msg.targetNodeId === 'all') {
          for (const [id, node] of nodes) {
            if (node.info.nodeType === NODE_TYPE.CENTER) continue;
            await sendTTSToNode(id, msg.text);
          }
        } else {
          await sendTTSToNode(msg.targetNodeId, msg.text);
        }
        break;
      }

      case 'button_press': {
        broadcastToBrowsers({ type: 'button_press', button: msg.button, nodeId: msg.nodeId });
        break;
      }

      case 'update_node': {
        const node = nodes.get(msg.nodeId);
        if (node) {
          if (msg.label)    node.info.label    = msg.label;
          if (msg.location) node.info.location = msg.location;
          sendToNode(msg.nodeId, { type: 'config_update', label: node.info.label, location: node.info.location });
        }
        // Lưu config vào memory để node reconnect vẫn giữ được label/location
        dbSaveNodeConfig(msg.nodeId, {
          label:    msg.label    || node?.info.label,
          location: msg.location || node?.info.location,
          nodeType: msg.nodeType || node?.info.nodeType || 'sensor',
        });
        broadcastToBrowsers({ type: 'nodes_update', nodes: getNodeList() });
        break;
      }

      case 'reset_all': {
        for (const [id, node] of nodes) {
          node.info.status = 'normal';
          sendToNode(id, { type: 'reset' });
        }
        const cId = getCenterId();
        if (cId) sendToNode(cId, { type: 'lora_broadcast', command: 'BUZZER_OFF' });
        const entry = { id: uuidv4(), type: 'reset', label: 'Dashboard', location: 'Tất cả', source: 'manual_web', timestamp: new Date().toISOString() };
        dbLogAlert(entry);
        broadcastToBrowsers({ type: 'reset_all', nodes: getNodeList(), ...entry });
        break;
      }
    }
  });

  ws.on('close', () => {
    if (clientType === 'browser') browserClients.delete(ws);
    else if (clientType === 'node' && nodeId) {
      nodes.delete(nodeId);
      broadcastToBrowsers({ type: 'nodes_update', nodes: getNodeList() });
      console.log(`[DC] ${nodeId}`);
    }
  });
});

// ─── REST API ─────────────────────────────────────────────────────────────────
app.get('/api/nodes',  (_, res) => res.json(getNodeList()));
app.get('/api/alerts', (req, res) => res.json(dbGetAlerts(Math.min(parseInt(req.query.limit)||100, 500))));
app.get('/api/status', (_, res) => res.json({
  nodes: nodes.size, browsers: browserClients.size,
  alerts: alertLog.length, uptime: process.uptime(), tts: !!FPT_TTS_KEY,
}));

app.post('/api/tts/preview', async (req, res) => {
  const { text } = req.body;
  if (!text) return res.status(400).json({ error: 'text required' });
  if (!FPT_TTS_KEY) return res.status(503).json({ error: 'TTS not configured' });
  const buf = await fetchFptTTS(text);
  if (!buf) return res.status(500).json({ error: 'TTS failed' });
  res.set('Content-Type', 'audio/mpeg');
  res.send(buf);
});

app.get('*', (_, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

// ─── Start ────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🔥 FireGuard Pro v2.1 — port ${PORT}`);
  console.log(`   TTS: ${FPT_TTS_KEY ? '✅ OK' : '❌ No key'}`);
});
