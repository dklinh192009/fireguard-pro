const express    = require('express');
const http       = require('http');
const WebSocket  = require('ws');
const path       = require('path');
const { v4: uuidv4 } = require('uuid');
const fetch      = require('node-fetch');
const Database   = require('better-sqlite3');

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server });

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// ─── Google TTS Config ────────────────────────────────────────────────────────
// Đặt GOOGLE_TTS_KEY trong biến môi trường Render.com
const GOOGLE_TTS_KEY = process.env.GOOGLE_TTS_KEY || '';

// ─── SQLite Setup ─────────────────────────────────────────────────────────────
const db = new Database(path.join(__dirname, 'data', 'fireguard.db'));
// Tạo thư mục data nếu chưa có
require('fs').mkdirSync(path.join(__dirname, 'data'), { recursive: true });

db.exec(`
  CREATE TABLE IF NOT EXISTS alert_log (
    id        TEXT PRIMARY KEY,
    type      TEXT NOT NULL,
    node_id   TEXT,
    label     TEXT,
    location  TEXT,
    message   TEXT,
    source    TEXT DEFAULT 'auto',
    timestamp TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS nodes_config (
    node_id   TEXT PRIMARY KEY,
    label     TEXT,
    location  TEXT,
    node_type TEXT DEFAULT 'sensor',
    updated_at TEXT
  );
`);

// Prepared statements
const stmtInsertAlert = db.prepare(`
  INSERT INTO alert_log (id, type, node_id, label, location, message, source, timestamp)
  VALUES (@id, @type, @node_id, @label, @location, @message, @source, @timestamp)
`);
const stmtGetAlerts = db.prepare(`
  SELECT * FROM alert_log ORDER BY timestamp DESC LIMIT ?
`);
const stmtUpsertNodeConfig = db.prepare(`
  INSERT INTO nodes_config (node_id, label, location, node_type, updated_at)
  VALUES (@node_id, @label, @location, @node_type, @updated_at)
  ON CONFLICT(node_id) DO UPDATE SET
    label      = excluded.label,
    location   = excluded.location,
    node_type  = excluded.node_type,
    updated_at = excluded.updated_at
`);
const stmtGetNodeConfig = db.prepare(`SELECT * FROM nodes_config WHERE node_id = ?`);

// ─── In-memory state (kết nối sống) ──────────────────────────────────────────
const nodes          = new Map();   // nodeId → { ws, info }
const browserClients = new Set();   // browser dashboard connections

// Node types
const NODE_TYPE = { CENTER: 'center', SENSOR: 'sensor' };

// ─── Helpers ──────────────────────────────────────────────────────────────────
function broadcastToBrowsers(data) {
  const msg = JSON.stringify(data);
  for (const client of browserClients) {
    if (client.readyState === WebSocket.OPEN) client.send(msg);
  }
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
  return [...nodes.entries()].map(([id, node]) => ({ id, ...node.info, connected: true }));
}

function dbLogAlert(entry) {
  const row = {
    id:        entry.id || uuidv4(),
    type:      entry.type,
    node_id:   entry.nodeId   || null,
    label:     entry.label    || null,
    location:  entry.location || null,
    message:   entry.message  || null,
    source:    entry.source   || 'auto',
    timestamp: entry.timestamp || new Date().toISOString(),
  };
  stmtInsertAlert.run(row);
  return row;
}

// Tải cấu hình đã lưu vào node khi nó register
function loadNodeConfig(nodeId) {
  return stmtGetNodeConfig.get(nodeId);
}

// ─── Google TTS ───────────────────────────────────────────────────────────────
// Trả về Buffer audio mp3 từ Google TTS REST API
async function fetchGoogleTTS(text, langCode = 'vi-VN', voiceName = 'vi-VN-Wavenet-A') {
  if (!GOOGLE_TTS_KEY) {
    console.warn('[TTS] GOOGLE_TTS_KEY chưa được cấu hình!');
    return null;
  }
  const url = `https://texttospeech.googleapis.com/v1/text:synthesize?key=${GOOGLE_TTS_KEY}`;
  const body = {
    input: { text },
    voice: { languageCode: langCode, name: voiceName },
    audioConfig: { audioEncoding: 'MP3', speakingRate: 0.95 },
  };
  try {
    const res  = await fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    const data = await res.json();
    if (data.audioContent) return Buffer.from(data.audioContent, 'base64');
    console.error('[TTS] Google error:', data);
    return null;
  } catch (err) {
    console.error('[TTS] Fetch error:', err.message);
    return null;
  }
}

// Gửi audio binary qua WebSocket đến một node
async function sendTTSToNode(nodeId, text) {
  const node = nodes.get(nodeId);
  if (!node || node.ws.readyState !== WebSocket.OPEN) return;

  // Trước tiên gửi text để ESP32 biết nội dung (fallback nếu không có key)
  sendToNode(nodeId, { type: 'play_tts', text });

  // Nếu có key, stream audio binary
  if (GOOGLE_TTS_KEY) {
    const audioBuf = await fetchGoogleTTS(text);
    if (audioBuf && node.ws.readyState === WebSocket.OPEN) {
      // Gửi header báo chuẩn bị nhận audio
      sendToNode(nodeId, { type: 'tts_audio_start', size: audioBuf.length });
      // Gửi binary audio
      node.ws.send(audioBuf);
    }
  }
}

// ─── Xây dựng TTS text cho từng node ─────────────────────────────────────────
function buildTTSText(receiverNode, fireNode) {
  const fireLoc = fireNode.info.location || 'khu vực không xác định';
  const recvLoc = receiverNode.info.location || 'khu vực của bạn';

  if (receiverNode === fireNode || receiverNode.info.nodeId === fireNode.info.nodeId) {
    return `Cảnh báo cháy! Khu vực ${fireLoc} đang có cháy. Mọi người hãy nhanh chóng sơ tán!`;
  } else {
    return `Cảnh báo cháy! Bạn đang ở khu vực ${recvLoc}. Khu vực ${fireLoc} đang có cháy. Mọi người hãy nhanh chóng sơ tán!`;
  }
}

// ─── Core: xử lý sự kiện cháy ────────────────────────────────────────────────
// source: 'auto' (cảm biến) | 'manual_web' (người dùng click web) | 'manual_tft' (5 nút TFT)
async function handleFireEvent(fireNodeId, source = 'auto') {
  const fireEntry = nodes.get(fireNodeId);
  if (!fireEntry) return;

  fireEntry.info.status = 'fire';

  const alertId  = uuidv4();
  const timestamp = new Date().toISOString();
  const entry = {
    id: alertId, type: 'fire',
    nodeId: fireNodeId, label: fireEntry.info.label,
    location: fireEntry.info.location,
    message: `Phát hiện cháy tại ${fireEntry.info.location}`,
    source, timestamp,
  };

  dbLogAlert(entry);

  // 1. Báo browser dashboard
  broadcastToBrowsers({ type: 'fire_alert', ...entry, nodes: getNodeList() });

  // 2. Gửi lệnh còi + LoRa broadcast cho Node trung tâm
  //    Node trung tâm sẽ dùng LoRa broadcast "BUZZER_ON" đến tất cả Node A/B
  const centerNode = [...nodes.values()].find(n => n.info.nodeType === NODE_TYPE.CENTER);
  if (centerNode) {
    sendToNode(
      [...nodes.entries()].find(([,n]) => n === centerNode)?.[0],
      { type: 'lora_broadcast', command: 'BUZZER_ON', fireNodeId, location: fireEntry.info.location }
    );
  }

  // 3. Gửi TTS audio đến tất cả sensor node (qua WiFi)
  for (const [id, node] of nodes) {
    if (node.info.nodeType === NODE_TYPE.CENTER) continue; // không có loa
    const ttsText = buildTTSText({ info: node.info }, fireEntry);
    await sendTTSToNode(id, ttsText);
  }

  console.log(`[FIRE] ${source} — ${fireEntry.info.label} @ ${fireEntry.info.location}`);
}

// ─── WebSocket Handler ────────────────────────────────────────────────────────
wss.on('connection', (ws, req) => {
  let clientType = null;
  let nodeId     = null;

  ws.on('message', async (raw) => {
    // Nếu là binary (audio acknowledgement từ ESP32) → bỏ qua
    if (typeof raw !== 'string' && !Buffer.isBuffer(raw)) return;
    if (Buffer.isBuffer(raw) && raw[0] !== 0x7b) return; // không phải JSON

    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    switch (msg.type) {

      // ── ESP32 đăng ký ────────────────────────────────────────────────────
      case 'register': {
        clientType = 'node';
        nodeId = msg.nodeId || uuidv4();

        // Load cấu hình đã lưu từ DB (label/location do user đặt trước đó)
        const savedCfg = loadNodeConfig(nodeId);

        nodes.set(nodeId, {
          ws,
          info: {
            nodeId,
            nodeType:  savedCfg?.node_type || msg.nodeType || NODE_TYPE.SENSOR,
            label:     savedCfg?.label     || msg.label    || `Node-${nodeId.slice(0,4)}`,
            location:  savedCfg?.location  || msg.location || 'Chưa cấu hình',
            status:    'normal',
            lastSeen:  new Date().toISOString(),
            smoke: 0, temp: 0,
          },
        });

        // Xác nhận + gửi lại config hiện tại
        const info = nodes.get(nodeId).info;
        ws.send(JSON.stringify({
          type: 'registered', nodeId,
          label: info.label, location: info.location, nodeType: info.nodeType,
        }));

        broadcastToBrowsers({ type: 'nodes_update', nodes: getNodeList() });
        console.log(`[REGISTER] ${nodeId} (${info.label}) — ${info.nodeType}`);
        break;
      }

      // ── Browser kết nối ──────────────────────────────────────────────────
      case 'browser_connect': {
        clientType = 'browser';
        browserClients.add(ws);
        ws.send(JSON.stringify({ type: 'nodes_update', nodes: getNodeList() }));
        // Gửi 50 log gần nhất từ DB
        const logs = stmtGetAlerts.all(50).map(r => ({
          id: r.id, type: r.type,
          nodeId: r.node_id, label: r.label,
          location: r.location, message: r.message,
          source: r.source, timestamp: r.timestamp,
        }));
        ws.send(JSON.stringify({ type: 'alert_log', log: logs }));
        ws.send(JSON.stringify({ type: 'tts_configured', configured: !!GOOGLE_TTS_KEY }));
        break;
      }

      // ── Sensor data ──────────────────────────────────────────────────────
      case 'sensor_data': {
        const node = nodes.get(msg.nodeId);
        if (!node) break;
        node.info.smoke   = msg.smoke   ?? node.info.smoke;
        node.info.temp    = msg.temp    ?? node.info.temp;
        node.info.lastSeen = new Date().toISOString();
        broadcastToBrowsers({
          type: 'sensor_update', nodeId: msg.nodeId,
          smoke: node.info.smoke, temp: node.info.temp,
        });
        break;
      }

      // ── Cháy: Node trung tâm nhận LoRa từ A/B rồi báo server ────────────
      // msg: { type:'fire_detected', nodeId:'node-a-001', fromLora: true }
      case 'fire_detected': {
        // nodeId ở đây là ID của node bị cháy (A hoặc B), không phải node trung tâm
        const targetId = msg.fireNodeId || msg.nodeId;
        await handleFireEvent(targetId, 'auto');
        break;
      }

      // ── Tắt cảnh báo ─────────────────────────────────────────────────────
      case 'fire_clear': {
        const node = nodes.get(msg.nodeId);
        if (!node) break;
        node.info.status = 'normal';
        const entry = {
          id: uuidv4(), type: 'clear',
          nodeId: msg.nodeId, label: node.info.label,
          location: node.info.location, source: 'auto',
          timestamp: new Date().toISOString(),
        };
        dbLogAlert(entry);

        // Lệnh tắt còi qua LoRa
        const centerNodeId = [...nodes.entries()].find(([,n]) => n.info.nodeType === NODE_TYPE.CENTER)?.[0];
        if (centerNodeId) sendToNode(centerNodeId, { type: 'lora_broadcast', command: 'BUZZER_OFF' });

        broadcastToBrowsers({ type: 'fire_clear', ...entry, nodes: getNodeList() });
        break;
      }

      // ── Browser / TFT kích hoạt cháy thủ công ────────────────────────────
      // source: 'manual_web' hoặc 'manual_tft'
      case 'manual_alert': {
        await handleFireEvent(msg.targetNodeId, msg.source || 'manual_web');
        break;
      }

      // ── Gửi TTS tùy chỉnh (từ trang TTS) ────────────────────────────────
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

      // ── Nút bấm từ TFT (node trung tâm) ─────────────────────────────────
      case 'button_press': {
        broadcastToBrowsers({ type: 'button_press', button: msg.button, nodeId: msg.nodeId });
        break;
      }

      // ── Cập nhật config node từ dashboard ────────────────────────────────
      case 'update_node': {
        const node = nodes.get(msg.nodeId);
        if (node) {
          if (msg.label)    node.info.label    = msg.label;
          if (msg.location) node.info.location = msg.location;
        }
        // Lưu vào DB dù node có online hay không
        stmtUpsertNodeConfig.run({
          node_id:    msg.nodeId,
          label:      msg.label    || node?.info.label    || msg.nodeId,
          location:   msg.location || node?.info.location || 'Chưa cấu hình',
          node_type:  msg.nodeType || node?.info.nodeType || 'sensor',
          updated_at: new Date().toISOString(),
        });
        if (node) {
          sendToNode(msg.nodeId, {
            type: 'config_update',
            label: node.info.label, location: node.info.location,
          });
        }
        broadcastToBrowsers({ type: 'nodes_update', nodes: getNodeList() });
        break;
      }

      // ── Reset tất cả ─────────────────────────────────────────────────────
      case 'reset_all': {
        for (const [id, node] of nodes) {
          node.info.status = 'normal';
          sendToNode(id, { type: 'reset' });
        }
        // Lệnh tắt còi qua LoRa từ node trung tâm
        const cId = [...nodes.entries()].find(([,n]) => n.info.nodeType === NODE_TYPE.CENTER)?.[0];
        if (cId) sendToNode(cId, { type: 'lora_broadcast', command: 'BUZZER_OFF' });

        const entry = {
          id: uuidv4(), type: 'reset',
          label: 'Dashboard', location: 'Tất cả',
          source: 'manual_web', timestamp: new Date().toISOString(),
        };
        dbLogAlert(entry);
        broadcastToBrowsers({ type: 'reset_all', nodes: getNodeList(), ...entry });
        break;
      }
    }
  });

  ws.on('close', () => {
    if (clientType === 'browser') {
      browserClients.delete(ws);
    } else if (clientType === 'node' && nodeId) {
      nodes.delete(nodeId);
      broadcastToBrowsers({ type: 'nodes_update', nodes: getNodeList() });
      console.log(`[DISCONNECT] ${nodeId}`);
    }
  });
});

// ─── REST API ─────────────────────────────────────────────────────────────────
app.get('/api/nodes', (_, res) => res.json(getNodeList()));

app.get('/api/alerts', (req, res) => {
  const limit  = Math.min(parseInt(req.query.limit) || 100, 500);
  const rows   = stmtGetAlerts.all(limit);
  res.json(rows.map(r => ({
    id: r.id, type: r.type,
    nodeId: r.node_id, label: r.label,
    location: r.location, message: r.message,
    source: r.source, timestamp: r.timestamp,
  })));
});

app.get('/api/status', (_, res) => res.json({
  nodes:    nodes.size,
  browsers: browserClients.size,
  alerts:   stmtGetAlerts.all(1).length,
  uptime:   process.uptime(),
  tts:      !!GOOGLE_TTS_KEY,
}));

// Endpoint để test TTS (trả về audio MP3 cho browser preview)
app.post('/api/tts/preview', express.json(), async (req, res) => {
  const { text } = req.body;
  if (!text) return res.status(400).json({ error: 'text required' });
  if (!GOOGLE_TTS_KEY) return res.status(503).json({ error: 'TTS not configured' });
  const buf = await fetchGoogleTTS(text);
  if (!buf) return res.status(500).json({ error: 'TTS failed' });
  res.set('Content-Type', 'audio/mpeg');
  res.send(buf);
});

app.get('*', (_, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

// ─── Start ────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🔥 FireGuard Pro v2.1 running on port ${PORT}`);
  console.log(`   TTS: ${GOOGLE_TTS_KEY ? '✅ Configured' : '❌ No API key (set GOOGLE_TTS_KEY)'}`);
});