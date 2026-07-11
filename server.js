const express        = require('express');
const http           = require('http');
const WebSocket      = require('ws');
const path           = require('path');
const { v4: uuidv4 } = require('uuid');
const fetch          = require('node-fetch');
const mongoose       = require('mongoose');
const session        = require('express-session');
const MongoStore     = require('connect-mongo');
const passport       = require('passport');
const GoogleStrategy = require('passport-google-oauth20').Strategy;

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ server });

// ─── ENV ──────────────────────────────────────────────────────────────────────
const VBEE_APP_ID      = process.env.VBEE_APP_ID           || '';
const VBEE_TOKEN       = process.env.VBEE_TOKEN            || '';
const VBEE_VOICE_CODE  = process.env.VBEE_VOICE_CODE       || 'hn_female_ngochuyen_full_48k-fhg';
const MONGODB_URI      = process.env.MONGODB_URI           || '';
const SESSION_SECRET   = process.env.SESSION_SECRET        || 'fireguard_secret_change_me';
const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID      || '';
const GOOGLE_SECRET    = process.env.GOOGLE_CLIENT_SECRET  || '';
const BASE_URL         = process.env.BASE_URL              || 'http://localhost:3000';
const ADMIN_EMAIL      = process.env.ADMIN_EMAIL           || '';

// ─── MONGODB SCHEMAS ──────────────────────────────────────────────────────────
const userSchema = new mongoose.Schema({
  googleId:  { type: String, required: true, unique: true },
  email:     { type: String, required: true },
  name:      String,
  avatar:    String,
  role:      { type: String, enum: ['admin', 'operator', 'viewer'], default: 'viewer' },
  createdAt: { type: Date, default: Date.now },
  lastLogin: { type: Date, default: Date.now },
});
const User = mongoose.model('User', userSchema);

const alertSchema = new mongoose.Schema({
  id:        String,
  type:      String,
  nodeId:    String,
  label:     String,
  location:  String,
  message:   String,
  source:    String,
  timestamp: { type: Date, default: Date.now },
});
const Alert = mongoose.model('Alert', alertSchema);

const nodeConfigSchema = new mongoose.Schema({
  nodeId:    { type: String, unique: true },
  label:     String,
  location:  String,
  nodeType:  String,
  updatedAt: { type: Date, default: Date.now },
});
const NodeConfig = mongoose.model('NodeConfig', nodeConfigSchema);

// ─── MONGODB CONNECT ──────────────────────────────────────────────────────────
let mongoConnected = false;
if (MONGODB_URI) {
  mongoose.connect(MONGODB_URI)
    .then(() => { mongoConnected = true; console.log('[DB] MongoDB connected'); })
    .catch(e  => console.error('[DB] MongoDB error:', e.message));
} else {
  console.warn('[DB] MONGODB_URI not set — using in-memory fallback');
}

// ─── DB HELPERS ───────────────────────────────────────────────────────────────
const alertLog    = [];
const nodesConfig = new Map();

async function dbLogAlert(entry) {
  if (mongoConnected) {
    try { await Alert.create({ ...entry, timestamp: new Date(entry.timestamp) }); }
    catch (e) { console.error('[DB] Alert save error:', e.message); }
  }
  alertLog.unshift(entry);
  if (alertLog.length > 500) alertLog.pop();
  return entry;
}

async function dbGetAlerts(limit = 100) {
  if (mongoConnected) {
    try {
      const docs = await Alert.find().sort({ timestamp: -1 }).limit(limit).lean();
      return docs.map(d => ({ ...d, id: d.id || d._id.toString(), timestamp: d.timestamp.toISOString() }));
    } catch (e) { console.error('[DB] Alert fetch error:', e.message); }
  }
  return alertLog.slice(0, limit);
}

async function dbSaveNodeConfig(nodeId, cfg) {
  nodesConfig.set(nodeId, { ...nodesConfig.get(nodeId), ...cfg });
  if (mongoConnected) {
    try {
      await NodeConfig.findOneAndUpdate(
        { nodeId },
        { ...cfg, nodeId, updatedAt: new Date() },
        { upsert: true, new: true }
      );
    } catch (e) { console.error('[DB] NodeConfig save error:', e.message); }
  }
}

async function dbGetNodeConfig(nodeId) {
  if (mongoConnected) {
    try {
      const doc = await NodeConfig.findOne({ nodeId }).lean();
      if (doc) return doc;
    } catch (e) { console.error('[DB] NodeConfig fetch error:', e.message); }
  }
  return nodesConfig.get(nodeId) || null;
}

// ─── PASSPORT / GOOGLE OAUTH ──────────────────────────────────────────────────
passport.use(new GoogleStrategy({
  clientID:     GOOGLE_CLIENT_ID,
  clientSecret: GOOGLE_SECRET,
  callbackURL:  `${BASE_URL}/auth/google/callback`,
}, async (accessToken, refreshToken, profile, done) => {
  try {
    const email = profile.emails?.[0]?.value || '';
    const role  = email === ADMIN_EMAIL ? 'admin' : 'viewer';

    let user = await User.findOne({ googleId: profile.id });
    if (user) {
      user.lastLogin = new Date();
      user.name      = profile.displayName;
      user.avatar    = profile.photos?.[0]?.value || '';
      if (email === ADMIN_EMAIL) user.role = 'admin';
      await user.save();
    } else {
      user = await User.create({
        googleId: profile.id,
        email,
        name:   profile.displayName,
        avatar: profile.photos?.[0]?.value || '',
        role,
      });
      console.log(`[AUTH] New user: ${email} (${role})`);
    }
    return done(null, user);
  } catch (e) {
    return done(e, null);
  }
}));

passport.serializeUser((user, done) => done(null, user._id.toString()));
passport.deserializeUser(async (id, done) => {
  try {
    const user = await User.findById(id).lean();
    done(null, user);
  } catch (e) { done(e, null); }
});

// ─── EXPRESS MIDDLEWARE ───────────────────────────────────────────────────────
app.use(express.json());

// ✅ FIX: Dùng MONGODB_URI thay vì mongoConnected (vì Promise chưa resolve tại đây)
app.use(session({
  secret: SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  store: MONGODB_URI
    ? MongoStore.create({ mongoUrl: MONGODB_URI, ttl: 7 * 24 * 3600 })
    : undefined,
  cookie: { maxAge: 7 * 24 * 3600 * 1000 },
}));

app.use(passport.initialize());
app.use(passport.session());

// ─── AUTH MIDDLEWARE ──────────────────────────────────────────────────────────
function requireAuth(req, res, next) {
  if (!MONGODB_URI || !GOOGLE_CLIENT_ID) return next();
  if (req.isAuthenticated()) return next();
  if (req.path.startsWith('/api/') || req.path.startsWith('/ws')) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  res.redirect('/login.html');
}

function requireRole(...roles) {
  return (req, res, next) => {
    if (!MONGODB_URI || !GOOGLE_CLIENT_ID) return next();
    if (!req.isAuthenticated()) return res.status(401).json({ error: 'Unauthorized' });
    if (!roles.includes(req.user?.role)) return res.status(403).json({ error: 'Forbidden' });
    next();
  };
}

// ─── AUTH ROUTES ──────────────────────────────────────────────────────────────
app.get('/auth/google',
  passport.authenticate('google', { scope: ['profile', 'email'] })
);

app.get('/auth/google/callback',
  passport.authenticate('google', { failureRedirect: '/login.html?error=1' }),
  (req, res) => { res.redirect('/'); }
);

app.get('/auth/logout', (req, res) => {
  req.logout(() => { res.redirect('/login.html'); });
});

app.get('/api/me', (req, res) => {
  if (!req.isAuthenticated()) return res.json({ authenticated: false });
  const { name, email, avatar, role } = req.user;
  res.json({ authenticated: true, name, email, avatar, role });
});

app.get('/api/users', requireRole('admin'), async (req, res) => {
  try {
    const users = await User.find().select('-__v').sort({ createdAt: -1 }).lean();
    res.json(users);
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.patch('/api/users/:id/role', requireRole('admin'), async (req, res) => {
  const { role } = req.body;
  if (!['admin', 'operator', 'viewer'].includes(role))
    return res.status(400).json({ error: 'Invalid role' });
  try {
    const user = await User.findByIdAndUpdate(req.params.id, { role }, { new: true }).lean();
    if (!user) return res.status(404).json({ error: 'User not found' });
    res.json({ success: true, user });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// ✅ FIX: Dùng res.sendFile thay vì express.static cho single file
// express.static() cần nhận DIRECTORY, không nhận FILE path
// Khi truyền file path, nó không tìm thấy → fall through → requireAuth → redirect loop
app.get('/login.html', (req, res) => {
  res.sendFile(path.join(__dirname, 'public', 'login.html'));
});
app.post('/api/tts/callback', (req, res) => {
  const { request_id, status, audio_link } = req.body;
  const pending = pendingTTS.get(request_id);
  if (pending) {
    clearTimeout(pending.timer);
    pendingTTS.delete(request_id);
    pending.resolve(status === 'SUCCESS' ? audio_link : null);
  }
  res.json({ received: true });
});
// Bảo vệ toàn bộ dashboard
app.use(requireAuth);
app.use(express.static(path.join(__dirname, 'public')));

// ─── IN-MEMORY STATE ──────────────────────────────────────────────────────────
const nodes          = new Map();
const browserClients = new Set();
const NODE_TYPE      = { CENTER: 'center', SENSOR: 'sensor' };

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

// ─── TTS (Vbee) ───────────────────────────────────────────────────────────
const pendingTTS = new Map(); // requestId -> { resolve, timer }

async function vbeeSynthesize(text, timeoutMs = 15000) {
  if (!VBEE_TOKEN || !VBEE_APP_ID) return null;
  try {
    const res = await fetch('https://api.vbee.vn/v1/tts', {
      method: 'POST',
      headers: {
        'Content-Type':  'application/json',
        'Authorization': `Bearer ${VBEE_TOKEN}`,
        'App-Id':        VBEE_APP_ID,
      },
      body: JSON.stringify({
        text,
        mode:         'async',
        voiceCode:    VBEE_VOICE_CODE,
        outputFormat: 'mp3',
        bitrate:      128,
        speed:        1.0,
        webhookUrl:   `${BASE_URL}/api/tts/callback`,
      }),
    });
    const data = await res.json();
    if (!data.requestId) {
      console.error('[TTS] Vbee loi:', JSON.stringify(data));
      return null;
    }
    return new Promise((resolve) => {
      const timer = setTimeout(() => {
        pendingTTS.delete(data.requestId);
        console.warn('[TTS] Vbee timeout requestId=' + data.requestId);
        resolve(null);
      }, timeoutMs);
      pendingTTS.set(data.requestId, { resolve, timer });
    });
  } catch (e) { console.error('[TTS]', e.message); return null; }
}

async function sendTTSToNode(nodeId, text) {
  const node = nodes.get(nodeId);
  if (!node || node.ws.readyState !== WebSocket.OPEN) return;
  sendToNode(nodeId, { type: 'play_tts', text });
  const url = await vbeeSynthesize(text);
  if (!url) return;
  if (node.ws.readyState !== WebSocket.OPEN) return;
  sendToNode(nodeId, { type: 'tts_url', url });
  console.log(`[TTS] Gui URL cho node ${nodeId}`);
}
function buildTTSText(receiverId, fireNodeInfo) {
  const fireLoc  = fireNodeInfo.location || 'khu vực không xác định';
  const recvInfo = nodes.get(receiverId)?.info;
  const recvLoc  = recvInfo?.location || 'khu vực của bạn';
  if (receiverId === fireNodeInfo.nodeId)
    return `Cảnh báo cháy! Khu vực ${fireLoc} đang có cháy. Mọi người hãy nhanh chóng sơ tán!`;
  return `Cảnh báo cháy! Bạn đang ở khu vực ${recvLoc}. Khu vực ${fireLoc} đang có cháy. Mọi người hãy nhanh chóng sơ tán!`;
}

async function handleFireEvent(fireNodeId, source = 'auto') {
  const fireNode = nodes.get(fireNodeId);
  if (!fireNode) { console.warn('[FIRE] Unknown node:', fireNodeId); return; }
  fireNode.info.status = 'fire';
  const entry = {
    id: uuidv4(), type: 'fire',
    nodeId:   fireNodeId,
    label:    fireNode.info.label,
    location: fireNode.info.location,
    message:  `Phát hiện cháy tại ${fireNode.info.location}`,
    source, timestamp: new Date().toISOString(),
  };
  await dbLogAlert(entry);
  broadcastToBrowsers({ type: 'fire_alert', ...entry, nodes: getNodeList() });
  const centerId = getCenterId();
  if (centerId) sendToNode(centerId, { type: 'lora_broadcast', command: 'BUZZER_ON', fireNodeId, location: fireNode.info.location });
  for (const [id, node] of nodes) {
    if (node.info.nodeType === NODE_TYPE.CENTER) continue;
    const text = buildTTSText(id, { ...fireNode.info, nodeId: fireNodeId });
    await sendTTSToNode(id, text);
  }
  console.log(`[FIRE] ${source} — ${fireNode.info.label} @ ${fireNode.info.location}`);
}

// ─── WEBSOCKET ────────────────────────────────────────────────────────────────
wss.on('connection', (ws) => {
  let clientType = null;
  let nodeId     = null;

  ws.on('message', async (raw) => {
    if (Buffer.isBuffer(raw) && raw[0] !== 0x7b) return;
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    switch (msg.type) {

      case 'register': {
        clientType = 'node';
        nodeId     = msg.nodeId || uuidv4();
        const saved = await dbGetNodeConfig(nodeId);
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
        ws.send(JSON.stringify({ type: 'nodes_update',   nodes: getNodeList() }));
        ws.send(JSON.stringify({ type: 'alert_log',      log: await dbGetAlerts(50) }));
        ws.send(JSON.stringify({ type: 'tts_configured', configured: !!(VBEE_TOKEN && VBEE_APP_ID) }));
        break;
      }

      case 'sensor_data': {
        const node = nodes.get(msg.nodeId);
        if (!node) break;
        node.info.smoke    = msg.smoke ?? node.info.smoke;
        node.info.temp     = msg.temp  ?? node.info.temp;
        node.info.lastSeen = new Date().toISOString();
        broadcastToBrowsers({ type: 'sensor_update', nodeId: msg.nodeId, smoke: node.info.smoke, temp: node.info.temp });
        break;
      }

      case 'fire_detected':
        await handleFireEvent(msg.fireNodeId || msg.nodeId, 'auto'); break;

      case 'fire_clear': {
        const node = nodes.get(msg.nodeId);
        if (!node) break;
        node.info.status = 'normal';
        const entry = {
          id: uuidv4(), type: 'clear',
          nodeId: msg.nodeId, label: node.info.label, location: node.info.location,
          message: `Hết cháy tại ${node.info.location}`, source: 'auto',
          timestamp: new Date().toISOString(),
        };
        await dbLogAlert(entry);
        const cId = getCenterId();
        if (cId) sendToNode(cId, { type: 'lora_broadcast', command: 'BUZZER_OFF' });
        broadcastToBrowsers({ type: 'fire_clear', ...entry, nodes: getNodeList() });
        break;
      }

      case 'auto_clear': {
        const node = nodes.get(msg.nodeId);
        if (!node) break;
        node.info.status   = 'normal';
        node.info.lastSeen = new Date().toISOString();
        const entry = {
          id: uuidv4(), type: 'auto_clear',
          nodeId: msg.nodeId, label: node.info.label, location: node.info.location,
          message: `Khói giảm, tự động dừng cảnh báo tại ${node.info.location}`,
          source: 'auto', timestamp: new Date().toISOString(),
        };
        await dbLogAlert(entry);
        broadcastToBrowsers({ type: 'fire_clear', ...entry, nodes: getNodeList() });
        console.log(`[AUTO_CLEAR] ${node.info.label} @ ${node.info.location}`);
        break;
      }

      case 'manual_alert':
        await handleFireEvent(msg.targetNodeId, msg.source || 'manual_web'); break;

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

      case 'button_press':
        broadcastToBrowsers({ type: 'button_press', button: msg.button, nodeId: msg.nodeId }); break;

      case 'update_node': {
        const node = nodes.get(msg.nodeId);
        if (node) {
          if (msg.label)    node.info.label    = msg.label;
          if (msg.location) node.info.location = msg.location;
          sendToNode(msg.nodeId, { type: 'config_update', label: node.info.label, location: node.info.location });
        }
        await dbSaveNodeConfig(msg.nodeId, {
          label:    msg.label    || node?.info.label,
          location: msg.location || node?.info.location,
          nodeType: msg.nodeType || node?.info.nodeType || 'sensor',
        });
        broadcastToBrowsers({ type: 'nodes_update', nodes: getNodeList() });
        break;
      }

      case 'reset_all': {
        for (const [id, node] of nodes) { node.info.status = 'normal'; sendToNode(id, { type: 'reset' }); }
        const cId = getCenterId();
        if (cId) sendToNode(cId, { type: 'lora_broadcast', command: 'BUZZER_OFF' });
        const entry = {
          id: uuidv4(), type: 'reset',
          label: 'Dashboard', location: 'Tất cả',
          message: 'Reset toàn hệ thống từ dashboard',
          source: 'manual_web', timestamp: new Date().toISOString(),
        };
        await dbLogAlert(entry);
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
app.get('/api/alerts', async (req, res) => res.json(await dbGetAlerts(Math.min(parseInt(req.query.limit)||100, 500))));
app.get('/api/status', (_, res) => res.json({
  nodes: nodes.size, browsers: browserClients.size,
  alerts: alertLog.length, uptime: process.uptime(),
  tts: !!(VBEE_TOKEN && VBEE_APP_ID), db: mongoConnected,
}));

app.post('/api/tts/preview', async (req, res) => {
  const { text } = req.body;
  if (!text) return res.status(400).json({ error: 'text required' });
  if (!VBEE_TOKEN || !VBEE_APP_ID) return res.status(503).json({ error: 'TTS not configured' });
  const url = await vbeeSynthesize(text);
  if (!url) return res.status(500).json({ error: 'TTS failed' });
  res.json({ url });
});

app.get('*', (_, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

// ─── START ────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🔥 FireGuard Pro v2.2 — port ${PORT}`);
  console.log(`   TTS:  ${(VBEE_TOKEN && VBEE_APP_ID) ? '✅ OK' : '❌ No key'}`);
  console.log(`   DB:   ${MONGODB_URI      ? '⏳ Connecting...' : '❌ No URI (in-memory)'}`);
  console.log(`   Auth: ${GOOGLE_CLIENT_ID ? '✅ Google OAuth'  : '❌ No OAuth (open access)'}`);
});
