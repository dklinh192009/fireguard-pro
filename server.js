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
const fs             = require('fs');
const jwt            = require('jsonwebtoken');

function signMobileToken(user) {
  return jwt.sign(
    { _id: String(user._id), email: user.email, name: user.name, avatar: user.avatar, role: user.role },
    SESSION_SECRET,
    { expiresIn: '30d' }
  );
}

const app    = express();
const server = http.createServer(app);
const wss    = new WebSocket.Server({ noServer: true });

const VBEE_APP_ID      = process.env.VBEE_APP_ID           || '';
const VBEE_TOKEN       = process.env.VBEE_TOKEN            || '';
const VBEE_VOICE_CODE  = process.env.VBEE_VOICE_CODE       || 'hn_female_ngochuyen_full_48k-fhg';
const MONGODB_URI      = process.env.MONGODB_URI           || '';
const SESSION_SECRET   = process.env.SESSION_SECRET        || 'fireguard_secret_change_me';
const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID      || '';
const GOOGLE_SECRET    = process.env.GOOGLE_CLIENT_SECRET  || '';
const BASE_URL         = process.env.BASE_URL              || 'http://localhost:3000';
const ADMIN_EMAIL      = process.env.ADMIN_EMAIL           || '';

const userSchema = new mongoose.Schema({
  googleId:   { type: String, required: true, unique: true },
  email:      { type: String, required: true },
  name:       String,
  avatar:     String,
  role:       { type: String, enum: ['admin', 'operator', 'viewer'], default: 'viewer' },
  belongsToOwnerId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', default: null },
  pushTokens: { type: [String], default: [] },
  alertThresholds: {
    warn:   { type: Number, default: 150 },
    danger: { type: Number, default: 400 },
  },
  createdAt:  { type: Date, default: Date.now },
  lastLogin:  { type: Date, default: Date.now },
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
  ownerId:   { type: mongoose.Schema.Types.ObjectId, ref: 'User', index: true },
  timestamp: { type: Date, default: Date.now },
});
const Alert = mongoose.model('Alert', alertSchema);

const nodeConfigSchema = new mongoose.Schema({
  nodeId:    { type: String, unique: true },
  label:     String,
  location:  String,
  nodeType:  String,
  ownerId:   { type: mongoose.Schema.Types.ObjectId, ref: 'User', index: true, default: null },
  thresholdOverride: {
    type: new mongoose.Schema({ warn: Number, danger: Number }, { _id: false }),
    default: null,
  },
  updatedAt: { type: Date, default: Date.now },
});
const NodeConfig = mongoose.model('NodeConfig', nodeConfigSchema);

const pairingCodeSchema = new mongoose.Schema({
  code:      { type: String, required: true, unique: true },
  ownerId:   { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
  used:      { type: Boolean, default: false },
  createdAt: { type: Date, default: Date.now, expires: 600 },
});
const PairingCode = mongoose.model('PairingCode', pairingCodeSchema);

function tenantIdOf(user) {
  return user?.belongsToOwnerId || user?._id;
}

const inviteCodeSchema = new mongoose.Schema({
  code:      { type: String, required: true, unique: true },
  ownerId:   { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
  role:      { type: String, enum: ['operator', 'viewer'], default: 'viewer' },
  used:      { type: Boolean, default: false },
  createdAt: { type: Date, default: Date.now, expires: 3600 },
});
const InviteCode = mongoose.model('InviteCode', inviteCodeSchema);

let mongoConnected = false;
if (MONGODB_URI) {
  mongoose.connect(MONGODB_URI)
    .then(() => { mongoConnected = true; console.log('[DB] MongoDB connected'); })
    .catch(e  => console.error('[DB] MongoDB error:', e.message));
} else {
  console.warn('[DB] MONGODB_URI not set');
}

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

async function dbGetAlerts(ownerId, isAdmin, limit = 100) {
  if (mongoConnected) {
    try {
      const filter = isAdmin ? {} : { ownerId };
      const docs = await Alert.find(filter).sort({ timestamp: -1 }).limit(limit).lean();
      return docs.map(d => ({ ...d, id: d.id || d._id.toString(), timestamp: d.timestamp.toISOString() }));
    } catch (e) { console.error('[DB] Alert fetch error:', e.message); }
  }
  if (isAdmin) return alertLog.slice(0, limit);
  return alertLog.filter(a => String(a.ownerId) === String(ownerId)).slice(0, limit);
}

async function dbCreatePairingCode(ownerId) {
  let code;
  do { code = String(Math.floor(100000 + Math.random() * 900000)); }
  while (await PairingCode.findOne({ code, used: false }).lean());
  await PairingCode.create({ code, ownerId });
  return code;
}

async function dbClaimPairingCode(code) {
  if (!mongoConnected || !code) return null;
  try {
    const doc = await PairingCode.findOneAndUpdate(
      { code, used: false },
      { used: true },
      { new: true }
    );
    return doc ? doc.ownerId : null;
  } catch (e) { console.error('[DB] Pairing claim error:', e.message); return null; }
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

app.use(express.json());

const sessionMiddleware = session({
  secret: SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  store: MONGODB_URI
    ? MongoStore.create({ mongoUrl: MONGODB_URI, ttl: 7 * 24 * 3600 })
    : undefined,
  cookie: { maxAge: 7 * 24 * 3600 * 1000 },
});
app.use(sessionMiddleware);

const passportInit    = passport.initialize();
const passportSession = passport.session();
app.use(passportInit);
app.use(passportSession);

async function attachTokenAuth(req, res, next) {
  if (req.isAuthenticated()) return next();
  const authHeader = req.headers.authorization;
  if (authHeader?.startsWith('Bearer ')) {
    try {
      const payload = jwt.verify(authHeader.slice(7), SESSION_SECRET);
      // Tra lại DB lấy dữ liệu MỚI NHẤT (role, belongsToOwnerId, alertThresholds...) —
      // không tin hẳn vào token vì token sống tới 30 ngày, có thể đã lỗi thời
      // (VD: user vừa join-household, token cũ chưa biết chuyện này)
      const freshUser = await User.findById(payload._id).lean();
      if (freshUser) {
        req.user = freshUser;
        req.isAuthenticated = () => true;
      }
    } catch (e) { }
  }
  next();
}
app.use(attachTokenAuth);

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

app.get('/auth/google',
  passport.authenticate('google', { scope: ['profile', 'email'] })
);

app.get('/auth/google/mobile', (req, res, next) => {
  const redirectUri = req.query.redirect_uri || 'fireguardapp://auth-callback';
  const state = Buffer.from(JSON.stringify({ mobile: true, redirectUri })).toString('base64');
  passport.authenticate('google', { scope: ['profile', 'email'], state })(req, res, next);
});

app.get('/auth/google/callback',
  passport.authenticate('google', { failureRedirect: '/login.html?error=1' }),
  (req, res) => {
    let stateData = null;
    try { stateData = JSON.parse(Buffer.from(req.query.state || '', 'base64').toString('utf8')); }
    catch (e) { }

    if (stateData?.mobile) {
      const token = signMobileToken(req.user);
      return res.redirect(`${stateData.redirectUri}?token=${token}`);
    }
    res.redirect('/');
  }
);

app.get('/auth/logout', (req, res) => {
  req.logout(() => { res.redirect('/login.html'); });
});

app.get('/api/me', (req, res) => {
  if (!req.isAuthenticated()) return res.json({ authenticated: false });
  const { name, email, avatar, role, alertThresholds, belongsToOwnerId } = req.user;
  res.json({ authenticated: true, name, email, avatar, role, alertThresholds, belongsToOwnerId });
});

app.patch('/api/settings/thresholds', requireAuth, async (req, res) => {
  const { warn, danger } = req.body;
  if (typeof warn !== 'number' || typeof danger !== 'number' || warn >= danger) {
    return res.status(400).json({ error: 'warn/danger phai la so, va warn < danger' });
  }
  await User.updateOne({ _id: req.user._id }, { alertThresholds: { warn, danger } });
  res.json({ success: true, alertThresholds: { warn, danger } });
});

app.patch('/api/nodes/:nodeId/threshold', requireRole('admin', 'operator'), async (req, res) => {
  const { warn, danger } = req.body;
  const cfg = await dbGetNodeConfig(req.params.nodeId);
  if (!cfg) return res.status(404).json({ error: 'Khong tim thay node' });
  if (req.user.role !== 'admin' && String(cfg.ownerId) !== String(tenantIdOf(req.user))) {
    return res.status(403).json({ error: 'Khong phai chu so huu node nay' });
  }
  const override = (typeof warn === 'number' && typeof danger === 'number') ? { warn, danger } : null;
  await dbSaveNodeConfig(req.params.nodeId, { thresholdOverride: override });
  const live = nodes.get(req.params.nodeId);
  if (live) live.info.thresholdOverride = override;
  res.json({ success: true, thresholdOverride: override });
});

app.post('/api/push-token', requireAuth, async (req, res) => {
  const { token } = req.body;
  if (!token) return res.status(400).json({ error: 'token required' });
  try {
    await User.updateOne({ _id: req.user._id }, { $addToSet: { pushTokens: token } });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.delete('/api/push-token', requireAuth, async (req, res) => {
  const { token } = req.body;
  if (!token) return res.status(400).json({ error: 'token required' });
  try {
    await User.updateOne({ _id: req.user._id }, { $pull: { pushTokens: token } });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
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

const TTS_CACHE_DIR = path.join(__dirname, 'tts-cache');
fs.mkdirSync(TTS_CACHE_DIR, { recursive: true });
app.use('/tts-audio', express.static(TTS_CACHE_DIR));

app.use(requireAuth);
app.use(express.static(path.join(__dirname, 'public')));

const nodes          = new Map();
const browserClients = new Set();
const NODE_TYPE      = { CENTER: 'center', SENSOR: 'sensor' };

function broadcastToBrowsers(data, targetOwnerId = null) {
  const msg = JSON.stringify(data);
  for (const c of browserClients) {
    if (c.readyState !== WebSocket.OPEN) continue;
    const allowed = c.isAdmin || targetOwnerId === null || String(c.ownerId) === String(targetOwnerId);
    if (allowed) c.send(msg);
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

function getNodeList(ownerId = null, isAdmin = false) {
  return [...nodes.entries()]
    .filter(([, n]) => isAdmin || String(n.info.ownerId) === String(ownerId))
    .map(([id, n]) => ({ id, ...n.info, connected: true }));
}

function getCenterId() {
  return [...nodes.entries()].find(([, n]) => n.info.nodeType === NODE_TYPE.CENTER)?.[0];
}

const pendingTTS = new Map();

async function vbeeSynthesize(text, timeoutMs = 20000, pollInterval = 1500) {
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
    const raw = await res.text();
    let data;
    try { data = JSON.parse(raw); }
    catch {
      console.error(`[TTS] Vbee tra ve khong phai JSON (status ${res.status}):`, raw.slice(0, 300));
      return null;
    }
    const requestId = data.requestId;
    if (!requestId) {
      console.error('[TTS] Vbee loi tao request:', JSON.stringify(data));
      return null;
    }

    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline) {
      await new Promise(r => setTimeout(r, pollInterval));
      const pollRes = await fetch(`https://api.vbee.vn/v1/tts/requests/${requestId}`, {
        headers: {
          'Authorization': `Bearer ${VBEE_TOKEN}`,
          'App-Id':        VBEE_APP_ID,
          'Content-Type':  'application/json',
        },
      });
      const pollRaw = await pollRes.text();
      let pollData;
      try { pollData = JSON.parse(pollRaw); } catch { continue; }

      if (pollData.status === 'COMPLETED') {
        const remoteUrl = pollData.audioLink;
        if (!remoteUrl) return null;
        try {
          const audioRes = await fetch(remoteUrl);
          const buffer   = await audioRes.buffer();
          const filename = `${uuidv4()}.mp3`;
          fs.writeFileSync(path.join(TTS_CACHE_DIR, filename), buffer);
          setTimeout(() => {
            fs.unlink(path.join(TTS_CACHE_DIR, filename), () => {});
          }, 2 * 60 * 1000);
          return `${BASE_URL}/tts-audio/${filename}`;
        } catch (e) {
          console.error('[TTS] Loi tai audio ve cache:', e.message);
          return remoteUrl;
        }
      }
      if (pollData.status === 'FAILED') {
        console.error('[TTS] Vbee request FAILED:', JSON.stringify(pollData));
        return null;
      }
    }
    console.warn('[TTS] Vbee polling timeout requestId=' + requestId);
    return null;
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
  const fireLoc  = fireNodeInfo.location || 'khu vuc khong xac dinh';
  const recvInfo = nodes.get(receiverId)?.info;
  const recvLoc  = recvInfo?.location || 'khu vuc cua ban';
  if (receiverId === fireNodeInfo.nodeId)
    return `Canh bao chay! Khu vuc ${fireLoc} dang co chay. Moi nguoi hay nhanh chong so tan!`;
  return `Canh bao chay! Ban dang o khu vuc ${recvLoc}. Khu vuc ${fireLoc} dang co chay. Moi nguoi hay nhanh chong so tan!`;
}

// ─── PUSH NOTIFICATION (qua Expo Push Service) ─────────────────────────────
// Không cần Firebase service account nữa — chỉ cần "Expo Push Token" mà app
// lấy được từ expo-notifications, gửi lên đây, server chuyển tiếp cho Expo lo phần còn lại
async function sendPushToOwner(ownerId, title, body, data = {}) {
  if (!ownerId) return;
  try {
    const user = await User.findById(ownerId).lean();
    const tokens = (user?.pushTokens || []).filter(t => t.startsWith('ExponentPushToken'));
    if (tokens.length === 0) return;

    const messages = tokens.map(to => ({
      to, title, body, data,
      sound: 'default',
      priority: 'high',
      channelId: 'fire-alerts', // phải khớp với channel app tạo (xem utils/notifications.ts)
    }));

    const res = await fetch('https://exp.host/--/api/v2/push/send', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Accept: 'application/json', 'Accept-Encoding': 'gzip, deflate' },
      body: JSON.stringify(messages),
    });
    const result = await res.json();

    // Dọn token chết (app đã gỡ cài đặt...)
    const deadTokens = [];
    (result.data || []).forEach((r, i) => {
      if (r.status === 'error' && r.details?.error === 'DeviceNotRegistered') {
        deadTokens.push(tokens[i]);
      }
    });
    if (deadTokens.length > 0) {
      await User.updateOne({ _id: ownerId }, { $pullAll: { pushTokens: deadTokens } });
      console.log(`[PUSH] Da don ${deadTokens.length} token chet cua user ${ownerId}`);
    }
    console.log(`[PUSH] Gui push toi user ${ownerId}: ${tokens.length} token`);
  } catch (e) {
    console.error('[PUSH] Loi gui push:', e.message);
  }
}

async function handleFireEvent(fireNodeId, source = 'auto') {
  const fireNode = nodes.get(fireNodeId);
  if (!fireNode) { console.warn('[FIRE] Unknown node:', fireNodeId); return; }
  fireNode.info.status = 'fire';
  const ownerId = fireNode.info.ownerId;
  const entry = {
    id: uuidv4(), type: 'fire',
    nodeId:   fireNodeId,
    label:    fireNode.info.label,
    location: fireNode.info.location,
    message:  `Phat hien chay tai ${fireNode.info.location}`,
    ownerId,
    source, timestamp: new Date().toISOString(),
  };
  await dbLogAlert(entry);
  broadcastToBrowsers({ type: 'fire_alert', ...entry, nodes: getNodeList(ownerId, false) }, ownerId);
  sendPushToOwner(
    ownerId,
    'Canh bao chay!',
    `Phat hien chay tai ${fireNode.info.location} (${fireNode.info.label})`,
    { type: 'fire_alert', nodeId: fireNodeId, location: fireNode.info.location }
  );
  const centerId = getCenterId();
  if (centerId) sendToNode(centerId, { type: 'lora_broadcast', command: 'BUZZER_ON', fireNodeId, location: fireNode.info.location });
  for (const [id, node] of nodes) {
    if (node.info.nodeType === NODE_TYPE.CENTER) continue;
    const text = buildTTSText(id, { ...fireNode.info, nodeId: fireNodeId });
    await sendTTSToNode(id, text);
  }
  console.log(`[FIRE] ${source} - ${fireNode.info.label} @ ${fireNode.info.location}`);
}

server.on('upgrade', (req, socket, head) => {
  sessionMiddleware(req, {}, () => {
    passportInit(req, {}, () => {
      passportSession(req, {}, async () => {
        if (!req.user) {
          try {
            const { query } = require('url').parse(req.url, true);
            if (query.token) {
              const payload = jwt.verify(query.token, SESSION_SECRET);
              // Cùng lý do như attachTokenAuth: tra lại DB thay vì tin hẳn vào token cũ
              const freshUser = await User.findById(payload._id).lean();
              if (freshUser) req.user = freshUser;
            }
          } catch (e) { }
        }
        wss.handleUpgrade(req, socket, head, (ws) => {
          wss.emit('connection', ws, req);
        });
      });
    });
  });
});

wss.on('connection', (ws, req) => {
  let clientType = null;
  let nodeId     = null;
  const wsUser   = req.user || null;
  const isAdmin  = wsUser?.role === 'admin';
  const canControl = isAdmin || wsUser?.role === 'operator';

  ws.on('message', async (raw) => {
    if (Buffer.isBuffer(raw) && raw[0] !== 0x7b) return;
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    switch (msg.type) {

      case 'register': {
        clientType = 'node';
        nodeId     = msg.nodeId || uuidv4();
        let saved  = await dbGetNodeConfig(nodeId);

        if (!saved?.ownerId && msg.pairingCode) {
          const claimedOwnerId = await dbClaimPairingCode(msg.pairingCode);
          if (claimedOwnerId) {
            await dbSaveNodeConfig(nodeId, {
              ownerId:  claimedOwnerId,
              nodeType: saved?.nodeType || msg.nodeType || NODE_TYPE.SENSOR,
              label:    saved?.label    || msg.label    || `Node-${nodeId.slice(0,4)}`,
              location: saved?.location || msg.location || 'Chua cau hinh',
            });
            saved = await dbGetNodeConfig(nodeId);
            console.log(`[PAIR] Node ${nodeId} da ghep voi user ${claimedOwnerId}`);
          } else {
            console.warn(`[PAIR] Ma pairing khong hop le/het han cho node ${nodeId}`);
          }
        }

        nodes.set(nodeId, {
          ws,
          info: {
            nodeId,
            ownerId:  saved?.ownerId || null,
            nodeType: saved?.nodeType || msg.nodeType || NODE_TYPE.SENSOR,
            label:    saved?.label    || msg.label    || `Node-${nodeId.slice(0,4)}`,
            location: saved?.location || msg.location || 'Chua cau hinh',
            thresholdOverride: saved?.thresholdOverride || null,
            status: 'normal', lastSeen: new Date().toISOString(),
            smoke: 0, temp: 0,
          },
        });
        const info = nodes.get(nodeId).info;
        ws.send(JSON.stringify({
          type: 'registered', nodeId, label: info.label, location: info.location,
          nodeType: info.nodeType, claimed: !!info.ownerId,
        }));
        // ✅ FIX: trước đây gọi getNodeList() không tham số -> mặc định lọc theo ownerId=null,
        // khiến broadcast ra danh sách gần như rỗng cho MỌI browser mỗi khi có ESP32 connect
        broadcastToBrowsers({ type: 'nodes_update', nodes: getNodeList(info.ownerId, false) }, info.ownerId);
        console.log(`[REG] ${nodeId} (${info.label}) ${info.nodeType} owner=${info.ownerId || 'chua ghep'}`);
        break;
      }

      case 'browser_connect': {
        clientType = 'browser';
        if (!wsUser && MONGODB_URI && GOOGLE_CLIENT_ID) {
          ws.send(JSON.stringify({ type: 'error', message: 'Unauthorized' }));
          break;
        }
        ws.ownerId = wsUser ? String(tenantIdOf(wsUser)) : null;
        ws.isAdmin = isAdmin;
        browserClients.add(ws);
        ws.send(JSON.stringify({ type: 'nodes_update',   nodes: getNodeList(ws.ownerId, ws.isAdmin) }));
        ws.send(JSON.stringify({ type: 'alert_log',      log: await dbGetAlerts(ws.ownerId, ws.isAdmin, 50) }));
        ws.send(JSON.stringify({ type: 'tts_configured', configured: !!(VBEE_TOKEN && VBEE_APP_ID) }));
        break;
      }

      case 'sensor_data': {
        const node = nodes.get(msg.nodeId);
        if (!node) break;
        node.info.smoke    = msg.smoke ?? node.info.smoke;
        node.info.temp     = msg.temp  ?? node.info.temp;
        node.info.lastSeen = new Date().toISOString();
        broadcastToBrowsers({ type: 'sensor_update', nodeId: msg.nodeId, smoke: node.info.smoke, temp: node.info.temp }, node.info.ownerId);
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
          message: `Het chay tai ${node.info.location}`, source: 'auto',
          ownerId: node.info.ownerId, timestamp: new Date().toISOString(),
        };
        await dbLogAlert(entry);
        const cId = getCenterId();
        if (cId) sendToNode(cId, { type: 'lora_broadcast', command: 'BUZZER_OFF' });
        broadcastToBrowsers({ type: 'fire_clear', ...entry, nodes: getNodeList(node.info.ownerId, false) }, node.info.ownerId);
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
          message: `Khoi giam, tu dong dung canh bao tai ${node.info.location}`,
          source: 'auto', ownerId: node.info.ownerId, timestamp: new Date().toISOString(),
        };
        await dbLogAlert(entry);
        broadcastToBrowsers({ type: 'fire_clear', ...entry, nodes: getNodeList(node.info.ownerId, false) }, node.info.ownerId);
        console.log(`[AUTO_CLEAR] ${node.info.label} @ ${node.info.location}`);
        break;
      }

      case 'manual_alert': {
        if (!canControl) break;
        const targetNode = nodes.get(msg.targetNodeId);
        if (!targetNode) break;
        if (!isAdmin && String(targetNode.info.ownerId) !== String(tenantIdOf(wsUser))) break;
        await handleFireEvent(msg.targetNodeId, msg.source || 'manual_web');
        break;
      }

      case 'send_tts': {
        if (!canControl) break;
        const ownsNode = (n) => isAdmin || String(n.info.ownerId) === String(tenantIdOf(wsUser));
        if (msg.targetNodeId === 'all') {
          for (const [id, node] of nodes) {
            if (node.info.nodeType === NODE_TYPE.CENTER) continue;
            if (!ownsNode(node)) continue;
            await sendTTSToNode(id, msg.text);
          }
        } else {
          const node = nodes.get(msg.targetNodeId);
          if (node && ownsNode(node)) await sendTTSToNode(msg.targetNodeId, msg.text);
        }
        break;
      }

      case 'button_press': {
        if (!canControl) break;
        const btnNode = nodes.get(msg.nodeId);
        broadcastToBrowsers({ type: 'button_press', button: msg.button, nodeId: msg.nodeId }, btnNode?.info.ownerId);
        break;
      }

      case 'update_node': {
        if (!canControl) break;
        const node = nodes.get(msg.nodeId);
        if (node && !isAdmin && String(node.info.ownerId) !== String(tenantIdOf(wsUser))) break;
        if (node) {
          if (msg.label)    node.info.label    = msg.label;
          if (msg.location) node.info.location = msg.location;
          sendToNode(msg.nodeId, { type: 'config_update', label: node.info.label, location: node.info.location });
        }
        await dbSaveNodeConfig(msg.nodeId, {
          label:    msg.label    || node?.info.label,
          location: msg.location || node?.info.location,
          nodeType: msg.nodeType || node?.info.nodeType || 'sensor',
          ownerId:  node?.info.ownerId,
        });
        broadcastToBrowsers({ type: 'nodes_update', nodes: getNodeList(node?.info.ownerId, false) }, node?.info.ownerId);
        break;
      }

      case 'reset_all': {
        if (!canControl) break;
        for (const [id, node] of nodes) {
          if (!isAdmin && String(node.info.ownerId) !== String(tenantIdOf(wsUser))) continue;
          node.info.status = 'normal';
          sendToNode(id, { type: 'reset' });
        }
        const cId = getCenterId();
        if (cId) sendToNode(cId, { type: 'lora_broadcast', command: 'BUZZER_OFF' });
        const ownerId = isAdmin ? null : String(tenantIdOf(wsUser));
        const entry = {
          id: uuidv4(), type: 'reset',
          label: 'Dashboard', location: 'Tat ca',
          message: 'Reset toan he thong tu dashboard',
          source: 'manual_web', ownerId, timestamp: new Date().toISOString(),
        };
        await dbLogAlert(entry);
        broadcastToBrowsers({ type: 'reset_all', nodes: getNodeList(ownerId, isAdmin), ...entry }, ownerId);
        break;
      }
    }
  });

  ws.on('close', () => {
    if (clientType === 'browser') browserClients.delete(ws);
    else if (clientType === 'node' && nodeId) {
      const closedInfo = nodes.get(nodeId)?.info;
      nodes.delete(nodeId);
      // ✅ FIX: lấy ownerId TRƯỚC khi xoá khỏi Map, và truyền đúng vào getNodeList()/broadcastToBrowsers()
      // — lỗi cũ gọi getNodeList() không tham số khi ESP32 ngắt kết nối, làm sai danh sách gửi về browser
      broadcastToBrowsers({ type: 'nodes_update', nodes: getNodeList(closedInfo?.ownerId, false) }, closedInfo?.ownerId);
      console.log(`[DC] ${nodeId}`);
    }
  });
});

app.get('/api/nodes', (req, res) => {
  const isAdmin = req.user?.role === 'admin';
  res.json(getNodeList(tenantIdOf(req.user), isAdmin));
});
app.get('/api/alerts', async (req, res) => {
  const isAdmin = req.user?.role === 'admin';
  const limit   = Math.min(parseInt(req.query.limit) || 100, 500);
  res.json(await dbGetAlerts(tenantIdOf(req.user), isAdmin, limit));
});

app.delete('/api/alerts', async (req, res) => {
  const { ids } = req.body; // mảng các id (string) của các alert cần xoá, VD: ["abc123", "def456"]
  if (!Array.isArray(ids) || ids.length === 0) {
    return res.status(400).json({ error: 'Thieu danh sach id can xoa' });
  }
 
  const isAdmin = req.user?.role === 'admin';
  const tenantId = tenantIdOf(req.user);
 
  // Chỉ cho phép xoá alert THUỘC ĐÚNG tenant của mình — tránh trường hợp
  // 1 user cố tình gửi id của alert người khác lên để xoá trộm dữ liệu.
  // Admin (isAdmin) được xoá mọi alert, user thường chỉ xoá alert của mình.
  const filter = isAdmin
    ? { id: { $in: ids } }
    : { id: { $in: ids }, ownerId: tenantId };
 
  const result = await Alert.deleteMany(filter);
  res.json({ success: true, deletedCount: result.deletedCount });
});
 
app.post('/api/invite-code', requireRole('admin', 'operator'), async (req, res) => {
  if (!mongoConnected) return res.status(503).json({ error: 'Can MongoDB de dung tinh nang moi' });
  const role = req.body.role === 'operator' ? 'operator' : 'viewer';
  let code;
  do { code = String(Math.floor(100000 + Math.random() * 900000)); }
  while (await InviteCode.findOne({ code, used: false }).lean());
  await InviteCode.create({ code, ownerId: tenantIdOf(req.user), role });
  res.json({ code, role, expiresInSeconds: 3600 });
});

app.post('/api/join-household', requireAuth, async (req, res) => {
  const { code } = req.body;
  const invite = await InviteCode.findOneAndUpdate({ code, used: false }, { used: true }, { new: true });
  if (!invite) return res.status(400).json({ error: 'Ma moi khong hop le hoac da het han' });
  if (String(invite.ownerId) === String(req.user._id)) {
    return res.status(400).json({ error: 'Khong the tu moi chinh minh' });
  }
  await User.updateOne({ _id: req.user._id }, { belongsToOwnerId: invite.ownerId, role: invite.role });
  res.json({ success: true, role: invite.role });
});

app.post('/api/leave-household', requireAuth, async (req, res) => {
  await User.updateOne({ _id: req.user._id }, { belongsToOwnerId: null, role: 'viewer' });
  res.json({ success: true });
});

// ============================================================================
// ENDPOINT MỚI — Quản lý thành viên đã mời (dành cho CHỦ HỘ)
// Cho phép chủ hộ xem danh sách những tài khoản Google đã được mời vào xem
// chung hệ thống của mình, và có thể "gỡ" (thu hồi quyền truy cập) từng người.
//
// CÁCH DÁN: mở server.js, tìm dòng:
//   app.post('/api/leave-household', ...);
// Dán NGUYÊN ĐOẠN BÊN DƯỚI ngay phía DƯỚI route đó.
// ============================================================================

// Xem danh sách thành viên đã mời — CHỈ chủ hộ (người không có belongsToOwnerId)
// mới gọi được; người được mời gọi endpoint này sẽ bị từ chối.
app.get('/api/household-members', async (req, res) => {
  if (req.user.belongsToOwnerId) {
    return res.status(403).json({ error: 'Chi chu ho moi xem duoc danh sach thanh vien' });
  }
  const members = await User.find({ belongsToOwnerId: req.user._id })
    .select('name email role avatar')
    .lean();
  res.json(members);
});

// Gỡ 1 thành viên khỏi hộ — đưa họ về trạng thái độc lập (giống hệt khi họ
// tự bấm "Rời nhóm"), chỉ chủ hộ mới gỡ được, và chỉ gỡ được đúng thành viên
// thuộc hộ của mình (tránh chủ hộ A gỡ nhầm thành viên của chủ hộ B).
app.delete('/api/household-members/:userId', async (req, res) => {
  if (req.user.belongsToOwnerId) {
    return res.status(403).json({ error: 'Chi chu ho moi thuc hien duoc thao tac nay' });
  }
  const member = await User.findOne({ _id: req.params.userId, belongsToOwnerId: req.user._id });
  if (!member) {
    return res.status(404).json({ error: 'Khong tim thay thanh vien nay trong he thong cua ban' });
  }
  member.belongsToOwnerId = null;
  member.role = 'viewer';
  await member.save();
  res.json({ success: true });
});

app.post('/api/pairing-code', requireRole('admin', 'operator'), async (req, res) => {
  if (!mongoConnected) return res.status(503).json({ error: 'Can MongoDB de dung tinh nang ghep thiet bi' });
  try {
    const code = await dbCreatePairingCode(tenantIdOf(req.user));
    res.json({ code, expiresInSeconds: 600 });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

app.patch('/api/nodes/:nodeId/owner', requireRole('admin'), async (req, res) => {
  const { newOwnerEmail } = req.body;
  const targetUser = await User.findOne({ email: newOwnerEmail }).lean();
  if (!targetUser) return res.status(404).json({ error: 'Khong tim thay user voi email nay' });
  await dbSaveNodeConfig(req.params.nodeId, { ownerId: targetUser._id });
  const live = nodes.get(req.params.nodeId);
  if (live) live.info.ownerId = targetUser._id;
  broadcastToBrowsers({ type: 'nodes_update', nodes: getNodeList(null, true) }, null);
  res.json({ success: true, newOwnerId: targetUser._id });
});

app.post('/api/nodes/:nodeId/release', requireRole('admin', 'operator'), async (req, res) => {
  const cfg = await dbGetNodeConfig(req.params.nodeId);
  if (!cfg) return res.status(404).json({ error: 'Khong tim thay node' });
  const isOwner = String(cfg.ownerId) === String(tenantIdOf(req.user));
  if (!isOwner && req.user.role !== 'admin') return res.status(403).json({ error: 'Khong phai chu so huu node nay' });

  const oldOwnerId = cfg.ownerId;
  await dbSaveNodeConfig(req.params.nodeId, { ownerId: null });
  const live = nodes.get(req.params.nodeId);
  if (live) live.info.ownerId = null;
  broadcastToBrowsers({ type: 'nodes_update', nodes: getNodeList(oldOwnerId, false) }, oldOwnerId);
  res.json({ success: true });
});

app.post('/api/nodes/:nodeId/claim', requireRole('admin', 'operator'), async (req, res) => {
  const { code } = req.body;
  const cfg = await dbGetNodeConfig(req.params.nodeId);
  if (!cfg) return res.status(404).json({ error: 'Khong tim thay node' });
  if (cfg.ownerId) return res.status(409).json({ error: 'Node nay da co chu so huu, can release truoc' });

  const pairing = await PairingCode.findOneAndUpdate(
    { code, used: false, ownerId: tenantIdOf(req.user) },
    { used: true }, { new: true }
  );
  if (!pairing) return res.status(400).json({ error: 'Ma pairing khong hop le, het han, hoac khong phai cua ban' });

  await dbSaveNodeConfig(req.params.nodeId, { ownerId: tenantIdOf(req.user) });
  const live = nodes.get(req.params.nodeId);
  if (live) live.info.ownerId = tenantIdOf(req.user);
  broadcastToBrowsers({ type: 'nodes_update', nodes: getNodeList(tenantIdOf(req.user), false) }, tenantIdOf(req.user));
  res.json({ success: true });
});
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

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`FireGuard Pro v2.2 - port ${PORT}`);
  console.log(`   TTS:  ${(VBEE_TOKEN && VBEE_APP_ID) ? 'OK' : 'No key'}`);
  console.log(`   DB:   ${MONGODB_URI      ? 'Connecting...' : 'No URI (in-memory)'}`);
  console.log(`   Auth: ${GOOGLE_CLIENT_ID ? 'Google OAuth'  : 'No OAuth (open access)'}`);
  console.log(`   Push: Expo Push Service (khong can cau hinh them)`);
});
