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
const admin          = require('firebase-admin');
const jwt            = require('jsonwebtoken');

// Token dùng RIÊNG cho app di động/desktop (web vẫn dùng session cookie như cũ)
function signMobileToken(user) {
  return jwt.sign(
    { _id: String(user._id), email: user.email, name: user.name, avatar: user.avatar, role: user.role },
    SESSION_SECRET,
    { expiresIn: '30d' } // Token sống 30 ngày, người dùng không phải đăng nhập lại liên tục
  );
}

const app    = express();
const server = http.createServer(app);
// noServer: true -> tự quản lý bước "upgrade" để có thể đọc session/user
// TRƯỚC KHI chấp nhận kết nối WebSocket (xem phần gắn session bên dưới)
const wss    = new WebSocket.Server({ noServer: true });

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
// Chuỗi JSON của Firebase Service Account, đã encode base64 (xem hướng dẫn lấy ở dưới)
const FIREBASE_SA_B64  = process.env.FIREBASE_SERVICE_ACCOUNT_BASE64 || '';

// ─── FIREBASE ADMIN (dùng để gửi push notification qua FCM) ───────────────────
let fcmReady = false;
if (FIREBASE_SA_B64) {
  try {
    const serviceAccount = JSON.parse(Buffer.from(FIREBASE_SA_B64, 'base64').toString('utf8'));
    admin.initializeApp({ credential: admin.credential.cert(serviceAccount) });
    fcmReady = true;
    console.log('[FCM] Firebase Admin đã khởi tạo');
  } catch (e) {
    console.error('[FCM] Lỗi khởi tạo Firebase Admin:', e.message);
  }
} else {
  console.warn('[FCM] Chưa cấu hình FIREBASE_SERVICE_ACCOUNT_BASE64 — push notification sẽ tắt');
}

// ─── MONGODB SCHEMAS ──────────────────────────────────────────────────────────
const userSchema = new mongoose.Schema({
  googleId:   { type: String, required: true, unique: true },
  email:      { type: String, required: true },
  name:       String,
  avatar:     String,
  role:       { type: String, enum: ['admin', 'operator', 'viewer'], default: 'viewer' },
  // null = đây là 1 "chủ hệ thống" độc lập. Có giá trị = "khách" được mời,
  // mọi dữ liệu họ xem là dữ liệu của user có _id này (không phải của chính họ)
  belongsToOwnerId: { type: mongoose.Schema.Types.ObjectId, ref: 'User', default: null },
  pushTokens: { type: [String], default: [] }, // token thiết bị (app di động/desktop) để gửi FCM
  // Ngưỡng ppm mặc định áp dụng cho MỌI node của user, trừ khi node đó có thresholdOverride riêng
  alertThresholds: {
    warn:   { type: Number, default: 150 }, // bắt đầu "cảnh báo sớm" (cam)
    danger: { type: Number, default: 400 }, // bắt đầu "nguy hiểm/cháy" (đỏ)
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
  // Ghi đè ngưỡng riêng cho node này; null = dùng ngưỡng mặc định của user (alertThresholds)
  thresholdOverride: {
    type: new mongoose.Schema({ warn: Number, danger: Number }, { _id: false }),
    default: null,
  },
  updatedAt: { type: Date, default: Date.now },
});
const NodeConfig = mongoose.model('NodeConfig', nodeConfigSchema);

// Mã ghép nối thiết bị: user tạo mã -> nhập vào ESP32 -> server gắn quyền sở hữu
const pairingCodeSchema = new mongoose.Schema({
  code:      { type: String, required: true, unique: true },
  ownerId:   { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
  used:      { type: Boolean, default: false },
  createdAt: { type: Date, default: Date.now, expires: 600 }, // tự xoá sau 10 phút (TTL index)
});
const PairingCode = mongoose.model('PairingCode', pairingCodeSchema);

// Trả về "chủ hệ thống thật sự" của 1 user — nếu là khách được mời thì trả về id của người mời họ
function tenantIdOf(user) {
  return user?.belongsToOwnerId || user?._id;
}

// Mã mời người (khác PairingCode là mã ghép THIẾT BỊ) — chủ hệ thống tạo mã này để mời thành viên gia đình
const inviteCodeSchema = new mongoose.Schema({
  code:      { type: String, required: true, unique: true },
  ownerId:   { type: mongoose.Schema.Types.ObjectId, ref: 'User', required: true },
  role:      { type: String, enum: ['operator', 'viewer'], default: 'viewer' }, // quyền của người được mời
  used:      { type: Boolean, default: false },
  createdAt: { type: Date, default: Date.now, expires: 3600 }, // hết hạn sau 1 giờ
});
const InviteCode = mongoose.model('InviteCode', inviteCodeSchema);

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

// ownerId=null + isAdmin=true -> trả về tất cả (dùng cho admin)
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

// Tạo mã pairing 6 số cho user, hết hạn sau 10 phút
async function dbCreatePairingCode(ownerId) {
  let code;
  do { code = String(Math.floor(100000 + Math.random() * 900000)); }
  while (await PairingCode.findOne({ code, used: false }).lean());
  await PairingCode.create({ code, ownerId });
  return code;
}

// Kiểm tra mã pairing hợp lệ, đánh dấu đã dùng, trả về ownerId
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
// Tách riêng thành biến `sessionMiddleware` để dùng lại được cho WebSocket upgrade bên dưới
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

// App di động gửi kèm header "Authorization: Bearer <token>" thay vì cookie.
// Nếu web đã có session rồi thì bỏ qua (ưu tiên session), chỉ dùng token khi CHƯA có session.
function attachTokenAuth(req, res, next) {
  if (req.isAuthenticated()) return next();
  const authHeader = req.headers.authorization;
  if (authHeader?.startsWith('Bearer ')) {
    try {
      const payload = jwt.verify(authHeader.slice(7), SESSION_SECRET);
      req.user = payload;
      req.isAuthenticated = () => true;
    } catch (e) { /* token sai/hết hạn -> coi như chưa đăng nhập, không chặn ở đây */ }
  }
  next();
}
app.use(attachTokenAuth);

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

// App di động gọi route này thay vì /auth/google — thêm state=mobile để callback biết đường trả token
app.get('/auth/google/mobile',
  passport.authenticate('google', { scope: ['profile', 'email'], state: 'mobile' })
);

app.get('/auth/google/callback',
  passport.authenticate('google', { failureRedirect: '/login.html?error=1' }),
  (req, res) => {
    if (req.query.state === 'mobile') {
      // Deep link quay lại app: fireguardapp://auth-callback?token=xxx
      const token = signMobileToken(req.user);
      return res.redirect(`fireguardapp://auth-callback?token=${token}`);
    }
    res.redirect('/');
  }
);

app.get('/auth/logout', (req, res) => {
  req.logout(() => { res.redirect('/login.html'); });
});

app.get('/api/me', (req, res) => {
  if (!req.isAuthenticated()) return res.json({ authenticated: false });
  const { name, email, avatar, role, alertThresholds } = req.user;
  res.json({ authenticated: true, name, email, avatar, role, alertThresholds });
});

// Đổi ngưỡng ppm mặc định cho toàn bộ node của user (trừ node đã có thresholdOverride riêng)
app.patch('/api/settings/thresholds', requireAuth, async (req, res) => {
  const { warn, danger } = req.body;
  if (typeof warn !== 'number' || typeof danger !== 'number' || warn >= danger) {
    return res.status(400).json({ error: 'warn/danger phải là số, và warn < danger' });
  }
  await User.updateOne({ _id: req.user._id }, { alertThresholds: { warn, danger } });
  res.json({ success: true, alertThresholds: { warn, danger } });
});

// Ghi đè ngưỡng riêng cho 1 node cụ thể (VD: node đặt trong bếp cần ngưỡng cao hơn)
// Gửi body = {} hoặc {warn:null, danger:null} để xoá override, quay về dùng ngưỡng mặc định của user
app.patch('/api/nodes/:nodeId/threshold', requireRole('admin', 'operator'), async (req, res) => {
  const { warn, danger } = req.body;
  const cfg = await dbGetNodeConfig(req.params.nodeId);
  if (!cfg) return res.status(404).json({ error: 'Không tìm thấy node' });
  if (req.user.role !== 'admin' && String(cfg.ownerId) !== String(tenantIdOf(req.user))) {
    return res.status(403).json({ error: 'Không phải chủ sở hữu node này' });
  }
  const override = (typeof warn === 'number' && typeof danger === 'number') ? { warn, danger } : null;
  await dbSaveNodeConfig(req.params.nodeId, { thresholdOverride: override });
  const live = nodes.get(req.params.nodeId);
  if (live) live.info.thresholdOverride = override;
  res.json({ success: true, thresholdOverride: override });
});

// App di động/desktop gọi route này sau khi lấy được FCM token từ thiết bị
app.post('/api/push-token', requireAuth, async (req, res) => {
  const { token } = req.body;
  if (!token) return res.status(400).json({ error: 'token required' });
  try {
    await User.updateOne({ _id: req.user._id }, { $addToSet: { pushTokens: token } });
    res.json({ success: true });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Gọi khi user đăng xuất trên 1 thiết bị cụ thể, để không nhận push nữa trên máy đó
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

const TTS_CACHE_DIR = path.join(__dirname, 'tts-cache');
fs.mkdirSync(TTS_CACHE_DIR, { recursive: true });
app.use('/tts-audio', express.static(TTS_CACHE_DIR));

app.use(requireAuth);
app.use(express.static(path.join(__dirname, 'public')));

// ─── IN-MEMORY STATE ──────────────────────────────────────────────────────────
const nodes          = new Map();
const browserClients = new Set();
const NODE_TYPE      = { CENTER: 'center', SENSOR: 'sensor' };

// targetOwnerId=null -> sự kiện hệ thống chung (VD: reset), gửi cho mọi browser đã đăng nhập
// targetOwnerId='xxx' -> chỉ gửi cho browser của đúng chủ sở hữu node đó (+ admin luôn thấy hết)
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

// ownerId/isAdmin: nếu isAdmin=true trả về tất cả node, ngược lại chỉ node của user đó
function getNodeList(ownerId = null, isAdmin = false) {
  return [...nodes.entries()]
    .filter(([, n]) => isAdmin || String(n.info.ownerId) === String(ownerId))
    .map(([id, n]) => ({ id, ...n.info, connected: true }));
}

function getCenterId() {
  return [...nodes.entries()].find(([, n]) => n.info.nodeType === NODE_TYPE.CENTER)?.[0];
}

// ─── TTS (Vbee) ───────────────────────────────────────────────────────────
const pendingTTS = new Map(); // requestId -> { resolve, timer }

async function vbeeSynthesize(text, timeoutMs = 20000, pollInterval = 1500) {
  if (!VBEE_TOKEN || !VBEE_APP_ID) return null;
  try {
    // Bước 1: Tạo request
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

    // Bước 2: Poll cho tới khi COMPLETED
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
          }, 2 * 60 * 1000); // tự xoá sau 2 phút
          return `${BASE_URL}/tts-audio/${filename}`;
        } catch (e) {
          console.error('[TTS] Loi tai audio ve cache:', e.message);
          return remoteUrl; // fallback: thử gửi thẳng link Vbee nếu tải lỗi
        }
      }
      if (pollData.status === 'FAILED') {
        console.error('[TTS] Vbee request FAILED:', JSON.stringify(pollData));
        return null;
      }
      // status === 'PROCESSING' -> vòng lặp tiếp tục poll
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
  const fireLoc  = fireNodeInfo.location || 'khu vực không xác định';
  const recvInfo = nodes.get(receiverId)?.info;
  const recvLoc  = recvInfo?.location || 'khu vực của bạn';
  if (receiverId === fireNodeInfo.nodeId)
    return `Cảnh báo cháy! Khu vực ${fireLoc} đang có cháy. Mọi người hãy nhanh chóng sơ tán!`;
  return `Cảnh báo cháy! Bạn đang ở khu vực ${recvLoc}. Khu vực ${fireLoc} đang có cháy. Mọi người hãy nhanh chóng sơ tán!`;
}

// ─── PUSH NOTIFICATION (FCM) ────────────────────────────────────────────────
// Gửi push đến TẤT CẢ thiết bị (điện thoại/desktop) mà user `ownerId` đã đăng ký
async function sendPushToOwner(ownerId, title, body, data = {}) {
  if (!fcmReady || !ownerId) return;
  try {
    const user = await User.findById(ownerId).lean();
    const tokens = user?.pushTokens || [];
    if (tokens.length === 0) return;

    const res = await admin.messaging().sendEachForMulticast({
      tokens,
      notification: { title, body },
      data: Object.fromEntries(Object.entries(data).map(([k, v]) => [k, String(v)])),
      android: { priority: 'high' },
      apns: { payload: { aps: { sound: 'default', 'content-available': 1 } } },
    });

    // Dọn dẹp các token không còn hợp lệ (app đã gỡ cài đặt, token hết hạn...)
    const deadTokens = [];
    res.responses.forEach((r, i) => {
      if (!r.success && ['messaging/invalid-registration-token', 'messaging/registration-token-not-registered'].includes(r.error?.code)) {
        deadTokens.push(tokens[i]);
      }
    });
    if (deadTokens.length > 0) {
      await User.updateOne({ _id: ownerId }, { $pullAll: { pushTokens: deadTokens } });
      console.log(`[FCM] Đã dọn ${deadTokens.length} token chết của user ${ownerId}`);
    }
    console.log(`[FCM] Gửi push tới user ${ownerId}: ${res.successCount}/${tokens.length} thành công`);
  } catch (e) {
    console.error('[FCM] Lỗi gửi push:', e.message);
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
    message:  `Phát hiện cháy tại ${fireNode.info.location}`,
    ownerId,
    source, timestamp: new Date().toISOString(),
  };
  await dbLogAlert(entry);
  broadcastToBrowsers({ type: 'fire_alert', ...entry, nodes: getNodeList(ownerId, false) }, ownerId);
  // Push notification: gửi ngay cả khi app đang tắt hẳn (đúng yêu cầu ban đầu của bạn)
  sendPushToOwner(
    ownerId,
    '🔥 Cảnh báo cháy!',
    `Phát hiện cháy tại ${fireNode.info.location} (${fireNode.info.label})`,
    { type: 'fire_alert', nodeId: fireNodeId, location: fireNode.info.location }
  );
  const centerId = getCenterId();
  if (centerId) sendToNode(centerId, { type: 'lora_broadcast', command: 'BUZZER_ON', fireNodeId, location: fireNode.info.location });
  for (const [id, node] of nodes) {
    if (node.info.nodeType === NODE_TYPE.CENTER) continue;
    const text = buildTTSText(id, { ...fireNode.info, nodeId: fireNodeId });
    await sendTTSToNode(id, text);
  }
  console.log(`[FIRE] ${source} — ${fireNode.info.label} @ ${fireNode.info.location}`);
}

// ─── WEBSOCKET UPGRADE (xác thực user TRƯỚC khi accept kết nối) ───────────────
// Node/ESP32 không gửi cookie session -> req.user sẽ là undefined, coi là "device"
// Browser đã đăng nhập -> cookie session hợp lệ -> req.user chứa thông tin user
server.on('upgrade', (req, socket, head) => {
  sessionMiddleware(req, {}, () => {
    passportInit(req, {}, () => {
      passportSession(req, {}, () => {
        // Web đăng nhập qua session (cookie) đã xong ở trên.
        // Mobile không gửi cookie -> đọc token từ query string: wss://host/?token=xxx
        if (!req.user) {
          try {
            const { query } = require('url').parse(req.url, true);
            if (query.token) req.user = jwt.verify(query.token, SESSION_SECRET);
          } catch (e) { /* token sai -> req.user vẫn undefined, coi như thiết bị (ESP32) */ }
        }
        wss.handleUpgrade(req, socket, head, (ws) => {
          wss.emit('connection', ws, req);
        });
      });
    });
  });
});

// ─── WEBSOCKET ────────────────────────────────────────────────────────────────
wss.on('connection', (ws, req) => {
  let clientType = null;
  let nodeId     = null;
  // req.user chỉ tồn tại nếu trình duyệt đã đăng nhập (ESP32 sẽ không có)
  const wsUser   = req.user || null;
  const isAdmin  = wsUser?.role === 'admin';
  const canControl = isAdmin || wsUser?.role === 'operator'; // viewer = chỉ xem, không được điều khiển

  ws.on('message', async (raw) => {
    if (Buffer.isBuffer(raw) && raw[0] !== 0x7b) return;
    let msg;
    try { msg = JSON.parse(raw.toString()); } catch { return; }

    switch (msg.type) {

      case 'register': {
        clientType = 'node';
        nodeId     = msg.nodeId || uuidv4();
        let saved  = await dbGetNodeConfig(nodeId);

        // Node chưa có chủ (ownerId null/chưa tồn tại) -> thử claim bằng pairing code
        if (!saved?.ownerId && msg.pairingCode) {
          const claimedOwnerId = await dbClaimPairingCode(msg.pairingCode);
          if (claimedOwnerId) {
            await dbSaveNodeConfig(nodeId, {
              ownerId:  claimedOwnerId,
              nodeType: saved?.nodeType || msg.nodeType || NODE_TYPE.SENSOR,
              label:    saved?.label    || msg.label    || `Node-${nodeId.slice(0,4)}`,
              location: saved?.location || msg.location || 'Chưa cấu hình',
            });
            saved = await dbGetNodeConfig(nodeId);
            console.log(`[PAIR] Node ${nodeId} đã ghép với user ${claimedOwnerId}`);
          } else {
            console.warn(`[PAIR] Mã pairing không hợp lệ/hết hạn cho node ${nodeId}`);
          }
        }

        nodes.set(nodeId, {
          ws,
          info: {
            nodeId,
            ownerId:  saved?.ownerId || null,
            nodeType: saved?.nodeType || msg.nodeType || NODE_TYPE.SENSOR,
            label:    saved?.label    || msg.label    || `Node-${nodeId.slice(0,4)}`,
            location: saved?.location || msg.location || 'Chưa cấu hình',
            thresholdOverride: saved?.thresholdOverride || null, // null = dùng ngưỡng mặc định của user
            status: 'normal', lastSeen: new Date().toISOString(),
            smoke: 0, temp: 0,
          },
        });
        const info = nodes.get(nodeId).info;
        ws.send(JSON.stringify({
          type: 'registered', nodeId, label: info.label, location: info.location,
          nodeType: info.nodeType, claimed: !!info.ownerId,
        }));
        broadcastToBrowsers({ type: 'nodes_update', nodes: getNodeList() }, info.ownerId);
        console.log(`[REG] ${nodeId} (${info.label}) ${info.nodeType} owner=${info.ownerId || 'chưa ghép'}`);
        break;
      }

      case 'browser_connect': {
        clientType = 'browser';
        // Chưa đăng nhập (và server đang bật auth) -> không cho nghe dữ liệu
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
          message: `Hết cháy tại ${node.info.location}`, source: 'auto',
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
          message: `Khói giảm, tự động dừng cảnh báo tại ${node.info.location}`,
          source: 'auto', ownerId: node.info.ownerId, timestamp: new Date().toISOString(),
        };
        await dbLogAlert(entry);
        broadcastToBrowsers({ type: 'fire_clear', ...entry, nodes: getNodeList(node.info.ownerId, false) }, node.info.ownerId);
        console.log(`[AUTO_CLEAR] ${node.info.label} @ ${node.info.location}`);
        break;
      }

      case 'manual_alert': {
        if (!canControl) break; // Khách "chỉ xem" (viewer) không được báo cháy thử
        const targetNode = nodes.get(msg.targetNodeId);
        if (!targetNode) break;
        if (!isAdmin && String(targetNode.info.ownerId) !== String(tenantIdOf(wsUser))) break; // không phải chủ -> bỏ qua
        await handleFireEvent(msg.targetNodeId, msg.source || 'manual_web');
        break;
      }

      case 'send_tts': {
        if (!canControl) break; // Khách "chỉ xem" không được phát loa
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
        if (!canControl) break; // Khách "chỉ xem" không được bấm remote node trung tâm
        const btnNode = nodes.get(msg.nodeId);
        broadcastToBrowsers({ type: 'button_press', button: msg.button, nodeId: msg.nodeId }, btnNode?.info.ownerId);
        break;
      }

      case 'update_node': {
        if (!canControl) break; // Khách "chỉ xem" không được sửa tên/vị trí node
        const node = nodes.get(msg.nodeId);
        // Chỉ cho sửa node của chính mình (trừ admin)
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
        if (!canControl) break; // Khách "chỉ xem" không được reset hệ thống
        // Chỉ reset các node thuộc về user đang gọi (admin reset được tất cả)
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
          label: 'Dashboard', location: 'Tất cả',
          message: 'Reset toàn hệ thống từ dashboard',
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
      nodes.delete(nodeId);
      broadcastToBrowsers({ type: 'nodes_update', nodes: getNodeList() });
      console.log(`[DC] ${nodeId}`);
    }
  });
});

// ─── REST API ─────────────────────────────────────────────────────────────────
app.get('/api/nodes', (req, res) => {
  const isAdmin = req.user?.role === 'admin';
  res.json(getNodeList(tenantIdOf(req.user), isAdmin));
});
app.get('/api/alerts', async (req, res) => {
  const isAdmin = req.user?.role === 'admin';
  const limit   = Math.min(parseInt(req.query.limit) || 100, 500);
  res.json(await dbGetAlerts(tenantIdOf(req.user), isAdmin, limit));
});

// Tạo mã mời để thêm 1 thành viên (VD: người thân) vào xem chung hệ thống của mình
app.post('/api/invite-code', requireRole('admin', 'operator'), async (req, res) => {
  if (!mongoConnected) return res.status(503).json({ error: 'Cần MongoDB để dùng tính năng mời' });
  const role = req.body.role === 'operator' ? 'operator' : 'viewer'; // mặc định chỉ xem
  let code;
  do { code = String(Math.floor(100000 + Math.random() * 900000)); }
  while (await InviteCode.findOne({ code, used: false }).lean());
  // Mã mời luôn gắn với chủ hệ thống thật (nếu chính người tạo mã cũng đang là khách của ai đó)
  await InviteCode.create({ code, ownerId: tenantIdOf(req.user), role });
  res.json({ code, role, expiresInSeconds: 3600 });
});

// User đang đăng nhập nhập mã mời để trở thành "khách" xem hệ thống của người khác
app.post('/api/join-household', requireAuth, async (req, res) => {
  const { code } = req.body;
  const invite = await InviteCode.findOneAndUpdate({ code, used: false }, { used: true }, { new: true });
  if (!invite) return res.status(400).json({ error: 'Mã mời không hợp lệ hoặc đã hết hạn' });
  if (String(invite.ownerId) === String(req.user._id)) {
    return res.status(400).json({ error: 'Không thể tự mời chính mình' });
  }
  await User.updateOne({ _id: req.user._id }, { belongsToOwnerId: invite.ownerId, role: invite.role });
  res.json({ success: true, role: invite.role });
});

// Rời khỏi hệ thống đang xem chung, quay về làm chủ hệ thống độc lập của chính mình
app.post('/api/leave-household', requireAuth, async (req, res) => {
  await User.updateOne({ _id: req.user._id }, { belongsToOwnerId: null, role: 'viewer' });
  res.json({ success: true });
});
app.post('/api/pairing-code', requireRole('admin', 'operator'), async (req, res) => {
  if (!mongoConnected) return res.status(503).json({ error: 'Cần MongoDB để dùng tính năng ghép thiết bị' });
  try {
    const code = await dbCreatePairingCode(tenantIdOf(req.user));
    res.json({ code, expiresInSeconds: 600 });
  } catch (e) { res.status(500).json({ error: e.message }); }
});

// Admin đổi thẳng chủ sở hữu node bằng email người mới
app.patch('/api/nodes/:nodeId/owner', requireRole('admin'), async (req, res) => {
  const { newOwnerEmail } = req.body;
  const targetUser = await User.findOne({ email: newOwnerEmail }).lean();
  if (!targetUser) return res.status(404).json({ error: 'Không tìm thấy user với email này' });
  await dbSaveNodeConfig(req.params.nodeId, { ownerId: targetUser._id });
  const live = nodes.get(req.params.nodeId);
  if (live) live.info.ownerId = targetUser._id;
  broadcastToBrowsers({ type: 'nodes_update', nodes: getNodeList(null, true) }, null); // báo lại cho mọi admin
  res.json({ success: true, newOwnerId: targetUser._id });
});

// Chủ hiện tại (hoặc admin) "nhả" node ra -> node về trạng thái chưa ai sở hữu
app.post('/api/nodes/:nodeId/release', requireRole('admin', 'operator'), async (req, res) => {
  const cfg = await dbGetNodeConfig(req.params.nodeId);
  if (!cfg) return res.status(404).json({ error: 'Không tìm thấy node' });
  const isOwner = String(cfg.ownerId) === String(tenantIdOf(req.user));
  if (!isOwner && req.user.role !== 'admin') return res.status(403).json({ error: 'Không phải chủ sở hữu node này' });

  const oldOwnerId = cfg.ownerId;
  await dbSaveNodeConfig(req.params.nodeId, { ownerId: null });
  const live = nodes.get(req.params.nodeId);
  if (live) live.info.ownerId = null;
  broadcastToBrowsers({ type: 'nodes_update', nodes: getNodeList(oldOwnerId, false) }, oldOwnerId); // node biến mất khỏi màn hình chủ cũ
  res.json({ success: true });
});

// User đang đăng nhập tự nhận 1 node đang "chưa ai sở hữu" bằng mã pairing của chính họ
app.post('/api/nodes/:nodeId/claim', requireRole('admin', 'operator'), async (req, res) => {
  const { code } = req.body;
  const cfg = await dbGetNodeConfig(req.params.nodeId);
  if (!cfg) return res.status(404).json({ error: 'Không tìm thấy node' });
  if (cfg.ownerId) return res.status(409).json({ error: 'Node này đã có chủ sở hữu, cần release trước' });

  const pairing = await PairingCode.findOneAndUpdate(
    { code, used: false, ownerId: tenantIdOf(req.user) },
    { used: true }, { new: true }
  );
  if (!pairing) return res.status(400).json({ error: 'Mã pairing không hợp lệ, hết hạn, hoặc không phải của bạn' });

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

// ─── START ────────────────────────────────────────────────────────────────────
const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
  console.log(`🔥 FireGuard Pro v2.2 — port ${PORT}`);
  console.log(`   TTS:  ${(VBEE_TOKEN && VBEE_APP_ID) ? '✅ OK' : '❌ No key'}`);
  console.log(`   DB:   ${MONGODB_URI      ? '⏳ Connecting...' : '❌ No URI (in-memory)'}`);
  console.log(`   Auth: ${GOOGLE_CLIENT_ID ? '✅ Google OAuth'  : '❌ No OAuth (open access)'}`);
  console.log(`   FCM:  ${fcmReady ? '✅ Push notification bật' : '❌ Chưa cấu hình'}`);
});
