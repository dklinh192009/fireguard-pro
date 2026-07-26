/* ═══════════════════════════════════════════════════════
   FireGuard Pro — Frontend App JS
═══════════════════════════════════════════════════════ */

// ─── State ──────────────────────────────────────────────
let ws = null;
let nodes = {};
let alertLog = [];
let reconnectTimer = null;
let alertCount = 0;

// ─── WebSocket ──────────────────────────────────────────
function connectWS() {
  const protocol = location.protocol === 'https:' ? 'wss' : 'ws';
  ws = new WebSocket(`${protocol}://${location.host}`);

  ws.onopen = () => {
    setWSStatus('connected', 'Đã kết nối');
    ws.send(JSON.stringify({ type: 'browser_connect' }));
    clearTimeout(reconnectTimer);
    fetchStatus();
  };

  ws.onmessage = (e) => {
    try { handleMessage(JSON.parse(e.data)); }
    catch (err) { console.warn('WS parse error', err); }
  };

  ws.onclose = () => {
    setWSStatus('error', 'Mất kết nối...');
    reconnectTimer = setTimeout(connectWS, 3000);
  };

  ws.onerror = () => ws.close();
}

function sendWS(data) {
  if (ws && ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(data));
}

function setWSStatus(state, label) {
  const dot = document.getElementById('ws-dot');
  const lbl = document.getElementById('ws-label');
  dot.className = `ws-indicator ${state}`;
  lbl.textContent = label;
}

// ─── Message Handler ─────────────────────────────────────
function handleMessage(msg) {
  switch (msg.type) {
    case 'nodes_update':
      updateNodes(msg.nodes);
      break;
    case 'sensor_update':
      updateSensorData(msg);
      break;
    case 'fire_alert':
      if (msg.nodes) updateNodes(msg.nodes);
      addAlertEntry(msg);
      triggerFireOverlay(msg);
      playAlertSound();
      updateAlertBadge();
      break;
    case 'fire_clear':
      if (msg.nodes) updateNodes(msg.nodes);
      addAlertEntry(msg);
      break;
    case 'manual_alert_sent':
      if (msg.nodes) updateNodes(msg.nodes);
      addAlertEntry(msg);
      break;
    case 'reset_all':
      if (msg.nodes) updateNodes(msg.nodes);
      addAlertEntry(msg);
      break;
    case 'alert_log':
      alertLog = msg.log || [];
      renderAlertLog();
      updateAlertBadge();
      break;
    case 'button_press':
      showRemoteStatus(msg.button);
      break;
  }
}

// ─── Nodes ───────────────────────────────────────────────
function updateNodes(list) {
  nodes = {};
  list.forEach(n => { nodes[n.id] = n; });
  renderNodeGrid();
  renderNodesTable();
  renderControlGrid();
  updateTTSSelect();
  updateStats();
}

function updateSensorData(msg) {
  if (nodes[msg.nodeId]) {
    nodes[msg.nodeId].smoke = msg.smoke;
    nodes[msg.nodeId].temp = msg.temp;
    // Update card if visible
    const smokeEl = document.getElementById(`smoke-${msg.nodeId}`);
    const tempEl = document.getElementById(`temp-${msg.nodeId}`);
    if (smokeEl) smokeEl.textContent = `${msg.smoke} ppm`;
    if (tempEl) tempEl.textContent = `${msg.temp}°C`;
  }
}

function renderNodeGrid() {
  const grid = document.getElementById('node-grid');
  const nodeList = Object.values(nodes);
  if (!nodeList.length) {
    grid.innerHTML = `<div class="node-empty">
      <span class="material-icons-round">sensors_off</span>
      <p>Chưa có thiết bị nào kết nối</p>
      <small>ESP32 sẽ xuất hiện ở đây khi online</small>
    </div>`;
    return;
  }
  grid.innerHTML = nodeList.map(n => nodeCardHTML(n)).join('');
}

function nodeCardHTML(n) {
  const isCenter = n.nodeType === 'center';
  const isFire = n.status === 'fire';
  const typeLabel = isCenter ? 'Trung tâm' : 'Cảm biến';
  const typeClass = isCenter ? 'center' : 'sensor';
  const time = n.lastSeen ? new Date(n.lastSeen).toLocaleTimeString('vi-VN') : '—';

  return `
  <div class="node-card ${isFire ? 'fire' : ''}" id="card-${n.id}">
    <div class="node-card-header">
      <div>
        <span class="node-type-badge ${typeClass}">${typeLabel}</span>
        <div class="node-label" style="margin-top:8px">${esc(n.label)}</div>
        <div class="node-location">
          <span class="material-icons-round">place</span>
          ${esc(n.location)}
        </div>
      </div>
      <div class="node-status-dot ${isFire ? 'fire' : ''}"></div>
    </div>
    ${!isCenter ? `
    <div class="node-sensors">
      <div class="sensor-chip">
        <div class="sensor-chip-label">💨 Khói</div>
        <div class="sensor-chip-val" id="smoke-${n.id}">${n.smoke ?? 0} ppm</div>
      </div>
      <div class="sensor-chip">
        <div class="sensor-chip-label">🌡️ Nhiệt độ</div>
        <div class="sensor-chip-val" id="temp-${n.id}">${n.temp ?? 0}°C</div>
      </div>
    </div>` : `<div style="height:14px"></div>`}
    <div class="node-footer">
      <span class="node-lastseen">⏱ ${time}</span>
      <button class="btn-node-edit" onclick="openEditModal('${n.id}')">
        <span class="material-icons-round" style="font-size:0.9rem">edit</span> Cấu hình
      </button>
    </div>
  </div>`;
}

function renderNodesTable() {
  const tbody = document.getElementById('nodes-tbody');
  const nodeList = Object.values(nodes);
  if (!nodeList.length) {
    tbody.innerHTML = `<tr><td colspan="8" class="tbl-empty">Chưa có thiết bị</td></tr>`;
    return;
  }
  tbody.innerHTML = nodeList.map(n => {
    const isCenter = n.nodeType === 'center';
    const isFire = n.status === 'fire';
    return `<tr>
      <td><span class="node-type-badge ${isCenter?'center':'sensor'}">${isCenter?'Trung tâm':'Cảm biến'}</span></td>
      <td><code style="font-size:0.78rem;color:var(--text3)">${n.id.slice(0,8)}</code></td>
      <td>${esc(n.label)}</td>
      <td>${esc(n.location)}</td>
      <td>${isCenter ? '—' : `${n.smoke??0} ppm`}</td>
      <td>${isCenter ? '—' : `${n.temp??0}°C`}</td>
      <td><span class="status-pill ${isFire?'fire':'normal'}">
        <span class="material-icons-round" style="font-size:0.75rem">${isFire?'local_fire_department':'check_circle'}</span>
        ${isFire?'Đang cháy':'Bình thường'}
      </span></td>
      <td>
        <button class="btn-tbl edit" onclick="openEditModal('${n.id}')">
          <span class="material-icons-round" style="font-size:0.85rem">edit</span> Sửa
        </button>
        ${!isCenter ? `<button class="btn-tbl alert" onclick="manualAlert('${n.id}')">
          <span class="material-icons-round" style="font-size:0.85rem">warning</span> Báo động
        </button>` : ''}
      </td>
    </tr>`;
  }).join('');
}

function renderControlGrid() {
  const grid = document.getElementById('control-grid');
  const sensors = Object.values(nodes).filter(n => n.nodeType !== 'center');
  if (!sensors.length) {
    grid.innerHTML = `<div class="node-empty">
      <span class="material-icons-round">sensors_off</span>
      <p>Chưa có thiết bị sensor nào kết nối</p>
    </div>`;
    return;
  }
  grid.innerHTML = sensors.map(n => `
    <div class="control-card">
      <div class="control-card-title">${esc(n.label)}</div>
      <div class="control-card-loc">📍 ${esc(n.location)}</div>
      <div class="control-actions">
        <button class="btn-alarm fire" onclick="manualAlert('${n.id}')">
          <span class="material-icons-round">local_fire_department</span>
          Kích hoạt báo cháy + TTS
        </button>
        <button class="btn-alarm buzzer" onclick="activateBuzzer('${n.id}')">
          <span class="material-icons-round">notifications_active</span>
          Chỉ bật còi buzzer
        </button>
      </div>
    </div>`).join('');
}

function updateTTSSelect() {
  const sel = document.getElementById('tts-target');
  const val = sel.value;
  const sensors = Object.values(nodes).filter(n => n.nodeType !== 'center');
  sel.innerHTML = `<option value="all">📡 Phát tất cả node</option>` +
    sensors.map(n => `<option value="${n.id}">${esc(n.label)} — ${esc(n.location)}</option>`).join('');
  if (val) sel.value = val;
}

function updateStats() {
  const list = Object.values(nodes);
  document.getElementById('stat-online').textContent = list.length;
  document.getElementById('stat-fire').textContent = list.filter(n => n.status === 'fire').length;
}

// ─── Alert Feed ──────────────────────────────────────────
function addAlertEntry(entry) {
  alertLog.unshift(entry);
  renderAlertFeed();
  renderAlertLog();
}

function renderAlertFeed() {
  const feed = document.getElementById('alert-feed');
  const items = alertLog.slice(0, 8);
  if (!items.length) { feed.innerHTML = `<div class="feed-empty">Chưa có sự kiện nào.</div>`; return; }
  feed.innerHTML = items.map(e => feedItemHTML(e)).join('');
}

function renderAlertLog() {
  const wrap = document.getElementById('alert-log-wrap');
  if (!alertLog.length) { wrap.innerHTML = `<div class="feed-empty">Chưa có sự kiện nào.</div>`; return; }
  wrap.innerHTML = alertLog.map(e => feedItemHTML(e)).join('');
}

function feedItemHTML(e) {
  const icons = { fire: '🔥', clear: '✅', manual: '⚠️', reset: '🔄' };
  const labels = {
    fire: `Phát hiện cháy tại ${e.location||'—'}`,
    clear: `Đã tắt cảnh báo tại ${e.location||'—'}`,
    manual: `Báo động thủ công tại ${e.location||'—'}`,
    reset: `Reset tất cả hệ thống`,
  };
  const t = e.type || 'fire';
  const time = e.timestamp ? new Date(e.timestamp).toLocaleString('vi-VN') : '—';
  return `<div class="feed-item feed-type-${t}">
    <span class="feed-icon">${icons[t]||'📋'}</span>
    <div class="feed-body">
      <div class="feed-title">${labels[t]||t}</div>
      <div class="feed-meta">${esc(e.label||'—')} · ${time}</div>
    </div>
  </div>`;
}

function clearFeedUI() {
  alertLog = [];
  renderAlertFeed();
  renderAlertLog();
  alertCount = 0;
  updateAlertBadge();
}

function updateAlertBadge() {
  const count = alertLog.filter(e => e.type === 'fire' || e.type === 'manual').length;
  document.getElementById('badge-alerts').textContent = count;
  document.getElementById('stat-alerts').textContent = count;
}

// ─── Fire Overlay ─────────────────────────────────────────
function triggerFireOverlay(entry) {
  document.getElementById('overlay-location').textContent = `📍 ${entry.location || '—'}`;
  document.getElementById('overlay-time').textContent = new Date(entry.timestamp).toLocaleString('vi-VN');
  document.getElementById('fire-overlay').classList.remove('hidden');
}

function dismissOverlay() {
  document.getElementById('fire-overlay').classList.add('hidden');
}

function playAlertSound() {
  const audio = document.getElementById('alert-sound');
  if (audio) { audio.currentTime = 0; audio.play().catch(() => {}); }
}

// ─── Actions ─────────────────────────────────────────────
function manualAlert(nodeId) {
  if (!confirm('Kích hoạt báo cháy + TTS cho node này?')) return;
  sendWS({ type: 'manual_alert', targetNodeId: nodeId });
}

function activateBuzzer(nodeId) {
  sendWS({ type: 'manual_alert', targetNodeId: nodeId });
}

function resetAll() {
  if (!confirm('Reset tất cả node về trạng thái bình thường?')) return;
  sendWS({ type: 'reset_all' });
}

function sendButton(btn) {
  const centerNode = Object.values(nodes).find(n => n.nodeType === 'center');
  if (!centerNode) {
    setRemoteStatus('⚠️ Chưa có Node Trung Tâm kết nối');
    return;
  }
  sendWS({ type: 'button_press', button: btn, nodeId: centerNode.id });
  setRemoteStatus(`Đã gửi: ${btn.toUpperCase()} → ${centerNode.label}`);
}

function showRemoteStatus(btn) {
  setRemoteStatus(`Nhận tín hiệu nút: ${btn.toUpperCase()} từ node trung tâm`);
}

function setRemoteStatus(text) {
  document.getElementById('remote-status').textContent = text;
}

function sendTTS() {
  const text = document.getElementById('tts-text').value.trim();
  const target = document.getElementById('tts-target').value;
  if (!text) { alert('Vui lòng nhập nội dung cần phát'); return; }

  if (target === 'all') {
    Object.values(nodes).filter(n => n.nodeType !== 'center').forEach(n => {
      sendWS({ type: 'send_tts', targetNodeId: n.id, text });
    });
  } else {
    sendWS({ type: 'send_tts', targetNodeId: target, text });
  }

  // Feedback
  document.getElementById('tts-text').value = '';
  const btn = document.querySelector('.btn-send-tts');
  const orig = btn.innerHTML;
  btn.innerHTML = `<span class="material-icons-round">check</span> Đã gửi!`;
  btn.style.background = 'var(--green)';
  setTimeout(() => { btn.innerHTML = orig; btn.style.background = ''; }, 2000);
}

function setPreset(text) {
  document.getElementById('tts-text').value = text;
}

// ─── Modal ────────────────────────────────────────────────
function openEditModal(nodeId) {
  const n = nodes[nodeId];
  if (!n) return;
  document.getElementById('modal-node-id').value = nodeId;
  document.getElementById('modal-label').value = n.label;
  document.getElementById('modal-location').value = n.location;
  document.getElementById('modal-backdrop').classList.remove('hidden');
  document.getElementById('modal-edit').classList.remove('hidden');
  setTimeout(() => document.getElementById('modal-label').focus(), 50);
}

function closeModal() {
  document.getElementById('modal-backdrop').classList.add('hidden');
  document.getElementById('modal-edit').classList.add('hidden');
}

// Đóng bất kỳ modal nào đang mở khi bấm ra ngoài backdrop
function closeAnyModal() {
  if (!document.getElementById('modal-pairing').classList.contains('hidden')) closePairingModal();
  else closeModal();
}

// ─── Pairing (ghép nối thiết bị mới) ─────────────────────────
async function requestPairingCode() {
  const modal   = document.getElementById('modal-pairing');
  const display = document.getElementById('pairing-code-display');
  const status  = document.getElementById('pairing-code-status');
  document.getElementById('modal-backdrop').classList.remove('hidden');
  modal.classList.remove('hidden');
  display.textContent = '------';
  status.textContent = 'Đang tạo mã...';
  try {
    const res = await fetch('/api/pairing-code', { method: 'POST' });
    const data = await res.json();
    if (!res.ok) { status.textContent = data.error || 'Lỗi tạo mã'; return; }
    display.textContent = data.code;
    let remaining = data.expiresInSeconds;
    status.textContent = `Hết hạn sau ${remaining}s`;
    const timer = setInterval(() => {
      remaining--;
      if (remaining <= 0) { status.textContent = 'Mã đã hết hạn'; clearInterval(timer); return; }
      status.textContent = `Hết hạn sau ${remaining}s`;
    }, 1000);
    modal.dataset.timerId = timer;
  } catch (e) {
    status.textContent = 'Lỗi kết nối server';
  }
}

function closePairingModal() {
  const modal = document.getElementById('modal-pairing');
  if (modal.dataset.timerId) clearInterval(Number(modal.dataset.timerId));
  modal.classList.add('hidden');
  document.getElementById('modal-backdrop').classList.add('hidden');
}

function saveNodeConfig() {
  const nodeId = document.getElementById('modal-node-id').value;
  const label = document.getElementById('modal-label').value.trim();
  const location = document.getElementById('modal-location').value.trim();
  if (!label || !location) { alert('Vui lòng điền đủ thông tin'); return; }
  sendWS({ type: 'update_node', nodeId, label, location });
  closeModal();
}

// ─── Page Navigation ─────────────────────────────────────
const pageTitles = {
  dashboard: 'Tổng quan',
  nodes: 'Quản lý thiết bị',
  alerts: 'Lịch sử cảnh báo',
  control: 'Điều khiển',
  tts: 'TTS / Phát loa',
};

function showPage(page) {
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  document.getElementById(`page-${page}`).classList.add('active');
  document.querySelector(`[data-page="${page}"]`).classList.add('active');
  document.getElementById('page-title').textContent = pageTitles[page] || page;
}

// ─── Clock ────────────────────────────────────────────────
function updateClock() {
  document.getElementById('system-time').textContent =
    new Date().toLocaleTimeString('vi-VN', { hour12: false });
}

// ─── Status polling ──────────────────────────────────────
async function fetchStatus() {
  try {
    const res = await fetch('/api/status');
    const data = await res.json();
    const h = Math.floor(data.uptime / 3600);
    const m = Math.floor((data.uptime % 3600) / 60);
    document.getElementById('stat-uptime').textContent = `${h}h${m}m`;
  } catch {}
}

// ─── Utils ────────────────────────────────────────────────
function esc(str) {
  if (!str) return '';
  return String(str).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}

// ─── Keyboard shortcuts ──────────────────────────────────
document.addEventListener('keydown', (e) => {
  if (e.target.tagName === 'INPUT' || e.target.tagName === 'TEXTAREA') return;
  const map = { ArrowUp:'up', ArrowDown:'down', ArrowLeft:'left', ArrowRight:'right', Enter:'ok', ' ':'ok' };
  if (map[e.key]) { e.preventDefault(); sendButton(map[e.key]); }
  if (e.key === 'Escape') { dismissOverlay(); closeModal(); }
});

// ─── Init ─────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', () => {
  connectWS();
  updateClock();
  setInterval(updateClock, 1000);
  setInterval(fetchStatus, 30000);
});
