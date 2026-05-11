"use strict";

// ── State ─────────────────────────────────────────────────────
let selectedFile = null;
let generatedKey = null;
let keys = [];
let t0 = Date.now();

// ── Navigation ────────────────────────────────────────────────
function showPage(name, el) {
  document.querySelectorAll('.page').forEach(p => p.classList.remove('active'));
  document.querySelectorAll('.nav-item').forEach(n => n.classList.remove('active'));
  document.getElementById('page-' + name).classList.add('active');
  el.classList.add('active');
  if (name === 'files')  loadFiles();
  if (name === 'nodes')  loadNodeStatus();
  if (name === 'log')    syncLog();
}

// ── Timestamp ─────────────────────────────────────────────────
function ts() {
  let s = Math.floor((Date.now() - t0) / 1000);
  let h = String(Math.floor(s / 3600)).padStart(2,'0');
  let m = String(Math.floor((s % 3600) / 60)).padStart(2,'0');
  let sec = String(s % 60).padStart(2,'0');
  return h + ':' + m + ':' + sec;
}

// ── Logging ───────────────────────────────────────────────────
function log(msg, type='info') {
  ['uploadLog','sysLog'].forEach(id => {
    let box = document.getElementById(id);
    if (!box) return;
    let div = document.createElement('div');
    div.className = 'log-' + type;
    div.textContent = '[' + ts() + '] ' + msg;
    box.appendChild(div);
    box.scrollTop = box.scrollHeight;
  });
}

function syncLog() {
  let src = document.getElementById('uploadLog');
  let dst = document.getElementById('sysLog');
  if (src && dst) dst.innerHTML = src.innerHTML;
}

function clearSysLog() {
  document.getElementById('sysLog').innerHTML = '';
  document.getElementById('uploadLog').innerHTML = '';
}

// ── Formatting ────────────────────────────────────────────────
function formatBytes(n) {
  if (n < 1024) return n + ' B';
  if (n < 1024*1024) return (n/1024).toFixed(1) + ' KB';
  return (n/1024/1024).toFixed(2) + ' MB';
}

function esc(str) {
  let d = document.createElement('div');
  d.appendChild(document.createTextNode(str));
  return d.innerHTML;
}

// ── Key Generation ────────────────────────────────────────────
function generateKey() {
  let arr = new Uint8Array(32);
  crypto.getRandomValues(arr);
  generatedKey = Array.from(arr).map(b => b.toString(16).padStart(2,'0')).join('');
  document.getElementById('keyDisplay').textContent = generatedKey;
  log('AES-256 key generated: ' + generatedKey.slice(0,16) + '...', 'ok');
  if (selectedFile) document.getElementById('uploadBtn').disabled = false;
}

function genAndAddKey() {
  generateKey();
  let id = 'KEY-' + Math.random().toString(36).slice(2,7).toUpperCase();
  keys.push({ id, algo: 'AES-256-GCM', key: generatedKey, created: new Date().toLocaleTimeString() });
  renderKeyTable();
}

function renderKeyTable() {
  let tb = document.getElementById('keyTable');
  if (!keys.length) {
    tb.innerHTML = '<tr><td colspan="4" style="text-align:center;padding:30px;color:var(--text3)">No keys generated yet</td></tr>';
    return;
  }
  tb.innerHTML = keys.map(k => `
    <tr>
      <td><span class="badge badge-teal">${k.id}</span></td>
      <td style="color:#aa44ff">${k.algo}</td>
      <td style="color:#5090d0;font-size:10px">${k.key.slice(0,32)}…</td>
      <td style="color:var(--text3)">${k.created}</td>
    </tr>`).join('');
}

// ── Drag and Drop ─────────────────────────────────────────────
function onDragOver(e) {
  e.preventDefault();
  document.getElementById('dropZone').classList.add('dragover');
}
function onDragLeave(e) {
  document.getElementById('dropZone').classList.remove('dragover');
}
function onDrop(e) {
  e.preventDefault();
  document.getElementById('dropZone').classList.remove('dragover');
  let f = e.dataTransfer.files[0];
  if (f) applyFile(f);
}
function onFileSelect(e) {
  let f = e.target.files[0];
  if (f) applyFile(f);
}
function applyFile(file) {
  selectedFile = file;
  let zone = document.getElementById('dropZone');
  zone.classList.add('has-file');
  document.getElementById('dropIcon').textContent = '✓';
  document.getElementById('dropText').textContent = file.name;
  document.getElementById('dropSub').textContent = formatBytes(file.size) + ' — ready to upload';
  document.getElementById('keyNotice').style.display = 'none';
  if (generatedKey) document.getElementById('uploadBtn').disabled = false;
  log('File selected: ' + file.name + ' (' + formatBytes(file.size) + ')', 'info');
}

// ── Upload ────────────────────────────────────────────────────
async function uploadFile() {
  if (!selectedFile) return;
  let btn = document.getElementById('uploadBtn');
  btn.disabled = true;

  let blockSize = parseInt(document.getElementById('blockSize').value);
  let numChunks = Math.max(1, Math.ceil(selectedFile.size / blockSize));

  // Show chunk grid
  document.getElementById('progWrap').style.display = 'block';
  let grid = document.getElementById('chunkGrid');
  grid.innerHTML = '';
  for (let i = 0; i < numChunks; i++) {
    let c = document.createElement('div');
    c.className = 'chunk';
    c.id = 'ck-' + i;
    c.textContent = i;
    grid.appendChild(c);
  }

  setProgress(5, 'Preparing upload…');
  log('Uploading: ' + selectedFile.name, 'info');
  log('Splitting into ' + numChunks + ' block(s) of ' + (blockSize/1024) + ' KB', 'info');

  let formData = new FormData();
  formData.append('file', selectedFile);

  try {
    setProgress(20, 'Sending to cloud controller…');

    // Animate chunks
    let animInterval = animateChunks(numChunks);

    let response = await fetch('/api/upload', { method: 'POST', body: formData });
    clearInterval(animInterval);

    // Mark all done
    for (let i = 0; i < numChunks; i++) {
      let c = document.getElementById('ck-' + i);
      if (c) { c.className = 'chunk done'; c.textContent = '✓'; }
    }

    setProgress(100, 'Upload complete!');
    let data = await response.json();

    if (data.success) {
      log('✓ Upload complete: ' + data.filename, 'ok');
      log('  File ID : ' + data.file_id, 'dim');
      log('  Blocks  : ' + data.blocks + ' (AES-256-GCM encrypted, 3x replicated)', 'dim');
      log('  Size    : ' + formatBytes(data.size), 'dim');

      document.getElementById('keyVal').textContent = data.key;
      document.getElementById('keyDisplay').textContent = data.key;
      document.getElementById('keyNotice').style.display = 'block';

      // Add to key manager
      let id = 'KEY-' + Math.random().toString(36).slice(2,7).toUpperCase();
      keys.push({ id, algo: 'AES-256-GCM', key: data.key, created: new Date().toLocaleTimeString() });
      renderKeyTable();

      // Update node badges
      updateNodeBadges();
    } else {
      log('✗ Upload failed: ' + (data.error || 'unknown'), 'err');
    }
  } catch (err) {
    log('✗ Network error: ' + err.message, 'err');
    setProgress(0, 'Upload failed');
  }
  btn.disabled = false;
}

function animateChunks(total) {
  let i = 0;
  return setInterval(() => {
    if (i < total) {
      let c = document.getElementById('ck-' + i);
      if (c) { c.className = 'chunk encrypting'; }
      setTimeout(() => {
        if (c) { c.className = 'chunk uploading'; }
        setTimeout(() => {
          if (c) { c.className = 'chunk done'; c.textContent = '✓'; }
        }, 200);
      }, 150);
      i++;
      let pct = Math.round((i / total) * 85) + 15;
      setProgress(pct, 'Encrypting block ' + i + '/' + total + '…');
    }
  }, 180);
}

function setProgress(pct, label) {
  document.getElementById('progFill').style.width = pct + '%';
  document.getElementById('progPct').textContent = pct + '%';
  if (label) document.getElementById('progLabel').textContent = label;
}

// ── Copy Key ──────────────────────────────────────────────────
function copyKey() {
  let text = document.getElementById('keyVal').textContent;
  navigator.clipboard.writeText(text).then(() => {
    let ok = document.getElementById('copyOk');
    ok.style.display = 'inline';
    setTimeout(() => ok.style.display = 'none', 2000);
  });
}

// ── Download ──────────────────────────────────────────────────
function downloadFile(fileId, filename) {
  log('Downloading: ' + filename + ' (decrypting blocks…)', 'info');
  window.location.href = '/api/download/' + fileId;
  setTimeout(() => log('✓ Download started: ' + filename, 'ok'), 600);
}

// ── Delete ────────────────────────────────────────────────────
async function deleteFile(fileId, filename) {
  if (!confirm('Delete "' + filename + '"?\n\nThis permanently removes all encrypted blocks and the key. Cannot be undone.')) return;
  log('Deleting: ' + filename + '…', 'warn');
  try {
    let res = await fetch('/api/delete/' + fileId, { method: 'DELETE' });
    let data = await res.json();
    if (data.success) {
      log('✓ Deleted: ' + filename, 'ok');
      loadFiles();
      loadNodeStatus();
    } else {
      log('✗ Delete failed: ' + data.error, 'err');
    }
  } catch (err) {
    log('✗ Network error: ' + err.message, 'err');
  }
}

// ── File List ─────────────────────────────────────────────────
async function loadFiles() {
  try {
    let res = await fetch('/api/files');
    let data = await res.json();
    let tb = document.getElementById('fileTable');
    if (!data.files || !data.files.length) {
      tb.innerHTML = '<tr><td class="empty-td" colspan="5">No files uploaded yet</td></tr>';
      return;
    }
    tb.innerHTML = data.files.map(f => `
      <tr>
        <td><span class="fname">${esc(f.filename)}</span></td>
        <td>${formatBytes(f.size)}</td>
        <td><span class="badge badge-teal">${f.blocks} blocks</span></td>
        <td style="font-size:11px">${f.uploaded}</td>
        <td><div class="act-group">
          <button class="btn btn-green" onclick="downloadFile('${f.file_id}','${esc(f.filename)}')">&#8595; Download</button>
          <button class="btn btn-red"   onclick="deleteFile('${f.file_id}','${esc(f.filename)}')">&#128465; Delete</button>
        </div></td>
      </tr>`).join('');
  } catch (err) {
    log('✗ Could not load files: ' + err.message, 'err');
  }
}

// ── Node Status ───────────────────────────────────────────────
async function loadNodeStatus() {
  try {
    let res = await fetch('/api/status');
    let nodes = await res.json();
    let grid = document.getElementById('nodeGrid');
    let maxBytes = Math.max(...nodes.map(n => n.bytes_used), 1);
    grid.innerHTML = nodes.map(n => {
      let pct = Math.round((n.bytes_used / maxBytes) * 100);
      return `
        <div class="node-card">
          <div class="node-card-head">
            <div class="ndot"></div>
            <span class="nname">${n.node}</span>
            <span class="ntype">DATA</span>
          </div>
          <div class="nstat"><span>Blocks</span><strong>${n.blocks_stored}</strong></div>
          <div class="nstat"><span>Used</span><strong>${formatBytes(n.bytes_used)}</strong></div>
          <div class="nbar"><div class="nbar-fill" style="width:${pct}%"></div></div>
          <div style="font-size:10px;color:var(--green);margin-top:8px">● ${n.status}</div>
        </div>`;
    }).join('');
    updateNodeBadgesFromData(nodes);
  } catch (err) {
    log('✗ Node status error: ' + err.message, 'err');
  }
}

function updateNodeBadges() { loadNodeStatus(); }

function updateNodeBadgesFromData(nodes) {
  let ids = ['dn1badge','dn2badge','dn3badge'];
  nodes.forEach((n, i) => {
    let el = document.getElementById(ids[i]);
    if (el) el.textContent = n.node.toUpperCase() + ': ' + n.status;
  });
}

// ── Health Check ──────────────────────────────────────────────
async function checkHealth() {
  try {
    let res = await fetch('/api/health');
    let data = await res.json();
    if (data.status === 'ok') {
      log('Server connected | ' + data.datanodes + ' DataNodes | Block: ' + data.block_size/1024 + ' KB', 'ok');
    }
  } catch (err) {
    log('Cannot reach server — is python app.py running?', 'err');
  }
}

// ── Init ──────────────────────────────────────────────────────
window.addEventListener('load', async () => {
  await checkHealth();
  await loadFiles();
  await loadNodeStatus();
  setInterval(loadNodeStatus, 15000);
});
