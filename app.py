"""
app.py — Bybit Scalp Scanner Web App
Deploy ke Render.com sebagai Web Service.
Akses dari HP via browser.
"""

from flask import Flask, jsonify, render_template_string, request
import threading
import time

app = Flask(__name__)

# ======================================
# STATE
# ======================================

_state = {
    "rows"       : [],
    "scanning"   : False,
    "last_scan"  : None,
    "progress"   : (0, 0),
    "market_ctx" : "",
    "error"      : None,
    "scan_num"   : 0,
}

_lock = threading.Lock()

# ======================================
# SCANNER THREAD
# ======================================

def _do_scan():
    from scanner_scalp import scan_scalp, get_market_context_str, refresh_price_cache, refresh_news_cache

    with _lock:
        _state["scanning"] = True
        _state["error"]    = None
        _state["progress"] = (0, 0)
        _state["scan_num"] += 1

    def progress(done, total):
        with _lock:
            _state["progress"] = (done, total)

    try:
        rows = scan_scalp(progress_callback=progress)
        ctx  = get_market_context_str()

        # Filter default: score >= 62
        filtered = [r for r in rows if r[5] >= 62][:50]

        with _lock:
            _state["rows"]       = filtered
            _state["market_ctx"] = ctx
            _state["last_scan"]  = time.strftime("%Y-%m-%d %H:%M:%S")
            _state["scanning"]   = False

    except Exception as e:
        with _lock:
            _state["scanning"] = False
            _state["error"]    = str(e)

def trigger_scan():
    if _state["scanning"]:
        return False
    t = threading.Thread(target=_do_scan, daemon=True)
    t.start()
    return True

# ======================================
# AUTO REFRESH (tiap 10 menit)
# ======================================

def _auto_loop():
    while True:
        trigger_scan()
        time.sleep(10 * 60)   # 10 menit

threading.Thread(target=_auto_loop, daemon=True).start()

# ======================================
# HTML TEMPLATE
# ======================================

HTML = """<!DOCTYPE html>
<html lang="id">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>⚡ Scalp Scanner</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body { background: #0d0d0d; color: #e0e0e0; font-family: 'Courier New', monospace; font-size: 13px; }

  .topbar {
    background: #111;
    border-bottom: 1px solid #222;
    padding: 10px 16px;
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 10px;
    position: sticky; top: 0; z-index: 100;
  }
  .topbar h1 { color: #00d4ff; font-size: 15px; font-weight: bold; }
  .badge {
    padding: 3px 10px; border-radius: 20px; font-size: 11px; font-weight: bold;
  }
  .badge-live  { background: #003a00; color: #00ff88; border: 1px solid #00ff88; }
  .badge-scan  { background: #3a2000; color: #ffaa00; border: 1px solid #ffaa00; }
  .badge-ctx   { background: #1a1a2e; color: #aaa; border: 1px solid #333; }

  .btn {
    padding: 5px 14px; border-radius: 6px; border: none; cursor: pointer;
    font-size: 12px; font-weight: bold;
  }
  .btn-scan   { background: #005577; color: #fff; }
  .btn-scan:hover { background: #0077aa; }
  .btn-filter { background: #222; color: #ccc; border: 1px solid #444; }
  .btn-filter.active { background: #004400; color: #00ff88; border-color: #00ff88; }

  .filters {
    padding: 8px 16px;
    background: #111;
    border-bottom: 1px solid #1a1a1a;
    display: flex; flex-wrap: wrap; gap: 8px; align-items: center;
  }
  .filters select, .filters input {
    background: #1a1a1a; color: #ccc; border: 1px solid #333;
    padding: 4px 8px; border-radius: 4px; font-size: 12px;
  }
  .filters label { color: #777; font-size: 11px; }

  .progress-bar-wrap {
    background: #111; padding: 6px 16px;
    border-bottom: 1px solid #1a1a1a;
    display: none;
  }
  .progress-bar-wrap.show { display: block; }
  .progress-bar {
    height: 4px; background: #222; border-radius: 2px; overflow: hidden;
  }
  .progress-fill {
    height: 100%; background: #00d4ff;
    transition: width 0.3s ease;
  }
  .progress-txt { color: #555; font-size: 11px; margin-top: 3px; }

  .table-wrap { overflow-x: auto; padding: 8px 0; }
  table { width: 100%; border-collapse: collapse; min-width: 900px; }
  th {
    background: #111; color: #555; font-size: 11px; text-align: left;
    padding: 6px 8px; border-bottom: 1px solid #222; white-space: nowrap;
    cursor: pointer; user-select: none;
  }
  th:hover { color: #aaa; }
  td { padding: 5px 8px; border-bottom: 1px solid #151515; white-space: nowrap; }
  tr:hover td { background: #161616; }

  .coin  { color: #fff; font-weight: bold; font-size: 12px; }
  .long  { color: #00cc66; font-weight: bold; }
  .short { color: #ff4444; font-weight: bold; }
  .flat  { color: #555; }

  .score-s  { color: #00ff88; font-weight: bold; }
  .score-a  { color: #44cc44; }
  .score-b  { color: #cccc00; }
  .score-c  { color: #888; }

  .grade-ap { color: #ff44ff; font-weight: bold; }
  .grade-a  { color: #00cc66; }
  .grade-b  { color: #cccc00; }

  .dec-now   { color: #ff44ff; font-weight: bold; }
  .dec-entry { color: #00cc66; }
  .dec-watch { color: #cccc00; }
  .dec-skip  { color: #333; }

  .rvol-spike { color: #ff44ff; font-weight: bold; }
  .rvol-high  { color: #ffaa00; }
  .rvol-ok    { color: #666; }

  .chg-pos { color: #00cc66; }
  .chg-neg { color: #ff4444; }

  .entry-box {
    background: #0d1a0d; border: 1px solid #004400;
    border-radius: 6px; padding: 10px 14px; margin: 4px 8px;
    display: none;
  }
  .entry-box.show { display: block; }
  .entry-label { color: #555; font-size: 10px; }
  .entry-val   { color: #00ff88; font-size: 12px; }
  .sl-val      { color: #ff4444; }
  .tp-val      { color: #00cc66; }

  .empty { text-align: center; color: #333; padding: 40px; }
  .ts    { color: #333; font-size: 11px; }

  @media (max-width: 600px) {
    .topbar { padding: 8px 10px; }
    .filters { padding: 6px 10px; }
  }
</style>
</head>
<body>

<!-- TOP BAR -->
<div class="topbar">
  <h1>⚡ SCALP SCANNER</h1>
  <span id="status-badge" class="badge badge-live">IDLE</span>
  <span id="ctx-badge" class="badge badge-ctx">-</span>
  <button class="btn btn-scan" onclick="triggerScan()">🔄 Scan</button>
  <span id="ts" class="ts">-</span>
</div>

<!-- FILTERS -->
<div class="filters">
  <label>Min Score</label>
  <input type="number" id="f-score" value="62" min="0" max="100" style="width:60px"
    onchange="applyFilters()">

  <label>Bias</label>
  <select id="f-bias" onchange="applyFilters()">
    <option value="">Semua</option>
    <option value="LONG">LONG</option>
    <option value="SHORT">SHORT</option>
  </select>

  <label>Grade</label>
  <select id="f-grade" onchange="applyFilters()">
    <option value="">Semua</option>
    <option value="A+">A+</option>
    <option value="A">A</option>
    <option value="B">B</option>
  </select>

  <label>Min RVol5M</label>
  <select id="f-rvol" onchange="applyFilters()">
    <option value="0">Semua</option>
    <option value="1.2">≥1.2x</option>
    <option value="1.5">≥1.5x</option>
    <option value="2.0">≥2.0x</option>
  </select>

  <label>Decision</label>
  <select id="f-dec" onchange="applyFilters()">
    <option value="">Semua</option>
    <option value="NOW">NOW only</option>
    <option value="ENTRY">ENTRY+</option>
  </select>
</div>

<!-- PROGRESS -->
<div class="progress-bar-wrap" id="progress-wrap">
  <div class="progress-bar"><div class="progress-fill" id="progress-fill" style="width:0%"></div></div>
  <div class="progress-txt" id="progress-txt">Scanning...</div>
</div>

<!-- TABLE -->
<div class="table-wrap">
  <table id="main-table">
    <thead>
      <tr>
        <th>#</th>
        <th>COIN</th>
        <th onclick="sortBy(5)">SCORE ↕</th>
        <th>BIAS</th>
        <th>GRADE</th>
        <th onclick="sortBy(9)">RVol5M ↕</th>
        <th onclick="sortBy(10)">RVol15M ↕</th>
        <th>RSI5M</th>
        <th>PRICE</th>
        <th>CHG24%</th>
        <th>DECISION</th>
        <th>CATALYST</th>
        <th>ENTRY</th>
        <th>SL</th>
        <th>TP1</th>
        <th>RR1</th>
      </tr>
    </thead>
    <tbody id="table-body">
      <tr><td colspan="16" class="empty">Tekan Scan untuk mulai...</td></tr>
    </tbody>
  </table>
</div>

<script>
let allRows = [];
let sortCol = 5;
let sortAsc = false;

async function fetchStatus() {
  try {
    const r = await fetch('/api/status');
    const d = await r.json();

    // Progress
    const wrap = document.getElementById('progress-wrap');
    if (d.scanning) {
      wrap.classList.add('show');
      const pct = d.progress[1] > 0 ? Math.round(d.progress[0]/d.progress[1]*100) : 0;
      document.getElementById('progress-fill').style.width = pct + '%';
      document.getElementById('progress-txt').textContent =
        `Scanning ${d.progress[0]}/${d.progress[1]} (${pct}%)`;
      document.getElementById('status-badge').className = 'badge badge-scan';
      document.getElementById('status-badge').textContent = '⏳ SCANNING';
    } else {
      wrap.classList.remove('show');
      document.getElementById('status-badge').className = 'badge badge-live';
      document.getElementById('status-badge').textContent = '✅ LIVE';
    }

    // Context
    if (d.market_ctx)
      document.getElementById('ctx-badge').textContent = d.market_ctx;

    // Timestamp
    if (d.last_scan)
      document.getElementById('ts').textContent = 'Last scan: ' + d.last_scan
        + ' | #' + d.scan_num;

    // Rows (hanya update kalau ada data baru)
    if (d.rows && d.rows.length > 0 && !d.scanning) {
      allRows = d.rows;
      applyFilters();
    }
  } catch(e) { console.log(e); }
}

async function triggerScan() {
  await fetch('/api/scan', { method: 'POST' });
}

function applyFilters() {
  const minScore = parseFloat(document.getElementById('f-score').value) || 62;
  const bias     = document.getElementById('f-bias').value;
  const grade    = document.getElementById('f-grade').value;
  const minRvol  = parseFloat(document.getElementById('f-rvol').value) || 0;
  const dec      = document.getElementById('f-dec').value;

  let rows = allRows.filter(r => {
    if (r[5] < minScore) return false;
    if (bias && r[6] !== bias) return false;
    if (grade && !String(r[13]).startsWith(grade)) return false;
    if (r[9] < minRvol) return false;
    if (dec === 'NOW'   && !String(r[12]).includes('NOW'))   return false;
    if (dec === 'ENTRY' && !String(r[12]).includes('ENTRY') && !String(r[12]).includes('NOW')) return false;
    return true;
  });

  // Sort
  rows.sort((a, b) => {
    let va = a[sortCol], vb = b[sortCol];
    if (typeof va === 'string') va = va.replace(/[^0-9.-]/g,'');
    if (typeof vb === 'string') vb = vb.replace(/[^0-9.-]/g,'');
    return sortAsc ? va - vb : vb - va;
  });

  renderTable(rows);
}

function sortBy(col) {
  if (sortCol === col) sortAsc = !sortAsc;
  else { sortCol = col; sortAsc = false; }
  applyFilters();
}

function renderTable(rows) {
  const tbody = document.getElementById('table-body');
  if (!rows.length) {
    tbody.innerHTML = '<tr><td colspan="16" class="empty">Tidak ada coin yang memenuhi filter.</td></tr>';
    return;
  }

  tbody.innerHTML = rows.map((r, i) => {
    const bias    = r[6];
    const biasCls = bias === 'LONG' ? 'long' : bias === 'SHORT' ? 'short' : 'flat';
    const score   = r[5];
    const sCls    = score >= 80 ? 'score-s' : score >= 70 ? 'score-a' : score >= 62 ? 'score-b' : 'score-c';
    const grade   = String(r[13]);
    const gCls    = grade.startsWith('A+') ? 'grade-ap' : grade.startsWith('A') ? 'grade-a' : 'grade-b';
    const dec     = String(r[12]);
    const dCls    = dec.includes('NOW') ? 'dec-now' : dec.includes('ENTRY') || dec.includes('SHORT 👍') ? 'dec-entry' : dec.includes('WATCH') ? 'dec-watch' : 'dec-skip';
    const rv5     = r[9];
    const rvCls   = rv5 >= 2.0 ? 'rvol-spike' : rv5 >= 1.5 ? 'rvol-high' : 'rvol-ok';
    const chg     = String(r[23]);
    const chgCls  = chg.startsWith('+') ? 'chg-pos' : 'chg-neg';

    return `<tr>
      <td class="ts">${i+1}</td>
      <td class="coin">${r[0].replace('USDT','')}</td>
      <td class="${sCls}">${score}</td>
      <td class="${biasCls}">${bias}</td>
      <td class="${gCls}">${grade}</td>
      <td class="${rvCls}">${rv5}x</td>
      <td class="${r[10] >= 2 ? 'rvol-spike' : r[10] >= 1.5 ? 'rvol-high' : 'rvol-ok'}">${r[10]}x</td>
      <td>${r[7]}</td>
      <td>${r[22]}</td>
      <td class="${chgCls}">${chg}</td>
      <td class="${dCls}">${dec}</td>
      <td style="color:#aa7700">${r[11]}</td>
      <td>${r[15]}</td>
      <td style="color:#ff4444">${r[17]}</td>
      <td style="color:#00cc66">${r[18]}</td>
      <td style="color:#888">${r[20]}</td>
    </tr>`;
  }).join('');
}

// Poll status tiap 2 detik
setInterval(fetchStatus, 2000);
fetchStatus();
</script>
</body>
</html>
"""

# ======================================
# ROUTES
# ======================================

@app.route("/")
def index():
    return render_template_string(HTML)

@app.route("/api/status")
def api_status():
    with _lock:
        return jsonify({
            "scanning"   : _state["scanning"],
            "progress"   : list(_state["progress"]),
            "rows"       : _state["rows"],
            "last_scan"  : _state["last_scan"],
            "market_ctx" : _state["market_ctx"],
            "error"      : _state["error"],
            "scan_num"   : _state["scan_num"],
        })

@app.route("/api/scan", methods=["POST"])
def api_scan():
    ok = trigger_scan()
    return jsonify({"ok": ok, "msg": "scanning" if ok else "already scanning"})

# ======================================
# MAIN
# ======================================

if __name__ == "__main__":
    import os
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
