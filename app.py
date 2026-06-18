"""
app.py — Bybit Scalp Scanner Web App (WebSocket Edition)
Deploy ke Render.com sebagai Web Service.
Akses dari HP via browser.

Perubahan v3:
- Price real-time via Bybit WebSocket (bukan REST poll tiap 5 detik)
- WS status ditampilkan di UI
- _price_refresh_loop diganti ws_price_updater (baca dari WS cache)
- WS feed dimulai di startup, subscribe ke semua symbol setelah scan
"""

from flask import Flask, jsonify, render_template_string, request, abort
import threading
import time
import os
import secrets

app = Flask(__name__)
app.config["SECRET_KEY"] = os.environ.get("FLASK_SECRET_KEY", secrets.token_hex(32))

# ======================================
# SECURITY: Rate Limiting
# ======================================

_scan_rate = {"last": 0.0, "min_interval": 10}  # seconds between scans

# ======================================
# SECURITY: Optional API Key Auth
# ======================================

_API_KEY = os.environ.get("SCANNER_API_KEY", "")

def _check_api_key():
    """If SCANNER_API_KEY is set, require it on API requests."""
    if not _API_KEY:
        return
    key = request.headers.get("X-API-Key", "") or request.args.get("api_key", "")
    if not secrets.compare_digest(key, _API_KEY):
        abort(401)

@app.before_request
def _security_checks():
    if request.path.startswith("/api/"):
        _check_api_key()

@app.after_request
def _set_security_headers(response):
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    return response

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
    "price_ts"   : None,
    "ws_count"   : 0,    # jumlah symbol di WS cache
}

_lock = threading.Lock()

# ======================================
# WEBSOCKET PRICE FEED — Startup
# ======================================

def _init_ws():
    """Start WS feed saat startup. Subscribe semua USDT symbol dari Bybit."""
    from ws_price_feed import start_ws_feed
    from scanner_scalp import session

    try:
        info = session.get_instruments_info(category="linear")
        symbols = [
            x["symbol"]
            for x in info["result"]["list"]
            if x["symbol"].endswith("USDT")
        ]
        print(f"[ws-init] Starting WS feed for {len(symbols)} symbols...")
        start_ws_feed(symbols)
    except Exception as e:
        print(f"[ws-init] Error: {e}")
        # Fallback: start tanpa pre-defined symbol list, akan subscribe saat scan
        start_ws_feed()

threading.Thread(target=_init_ws, daemon=True, name="ws-init").start()

# ======================================
# WS PRICE UPDATER — update rows dari WS cache (sangat ringan, tiap 500ms)
# ======================================

def _ws_price_updater():
    """
    Loop ringan: baca harga dari WS cache (in-memory) dan update rows.
    Tidak ada network call — murni baca dict lokal.
    Jalankan tiap 500ms (atau lebih sering jika mau).
    """
    from ws_price_feed import get_ws_price, ws_cache_size
    while True:
        try:
            with _lock:
                updated = []
                for r in _state["rows"]:
                    symbol = r[0]
                    price, change24h, high24h, low24h, vol24h, turnover24h = get_ws_price(symbol)
                    if price > 0:
                        r = list(r)
                        r[22] = round(price, 6)
                        r[23] = f"+{round(change24h,2)}%" if change24h >= 0 else f"{round(change24h,2)}%"
                        r[24] = round(high24h, 6)
                        r[25] = round(low24h, 6)
                        r[26] = f"{round(turnover24h/1_000_000, 2)}M"
                        r = tuple(r)
                    updated.append(r)
                _state["rows"]     = updated
                _state["price_ts"] = time.strftime("%H:%M:%S")
                _state["ws_count"] = ws_cache_size()

        except Exception as e:
            print(f"[ws-updater] error: {e}")

        time.sleep(0.5)   # update setiap 500ms dari WS cache

threading.Thread(target=_ws_price_updater, daemon=True, name="ws-price-updater").start()

# ======================================
# SCANNER THREAD
# ======================================

def _do_scan():
    from scanner_scalp import scan_scalp, get_market_context_str

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
# AUTO REFRESH (tiap 10 menit) — FULL SCAN indikator
# ======================================

def _auto_loop():
    while True:
        trigger_scan()
        time.sleep(10 * 60)

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
  .badge-live    { background: #003a00; color: #00ff88; border: 1px solid #00ff88; }
  .badge-scan    { background: #3a2000; color: #ffaa00; border: 1px solid #ffaa00; }
  .badge-ctx     { background: #1a1a2e; color: #aaa; border: 1px solid #333; }
  .badge-ws-ok   { background: #001a3a; color: #00aaff; border: 1px solid #00aaff; }
  .badge-ws-wait { background: #2a1a00; color: #ff8800; border: 1px solid #ff8800; }

  .btn {
    padding: 5px 14px; border-radius: 6px; border: none; cursor: pointer;
    font-size: 12px; font-weight: bold;
  }
  .btn-scan   { background: #005577; color: #fff; }
  .btn-scan:hover { background: #0077aa; }

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

  /* Harga flash saat update */
  @keyframes flash-green { 0%{background:#003a00} 100%{background:transparent} }
  @keyframes flash-red   { 0%{background:#3a0000} 100%{background:transparent} }
  .price-up   { animation: flash-green 0.4s ease; }
  .price-down { animation: flash-red   0.4s ease; }

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
  <span id="ws-badge"     class="badge badge-ws-wait">🔌 WS: 0</span>
  <span id="ctx-badge"    class="badge badge-ctx">-</span>
  <button class="btn btn-scan" onclick="triggerScan()">🔄 Scan</button>
  <span id="ts"       class="ts">-</span>
  <span id="price-ts" class="ts" style="color:#00aaff">⚡ -</span>
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
        <th>TREND200</th>
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
      <tr><td colspan="17" class="empty">Tekan Scan untuk mulai...</td></tr>
    </tbody>
  </table>
</div>

<script>
let allRows = [];
let sortCol = 5;
let sortAsc = false;

// Track harga sebelumnya untuk flash effect
const prevPrices = {};

async function fetchStatus() {
  try {
    const r = await fetch('/api/status');
    const d = await r.json();

    // Progress bar
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

    // WS badge
    const wsN = d.ws_count || 0;
    const wsBadge = document.getElementById('ws-badge');
    wsBadge.textContent = `🔌 WS: ${wsN}`;
    wsBadge.className = wsN > 100
      ? 'badge badge-ws-ok'
      : 'badge badge-ws-wait';

    // Context
    if (d.market_ctx)
      document.getElementById('ctx-badge').textContent = d.market_ctx;

    // Timestamp
    if (d.last_scan)
      document.getElementById('ts').textContent = 'Last scan: ' + d.last_scan + ' | #' + d.scan_num;

    if (d.price_ts)
      document.getElementById('price-ts').textContent = '⚡ WS: ' + d.price_ts;

    // Rows update
    if (d.rows && d.rows.length > 0) {
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
    const emptyTr = document.createElement('tr');
    const emptyTd = document.createElement('td');
    emptyTd.colSpan = 17;
    emptyTd.className = 'empty';
    emptyTd.textContent = 'Tidak ada coin yang memenuhi filter.';
    emptyTr.appendChild(emptyTd);
    tbody.innerHTML = '';
    tbody.appendChild(emptyTr);
    return;
  }

  // Update in-place untuk flash effect
  const existingRows = {};
  tbody.querySelectorAll('tr[data-sym]').forEach(tr => {
    existingRows[tr.dataset.sym] = tr;
  });

  const fragment = document.createDocumentFragment();

  rows.forEach((r, i) => {
    const sym     = r[0];
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
    const price   = r[22];

    // Flash effect
    let priceCls = '';
    if (prevPrices[sym] !== undefined && price !== prevPrices[sym]) {
      priceCls = price > prevPrices[sym] ? 'price-up' : 'price-down';
    }
    prevPrices[sym] = price;

    // Reuse existing row jika ada
    let tr = existingRows[sym];
    if (tr) {
      // Update only price cell (index 9 in td)
      const tds = tr.querySelectorAll('td');
      if (tds[9]) {
        if (priceCls) { tds[9].className = priceCls; }
        tds[9].textContent = price;
      }
      if (tds[10]) { tds[10].className = chgCls; tds[10].textContent = chg; }
      tr.dataset.idx = i;
    } else {
      tr = document.createElement('tr');
      tr.dataset.sym = sym;
      tr.dataset.idx = i;
      const cells = [
        {cls: 'ts',     text: i+1},
        {cls: 'coin',   text: sym.replace('USDT','')},
        {cls: sCls,     text: score},
        {cls: biasCls,  text: bias},
        {cls: gCls,     text: grade},
        {style: 'color:' + (String(r[33]).includes('Bull') ? '#00cc66' : '#ff4444'), text: r[33]},
        {cls: rvCls,    text: rv5 + 'x'},
        {cls: r[10] >= 2 ? 'rvol-spike' : r[10] >= 1.5 ? 'rvol-high' : 'rvol-ok', text: r[10] + 'x'},
        {text: r[7]},
        {cls: priceCls, text: price},
        {cls: chgCls,   text: chg},
        {cls: dCls,     text: dec},
        {style: 'color:#aa7700', text: r[11]},
        {text: r[15]},
        {style: 'color:#ff4444', text: r[17]},
        {style: 'color:#00cc66', text: r[18]},
        {style: 'color:#888',    text: r[20]},
      ];
      cells.forEach(c => {
        const td = document.createElement('td');
        td.textContent = c.text;
        if (c.cls)   td.className = c.cls;
        if (c.style) td.setAttribute('style', c.style);
        tr.appendChild(td);
      });
    }
    fragment.appendChild(tr);
  });

  tbody.innerHTML = '';
  tbody.appendChild(fragment);
}

// Poll status tiap 1 detik (lebih sering karena ringan)
setInterval(fetchStatus, 1000);
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
            "price_ts"   : _state["price_ts"],
            "ws_count"   : _state["ws_count"],
        })

@app.route("/api/scan", methods=["POST"])
def api_scan():
    now = time.time()
    elapsed = now - _scan_rate["last"]
    if elapsed < _scan_rate["min_interval"]:
        wait = round(_scan_rate["min_interval"] - elapsed)
        return jsonify({"ok": False, "msg": f"rate limited, retry in {wait}s"}), 429
    ok = trigger_scan()
    if ok:
        _scan_rate["last"] = now
    return jsonify({"ok": ok, "msg": "scanning" if ok else "already scanning"})

# ======================================
# MAIN
# ======================================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=False)
