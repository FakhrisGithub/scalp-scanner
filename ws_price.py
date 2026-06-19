"""
ws_price.py — Bybit WebSocket Realtime Price Feed untuk Web App (Flask/Railway)
Menyediakan harga LIVE (bukan REST polling) untuk dipakai sebagai acuan
entry/SL/TP di scanner_scalp.py.

Cara pakai:
    import ws_price
    ws_price.start(["BTCUSDT", "ETHUSDT", ...])
    price = ws_price.get_price("BTCUSDT")   # 0.0 kalau belum ada data
"""

import threading
import time
from pybit.unified_trading import WebSocket

_lock              = threading.Lock()
_ws                = None
_price_cache: dict = {}     # {symbol: {"price": float, "ts": float}}
_subscribed: set   = set()
_running           = False

MAX_SYMBOLS_PER_WS = 190     # batas aman jumlah topic per koneksi Bybit WS


def _handle_ticker(msg: dict):
    try:
        data = msg.get("data", {})
        sym  = data.get("symbol", "")
        if not sym:
            return
        price = data.get("lastPrice")
        if price is None:
            return
        with _lock:
            _price_cache[sym] = {
                "price": float(price),
                "ts"   : time.time(),
            }
    except Exception as e:
        print(f"[ws_price] handle_ticker error: {e}")


def start(symbols: list):
    """Mulai koneksi WS dan subscribe ticker untuk semua symbol (dibatasi MAX_SYMBOLS_PER_WS)."""
    global _ws, _running, _subscribed

    if _running:
        # Sudah jalan — cukup tambah subscribe untuk symbol baru
        subscribe_more(symbols)
        return

    try:
        _ws = WebSocket(testnet=False, channel_type="linear")
        _running = True
        print("[ws_price] WebSocket connected")
    except Exception as e:
        print(f"[ws_price] connect error: {e}")
        _ws = None
        return

    subscribe_more(symbols)


def subscribe_more(symbols: list):
    global _subscribed
    if _ws is None:
        return
    to_add = [s for s in symbols if s not in _subscribed][:MAX_SYMBOLS_PER_WS]
    for sym in to_add:
        try:
            _ws.ticker_stream(symbol=sym, callback=_handle_ticker)
            _subscribed.add(sym)
        except Exception as e:
            print(f"[ws_price] subscribe {sym} error: {e}")
    if to_add:
        print(f"[ws_price] Subscribed {len(to_add)} new symbols (total {len(_subscribed)})")


def get_price(symbol: str) -> float:
    """Ambil harga realtime. Return 0.0 kalau belum ada data WS untuk symbol ini."""
    with _lock:
        d = _price_cache.get(symbol)
        if not d:
            return 0.0
        # Anggap stale kalau lebih dari 30 detik tidak update (WS putus diam-diam)
        if time.time() - d["ts"] > 30:
            return 0.0
        return d["price"]


def get_all_prices() -> dict:
    with _lock:
        return {k: v["price"] for k, v in _price_cache.items()}


def is_connected() -> bool:
    return _ws is not None and _running


def stop():
    global _ws, _running, _subscribed
    _running = False
    _subscribed.clear()
    if _ws:
        try:
            _ws.exit()
        except Exception:
            pass
        _ws = None
