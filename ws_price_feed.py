"""
ws_price_feed.py — Bybit WebSocket Real-Time Price Feed
Menggantikan REST polling tiap 5 detik dengan WebSocket push (~100ms latency).

Cara kerja:
- Subscribe ke topic 'tickers.SYMBOL' untuk semua coin yang sedang di-scan
- Harga, change24h, high24h, low24h, turnover diupdate on-the-fly
- Thread-safe: pakai threading.Lock
- Auto-reconnect jika koneksi putus
"""

import threading
import time
import json
import logging

logger = logging.getLogger(__name__)

# ======================================
# PRICE STORE — shared global
# ======================================

_ws_cache: dict = {}   # { "BTCUSDT": { price, change24h, high24h, low24h, vol24h, turnover24h } }
_ws_lock = threading.Lock()

def get_ws_price(symbol: str) -> tuple:
    """Return (price, change24h, high24h, low24h, vol24h, turnover24h) dari WS cache."""
    with _ws_lock:
        d = _ws_cache.get(symbol, {})
    return (
        d.get("price",        0.0),
        d.get("change24h",    0.0),
        d.get("high24h",      0.0),
        d.get("low24h",       0.0),
        d.get("vol24h",       0.0),
        d.get("turnover24h",  0.0),
    )

def ws_cache_size() -> int:
    with _ws_lock:
        return len(_ws_cache)

# ======================================
# WEBSOCKET MANAGER
# ======================================

class BybitWsPriceFeed:
    """
    Subscribe ke Bybit Public Linear WebSocket untuk ticker semua USDT pairs.
    Gunakan satu koneksi dengan topic 'tickers.*' (wildcard tidak tersedia),
    jadi subscribe per-batch maksimal 10 symbol setiap request.

    Alternatif lebih sederhana: gunakan topic 'tickers.linear' yang push
    semua ticker sekaligus via Bybit V5 WebSocket.
    """

    WS_URL = "wss://stream.bybit.com/v5/public/linear"
    # Topic tunggal yang push update semua ticker sekaligus
    SUBSCRIBE_MSG = {"op": "subscribe", "args": ["tickers.BTCUSDT"]}  # placeholder, diganti dinamis

    def __init__(self):
        self._ws       = None
        self._thread   = None
        self._running  = False
        self._symbols  = set()   # set symbol yang disubscribe
        self._subscribed = set() # sudah disubscribe di koneksi aktif

    # ---- Public API ----

    def start(self, symbols: list = None):
        """
        Mulai WebSocket feed.
        Jika symbols=None, subscribe ke semua linear tickers via wildcard stream.
        Jika symbols diberikan, subscribe hanya symbol tersebut.
        """
        if symbols:
            with _ws_lock:
                self._symbols = set(symbols)

        if not self._running:
            self._running = True
            self._thread = threading.Thread(target=self._run_loop, daemon=True, name="ws-price-feed")
            self._thread.start()
            logger.info("[WS] Price feed started")

    def stop(self):
        self._running = False
        if self._ws:
            try:
                self._ws.close()
            except Exception as e:
                logger.warning("[WS] Error closing connection: %s", e)

    def update_symbols(self, symbols: list):
        """Tambah/ganti symbol yang disubscribe. Akan diapply di reconnect berikutnya."""
        with _ws_lock:
            self._symbols = set(symbols)
        # Subscribe tambahan ke koneksi aktif jika ada
        if self._ws:
            self._subscribe_new(symbols)

    # ---- Internal ----

    def _run_loop(self):
        """Main reconnect loop."""
        import websocket  # pip install websocket-client

        backoff = 2
        while self._running:
            try:
                logger.info(f"[WS] Connecting to {self.WS_URL}")
                self._subscribed = set()

                ws = websocket.WebSocketApp(
                    self.WS_URL,
                    on_open    = self._on_open,
                    on_message = self._on_message,
                    on_error   = self._on_error,
                    on_close   = self._on_close,
                )
                self._ws = ws
                ws.run_forever(ping_interval=20, ping_timeout=10)

                if not self._running:
                    break

                logger.warning(f"[WS] Disconnected, reconnect in {backoff}s...")
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)

            except Exception as e:
                logger.error(f"[WS] Loop error: {e}")
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)

    def _on_open(self, ws):
        logger.info("[WS] Connected")
        # Subscribe ke semua symbol yang ada
        with _ws_lock:
            syms = list(self._symbols)
        self._do_subscribe(ws, syms)

    def _on_message(self, ws, message):
        try:
            data = json.loads(message)

            # Handle subscription confirmation
            if data.get("op") == "subscribe":
                if data.get("success"):
                    logger.debug("[WS] Subscribed OK: %s", data.get("ret_msg", ""))
                else:
                    logger.error("[WS] Subscription failed: %s", data.get("ret_msg", "unknown"))
                return

            # Handle ticker update
            topic = data.get("topic", "")
            if not topic.startswith("tickers."):
                return

            d = data.get("data", {})
            sym = d.get("symbol", "")
            if not sym:
                return

            # Parse fields (semua string dari Bybit WS)
            def _f(k):
                v = d.get(k, "")
                try:
                    return float(v) if v else 0.0
                except (ValueError, TypeError):
                    return 0.0

            price        = _f("lastPrice")
            change24h    = _f("price24hPcnt") * 100   # convert dari desimal ke persen
            high24h      = _f("highPrice24h")
            low24h       = _f("lowPrice24h")
            vol24h       = _f("volume24h")
            turnover24h  = _f("turnover24h")

            if price > 0:
                with _ws_lock:
                    _ws_cache[sym] = {
                        "price"      : price,
                        "change24h"  : change24h,
                        "high24h"    : high24h,
                        "low24h"     : low24h,
                        "vol24h"     : vol24h,
                        "turnover24h": turnover24h,
                    }

        except Exception as e:
            logger.error(f"[WS] Message parse error: {e}")

    def _on_error(self, ws, error):
        logger.error(f"[WS] Error: {error}")

    def _on_close(self, ws, code, msg):
        logger.warning(f"[WS] Closed: {code} {msg}")

    def _do_subscribe(self, ws, symbols: list):
        """Subscribe ke symbols dalam batch 10."""
        if not symbols:
            return
        batch_size = 10
        failed_count = 0
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i + batch_size]
            args  = [f"tickers.{s}" for s in batch]
            msg   = json.dumps({"op": "subscribe", "args": args})
            try:
                ws.send(msg)
                for s in batch:
                    self._subscribed.add(s)
                time.sleep(0.05)  # jangan flood
            except Exception as e:
                failed_count += len(batch)
                logger.error("[WS] Subscribe error for batch starting at %s: %s", batch[0] if batch else '?', e)
        if failed_count:
            logger.warning("[WS] Failed to subscribe %d/%d symbols", failed_count, len(symbols))

    def _subscribe_new(self, symbols: list):
        """Subscribe symbol baru yang belum ada di koneksi aktif."""
        new_syms = [s for s in symbols if s not in self._subscribed]
        if new_syms and self._ws:
            logger.info("[WS] Subscribing %d new symbols", len(new_syms))
            self._do_subscribe(self._ws, new_syms)


# ======================================
# SINGLETON
# ======================================

_feed = BybitWsPriceFeed()

def start_ws_feed(symbols: list = None):
    """Panggil sekali saat app startup."""
    _feed.start(symbols)

def update_ws_symbols(symbols: list):
    """Panggil setelah scan selesai untuk update daftar symbol."""
    _feed.update_symbols(symbols)

def stop_ws_feed():
    _feed.stop()
