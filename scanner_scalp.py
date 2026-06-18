"""
scanner_scalp.py — BYBIT SCALP SCANNER V3 (WebSocket Edition)
Timeframe acuan: 5M, 15M, 30M, 1H
Target win rate ≥80% via multi-layer confluence filter
News/Catalyst: CoinGecko Trending + Fear & Greed + Top Gainers
Price real-time: Bybit WebSocket (bukan REST poll)
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from pybit.unified_trading import HTTP
import pandas as pd
from ta.momentum import RSIIndicator, StochasticOscillator
from ta.volatility import BollingerBands, AverageTrueRange
from ta.trend import MACD

from shared_utils import (
    fetch_usdt_symbols,
    extract_price_tuple,
    format_change_pct,
    format_turnover,
    fetch_json,
)

# ======================================
# SESSION
# ======================================

session    = HTTP(testnet=False)
MAX_WORKERS = 20

# ======================================
# LIVE PRICE — WebSocket (primary) + REST fallback
# ======================================

# Import WS feed; jika belum start, get_live_price fallback ke REST cache
from ws_price_feed import get_ws_price, start_ws_feed, update_ws_symbols, ws_cache_size

# REST cache sebagai fallback awal (sebelum WS ready)
_rest_price_cache: dict = {}

def refresh_price_cache():
    """Fallback REST bulk fetch — dipanggil sekali di awal scan."""
    global _rest_price_cache
    try:
        resp  = session.get_tickers(category="linear")
        cache = {}
        for t in resp["result"]["list"]:
            sym = t["symbol"]
            try:
                cache[sym] = {
                    "price"      : float(t.get("lastPrice",    0) or 0),
                    "change24h"  : float(t.get("price24hPcnt", 0) or 0) * 100,
                    "high24h"    : float(t.get("highPrice24h", 0) or 0),
                    "low24h"     : float(t.get("lowPrice24h",  0) or 0),
                    "vol24h"     : float(t.get("volume24h",    0) or 0),
                    "turnover24h": float(t.get("turnover24h",  0) or 0),
                }
            except (ValueError, TypeError):
                pass
        _rest_price_cache = cache
    except Exception as e:
        print("REST price cache error:", e)
        _rest_price_cache = {}

def get_live_price(symbol: str) -> tuple:
    """
    Ambil harga dari WebSocket cache (real-time).
    Fallback ke REST cache jika WS belum punya data symbol ini.
    """
    # Coba WS cache dulu
    ws_price = get_ws_price(symbol)
    if ws_price[0] > 0:
        return ws_price

    # Fallback REST cache
    d = _rest_price_cache.get(symbol, {})
    return extract_price_tuple(d)

# ======================================
# NEWS & CATALYST — CoinGecko (no key)
# ======================================

_news_cache: dict = {
    "trending"    : [],
    "top_gainers" : [],
    "fear_greed"  : 50,
    "fg_label"    : "Neutral",
    "last_fetch"  : 0,
}

NEWS_TTL = 300  # 5 menit

def refresh_news_cache():
    global _news_cache
    now = time.time()
    if now - _news_cache["last_fetch"] < NEWS_TTL:
        return

    # CoinGecko Trending
    data     = fetch_json("https://api.coingecko.com/api/v3/search/trending")
    trending = []
    if data and "coins" in data:
        for item in data["coins"]:
            sym = item.get("item", {}).get("symbol", "").upper()
            if sym:
                trending.append(sym + "USDT")

    # Top Gainers 24H
    data2       = fetch_json(
        "https://api.coingecko.com/api/v3/coins/markets"
        "?vs_currency=usd&order=price_change_percentage_24h_desc"
        "&per_page=50&page=1&sparkline=false"
    )
    top_gainers = []
    if isinstance(data2, list):
        for coin in data2:
            sym = coin.get("symbol", "").upper()
            if sym:
                top_gainers.append(sym + "USDT")

    # Fear & Greed
    fg_data  = fetch_json("https://api.alternative.me/fng/?limit=1")
    fg_val   = 50
    fg_label = "Neutral"
    if fg_data and "data" in fg_data and fg_data["data"]:
        try:
            fg_val   = int(fg_data["data"][0]["value"])
            fg_label = fg_data["data"][0]["value_classification"]
        except Exception:
            pass

    _news_cache.update({
        "trending"   : trending,
        "top_gainers": top_gainers,
        "fear_greed" : fg_val,
        "fg_label"   : fg_label,
        "last_fetch" : now,
    })
    print(f"[news] F&G={fg_val}({fg_label}) | Trending={len(trending)} | Gainers={len(top_gainers)}")

def get_news_boost(symbol: str) -> tuple:
    is_trending = symbol in _news_cache["trending"]
    is_gainer   = symbol in _news_cache["top_gainers"]
    fg          = _news_cache["fear_greed"]

    boost  = 0
    labels = []

    if is_trending:
        boost += 10
        labels.append("🔥Trending")
    if is_gainer:
        boost += 8
        labels.append("📈Gainer")
    if fg >= 75:
        boost += 5
        labels.append("🟢Greed")
    elif fg >= 60:
        boost += 2
        labels.append("🟡F&G+")
    elif fg <= 25:
        boost -= 5
        labels.append("🔴Fear")

    return boost, ",".join(labels) if labels else "-"

def get_market_context() -> str:
    fg    = _news_cache["fear_greed"]
    label = _news_cache["fg_label"]
    n     = len(_news_cache["trending"])
    ws_n  = ws_cache_size()
    return f"F&G: {fg} ({label}) | Trending: {n} | WS: {ws_n}🔌"

# ======================================
# FETCH OHLCV (REST — untuk indikator historis)
# ======================================

def fetch_df(symbol: str, interval: str, limit: int = 250) -> pd.DataFrame:
    kline = session.get_kline(
        category="linear",
        symbol=symbol,
        interval=interval,
        limit=limit
    )
    df = pd.DataFrame(
        kline["result"]["list"],
        columns=["time","open","high","low","close","volume","turnover"]
    )
    df = df[::-1].reset_index(drop=True)
    for col in ["open","high","low","close","volume"]:
        df[col] = df[col].astype(float)
    return df

# ======================================
# INDICATORS
# ======================================

def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
    close = df["close"]
    high  = df["high"]
    low   = df["low"]
    vol   = df["volume"]

    df["ema8"]   = close.ewm(span=8,   adjust=False).mean()
    df["ema21"]  = close.ewm(span=21,  adjust=False).mean()
    df["ema50"]  = close.ewm(span=50,  adjust=False).mean()
    df["ema200"] = close.ewm(span=200, adjust=False).mean()

    df["rsi"]    = RSIIndicator(close, window=14).rsi()
    df["rsi_ma"] = df["rsi"].rolling(9).mean()

    stoch = StochasticOscillator(high, low, close, window=14, smooth_window=3)
    df["stoch_k"] = stoch.stoch()
    df["stoch_d"] = stoch.stoch_signal()

    macd_i = MACD(close, window_slow=26, window_fast=12, window_sign=9)
    df["macd"]        = macd_i.macd()
    df["macd_signal"] = macd_i.macd_signal()
    df["macd_hist"]   = macd_i.macd_diff()

    bb = BollingerBands(close, window=20, window_dev=2)
    df["bb_upper"] = bb.bollinger_hband()
    df["bb_lower"] = bb.bollinger_lband()
    df["bb_mid"]   = bb.bollinger_mavg()
    df["bb_width"] = (df["bb_upper"] - df["bb_lower"]) / df["bb_mid"].replace(0, 1)

    df["atr"]      = AverageTrueRange(high, low, close, window=14).average_true_range()

    df["vol_ma20"] = vol.rolling(20).mean()
    df["rel_vol"]  = vol / df["vol_ma20"].replace(0, 1)

    return df

# ======================================
# SIGNAL PER-TF
# ======================================

def scalp_signal_tf(df: pd.DataFrame) -> dict:
    last  = df.iloc[-1]
    prev  = df.iloc[-2]

    price    = last["close"]
    ema8     = last["ema8"]
    ema21    = last["ema21"]
    ema50    = last["ema50"]
    ema200   = last["ema200"]
    rsi      = last["rsi"]
    stoch_k  = last["stoch_k"]
    stoch_d  = last["stoch_d"]
    macd_h   = last["macd_hist"]
    macd_h_p = prev["macd_hist"]
    bb_width = last["bb_width"]
    rel_vol  = last["rel_vol"]

    # ---- LONG ----
    lp, lc = 0, []
    if ema8 > ema21 > ema50:           lp += 20; lc.append("EMA↑")
    if price > ema8:                   lp += 10; lc.append("P>E8")
    if price > ema200:                 lp += 8;  lc.append("P>E200")
    else:                              lp -= 5
    if 45 <= rsi <= 70:                lp += 10; lc.append(f"RSI{round(rsi)}")
    elif rsi > 70:                     lp +=  3
    if prev["stoch_k"] < prev["stoch_d"] and stoch_k > stoch_d and stoch_k < 80:
                                       lp += 15; lc.append("Stoch↑")
    elif stoch_k > stoch_d and stoch_k < 80: lp += 7; lc.append("Stoch+")
    if macd_h > 0 and macd_h_p <= 0:  lp += 20; lc.append("MACD✓")
    elif macd_h > macd_h_p:           lp +=  8; lc.append("MACD↑")
    if rel_vol >= 2.0:                 lp += 15; lc.append(f"RVol{round(rel_vol,1)}x")
    elif rel_vol >= 1.5:               lp += 10; lc.append(f"RVol{round(rel_vol,1)}x")
    elif rel_vol >= 1.2:               lp +=  5
    prev_bw = df["bb_width"].iloc[-5:-1].mean()
    if bb_width > prev_bw * 1.3 and price > last["bb_mid"]:
                                       lp += 10; lc.append("BB-Brk")

    # ---- SHORT ----
    sp, sc = 0, []
    if ema8 < ema21 < ema50:           sp += 20; sc.append("EMA↓")
    if price < ema8:                   sp += 10; sc.append("P<E8")
    if price < ema200:                 sp += 8;  sc.append("P<E200")
    else:                              sp -= 5
    if 30 <= rsi <= 55:                sp += 10; sc.append(f"RSI{round(rsi)}")
    elif rsi < 30:                     sp +=  3
    if prev["stoch_k"] > prev["stoch_d"] and stoch_k < stoch_d and stoch_k > 20:
                                       sp += 15; sc.append("Stoch↓")
    elif stoch_k < stoch_d and stoch_k > 20: sp += 7; sc.append("Stoch-")
    if macd_h < 0 and macd_h_p >= 0:  sp += 20; sc.append("MACD✗")
    elif macd_h < macd_h_p:           sp +=  8; sc.append("MACD↓")
    if rel_vol >= 2.0:                 sp += 15; sc.append(f"RVol{round(rel_vol,1)}x")
    elif rel_vol >= 1.5:               sp += 10; sc.append(f"RVol{round(rel_vol,1)}x")
    if bb_width > prev_bw * 1.3 and price < last["bb_mid"]:
                                       sp += 10; sc.append("BB-Brk")

    lp = min(lp, 100)
    sp = min(sp, 100)

    if lp >= sp + 10:
        bias, pts, cond = "LONG",  lp, ",".join(lc)
    elif sp >= lp + 10:
        bias, pts, cond = "SHORT", sp, ",".join(sc)
    else:
        bias, pts, cond = "FLAT",  max(lp, sp), "-"

    return {
        "bias"     : bias,
        "score"    : pts,
        "long_pts" : lp,
        "short_pts": sp,
        "rsi"      : round(rsi, 1),
        "stoch_k"  : round(stoch_k, 1),
        "rel_vol"  : round(rel_vol, 2),
        "atr"      : last["atr"],
        "price"    : price,
        "cond"     : cond,
        "ema200"   : round(ema200, 6),
        "above_ema200": price > ema200,
    }

# ======================================
# ENTRY CALCULATOR
# ======================================

def scalp_entry(df5m: pd.DataFrame, bias: str) -> dict:
    last  = df5m.iloc[-1]
    price = last["close"]
    atr   = last["atr"]

    if bias == "LONG":
        entry = round(price, 6)
        sl    = round(price - atr * 1.0, 6)
        tp1   = round(price + atr * 1.5, 6)
        tp2   = round(price + atr * 2.5, 6)
    elif bias == "SHORT":
        entry = round(price, 6)
        sl    = round(price + atr * 1.0, 6)
        tp1   = round(price - atr * 1.5, 6)
        tp2   = round(price - atr * 2.5, 6)
    else:
        return {"entry":"-","sl":"-","tp1":"-","tp2":"-","rr1":"-","rr2":"-","entry_note":"No Setup"}

    rr1 = round(abs(tp1 - entry) / max(abs(entry - sl), 1e-12), 2)
    rr2 = round(abs(tp2 - entry) / max(abs(entry - sl), 1e-12), 2)
    return {
        "entry"     : entry,
        "sl"        : sl,
        "tp1"       : tp1,
        "tp2"       : tp2,
        "rr1"       : f"1:{rr1}",
        "rr2"       : f"1:{rr2}",
        "entry_note": "Market",
    }

# ======================================
# CONFLUENCE SCORE & DECISION
# ======================================

def scalp_confluence(sig5m, sig15m, sig30m, sig1h, news_boost) -> tuple:
    sigs = [sig5m, sig15m, sig30m, sig1h]

    long_v  = sum(1 for s in sigs if s["bias"] == "LONG")
    short_v = sum(1 for s in sigs if s["bias"] == "SHORT")

    if long_v >= 3:
        bias = "LONG";  vote_sc = long_v * 8
    elif short_v >= 3:
        bias = "SHORT"; vote_sc = short_v * 8
    elif long_v == 2 and sig1h["bias"] == "LONG":
        bias = "LONG";  vote_sc = 12
    elif short_v == 2 and sig1h["bias"] == "SHORT":
        bias = "SHORT"; vote_sc = 12
    else:
        bias = "FLAT";  vote_sc = 0

    if bias != "FLAT":
        weighted = (
            sig5m["score"]  * 0.10 +
            sig15m["score"] * 0.15 +
            sig30m["score"] * 0.40 +
            sig1h["score"]  * 0.35
        )
    else:
        weighted = 0

    vol_ok  = sig30m["rel_vol"] >= 1.3 or sig1h["rel_vol"] >= 1.3
    macd_ok = any("MACD✓" in s["cond"] or "MACD✗" in s["cond"] for s in sigs)

    if not vol_ok:  weighted *= 0.6
    if not macd_ok: weighted *= 0.8

    final = min(max(round(weighted + vote_sc + news_boost), 0), 100)

    if bias == "FLAT" or final < 62:
        decision = "SKIP ❌"
    elif final >= 80 and vol_ok:
        decision = "SCALP NOW 🚀" if bias == "LONG" else "SHORT NOW 🔻"
    elif final >= 70:
        decision = "ENTRY 👍" if bias == "LONG" else "SHORT 👍"
    else:
        decision = "WATCH 👁"

    if   final >= 85: grade = "A+ 🔥"
    elif final >= 75: grade = "A"
    elif final >= 62: grade = "B"
    elif final >= 50: grade = "C"
    else:             grade = "D"

    detail = f"L:{long_v}/S:{short_v} Vol:{round(max(sig30m['rel_vol'], sig1h['rel_vol']),1)}x"
    return final, bias, decision, grade, detail

# ======================================
# ANALYZE SYMBOL
# ======================================

def analyze_scalp(symbol: str):
    try:
        df5m  = add_indicators(fetch_df(symbol, "5",   limit=250))
        df15m = add_indicators(fetch_df(symbol, "15",  limit=250))
        df30m = add_indicators(fetch_df(symbol, "30",  limit=250))
        df1h  = add_indicators(fetch_df(symbol, "60",  limit=250))

        sig5m  = scalp_signal_tf(df5m)
        sig15m = scalp_signal_tf(df15m)
        sig30m = scalp_signal_tf(df30m)
        sig1h  = scalp_signal_tf(df1h)

        news_boost, news_label = get_news_boost(symbol)

        final, bias, decision, grade, detail = scalp_confluence(
            sig5m, sig15m, sig30m, sig1h, news_boost
        )

        entry = scalp_entry(df5m, bias)

        # Gunakan WS price (real-time) saat dipanggil
        live_price, change24h, high24h, low24h, vol24h, turnover24h = get_live_price(symbol)

        chg_str = format_change_pct(change24h)
        vol_m   = format_turnover(turnover24h)

        return (
            symbol,                         # 0  Coin
            sig5m["score"],                 # 1  Sc5M
            sig15m["score"],                # 2  Sc15M
            sig30m["score"],                # 3  Sc30M
            sig1h["score"],                 # 4  Sc1H
            final,                          # 5  Score
            bias,                           # 6  Bias
            sig5m["rsi"],                   # 7  RSI5M
            sig5m["stoch_k"],               # 8  Stoch5M
            sig5m["rel_vol"],               # 9  RVol5M
            sig15m["rel_vol"],              # 10 RVol15M
            news_label,                     # 11 Catalyst
            decision,                       # 12 Decision
            grade,                          # 13 Grade
            detail,                         # 14 Detail
            entry["entry"],                 # 15 Entry
            entry["entry_note"],            # 16 EntryType
            entry["sl"],                    # 17 SL
            entry["tp1"],                   # 18 TP1
            entry["tp2"],                   # 19 TP2
            entry["rr1"],                   # 20 RR1
            entry["rr2"],                   # 21 RR2
            round(live_price, 6),           # 22 Price
            chg_str,                        # 23 Chg24h%
            round(high24h, 6),              # 24 High24h
            round(low24h,  6),              # 25 Low24h
            vol_m,                              # 26 Vol24h
            round(sig30m["rel_vol"], 2),    # 27 RVol30M_tbl
            round(sig1h["rel_vol"], 2),     # 28 RVol1H_tbl
            sig5m["cond"],                  # 29 Cond5M
            sig15m["cond"],                 # 30 Cond15M
            sig30m["cond"],                 # 31 Cond30M
            sig1h["cond"],                  # 32 Cond1H
            "Bull🟢" if sig1h["above_ema200"] else "Bear🔴",  # 33 Trend200(1H)
            sig1h["ema200"],                # 34 EMA200(1H)
        )

    except Exception as e:
        print(f"[scalp] ERROR {symbol}: {e}")
        return None

# ======================================
# SCAN — ambil semua symbol
# ======================================

def scan_scalp(progress_callback=None) -> list:
    """
    Scan semua USDT symbol.
    Setelah selesai, update WS subscriptions supaya harga terus real-time.
    """
    refresh_news_cache()

    # Ambil daftar symbol
    symbols = fetch_usdt_symbols(session)

    # Pastikan WS sudah subscribe semua symbol ini
    update_ws_symbols(symbols)

    # Fallback: satu kali REST fetch harga untuk symbol yang WS belum punya
    ws_ready = ws_cache_size() > 0
    if not ws_ready:
        print("[scan] WS cache kosong, fetch REST harga dulu...")
        refresh_price_cache()

    results = []
    total   = len(symbols)
    done    = 0

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = [ex.submit(analyze_scalp, s) for s in symbols]
        for f in as_completed(futures):
            row = f.result()
            if row:
                results.append(row)
            done += 1
            if progress_callback:
                progress_callback(done, total)

    results.sort(key=lambda x: x[5], reverse=True)  # sort by Score
    return results

def get_market_context_str() -> str:
    return get_market_context()
