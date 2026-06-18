"""
shared_utils.py — Shared utility functions for Scalp Scanner.
Consolidates duplicated logic across app.py, scanner_scalp.py, and ws_price_feed.py.
"""

import json
import ssl
import urllib.request

ssl._create_default_https_context = ssl._create_unverified_context

# Keys used consistently for price data dicts across the app
PRICE_KEYS = ("price", "change24h", "high24h", "low24h", "vol24h", "turnover24h")


# ======================================
# BYBIT SYMBOL FETCHING
# ======================================

def fetch_usdt_symbols(session) -> list:
    """
    Fetch all USDT-margined linear symbols from Bybit.
    Used by both app.py (WS init) and scanner_scalp.py (scan loop).
    """
    info = session.get_instruments_info(category="linear")
    return [
        x["symbol"]
        for x in info["result"]["list"]
        if x["symbol"].endswith("USDT")
    ]


# ======================================
# PRICE DATA EXTRACTION
# ======================================

def extract_price_tuple(data: dict) -> tuple:
    """
    Extract a standard 6-element price tuple from a price data dict.
    Returns: (price, change24h, high24h, low24h, vol24h, turnover24h)

    Used by ws_price_feed.get_ws_price() and scanner_scalp.get_live_price() REST fallback.
    """
    return (
        data.get("price", 0.0),
        data.get("change24h", 0.0),
        data.get("high24h", 0.0),
        data.get("low24h", 0.0),
        data.get("vol24h", 0.0),
        data.get("turnover24h", 0.0),
    )


# ======================================
# PRICE FORMATTING
# ======================================

def format_change_pct(change24h: float) -> str:
    """
    Format 24h price change as a percentage string with sign.
    e.g. +1.23% or -0.45%

    Used by app.py (_ws_price_updater) and scanner_scalp.py (analyze_scalp).
    """
    rounded = round(change24h, 2)
    if change24h >= 0:
        return f"+{rounded}%"
    return f"{rounded}%"


def format_turnover(turnover24h: float) -> str:
    """
    Format 24h turnover in millions (e.g. '12.34M').

    Used by app.py (_ws_price_updater) and scanner_scalp.py (analyze_scalp).
    """
    return f"{round(turnover24h / 1_000_000, 2)}M"


# ======================================
# HTTP / JSON FETCHING
# ======================================

def fetch_json(url: str, timeout: int = 8):
    """
    Fetch JSON from a URL with a Mozilla User-Agent header.
    Returns parsed dict/list on success, None on failure.

    Used by scanner_scalp.py for CoinGecko API calls.
    """
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        print(f"[fetch_json] {url}: {e}")
        return None
