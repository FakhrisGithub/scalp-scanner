"""Tests for scanner_scalp.py — the core scanning logic module."""

import time
from unittest.mock import patch, MagicMock

import numpy as np
import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# add_indicators
# ---------------------------------------------------------------------------

class TestAddIndicators:
    def test_adds_all_expected_columns(self, sample_ohlcv_df):
        from scanner_scalp import add_indicators
        df = add_indicators(sample_ohlcv_df)

        expected_cols = [
            "ema8", "ema21", "ema50", "ema200",
            "rsi", "rsi_ma",
            "stoch_k", "stoch_d",
            "macd", "macd_signal", "macd_hist",
            "bb_upper", "bb_lower", "bb_mid", "bb_width",
            "atr",
            "vol_ma20", "rel_vol",
        ]
        for col in expected_cols:
            assert col in df.columns, f"Missing column: {col}"

    def test_output_length_matches_input(self, sample_ohlcv_df):
        from scanner_scalp import add_indicators
        df = add_indicators(sample_ohlcv_df)
        assert len(df) == len(sample_ohlcv_df)

    def test_ema_ordering_on_uptrend(self):
        """On a strictly increasing series, shorter EMAs should be above longer ones."""
        from scanner_scalp import add_indicators
        n = 250
        prices = np.linspace(50, 150, n)
        df = pd.DataFrame({
            "time": range(n),
            "open": prices,
            "high": prices * 1.01,
            "low": prices * 0.99,
            "close": prices,
            "volume": np.full(n, 10000.0),
            "turnover": prices * 10000,
        })
        df = add_indicators(df)
        last = df.iloc[-1]
        assert last["ema8"] > last["ema21"] > last["ema50"] > last["ema200"]

    def test_rsi_within_bounds(self, sample_ohlcv_df):
        from scanner_scalp import add_indicators
        df = add_indicators(sample_ohlcv_df)
        rsi_valid = df["rsi"].dropna()
        assert (rsi_valid >= 0).all() and (rsi_valid <= 100).all()

    def test_rel_vol_positive(self, sample_ohlcv_df):
        from scanner_scalp import add_indicators
        df = add_indicators(sample_ohlcv_df)
        rv = df["rel_vol"].dropna()
        assert (rv >= 0).all()


# ---------------------------------------------------------------------------
# scalp_signal_tf
# ---------------------------------------------------------------------------

class TestScalpSignalTf:
    def test_returns_expected_keys(self, sample_df_with_indicators):
        from scanner_scalp import scalp_signal_tf
        sig = scalp_signal_tf(sample_df_with_indicators)
        expected_keys = [
            "bias", "score", "long_pts", "short_pts",
            "rsi", "stoch_k", "rel_vol", "atr", "price", "cond",
            "ema200", "above_ema200",
        ]
        for k in expected_keys:
            assert k in sig, f"Missing key: {k}"

    def test_bias_is_valid_value(self, sample_df_with_indicators):
        from scanner_scalp import scalp_signal_tf
        sig = scalp_signal_tf(sample_df_with_indicators)
        assert sig["bias"] in ("LONG", "SHORT", "FLAT")

    def test_score_capped_at_100(self):
        """Score should never exceed 100 even with very strong signals."""
        from scanner_scalp import add_indicators, scalp_signal_tf
        n = 250
        # Strongly bullish: steep uptrend + high volume at end
        prices = np.linspace(50, 200, n)
        volume = np.full(n, 5000.0)
        volume[-20:] = 100000  # volume spike at end
        df = pd.DataFrame({
            "time": range(n),
            "open": prices * 0.99,
            "high": prices * 1.02,
            "low": prices * 0.98,
            "close": prices,
            "volume": volume,
            "turnover": prices * volume,
        })
        df = add_indicators(df)
        sig = scalp_signal_tf(df)
        assert sig["long_pts"] <= 100
        assert sig["short_pts"] <= 100
        assert sig["score"] <= 100

    def test_long_bias_on_uptrend(self):
        from scanner_scalp import add_indicators, scalp_signal_tf
        n = 250
        prices = np.linspace(50, 200, n)
        volume = np.full(n, 5000.0)
        volume[-20:] = 80000
        df = pd.DataFrame({
            "time": range(n),
            "open": prices * 0.99,
            "high": prices * 1.02,
            "low": prices * 0.98,
            "close": prices,
            "volume": volume,
            "turnover": prices * volume,
        })
        df = add_indicators(df)
        sig = scalp_signal_tf(df)
        assert sig["long_pts"] >= sig["short_pts"]

    def test_short_bias_on_downtrend(self):
        from scanner_scalp import add_indicators, scalp_signal_tf
        n = 250
        prices = np.linspace(200, 50, n)
        volume = np.full(n, 5000.0)
        volume[-20:] = 80000
        df = pd.DataFrame({
            "time": range(n),
            "open": prices * 1.01,
            "high": prices * 1.02,
            "low": prices * 0.98,
            "close": prices,
            "volume": volume,
            "turnover": prices * volume,
        })
        df = add_indicators(df)
        sig = scalp_signal_tf(df)
        assert sig["short_pts"] >= sig["long_pts"]

    def test_above_ema200_flag(self, sample_df_with_indicators):
        from scanner_scalp import scalp_signal_tf
        sig = scalp_signal_tf(sample_df_with_indicators)
        last = sample_df_with_indicators.iloc[-1]
        expected = last["close"] > last["ema200"]
        assert sig["above_ema200"] == expected


# ---------------------------------------------------------------------------
# scalp_entry
# ---------------------------------------------------------------------------

class TestScalpEntry:
    def _make_df(self, close=100.0, atr=2.0):
        """Build a minimal DF with the fields scalp_entry reads."""
        data = {
            "close": [close - 1, close],
            "atr": [atr, atr],
        }
        return pd.DataFrame(data)

    def test_long_entry(self):
        from scanner_scalp import scalp_entry
        df = self._make_df(close=100.0, atr=2.0)
        e = scalp_entry(df, "LONG")
        assert e["entry"] == 100.0
        assert e["sl"] < e["entry"]
        assert e["tp1"] > e["entry"]
        assert e["tp2"] > e["tp1"]
        assert e["entry_note"] == "Market"

    def test_short_entry(self):
        from scanner_scalp import scalp_entry
        df = self._make_df(close=100.0, atr=2.0)
        e = scalp_entry(df, "SHORT")
        assert e["entry"] == 100.0
        assert e["sl"] > e["entry"]
        assert e["tp1"] < e["entry"]
        assert e["tp2"] < e["tp1"]

    def test_flat_returns_no_setup(self):
        from scanner_scalp import scalp_entry
        df = self._make_df()
        e = scalp_entry(df, "FLAT")
        assert e["entry"] == "-"
        assert e["sl"] == "-"
        assert e["entry_note"] == "No Setup"

    def test_rr_ratio_is_1_5_to_1(self):
        from scanner_scalp import scalp_entry
        df = self._make_df(close=100.0, atr=2.0)
        e = scalp_entry(df, "LONG")
        assert e["rr1"] == "1:1.5"
        assert e["rr2"] == "1:2.5"

    def test_rr_ratio_short(self):
        from scanner_scalp import scalp_entry
        df = self._make_df(close=100.0, atr=2.0)
        e = scalp_entry(df, "SHORT")
        assert e["rr1"] == "1:1.5"
        assert e["rr2"] == "1:2.5"


# ---------------------------------------------------------------------------
# scalp_confluence
# ---------------------------------------------------------------------------

class TestScalpConfluence:
    def _sig(self, bias="LONG", score=70, rel_vol=1.5, cond="MACD✓"):
        return {
            "bias": bias,
            "score": score,
            "long_pts": score if bias == "LONG" else 0,
            "short_pts": score if bias == "SHORT" else 0,
            "rel_vol": rel_vol,
            "cond": cond,
        }

    def test_all_long_returns_long_bias(self):
        from scanner_scalp import scalp_confluence
        sig = self._sig("LONG", 80, 2.0, "MACD✓")
        final, bias, decision, grade, detail = scalp_confluence(sig, sig, sig, sig, 0)
        assert bias == "LONG"
        assert final > 0

    def test_all_short_returns_short_bias(self):
        from scanner_scalp import scalp_confluence
        sig = self._sig("SHORT", 80, 2.0, "MACD✗")
        final, bias, decision, grade, detail = scalp_confluence(sig, sig, sig, sig, 0)
        assert bias == "SHORT"

    def test_mixed_signals_return_flat(self):
        from scanner_scalp import scalp_confluence
        long_sig = self._sig("LONG", 60, 1.0, "EMA↑")
        short_sig = self._sig("SHORT", 60, 1.0, "EMA↓")
        flat_sig = self._sig("FLAT", 30, 0.5, "-")
        # 1 LONG, 1 SHORT, 2 FLAT — no majority, 1H is FLAT
        final, bias, decision, grade, detail = scalp_confluence(
            long_sig, short_sig, flat_sig, flat_sig, 0
        )
        assert bias == "FLAT"

    def test_score_clamped_0_100(self):
        from scanner_scalp import scalp_confluence
        sig = self._sig("LONG", 100, 3.0, "MACD✓")
        final, bias, decision, grade, detail = scalp_confluence(sig, sig, sig, sig, 50)
        assert 0 <= final <= 100

    def test_decision_skip_when_flat(self):
        from scanner_scalp import scalp_confluence
        flat_sig = self._sig("FLAT", 30, 0.5, "-")
        final, bias, decision, grade, detail = scalp_confluence(
            flat_sig, flat_sig, flat_sig, flat_sig, 0
        )
        assert "SKIP" in decision

    def test_decision_now_on_high_score_long(self):
        from scanner_scalp import scalp_confluence
        sig = self._sig("LONG", 90, 2.0, "MACD✓")
        final, bias, decision, grade, detail = scalp_confluence(sig, sig, sig, sig, 10)
        if final >= 80:
            assert "NOW" in decision

    def test_decision_now_on_high_score_short(self):
        from scanner_scalp import scalp_confluence
        sig = self._sig("SHORT", 90, 2.0, "MACD✗")
        final, bias, decision, grade, detail = scalp_confluence(sig, sig, sig, sig, 10)
        if final >= 80:
            assert "NOW" in decision or "SHORT" in decision

    def test_grade_assignment(self):
        from scanner_scalp import scalp_confluence
        sig = self._sig("LONG", 95, 2.5, "MACD✓")
        final, bias, decision, grade, detail = scalp_confluence(sig, sig, sig, sig, 10)
        if final >= 85:
            assert "A+" in grade
        elif final >= 75:
            assert grade == "A"

    def test_news_boost_increases_score(self):
        from scanner_scalp import scalp_confluence
        sig = self._sig("LONG", 70, 1.5, "MACD✓")
        final_no_boost, _, _, _, _ = scalp_confluence(sig, sig, sig, sig, 0)
        final_boosted, _, _, _, _ = scalp_confluence(sig, sig, sig, sig, 15)
        assert final_boosted >= final_no_boost

    def test_low_volume_penalty(self):
        from scanner_scalp import scalp_confluence
        sig_hi = self._sig("LONG", 70, 2.0, "MACD✓")
        sig_lo = self._sig("LONG", 70, 0.5, "MACD✓")
        final_hi, _, _, _, _ = scalp_confluence(sig_hi, sig_hi, sig_hi, sig_hi, 0)
        final_lo, _, _, _, _ = scalp_confluence(sig_lo, sig_lo, sig_lo, sig_lo, 0)
        assert final_hi >= final_lo

    def test_two_long_plus_1h_long(self):
        from scanner_scalp import scalp_confluence
        long_sig = self._sig("LONG", 70, 1.5, "MACD✓")
        flat_sig = self._sig("FLAT", 30, 1.0, "-")
        final, bias, decision, grade, detail = scalp_confluence(
            long_sig, flat_sig, long_sig, long_sig, 0
        )
        assert bias == "LONG"

    def test_two_short_plus_1h_short(self):
        from scanner_scalp import scalp_confluence
        short_sig = self._sig("SHORT", 70, 1.5, "MACD✗")
        flat_sig = self._sig("FLAT", 30, 1.0, "-")
        final, bias, decision, grade, detail = scalp_confluence(
            short_sig, flat_sig, short_sig, short_sig, 0
        )
        assert bias == "SHORT"


# ---------------------------------------------------------------------------
# get_news_boost
# ---------------------------------------------------------------------------

class TestGetNewsBoost:
    def test_trending_gives_boost(self):
        from scanner_scalp import get_news_boost, _news_cache
        _news_cache["trending"] = ["BTCUSDT"]
        _news_cache["top_gainers"] = []
        _news_cache["fear_greed"] = 50
        boost, label = get_news_boost("BTCUSDT")
        assert boost >= 10
        assert "Trending" in label

    def test_gainer_gives_boost(self):
        from scanner_scalp import get_news_boost, _news_cache
        _news_cache["trending"] = []
        _news_cache["top_gainers"] = ["ETHUSDT"]
        _news_cache["fear_greed"] = 50
        boost, label = get_news_boost("ETHUSDT")
        assert boost >= 8
        assert "Gainer" in label

    def test_high_greed_gives_boost(self):
        from scanner_scalp import get_news_boost, _news_cache
        _news_cache["trending"] = []
        _news_cache["top_gainers"] = []
        _news_cache["fear_greed"] = 80
        boost, label = get_news_boost("ANYUSDT")
        assert boost >= 5
        assert "Greed" in label

    def test_fear_gives_negative_boost(self):
        from scanner_scalp import get_news_boost, _news_cache
        _news_cache["trending"] = []
        _news_cache["top_gainers"] = []
        _news_cache["fear_greed"] = 20
        boost, label = get_news_boost("ANYUSDT")
        assert boost < 0
        assert "Fear" in label

    def test_no_match_returns_dash(self):
        from scanner_scalp import get_news_boost, _news_cache
        _news_cache["trending"] = []
        _news_cache["top_gainers"] = []
        _news_cache["fear_greed"] = 50
        boost, label = get_news_boost("XYZUSDT")
        assert boost == 0
        assert label == "-"

    def test_combined_trending_and_gainer(self):
        from scanner_scalp import get_news_boost, _news_cache
        _news_cache["trending"] = ["SOLUSDT"]
        _news_cache["top_gainers"] = ["SOLUSDT"]
        _news_cache["fear_greed"] = 50
        boost, label = get_news_boost("SOLUSDT")
        assert boost >= 18
        assert "Trending" in label
        assert "Gainer" in label

    def test_moderate_fg_boost(self):
        from scanner_scalp import get_news_boost, _news_cache
        _news_cache["trending"] = []
        _news_cache["top_gainers"] = []
        _news_cache["fear_greed"] = 65
        boost, label = get_news_boost("ANYUSDT")
        assert boost == 2
        assert "F&G+" in label


# ---------------------------------------------------------------------------
# get_live_price (WS first, REST fallback)
# ---------------------------------------------------------------------------

class TestGetLivePrice:
    def test_uses_ws_when_available(self):
        from scanner_scalp import get_live_price
        with patch("scanner_scalp.get_ws_price", return_value=(50000.0, 2.5, 51000.0, 49000.0, 1000.0, 5e7)):
            result = get_live_price("BTCUSDT")
            assert result[0] == 50000.0

    def test_falls_back_to_rest_cache(self):
        import scanner_scalp
        scanner_scalp._rest_price_cache = {
            "ETHUSDT": {
                "price": 3000.0,
                "change24h": 1.5,
                "high24h": 3100.0,
                "low24h": 2900.0,
                "vol24h": 500.0,
                "turnover24h": 1e6,
            }
        }
        with patch("scanner_scalp.get_ws_price", return_value=(0.0, 0.0, 0.0, 0.0, 0.0, 0.0)):
            result = scanner_scalp.get_live_price("ETHUSDT")
            assert result[0] == 3000.0

    def test_returns_zeros_when_no_data(self):
        import scanner_scalp
        scanner_scalp._rest_price_cache = {}
        with patch("scanner_scalp.get_ws_price", return_value=(0.0, 0.0, 0.0, 0.0, 0.0, 0.0)):
            result = scanner_scalp.get_live_price("UNKNOWNUSDT")
            assert result[0] == 0.0


# ---------------------------------------------------------------------------
# get_market_context
# ---------------------------------------------------------------------------

class TestGetMarketContext:
    def test_returns_string_with_fg(self):
        from scanner_scalp import get_market_context, _news_cache
        _news_cache["fear_greed"] = 72
        _news_cache["fg_label"] = "Greed"
        _news_cache["trending"] = ["A", "B"]
        result = get_market_context()
        assert "72" in result
        assert "Greed" in result
        assert "Trending: 2" in result


# ---------------------------------------------------------------------------
# refresh_price_cache
# ---------------------------------------------------------------------------

class TestRefreshPriceCache:
    def test_populates_cache_on_success(self):
        import scanner_scalp
        mock_resp = {
            "result": {
                "list": [
                    {
                        "symbol": "BTCUSDT",
                        "lastPrice": "50000",
                        "price24hPcnt": "0.025",
                        "highPrice24h": "51000",
                        "lowPrice24h": "49000",
                        "volume24h": "1000",
                        "turnover24h": "50000000",
                    }
                ]
            }
        }
        with patch.object(scanner_scalp.session, "get_tickers", return_value=mock_resp):
            scanner_scalp.refresh_price_cache()
            assert "BTCUSDT" in scanner_scalp._rest_price_cache
            assert scanner_scalp._rest_price_cache["BTCUSDT"]["price"] == 50000.0
            assert scanner_scalp._rest_price_cache["BTCUSDT"]["change24h"] == 2.5

    def test_handles_api_error(self):
        import scanner_scalp
        with patch.object(scanner_scalp.session, "get_tickers", side_effect=Exception("API down")):
            scanner_scalp.refresh_price_cache()
            assert scanner_scalp._rest_price_cache == {}


# ---------------------------------------------------------------------------
# refresh_news_cache
# ---------------------------------------------------------------------------

class TestRefreshNewsCache:
    def test_skips_if_within_ttl(self):
        from scanner_scalp import _news_cache, refresh_news_cache
        _news_cache["last_fetch"] = time.time()  # just fetched
        original_trending = list(_news_cache["trending"])
        with patch("scanner_scalp._fetch_json") as mock_fetch:
            refresh_news_cache()
            mock_fetch.assert_not_called()

    def test_fetches_when_ttl_expired(self):
        from scanner_scalp import _news_cache, refresh_news_cache
        _news_cache["last_fetch"] = 0
        trending_resp = {"coins": [{"item": {"symbol": "BTC"}}]}
        markets_resp = [{"symbol": "ETH"}]
        fg_resp = {"data": [{"value": "75", "value_classification": "Greed"}]}

        with patch("scanner_scalp._fetch_json", side_effect=[trending_resp, markets_resp, fg_resp]):
            refresh_news_cache()
            assert "BTCUSDT" in _news_cache["trending"]
            assert "ETHUSDT" in _news_cache["top_gainers"]
            assert _news_cache["fear_greed"] == 75
            assert _news_cache["fg_label"] == "Greed"

    def test_handles_none_responses(self):
        from scanner_scalp import _news_cache, refresh_news_cache
        _news_cache["last_fetch"] = 0
        with patch("scanner_scalp._fetch_json", return_value=None):
            refresh_news_cache()
            # Should not crash


# ---------------------------------------------------------------------------
# _fetch_json
# ---------------------------------------------------------------------------

class TestFetchJson:
    def test_returns_parsed_json(self):
        from scanner_scalp import _fetch_json
        with patch("urllib.request.urlopen") as mock_open:
            mock_resp = MagicMock()
            mock_resp.read.return_value = b'{"key": "val"}'
            mock_resp.__enter__ = lambda s: s
            mock_resp.__exit__ = MagicMock(return_value=False)
            mock_open.return_value = mock_resp
            result = _fetch_json("https://example.com")
            assert result == {"key": "val"}

    def test_returns_none_on_error(self):
        from scanner_scalp import _fetch_json
        with patch("urllib.request.urlopen", side_effect=Exception("timeout")):
            result = _fetch_json("https://example.com")
            assert result is None
