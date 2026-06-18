"""Tests for ws_price_feed.py — WebSocket price feed module."""

import json
import threading
from unittest.mock import patch, MagicMock

import pytest


# ---------------------------------------------------------------------------
# get_ws_price / ws_cache_size
# ---------------------------------------------------------------------------

class TestWsCacheOps:
    def setup_method(self):
        """Clear the WS cache before each test."""
        import ws_price_feed
        with ws_price_feed._ws_lock:
            ws_price_feed._ws_cache.clear()

    def test_get_ws_price_returns_zeros_for_unknown_symbol(self):
        from ws_price_feed import get_ws_price
        result = get_ws_price("NONEXISTENT")
        assert result == (0.0, 0.0, 0.0, 0.0, 0.0, 0.0)

    def test_get_ws_price_returns_cached_data(self):
        import ws_price_feed
        with ws_price_feed._ws_lock:
            ws_price_feed._ws_cache["BTCUSDT"] = {
                "price": 65000.0,
                "change24h": 3.2,
                "high24h": 66000.0,
                "low24h": 64000.0,
                "vol24h": 12000.0,
                "turnover24h": 7.8e8,
            }
        result = ws_price_feed.get_ws_price("BTCUSDT")
        assert result[0] == 65000.0
        assert result[1] == 3.2
        assert result[2] == 66000.0

    def test_ws_cache_size_empty(self):
        from ws_price_feed import ws_cache_size
        assert ws_cache_size() == 0

    def test_ws_cache_size_after_insert(self):
        import ws_price_feed
        with ws_price_feed._ws_lock:
            ws_price_feed._ws_cache["BTCUSDT"] = {"price": 1.0}
            ws_price_feed._ws_cache["ETHUSDT"] = {"price": 2.0}
        assert ws_price_feed.ws_cache_size() == 2

    def test_partial_cache_entry_returns_defaults(self):
        import ws_price_feed
        with ws_price_feed._ws_lock:
            ws_price_feed._ws_cache["PARTIAL"] = {"price": 100.0}
        result = ws_price_feed.get_ws_price("PARTIAL")
        assert result[0] == 100.0
        assert result[1] == 0.0  # missing keys default to 0.0


# ---------------------------------------------------------------------------
# BybitWsPriceFeed
# ---------------------------------------------------------------------------

class TestBybitWsPriceFeed:
    def test_init_state(self):
        from ws_price_feed import BybitWsPriceFeed
        feed = BybitWsPriceFeed()
        assert feed._ws is None
        assert feed._running is False
        assert len(feed._symbols) == 0

    def test_start_sets_running(self):
        from ws_price_feed import BybitWsPriceFeed
        feed = BybitWsPriceFeed()
        with patch.object(feed, "_run_loop"):
            feed._running = False
            feed.start(["BTCUSDT"])
            assert feed._running is True
            assert "BTCUSDT" in feed._symbols
            feed.stop()

    def test_stop_clears_running(self):
        from ws_price_feed import BybitWsPriceFeed
        feed = BybitWsPriceFeed()
        feed._running = True
        feed._ws = MagicMock()
        feed.stop()
        assert feed._running is False

    def test_update_symbols(self):
        from ws_price_feed import BybitWsPriceFeed
        feed = BybitWsPriceFeed()
        feed._ws = None
        feed.update_symbols(["BTCUSDT", "ETHUSDT", "SOLUSDT"])
        assert feed._symbols == {"BTCUSDT", "ETHUSDT", "SOLUSDT"}

    def test_update_symbols_with_active_ws(self):
        from ws_price_feed import BybitWsPriceFeed
        feed = BybitWsPriceFeed()
        feed._ws = MagicMock()
        feed._subscribed = {"BTCUSDT"}
        with patch.object(feed, "_subscribe_new") as mock_sub:
            feed.update_symbols(["BTCUSDT", "ETHUSDT"])
            mock_sub.assert_called_once_with(["BTCUSDT", "ETHUSDT"])

    def test_on_message_ticker_update(self):
        import ws_price_feed
        with ws_price_feed._ws_lock:
            ws_price_feed._ws_cache.clear()

        feed = ws_price_feed.BybitWsPriceFeed()
        msg = json.dumps({
            "topic": "tickers.BTCUSDT",
            "data": {
                "symbol": "BTCUSDT",
                "lastPrice": "67500.50",
                "price24hPcnt": "0.032",
                "highPrice24h": "68000.00",
                "lowPrice24h": "66000.00",
                "volume24h": "15000",
                "turnover24h": "1000000000",
            }
        })
        feed._on_message(None, msg)

        result = ws_price_feed.get_ws_price("BTCUSDT")
        assert result[0] == 67500.50
        assert abs(result[1] - 3.2) < 0.01  # 0.032 * 100
        assert result[2] == 68000.0

    def test_on_message_ignores_non_ticker(self):
        import ws_price_feed
        with ws_price_feed._ws_lock:
            ws_price_feed._ws_cache.clear()

        feed = ws_price_feed.BybitWsPriceFeed()
        msg = json.dumps({"topic": "orderbook.BTCUSDT", "data": {}})
        feed._on_message(None, msg)
        assert ws_price_feed.ws_cache_size() == 0

    def test_on_message_subscription_confirmation(self):
        import ws_price_feed
        feed = ws_price_feed.BybitWsPriceFeed()
        msg = json.dumps({"op": "subscribe", "success": True, "ret_msg": "ok"})
        # Should not crash
        feed._on_message(None, msg)

    def test_on_message_malformed_json(self):
        import ws_price_feed
        feed = ws_price_feed.BybitWsPriceFeed()
        # Should not crash
        feed._on_message(None, "not-valid-json{{{")

    def test_on_message_zero_price_not_stored(self):
        import ws_price_feed
        with ws_price_feed._ws_lock:
            ws_price_feed._ws_cache.clear()

        feed = ws_price_feed.BybitWsPriceFeed()
        msg = json.dumps({
            "topic": "tickers.ZEROTEST",
            "data": {
                "symbol": "ZEROTEST",
                "lastPrice": "0",
                "price24hPcnt": "0",
                "highPrice24h": "0",
                "lowPrice24h": "0",
                "volume24h": "0",
                "turnover24h": "0",
            }
        })
        feed._on_message(None, msg)
        assert "ZEROTEST" not in ws_price_feed._ws_cache

    def test_on_message_empty_fields_default(self):
        import ws_price_feed
        with ws_price_feed._ws_lock:
            ws_price_feed._ws_cache.clear()

        feed = ws_price_feed.BybitWsPriceFeed()
        msg = json.dumps({
            "topic": "tickers.TESTUSDT",
            "data": {
                "symbol": "TESTUSDT",
                "lastPrice": "100.0",
                # other fields missing
            }
        })
        feed._on_message(None, msg)
        result = ws_price_feed.get_ws_price("TESTUSDT")
        assert result[0] == 100.0
        assert result[1] == 0.0  # missing change24h

    def test_do_subscribe_batching(self):
        from ws_price_feed import BybitWsPriceFeed
        feed = BybitWsPriceFeed()
        mock_ws = MagicMock()

        symbols = [f"SYM{i}USDT" for i in range(25)]
        feed._do_subscribe(mock_ws, symbols)

        # 25 symbols / batch_size 10 = 3 batches
        assert mock_ws.send.call_count == 3
        assert len(feed._subscribed) == 25

    def test_do_subscribe_empty_list(self):
        from ws_price_feed import BybitWsPriceFeed
        feed = BybitWsPriceFeed()
        mock_ws = MagicMock()
        feed._do_subscribe(mock_ws, [])
        mock_ws.send.assert_not_called()

    def test_subscribe_new_only_subscribes_new_symbols(self):
        from ws_price_feed import BybitWsPriceFeed
        feed = BybitWsPriceFeed()
        feed._subscribed = {"BTCUSDT", "ETHUSDT"}
        feed._ws = MagicMock()

        with patch.object(feed, "_do_subscribe") as mock_do:
            feed._subscribe_new(["BTCUSDT", "ETHUSDT", "SOLUSDT"])
            args = mock_do.call_args[0]
            assert "SOLUSDT" in args[1]
            assert "BTCUSDT" not in args[1]

    def test_on_error_does_not_crash(self):
        from ws_price_feed import BybitWsPriceFeed
        feed = BybitWsPriceFeed()
        feed._on_error(None, "some error")

    def test_on_close_does_not_crash(self):
        from ws_price_feed import BybitWsPriceFeed
        feed = BybitWsPriceFeed()
        feed._on_close(None, 1000, "normal closure")


# ---------------------------------------------------------------------------
# Module-level helper functions
# ---------------------------------------------------------------------------

class TestModuleFunctions:
    def test_start_ws_feed_delegates_to_singleton(self):
        from ws_price_feed import _feed
        with patch.object(_feed, "start") as mock_start:
            from ws_price_feed import start_ws_feed
            start_ws_feed(["BTCUSDT"])
            mock_start.assert_called_once_with(["BTCUSDT"])

    def test_update_ws_symbols_delegates_to_singleton(self):
        from ws_price_feed import _feed
        with patch.object(_feed, "update_symbols") as mock_update:
            from ws_price_feed import update_ws_symbols
            update_ws_symbols(["ETHUSDT"])
            mock_update.assert_called_once_with(["ETHUSDT"])

    def test_stop_ws_feed_delegates_to_singleton(self):
        from ws_price_feed import _feed
        with patch.object(_feed, "stop") as mock_stop:
            from ws_price_feed import stop_ws_feed
            stop_ws_feed()
            mock_stop.assert_called_once()
