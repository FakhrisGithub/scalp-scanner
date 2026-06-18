"""Tests for app.py — Flask web application routes and state management."""

import json
import time
import threading
from unittest.mock import patch, MagicMock

import pytest


# ---------------------------------------------------------------------------
# Fixtures — import the Flask app with WS/scanner side-effects suppressed
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def _suppress_startup_threads(monkeypatch):
    """
    Prevent background threads (WS init, price updater, auto-scan) from
    actually running during tests. We patch threading.Thread.start globally
    for the app module's import-time threads.
    """
    pass  # Threads are daemon and won't block pytest


@pytest.fixture()
def client():
    """Provide a Flask test client with a clean state."""
    # Patch external deps before importing app
    with patch("ws_price_feed.start_ws_feed"), \
         patch("ws_price_feed.get_ws_price", return_value=(0.0, 0.0, 0.0, 0.0, 0.0, 0.0)), \
         patch("ws_price_feed.ws_cache_size", return_value=0):
        import app as app_mod
        app_mod.app.config["TESTING"] = True

        # Reset state
        with app_mod._lock:
            app_mod._state.update({
                "rows": [],
                "scanning": False,
                "last_scan": None,
                "progress": (0, 0),
                "market_ctx": "",
                "error": None,
                "scan_num": 0,
                "price_ts": None,
                "ws_count": 0,
            })
        with app_mod.app.test_client() as c:
            yield c, app_mod


# ---------------------------------------------------------------------------
# Route: GET /
# ---------------------------------------------------------------------------

class TestIndexRoute:
    def test_returns_200(self, client):
        c, _ = client
        resp = c.get("/")
        assert resp.status_code == 200

    def test_returns_html(self, client):
        c, _ = client
        resp = c.get("/")
        assert b"SCALP SCANNER" in resp.data

    def test_contains_scan_button(self, client):
        c, _ = client
        resp = c.get("/")
        assert b"triggerScan" in resp.data


# ---------------------------------------------------------------------------
# Route: GET /api/status
# ---------------------------------------------------------------------------

class TestApiStatus:
    def test_returns_json(self, client):
        c, _ = client
        resp = c.get("/api/status")
        assert resp.status_code == 200
        data = json.loads(resp.data)
        assert "scanning" in data
        assert "rows" in data
        assert "progress" in data

    def test_default_state(self, client):
        c, _ = client
        resp = c.get("/api/status")
        data = json.loads(resp.data)
        assert data["scanning"] is False
        assert data["rows"] == []
        assert data["scan_num"] == 0
        assert data["error"] is None

    def test_reflects_state_changes(self, client):
        c, app_mod = client
        with app_mod._lock:
            app_mod._state["scanning"] = True
            app_mod._state["progress"] = (5, 100)
            app_mod._state["scan_num"] = 3
            app_mod._state["market_ctx"] = "F&G: 60 (Greed)"
            app_mod._state["ws_count"] = 150

        resp = c.get("/api/status")
        data = json.loads(resp.data)
        assert data["scanning"] is True
        assert data["progress"] == [5, 100]
        assert data["scan_num"] == 3
        assert data["market_ctx"] == "F&G: 60 (Greed)"
        assert data["ws_count"] == 150

    def test_rows_are_serialized(self, client):
        c, app_mod = client
        sample_row = tuple(range(35))
        with app_mod._lock:
            app_mod._state["rows"] = [sample_row]

        resp = c.get("/api/status")
        data = json.loads(resp.data)
        assert len(data["rows"]) == 1
        assert data["rows"][0] == list(range(35))


# ---------------------------------------------------------------------------
# Route: POST /api/scan
# ---------------------------------------------------------------------------

class TestApiScan:
    def test_triggers_scan(self, client):
        c, app_mod = client
        with patch.object(app_mod, "trigger_scan", return_value=True) as mock_trigger:
            resp = c.post("/api/scan")
            data = json.loads(resp.data)
            assert data["ok"] is True
            assert data["msg"] == "scanning"

    def test_already_scanning(self, client):
        c, app_mod = client
        with app_mod._lock:
            app_mod._state["scanning"] = True
        resp = c.post("/api/scan")
        data = json.loads(resp.data)
        assert data["ok"] is False
        assert data["msg"] == "already scanning"


# ---------------------------------------------------------------------------
# trigger_scan
# ---------------------------------------------------------------------------

class TestTriggerScan:
    def test_returns_false_when_scanning(self, client):
        _, app_mod = client
        with app_mod._lock:
            app_mod._state["scanning"] = True
        result = app_mod.trigger_scan()
        assert result is False

    def test_returns_true_when_idle(self, client):
        _, app_mod = client
        with app_mod._lock:
            app_mod._state["scanning"] = False
        with patch("app._do_scan"):
            result = app_mod.trigger_scan()
            assert result is True


# ---------------------------------------------------------------------------
# _do_scan (integration-ish, with mocked scanner)
# ---------------------------------------------------------------------------

class TestDoScan:
    def test_successful_scan_updates_state(self, client):
        _, app_mod = client
        fake_rows = [
            tuple([f"SYM{i}USDT"] + [0] * 4 + [70 + i] + ["LONG"] + [0] * 28)
            for i in range(5)
        ]
        with patch("scanner_scalp.scan_scalp", return_value=fake_rows), \
             patch("scanner_scalp.get_market_context_str", return_value="F&G: 50"):
            app_mod._do_scan()

        with app_mod._lock:
            assert app_mod._state["scanning"] is False
            assert len(app_mod._state["rows"]) > 0
            assert app_mod._state["market_ctx"] == "F&G: 50"
            assert app_mod._state["last_scan"] is not None

    def test_scan_filters_below_62(self, client):
        _, app_mod = client
        fake_rows = [
            tuple(["LOW"] + [0] * 4 + [50] + ["FLAT"] + [0] * 28),
            tuple(["HIGH"] + [0] * 4 + [80] + ["LONG"] + [0] * 28),
        ]
        with patch("scanner_scalp.scan_scalp", return_value=fake_rows), \
             patch("scanner_scalp.get_market_context_str", return_value=""):
            app_mod._do_scan()

        with app_mod._lock:
            symbols = [r[0] for r in app_mod._state["rows"]]
            assert "LOW" not in symbols
            assert "HIGH" in symbols

    def test_scan_limits_to_50_rows(self, client):
        _, app_mod = client
        fake_rows = [
            tuple([f"SYM{i}"] + [0] * 4 + [90] + ["LONG"] + [0] * 28)
            for i in range(100)
        ]
        with patch("scanner_scalp.scan_scalp", return_value=fake_rows), \
             patch("scanner_scalp.get_market_context_str", return_value=""):
            app_mod._do_scan()

        with app_mod._lock:
            assert len(app_mod._state["rows"]) <= 50

    def test_scan_error_sets_error_state(self, client):
        _, app_mod = client
        with patch("scanner_scalp.scan_scalp", side_effect=Exception("API Error")):
            app_mod._do_scan()

        with app_mod._lock:
            assert app_mod._state["scanning"] is False
            assert app_mod._state["error"] == "API Error"

    def test_progress_callback_updates(self, client):
        _, app_mod = client
        captured_progress = []

        def fake_scan(progress_callback=None):
            if progress_callback:
                progress_callback(5, 100)
                captured_progress.append(True)
            return []

        with patch("scanner_scalp.scan_scalp", side_effect=fake_scan), \
             patch("scanner_scalp.get_market_context_str", return_value=""):
            app_mod._do_scan()

        assert len(captured_progress) == 1
