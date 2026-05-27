"""
Unit tests for the FastAPI serving layer.
Uses FastAPI's TestClient (via httpx) and mocks the Redis client so no
live Redis is needed.
"""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Guard: skip if FastAPI/httpx aren't installed
pytest.importorskip("fastapi")
pytest.importorskip("httpx")

from fastapi.testclient import TestClient

import serving.api.main as api_module
from serving.api.main import app


# ─── helpers ──────────────────────────────────────────────────────────────────

def _make_redis_mock(position_data=None, meta_data=None, alert_ids=None,
                     alert_data=None, event_ids=None, event_data=None):
    """Return an async-compatible Redis mock with pre-loaded data."""
    r = AsyncMock()

    position_raw = json.dumps(position_data) if position_data else None
    r.get = AsyncMock(side_effect=lambda key: _get_side_effect(
        key, position_data, position_raw, alert_data, event_data
    ))
    r.keys = AsyncMock(return_value=[f"sat:meta:{k}" for k in (meta_data or {}).keys()])
    r.hgetall = AsyncMock(return_value=list(meta_data.values())[0] if meta_data else {})
    r.lrange = AsyncMock(return_value=alert_ids or [])
    r.smembers = AsyncMock(return_value=set(event_ids or []))
    r.ping = AsyncMock(return_value=True)
    return r


def _get_side_effect(key, position_data, position_raw, alert_data, event_data):
    if key.startswith("sat:position:") and position_data:
        return position_raw
    if key.startswith("alert:") and alert_data:
        aid = key.split(":", 1)[1]
        return json.dumps(alert_data.get(aid))
    if key.startswith("event:") and event_data:
        eid = key.split(":", 1)[1]
        return json.dumps(event_data.get(eid))
    return None


@pytest.fixture
def client():
    return TestClient(app, raise_server_exceptions=True)


# ─── /health ──────────────────────────────────────────────────────────────────

class TestHealth:
    def test_returns_ok_when_redis_reachable(self, client):
        mock_redis = _make_redis_mock()
        with patch.object(api_module, "_redis", mock_redis):
            resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"


# ─── /api/satellites/{id}/position ───────────────────────────────────────────

class TestGetPosition:
    def test_returns_position_when_present(self, client):
        pos = {"satellite_id": 25544, "satellite_name": "ISS", "altitude_km": 420.0}
        mock_redis = _make_redis_mock(position_data=pos)
        with patch.object(api_module, "_redis", mock_redis):
            resp = client.get("/api/satellites/25544/position")
        assert resp.status_code == 200
        assert resp.json()["satellite_id"] == 25544

    def test_returns_404_when_not_present(self, client):
        mock_redis = _make_redis_mock()
        with patch.object(api_module, "_redis", mock_redis):
            resp = client.get("/api/satellites/99999/position")
        assert resp.status_code == 404


# ─── /api/satellites/{id}/alerts ─────────────────────────────────────────────

class TestGetAlerts:
    def test_returns_alerts(self, client):
        alert_ids = ["alert-1", "alert-2"]
        alert_data = {
            "alert-1": {"alert_id": "alert-1", "alert_type": "VELOCITY_ANOMALY"},
            "alert-2": {"alert_id": "alert-2", "alert_type": "ALTITUDE_ANOMALY"},
        }
        mock_redis = _make_redis_mock(alert_ids=alert_ids, alert_data=alert_data)
        with patch.object(api_module, "_redis", mock_redis):
            resp = client.get("/api/satellites/25544/alerts")
        assert resp.status_code == 200
        body = resp.json()
        assert len(body) == 2
        assert body[0]["alert_type"] == "VELOCITY_ANOMALY"

    def test_empty_when_no_alerts(self, client):
        mock_redis = _make_redis_mock()
        with patch.object(api_module, "_redis", mock_redis):
            resp = client.get("/api/satellites/25544/alerts")
        assert resp.status_code == 200
        assert resp.json() == []

    def test_limit_query_param(self, client):
        mock_redis = _make_redis_mock()
        with patch.object(api_module, "_redis", mock_redis):
            resp = client.get("/api/satellites/25544/alerts?limit=5")
        assert resp.status_code == 200
        # Verify lrange was called with correct range
        mock_redis.lrange.assert_called_once_with("sat:alerts:25544", 0, 4)

    def test_limit_out_of_range_rejected(self, client):
        mock_redis = _make_redis_mock()
        with patch.object(api_module, "_redis", mock_redis):
            resp = client.get("/api/satellites/25544/alerts?limit=500")
        assert resp.status_code == 422  # FastAPI validation error


# ─── /api/events/active ───────────────────────────────────────────────────────

class TestGetActiveEvents:
    def test_returns_events(self, client):
        event_ids = ["evt-cme-1"]
        event_data = {"evt-cme-1": {"event_id": "evt-cme-1", "event_type": "CME",
                                     "start_time": "2024-01-15T08:00:00Z"}}
        mock_redis = _make_redis_mock(event_ids=event_ids, event_data=event_data)
        with patch.object(api_module, "_redis", mock_redis):
            resp = client.get("/api/events/active")
        assert resp.status_code == 200
        body = resp.json()
        assert len(body) == 1
        assert body[0]["event_type"] == "CME"


# ─── /api/reports validation ──────────────────────────────────────────────────

class TestReportValidation:
    def test_bad_date_format_rejected(self, client):
        mock_redis = _make_redis_mock()
        with patch.object(api_module, "_redis", mock_redis):
            resp = client.get("/api/reports/daily/not-a-date")
        assert resp.status_code == 400

    def test_valid_date_accepted(self, client):
        mock_redis = _make_redis_mock()
        with patch.object(api_module, "_redis", mock_redis), \
             patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stdout="", stderr="")
            resp = client.get("/api/reports/daily/2024-01-15")
        # Path doesn't exist in test, expect empty/message response but not 400/422
        assert resp.status_code == 200
