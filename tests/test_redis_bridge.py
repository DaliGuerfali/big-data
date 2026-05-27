"""
Unit tests for the Kafka-to-Redis bridge message handlers.
Uses a simple fake Redis object — no live Redis or Kafka needed.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, call, patch

import pytest

from serving.bridge.kafka_redis_bridge import (
    handle_alert,
    handle_event,
    handle_position,
    key_alert,
    key_alerts_list,
    key_channel_position,
    key_event,
    key_meta,
    key_position,
)


class FakeRedis:
    """Minimal synchronous Redis fake for testing handlers."""

    def __init__(self):
        self._data: dict = {}
        self._sets: dict = {}
        self._lists: dict = {}
        self._published: list = []
        self._hashes: dict = {}

    def setex(self, key, ttl, value):
        self._data[key] = value

    def hset(self, key, mapping=None):
        self._hashes.setdefault(key, {}).update(mapping or {})

    def expire(self, key, ttl):
        pass

    def publish(self, channel, data):
        self._published.append((channel, data))

    def lpush(self, key, value):
        self._lists.setdefault(key, []).insert(0, value)

    def ltrim(self, key, start, end):
        self._lists[key] = self._lists.get(key, [])[start : end + 1]

    def sadd(self, key, value):
        self._sets.setdefault(key, set()).add(value)

    def pipeline(self):
        return _FakePipeline(self)


class _FakePipeline:
    def __init__(self, r: FakeRedis):
        self._r = r
        self._cmds = []

    def lpush(self, key, value):
        self._cmds.append(("lpush", key, value))
        return self

    def ltrim(self, key, start, end):
        self._cmds.append(("ltrim", key, start, end))
        return self

    def execute(self):
        for cmd, *args in self._cmds:
            getattr(self._r, cmd)(*args)


# ─── handle_position ──────────────────────────────────────────────────────────

class TestHandlePosition:
    def _data(self, **kw):
        return {
            "satellite_id": 25544,
            "satellite_name": "ISS",
            "orbit": {"type": "LEO"},
            **kw,
        }

    def test_sets_position_key(self):
        r = FakeRedis()
        data = self._data()
        handle_position(r, data)
        assert key_position(25544) in r._data

    def test_stored_value_is_json(self):
        r = FakeRedis()
        handle_position(r, self._data())
        stored = json.loads(r._data[key_position(25544)])
        assert stored["satellite_id"] == 25544

    def test_publishes_to_channel(self):
        r = FakeRedis()
        handle_position(r, self._data())
        channels = [ch for ch, _ in r._published]
        assert key_channel_position(25544) in channels

    def test_updates_meta_hash(self):
        r = FakeRedis()
        handle_position(r, self._data())
        assert key_meta(25544) in r._hashes
        assert r._hashes[key_meta(25544)]["name"] == "ISS"
        assert r._hashes[key_meta(25544)]["orbit_type"] == "LEO"

    def test_missing_satellite_id_skipped(self):
        r = FakeRedis()
        handle_position(r, {"latitude": 0.0, "longitude": 0.0})
        assert not r._data
        assert not r._published


# ─── handle_alert ─────────────────────────────────────────────────────────────

class TestHandleAlert:
    def _data(self, **kw):
        return {
            "alert_id": "abc-123",
            "alert_type": "VELOCITY_ANOMALY",
            "satellite_id": 25544,
            **kw,
        }

    def test_stores_alert_detail(self):
        r = FakeRedis()
        handle_alert(r, self._data())
        assert key_alert("abc-123") in r._data
        stored = json.loads(r._data[key_alert("abc-123")])
        assert stored["alert_type"] == "VELOCITY_ANOMALY"

    def test_prepends_to_satellite_list(self):
        r = FakeRedis()
        handle_alert(r, self._data())
        assert "abc-123" in r._lists.get(key_alerts_list(25544), [])

    def test_missing_alert_id_skipped(self):
        r = FakeRedis()
        handle_alert(r, {"satellite_id": 25544})
        assert not r._data


# ─── handle_event ─────────────────────────────────────────────────────────────

class TestHandleEvent:
    def _data(self, **kw):
        return {
            "event_id": "2024-01-15T08:00:00-CME-001",
            "event_type": "CME",
            **kw,
        }

    def test_stores_event_detail(self):
        r = FakeRedis()
        handle_event(r, self._data())
        assert key_event("2024-01-15T08:00:00-CME-001") in r._data

    def test_adds_to_active_set(self):
        r = FakeRedis()
        handle_event(r, self._data())
        assert "2024-01-15T08:00:00-CME-001" in r._sets.get("events:active", set())

    def test_missing_event_id_skipped(self):
        r = FakeRedis()
        handle_event(r, {"event_type": "FLR"})
        assert not r._data
