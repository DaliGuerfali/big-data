"""
Satellite Tracker — FastAPI Serving Layer

Endpoints
---------
WebSocket
  WS  /ws/position/{satellite_id}   — live position stream (Redis pub/sub)

REST
  GET /api/satellites                — list tracked satellites + live status
  GET /api/satellites/{id}/position  — latest position
  GET /api/satellites/{id}/alerts    — recent alerts (default 10)
  GET /api/events/active             — active space weather events
  GET /api/reports/daily/{date}      — daily aggregation from HDFS (JSON)
  GET /api/reports/drift/{week}      — weekly TLE drift report from HDFS (JSON)
  GET /health                        — liveness probe

Run with:
  uvicorn serving.api.main:app --host 0.0.0.0 --port 8084 --reload
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
from typing import Any

from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

log = logging.getLogger("api")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s — %(message)s")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
HDFS_NAMENODE = os.getenv("HDFS_NAMENODE_URL", "hdfs://namenode:8020")

app = FastAPI(
    title="Satellite Tracker API",
    description="Real-time satellite tracking and space weather serving layer",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Redis client (module-level, lazily initialised on first request) ─────────

_redis: aioredis.Redis | None = None


async def get_redis():
    global _redis
    if _redis is None:
        import redis.asyncio as aioredis
        _redis = aioredis.from_url(REDIS_URL, decode_responses=True)
    return _redis


# ─── WebSocket connection manager ─────────────────────────────────────────────

class ConnectionManager:
    def __init__(self):
        self._connections: dict[str, list[WebSocket]] = {}

    async def connect(self, ws: WebSocket, satellite_id: str) -> None:
        await ws.accept()
        self._connections.setdefault(satellite_id, []).append(ws)
        log.info("[ws] client connected for sat=%s (total=%d)", satellite_id,
                 len(self._connections[satellite_id]))

    def disconnect(self, ws: WebSocket, satellite_id: str) -> None:
        bucket = self._connections.get(satellite_id, [])
        if ws in bucket:
            bucket.remove(ws)
        log.info("[ws] client disconnected sat=%s", satellite_id)


manager = ConnectionManager()


# ─── WebSocket endpoint ───────────────────────────────────────────────────────

@app.websocket("/ws/position/{satellite_id}")
async def websocket_position(websocket: WebSocket, satellite_id: str) -> None:
    """
    Stream live enriched position updates for a satellite.
    Messages are pushed whenever the Kafka→Redis bridge publishes a new position.
    """
    await manager.connect(websocket, satellite_id)
    r = await get_redis()
    pubsub = r.pubsub()
    await pubsub.subscribe(f"channel:position:{satellite_id}")

    try:
        async for message in pubsub.listen():
            if message["type"] == "message":
                try:
                    data = json.loads(message["data"])
                    await websocket.send_json(data)
                except Exception as exc:
                    log.warning("[ws] failed to send to client: %s", exc)
    except WebSocketDisconnect:
        pass
    finally:
        await pubsub.unsubscribe(f"channel:position:{satellite_id}")
        manager.disconnect(websocket, satellite_id)


# ─── REST endpoints ───────────────────────────────────────────────────────────

@app.get("/health")
async def health() -> dict:
    """Liveness probe."""
    try:
        r = await get_redis()
        await r.ping()
        redis_ok = True
    except Exception:
        redis_ok = False
    return {"status": "ok", "redis": redis_ok}


@app.get("/api/satellites", summary="List tracked satellites")
async def list_satellites() -> list[dict]:
    """Return all satellites in the metadata cache with their live status."""
    r = await get_redis()
    keys = await r.keys("sat:meta:*")
    result = []
    for key in keys:
        sat_id = key.split(":")[-1]
        meta = await r.hgetall(key)
        position_raw = await r.get(f"sat:position:{sat_id}")
        result.append({
            "id": sat_id,
            "meta": meta,
            "has_live_position": position_raw is not None,
            "last_position": json.loads(position_raw) if position_raw else None,
        })
    return result


@app.get("/api/satellites/{satellite_id}/position", summary="Latest satellite position")
async def get_position(satellite_id: str) -> dict:
    """Return the most-recent enriched position for a satellite (max 60s old)."""
    r = await get_redis()
    raw = await r.get(f"sat:position:{satellite_id}")
    if raw is None:
        raise HTTPException(
            status_code=404,
            detail=f"No live position for satellite {satellite_id}. "
                   "Data may be older than 60s or the satellite is not being tracked.",
        )
    return json.loads(raw)


@app.get("/api/satellites/{satellite_id}/alerts", summary="Recent alerts for a satellite")
async def get_alerts(
    satellite_id: str,
    limit: int = Query(default=10, ge=1, le=100),
) -> list[dict]:
    """Return up to *limit* recent anomaly alerts for a satellite."""
    r = await get_redis()
    alert_ids = await r.lrange(f"sat:alerts:{satellite_id}", 0, limit - 1)
    alerts = []
    for alert_id in alert_ids:
        raw = await r.get(f"alert:{alert_id}")
        if raw:
            alerts.append(json.loads(raw))
    return alerts


@app.get("/api/events/active", summary="Active space weather events")
async def get_active_events() -> list[dict]:
    """Return all currently active space weather events (CME, FLR, GST, etc.)."""
    r = await get_redis()
    event_ids = await r.smembers("events:active")
    events = []
    for event_id in event_ids:
        raw = await r.get(f"event:{event_id}")
        if raw:
            events.append(json.loads(raw))
    return sorted(events, key=lambda e: e.get("start_time", ""), reverse=True)


@app.get("/api/reports/daily/{date}", summary="Daily aggregation report")
async def get_daily_report(date: str) -> Any:
    """
    Return the daily satellite statistics for *date* (format: YYYY-MM-DD).
    Reads JSON lines from HDFS /satellite/aggregated/daily/date={date} by
    executing `hdfs dfs -cat` inside the namenode container via subprocess.
    """
    _validate_date(date)
    path = f"{HDFS_NAMENODE}/satellite/aggregated/daily/date={date}"
    return _read_hdfs_json(path, label=f"daily report for {date}")


@app.get("/api/reports/drift/{week}", summary="Weekly TLE drift report")
async def get_drift_report(week: str) -> Any:
    """
    Return the weekly TLE drift analysis for *week* (format: YYYY-WW, e.g. 2024-03).
    Reads JSON lines from HDFS /satellite/reports/drift/week={week}.
    """
    path = f"{HDFS_NAMENODE}/satellite/reports/drift/week={week}"
    return _read_hdfs_json(path, label=f"drift report for week {week}")


# ─── HDFS helpers ─────────────────────────────────────────────────────────────

def _validate_date(date: str) -> None:
    import re
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
        raise HTTPException(status_code=400, detail="date must be YYYY-MM-DD")


def _read_hdfs_json(hdfs_path: str, label: str) -> list[dict]:
    """
    Cat all part files under *hdfs_path* and parse as JSON lines.
    Runs `hdfs dfs -cat <path>/*.json` (also tries `part-*.json` and `*.orc` hint).
    Falls back to an empty list with a message if the path doesn't exist.
    """
    # Try JSON lines first, then plain cat of the directory
    for glob in [f"{hdfs_path}/part-*.json", f"{hdfs_path}/*.json", f"{hdfs_path}/*"]:
        try:
            result = subprocess.run(
                ["hdfs", "dfs", "-cat", glob],
                capture_output=True, text=True, timeout=30,
            )
            if result.returncode == 0 and result.stdout.strip():
                records = []
                for line in result.stdout.strip().splitlines():
                    line = line.strip()
                    if line:
                        try:
                            records.append(json.loads(line))
                        except json.JSONDecodeError:
                            pass  # skip non-JSON lines (ORC binary etc.)
                if records:
                    return records
        except (subprocess.TimeoutExpired, FileNotFoundError):
            break

    # hdfs not on PATH inside API container — return empty with info message
    return {"message": f"No data found for {label}", "hdfs_path": hdfs_path}
