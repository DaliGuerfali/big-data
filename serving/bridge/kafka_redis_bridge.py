"""
Kafka-to-Redis Bridge

Consumes three Kafka topics and keeps Redis hot with the latest serving data:

  sat.position.enriched  → sat:position:{id}  (60s TTL)
                           sat:meta:{id}       (1h TTL, Hash)
                           channel:position:{id} (pub/sub for WebSockets)

  sat.alerts             → alert:{alert_id}   (24h TTL)
                           sat:alerts:{id}     (List, 100 max)

  sat.events.raw         → event:{event_id}   (72h TTL)
                           events:active       (Set — cleared+rebuilt each run)

Run with: python -m serving.bridge.kafka_redis_bridge
"""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Any

# redis and kafka-python are lazy-imported inside run_bridge()/create_consumer()
# so the handler functions can be imported and unit-tested without those packages.

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger("bridge")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")

POSITION_TTL = 60       # seconds
ALERT_TTL = 86_400      # 24 hours
EVENT_TTL = 259_200     # 72 hours
META_TTL = 3_600        # 1 hour
ALERT_LIST_MAX = 100

TOPICS = [
    "sat.position.enriched",
    "sat.alerts",
    "sat.events.raw",
]


# ─── Redis key helpers ────────────────────────────────────────────────────────

def key_position(sat_id: int | str) -> str:
    return f"sat:position:{sat_id}"

def key_meta(sat_id: int | str) -> str:
    return f"sat:meta:{sat_id}"

def key_alert(alert_id: str) -> str:
    return f"alert:{alert_id}"

def key_alerts_list(sat_id: int | str) -> str:
    return f"sat:alerts:{sat_id}"

def key_event(event_id: str) -> str:
    return f"event:{event_id}"

def key_channel_position(sat_id: int | str) -> str:
    return f"channel:position:{sat_id}"

EVENTS_ACTIVE = "events:active"


# ─── message handlers ─────────────────────────────────────────────────────────

def handle_position(r: redis.Redis, data: dict[str, Any]) -> None:
    sat_id = data.get("satellite_id") or data.get("satelliteId")
    if sat_id is None:
        log.warning("[position] missing satellite_id — skipping")
        return

    payload = json.dumps(data)

    # Latest position with TTL
    r.setex(key_position(sat_id), POSITION_TTL, payload)

    # Satellite metadata cache (Hash) — updated from position if fields present
    meta_fields: dict[str, str] = {}
    if name := (data.get("satellite_name") or data.get("satelliteName")):
        meta_fields["name"] = name
    meta_fields["norad_id"] = str(sat_id)
    if orbit := (data.get("orbit") or {}).get("type"):
        meta_fields["orbit_type"] = orbit
    if tle_epoch := data.get("tle_epoch"):
        meta_fields["last_tle_epoch"] = tle_epoch
    meta_fields["last_seen"] = datetime.now(timezone.utc).isoformat()

    if meta_fields:
        r.hset(key_meta(sat_id), mapping=meta_fields)
        r.expire(key_meta(sat_id), META_TTL)

    # Flat hash for Grafana (HGETALL displays as clean key/value table)
    pos = data.get("position") or {}
    geo = data.get("geo") or {}
    orbit = data.get("orbit") or {}
    lighting = data.get("lighting") or {}
    flat = {
        "satellite_name":  str(data.get("satellite_name") or sat_id),
        "latitude":        str(pos.get("latitude", "")),
        "longitude":       str(pos.get("longitude", "")),
        "altitude_km":     str(pos.get("altitude_km", "")),
        "orbit_type":      str(orbit.get("type", "")),
        "velocity_km_s":   str(orbit.get("velocity_km_s", "")),
        "period_minutes":  str(orbit.get("period_minutes", "")),
        "country":         str(geo.get("country_name", "")),
        "region":          str(geo.get("region", "")),
        "over_ocean":      str(geo.get("over_ocean", "")),
        "in_sunlight":     str(lighting.get("in_sunlight", "")),
        "source":          str(data.get("source", "")),
        "last_updated":    datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
    }
    r.hset(f"sat:pos:flat:{sat_id}", mapping=flat)
    r.expire(f"sat:pos:flat:{sat_id}", POSITION_TTL)

    # Pub/sub for live WebSocket subscribers
    r.publish(key_channel_position(sat_id), payload)

    log.debug("[position] sat=%s updated", sat_id)


def handle_alert(r: redis.Redis, data: dict[str, Any]) -> None:
    alert_id = data.get("alert_id")
    sat_id = data.get("satellite_id")

    if not alert_id:
        log.warning("[alert] missing alert_id — skipping")
        return

    payload = json.dumps(data)

    # Store alert details
    r.setex(key_alert(alert_id), ALERT_TTL, payload)

    if sat_id is not None:
        pipe = r.pipeline()
        pipe.lpush(key_alerts_list(sat_id), alert_id)
        pipe.ltrim(key_alerts_list(sat_id), 0, ALERT_LIST_MAX - 1)
        pipe.execute()

    # Global recent-alerts list with full JSON (for Grafana)
    pipe2 = r.pipeline()
    pipe2.lpush("alerts:recent", payload)
    pipe2.ltrim("alerts:recent", 0, 49)
    pipe2.execute()

    log.info("[alert] type=%s sat=%s id=%s", data.get("alert_type"), sat_id, alert_id)


def handle_event(r: redis.Redis, data: dict[str, Any]) -> None:
    event_id = data.get("event_id")
    event_type = data.get("event_type", "UNKNOWN")

    if not event_id:
        log.warning("[event] missing event_id — skipping")
        return

    payload = json.dumps(data)

    r.setex(key_event(event_id), EVENT_TTL, payload)
    r.sadd(EVENTS_ACTIVE, event_id)

    pipe = r.pipeline()
    pipe.lpush("events:recent", payload)
    pipe.ltrim("events:recent", 0, 49)
    pipe.execute()

    log.info("[event] type=%s id=%s", event_type, event_id)


HANDLERS = {
    "sat.position.enriched": handle_position,
    "sat.alerts": handle_alert,
    "sat.events.raw": handle_event,
}


# ─── main loop ────────────────────────────────────────────────────────────────

def create_consumer(retries: int = 10, wait: int = 5):
    from kafka import KafkaConsumer
    from kafka.errors import NoBrokersAvailable

    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                *TOPICS,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                group_id="redis-bridge",
                auto_offset_reset="latest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                consumer_timeout_ms=1_000,
            )
            log.info("Kafka consumer connected to %s", KAFKA_BOOTSTRAP)
            return consumer
        except NoBrokersAvailable:
            log.warning("Kafka not ready (attempt %d/%d), retrying in %ds…", attempt, retries, wait)
            time.sleep(wait)
    raise RuntimeError(f"Could not connect to Kafka at {KAFKA_BOOTSTRAP} after {retries} attempts")


def _backfill_events_recent(r: Any) -> None:
    """Populate events:recent from existing event:* keys if the list is empty."""
    if r.exists("events:recent"):
        return
    event_keys = r.keys("event:*")
    if not event_keys:
        return
    pipe = r.pipeline()
    for key in event_keys[:50]:
        val = r.get(key)
        if val:
            pipe.lpush("events:recent", val)
    pipe.ltrim("events:recent", 0, 49)
    pipe.execute()
    log.info("Backfilled events:recent with %d events", min(len(event_keys), 50))


def run_bridge() -> None:
    import redis
    r = redis.from_url(REDIS_URL, decode_responses=False)
    r.ping()
    log.info("Redis connected at %s", REDIS_URL)
    _backfill_events_recent(r)

    consumer = create_consumer()

    shutdown = False

    def _shutdown(sig, frame):
        nonlocal shutdown
        log.info("Shutdown signal received")
        shutdown = True

    signal.signal(signal.SIGTERM, _shutdown)
    signal.signal(signal.SIGINT, _shutdown)

    log.info("Bridge running — subscribed to: %s", TOPICS)
    processed = errors = 0

    while not shutdown:
        try:
            for message in consumer:
                if shutdown:
                    break
                handler = HANDLERS.get(message.topic)
                if handler is None:
                    continue
                try:
                    handler(r, message.value)
                    processed += 1
                except Exception as exc:
                    errors += 1
                    log.exception("[%s] Error processing message: %s", message.topic, exc)

        except Exception as exc:
            log.exception("Consumer loop error: %s", exc)
            time.sleep(2)

    log.info("Bridge stopped. Processed: %d  Errors: %d", processed, errors)
    consumer.close()
    r.close()


if __name__ == "__main__":
    run_bridge()
