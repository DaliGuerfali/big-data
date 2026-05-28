"""
Pure utility functions shared across DAGs.
No Airflow imports — safe to test without an Airflow installation.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone


def get_week_boundaries(reference_date: str, **_context) -> dict:
    """
    Compute ISO week number and Monday–Sunday date range for *reference_date*.

    Returns:
        {
          "week_number": "2024-03",
          "start":       "2024-01-15",   # Monday
          "end":         "2024-01-21",   # Sunday
          "year":        "2024",
          "week":        "03",
        }
    """
    ref = datetime.strptime(reference_date, "%Y-%m-%d")
    # isocalendar() returns a plain tuple in Python 3.8, named tuple only in 3.9+
    year, week, weekday = ref.isocalendar()

    monday = ref - timedelta(days=weekday - 1)
    sunday = monday + timedelta(days=6)

    result = {
        "week_number": f"{year}-{week:02d}",
        "start": monday.strftime("%Y-%m-%d"),
        "end": sunday.strftime("%Y-%m-%d"),
        "year": str(year),
        "week": f"{week:02d}",
    }
    print(f"[week_boundaries] {result}")
    return result


def cache_batch_results(redis_key: str, api_path: str, ttl: int = 604_800, **context) -> None:
    """
    Cache batch results in Redis.  Strategy:
    1. If the key already exists (written by the Spark job itself) — done.
    2. Otherwise read JSON lines from HDFS via Docker exec on the namenode.
    Non-fatal: logs a warning and returns on any error.
    """
    import os
    import json as _json

    redis_url = os.getenv("REDIS_URL", "redis://redis:6379")

    # ── 1. Skip if both main key AND list key already populated ───────────────
    list_key = redis_key.replace(":latest", ":list")
    try:
        import redis as redis_lib
        r = redis_lib.from_url(redis_url, decode_responses=False)
        main_exists = r.exists(redis_key)
        list_exists = r.exists(list_key)
        r.close()
        if main_exists and list_exists:
            print(f"[cache_batch] {redis_key} and {list_key} already in Redis — skipping")
            return
    except Exception as exc:
        print(f"[cache_batch] WARNING: Redis check failed: {exc}")

    # ── 2. Derive HDFS path from api_path ─────────────────────────────────────
    if "/drift/" in api_path:
        segment = api_path.split("/drift/")[-1]
        hdfs_path = f"/satellite/reports/drift/week={segment}"
    elif "/daily/" in api_path:
        segment = api_path.split("/daily/")[-1]
        hdfs_path = f"/satellite/aggregated/daily/date={segment}"
    else:
        print(f"[cache_batch] WARNING: unknown api_path pattern: {api_path}")
        return

    # ── 3. Read JSON lines from HDFS via namenode container ───────────────────
    try:
        import docker
        client = docker.from_env()
        namenode = client.containers.get("namenode")
        exit_code, output = namenode.exec_run(
            ["bash", "-c", f"hdfs dfs -cat '{hdfs_path}/*' 2>/dev/null"],
        )
        text = (output or b"").decode("utf-8", errors="replace").strip()
        records = []
        for line in text.splitlines():
            line = line.strip()
            if line and not line.startswith("WARNING"):
                try:
                    records.append(_json.loads(line))
                except _json.JSONDecodeError:
                    pass
        if not records:
            print(f"[cache_batch] WARNING: no JSON records in {hdfs_path}")
            return
        payload = _json.dumps(records).encode("utf-8")
    except Exception as exc:
        print(f"[cache_batch] WARNING: HDFS read failed: {exc}")
        return

    # ── 4. Store in Redis ─────────────────────────────────────────────────────
    try:
        import redis as redis_lib
        r = redis_lib.from_url(redis_url, decode_responses=False)
        r.setex(redis_key, ttl, payload)

        # Also write per-record list for Grafana LRANGE + extractFields
        r.delete(list_key)
        pipe = r.pipeline()
        for rec in records:
            pipe.rpush(list_key, _json.dumps(rec, default=str))
        pipe.expire(list_key, ttl)
        pipe.execute()
        r.close()
        print(f"[cache_batch] Cached {redis_key} and {list_key} ({len(records)} records, ttl={ttl}s)")
    except Exception as exc:
        print(f"[cache_batch] WARNING: Redis write failed: {exc}")


def publish_kafka_trigger(job_type: str, date: str | None = None, **context) -> None:
    """Write a completion message to sat.batch.trigger (non-fatal on errors)."""
    try:
        from kafka import KafkaProducer

        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=10_000,
        )
        msg = {
            "job_type": job_type,
            "date": date or context.get("ds", ""),
            "triggered_at": datetime.now(timezone.utc).isoformat(),
            "triggered_by": "airflow",
        }
        producer.send("sat.batch.trigger", value=msg)
        producer.flush()
        producer.close()
        print(f"[publish_trigger] Sent trigger for {job_type} date={msg['date']}")
    except Exception as exc:
        print(f"[publish_trigger] WARNING: could not publish trigger: {exc}")
