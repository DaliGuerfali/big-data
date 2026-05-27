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
