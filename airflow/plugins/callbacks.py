"""
Airflow DAG-level callbacks.

task_failure_alert  — called via default_args["on_failure_callback"]
                      logs a structured failure entry and best-effort
                      publishes an alert to the sat.alerts Kafka topic.
"""

from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timezone


def task_failure_alert(context: dict) -> None:
    """
    Structured failure log + best-effort Kafka alert.
    Kafka errors are swallowed so the callback never re-raises.
    """
    dag_id = context.get("dag").dag_id if context.get("dag") else "unknown"
    task_id = context.get("task_instance").task_id if context.get("task_instance") else "unknown"
    execution_date = str(context.get("execution_date", ""))
    exception = str(context.get("exception", ""))

    alert = {
        "alert_id": str(uuid.uuid4()),
        "alert_type": "AIRFLOW_TASK_FAILURE",
        "severity": "ERROR",
        "detected_at": datetime.now(timezone.utc).isoformat(),
        "details": {
            "dag_id": dag_id,
            "task_id": task_id,
            "execution_date": execution_date,
            "exception": exception[:500],  # truncate for readability
        },
    }

    import logging
    log = logging.getLogger("airflow.task")
    log.error(
        "[task_failure_alert] DAG=%s TASK=%s DATE=%s | %s",
        dag_id, task_id, execution_date, exception,
    )

    try:
        from kafka import KafkaProducer  # installed via _PIP_ADDITIONAL_REQUIREMENTS

        bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
        producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            request_timeout_ms=5000,
            retries=0,
        )
        producer.send("sat.alerts", value=alert)
        producer.flush(timeout=5)
        producer.close()
        log.info("[task_failure_alert] Alert published to sat.alerts")
    except Exception as exc:
        log.warning("[task_failure_alert] Could not publish to Kafka (non-fatal): %s", exc)
