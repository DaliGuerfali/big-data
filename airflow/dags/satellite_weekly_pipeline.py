"""
Airflow DAG: Weekly TLE Drift Analysis Pipeline

Schedule: 04:00 UTC every Sunday
Tasks:
  1. calculate_week_range    — compute ISO week boundaries and week number
  2. tle_drift_mapreduce     — run Hadoop Streaming job (tle_drift_mapper/reducer)
  3. publish_batch_trigger   — write completion message to sat.batch.trigger
"""

import json
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# ─── Default args ─────────────────────────────────────────────────────────────

default_args = {
    "owner":           "satellite-team",
    "depends_on_past": False,
    "start_date":      datetime(2024, 1, 1),
    "retries":         1,
    "retry_delay":     timedelta(minutes=10),
    "email_on_failure": False,
    "email_on_retry":   False,
}

# ─── Task callables ───────────────────────────────────────────────────────────

def get_week_boundaries(reference_date: str, **context) -> dict:
    """
    Compute the ISO week number and the Monday–Sunday date range for the
    week that contains `reference_date`.

    Returns a dict pushed to XCom:
      {
        "week_number": "2024-03",
        "start":       "2024-01-15",  # Monday
        "end":         "2024-01-21",  # Sunday
        "year":        "2024",
        "week":        "03",
      }
    """
    ref = datetime.strptime(reference_date, "%Y-%m-%d")
    iso_cal = ref.isocalendar()
    year, week, _ = iso_cal

    # Monday of this ISO week
    monday = ref - timedelta(days=ref.weekday())
    sunday = monday + timedelta(days=6)

    result = {
        "week_number": f"{year}-{week:02d}",
        "start":       monday.strftime("%Y-%m-%d"),
        "end":         sunday.strftime("%Y-%m-%d"),
        "year":        str(year),
        "week":        f"{week:02d}",
    }
    print(f"[week_boundaries] {result}")
    return result


def publish_kafka_trigger(job_type: str, **context) -> None:
    """Publish a completion trigger to sat.batch.trigger Kafka topic."""
    import os
    from kafka import KafkaProducer

    week_info = context["ti"].xcom_pull(task_ids="calculate_week_range")
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

    msg = {
        "job_type":     job_type,
        "week_number":  week_info["week_number"],
        "triggered_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "triggered_by": "airflow",
        "dag_run_id":   context.get("run_id", ""),
        "status":       "completed",
    }
    try:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        future = producer.send("sat.batch.trigger", value=msg)
        future.get(timeout=10)
        producer.flush()
        print(f"[publish_trigger] Sent trigger for {job_type} week={week_info['week_number']}")
    except Exception as exc:
        print(f"[publish_trigger] WARNING: failed to publish trigger: {exc}")


# ─── DAG ─────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="satellite_weekly_pipeline",
    default_args=default_args,
    description="Weekly TLE drift analysis via Hadoop MapReduce",
    schedule_interval="0 4 * * 0",   # Sunday 04:00 UTC
    catchup=False,
    max_active_runs=1,
    tags=["satellite", "batch", "weekly", "mapreduce"],
) as dag:

    calculate_week = PythonOperator(
        task_id="calculate_week_range",
        python_callable=get_week_boundaries,
        op_kwargs={"reference_date": "{{ ds }}"},
    )

    # Bash command uses XCom values resolved at runtime via Jinja
    tle_drift_analysis = BashOperator(
        task_id="tle_drift_mapreduce",
        bash_command=(
            "/opt/mapreduce/run_mapreduce.sh "
            "{{ ti.xcom_pull(task_ids='calculate_week_range')['week'] }} "
            "{{ ti.xcom_pull(task_ids='calculate_week_range')['year'] }}"
        ),
        env={
            "HADOOP_HOME": "/opt/hadoop",
            "JAVA_HOME":   "/usr/lib/jvm/java-11-openjdk-amd64",
        },
    )

    publish_trigger = PythonOperator(
        task_id="publish_batch_trigger",
        python_callable=publish_kafka_trigger,
        op_kwargs={"job_type": "weekly_tle_drift"},
    )

    calculate_week >> tle_drift_analysis >> publish_trigger
