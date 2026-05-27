"""
Airflow DAG: Daily Satellite Data Aggregation Pipeline

Schedule: 02:00 UTC daily
Tasks:
  1. check_hdfs_data          — verify yesterday's partition exists in HDFS
  2. daily_orbital_aggregation — run Spark batch job (daily_aggregation.py)
  3. publish_batch_trigger    — write completion message to sat.batch.trigger
"""

import json
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# ─── Default args ─────────────────────────────────────────────────────────────

default_args = {
    "owner":          "satellite-team",
    "depends_on_past": True,
    "start_date":      datetime(2024, 1, 1),
    "retries":         2,
    "retry_delay":     timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry":   False,
}

# ─── Task callables ───────────────────────────────────────────────────────────

def check_hdfs_partition_exists(date: str, **context) -> bool:
    """
    Returns True if the HDFS partition for `date` exists and is non-empty.
    If the partition is missing we short-circuit the DAG (nothing to aggregate).
    """
    import subprocess
    import os

    hdfs_path = f"{os.getenv('HDFS_NAMENODE_URL', 'hdfs://namenode:8020')}/satellite/raw/positions/date={date}"
    try:
        result = subprocess.run(
            ["hdfs", "dfs", "-count", hdfs_path],
            capture_output=True, text=True, timeout=30,
        )
        if result.returncode != 0:
            print(f"[check_hdfs] Partition not found: {hdfs_path}")
            return False
        # -count output: <dir count> <file count> <content size> <path>
        parts = result.stdout.strip().split()
        file_count = int(parts[1]) if len(parts) >= 2 else 0
        if file_count == 0:
            print(f"[check_hdfs] Partition exists but is empty: {hdfs_path}")
            return False
        print(f"[check_hdfs] Partition OK — {file_count} file(s) at {hdfs_path}")
        return True
    except Exception as exc:
        print(f"[check_hdfs] Error checking partition: {exc}")
        return False


def publish_kafka_trigger(job_type: str, date: str, **context) -> None:
    """Publish a completion trigger to sat.batch.trigger Kafka topic."""
    import os
    from kafka import KafkaProducer

    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    msg = {
        "job_type":     job_type,
        "date":         date,
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
        print(f"[publish_trigger] Sent trigger for {job_type} date={date}")
    except Exception as exc:
        # Non-fatal: aggregation data is already written to HDFS
        print(f"[publish_trigger] WARNING: failed to publish trigger: {exc}")


# ─── DAG ─────────────────────────────────────────────────────────────────────

with DAG(
    dag_id="satellite_daily_pipeline",
    default_args=default_args,
    description="Daily satellite orbital data aggregation",
    schedule_interval="0 2 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["satellite", "batch", "daily"],
) as dag:

    check_data = ShortCircuitOperator(
        task_id="check_hdfs_data",
        python_callable=check_hdfs_partition_exists,
        op_kwargs={"date": "{{ ds }}"},
    )

    daily_aggregation = SparkSubmitOperator(
        task_id="daily_orbital_aggregation",
        application="/opt/spark/jobs/daily_aggregation.py",
        conn_id="spark_default",
        application_args=["--date", "{{ ds }}", "--publish-trigger"],
        conf={
            "spark.executor.memory":              "2g",
            "spark.executor.cores":               "2",
            "spark.sql.shuffle.partitions":       "4",
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
            "spark.dynamicAllocation.enabled":    "false",
        },
        jars="",
        packages=(
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.hadoop:hadoop-client:3.3.4"
        ),
    )

    publish_trigger = PythonOperator(
        task_id="publish_batch_trigger",
        python_callable=publish_kafka_trigger,
        op_kwargs={
            "job_type": "daily_aggregation",
            "date":     "{{ ds }}",
        },
    )

    check_data >> daily_aggregation >> publish_trigger
