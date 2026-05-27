"""
Airflow DAG: Daily Satellite Data Aggregation Pipeline

Schedule: 02:00 UTC daily
Tasks:
  1. check_hdfs_data            — verify yesterday's partition exists in HDFS
                                  (execs hdfs dfs -count in the namenode container)
  2. daily_orbital_aggregation  — run Spark batch job (batch/daily_aggregation.py)
                                  via SparkSubmitDockerOperator inside spark-master
  3. publish_batch_trigger      — write completion message to sat.batch.trigger

Failure callback: task_failure_alert → logs + best-effort sat.alerts Kafka publish
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.operators.python import PythonOperator, ShortCircuitOperator

# Custom operators live in airflow/plugins/operators/ which Airflow puts on sys.path.
from operators.docker_exec_operator import SparkSubmitDockerOperator
from callbacks import task_failure_alert
from dag_utils import publish_kafka_trigger


# ─── helpers ──────────────────────────────────────────────────────────────────

def check_hdfs_partition_exists(date: str, **context) -> bool:
    """
    Returns True if the HDFS partition for *date* exists and contains files.
    Execs 'hdfs dfs -count' inside the namenode container via docker-py so
    Airflow never needs the hdfs binary locally.
    """
    try:
        import docker
    except ImportError:
        raise RuntimeError(
            "docker-py not installed — ensure _PIP_ADDITIONAL_REQUIREMENTS includes 'docker==7.1.0'"
        )

    hdfs_path = f"/satellite/raw/positions/date={date}"

    client = docker.from_env()
    try:
        container = client.containers.get("namenode")
    except docker.errors.NotFound:
        print(f"[check_hdfs] namenode container not running; skipping partition check")
        return True  # let the job attempt; it will fail with a clear error if data is absent

    exec_obj = client.api.exec_create(
        container.id,
        ["hdfs", "dfs", "-count", hdfs_path],
        stdout=True,
        stderr=True,
    )
    raw_out = client.api.exec_start(exec_obj["Id"])
    exit_code = client.api.exec_inspect(exec_obj["Id"])["ExitCode"]
    client.close()

    if exit_code != 0:
        print(f"[check_hdfs] Partition not found or HDFS unavailable: {hdfs_path}")
        return False

    # hdfs dfs -count output: <dir_count> <file_count> <size> <path>
    # Scan for the first line whose first token is numeric (skip log4j lines).
    file_count = 0
    for line in raw_out.decode("utf-8", errors="replace").splitlines():
        parts = line.split()
        if len(parts) >= 2:
            try:
                int(parts[0])
                file_count = int(parts[1])
                break
            except ValueError:
                continue

    if file_count == 0:
        print(f"[check_hdfs] Partition exists but is empty: {hdfs_path}")
        return False

    print(f"[check_hdfs] Partition OK ({file_count} files): {hdfs_path}")
    return True



# ─── DAG definition ───────────────────────────────────────────────────────────

default_args = {
    "owner": "satellite-team",
    "depends_on_past": True,
    "start_date": datetime(2024, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": task_failure_alert,
}

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

    daily_aggregation = SparkSubmitDockerOperator(
        task_id="daily_orbital_aggregation",
        application="/opt/spark/jobs/batch/daily_aggregation.py",
        # local[*] runs in the driver process — no cluster executors needed.
        # This avoids resource contention with the streaming jobs in the dev cluster.
        master="local[*]",
        application_args=["--date", "{{ ds }}"],
        conf={
            "spark.sql.shuffle.partitions": "4",
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
            "spark.driver.memory": "1g",
        },
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
            "date": "{{ ds }}",
        },
    )

    check_data >> daily_aggregation >> publish_trigger
