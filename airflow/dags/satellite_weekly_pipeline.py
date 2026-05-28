"""
Airflow DAG: Weekly TLE Drift Analysis Pipeline

Schedule: 04:00 UTC every Sunday
Tasks:
  1. calculate_week_range  — compute ISO week boundaries and week number (XCom)
  2. tle_drift_mapreduce   — run Hadoop Streaming job (tle_drift_mapper/reducer)
                             via HadoopStreamingDockerOperator inside namenode
  3. publish_batch_trigger — write completion message to sat.batch.trigger

Failure callback: task_failure_alert → logs + best-effort sat.alerts Kafka publish
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from operators.docker_exec_operator import HadoopStreamingDockerOperator
from callbacks import task_failure_alert
from dag_utils import cache_batch_results, get_week_boundaries, publish_kafka_trigger


# ─── DAG definition ───────────────────────────────────────────────────────────

default_args = {
    "owner": "satellite-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
    "email_on_retry": False,
    "on_failure_callback": task_failure_alert,
}

with DAG(
    dag_id="satellite_weekly_pipeline",
    default_args=default_args,
    description="Weekly TLE drift analysis via Hadoop MapReduce",
    schedule_interval="0 4 * * 0",
    catchup=False,
    max_active_runs=1,
    tags=["satellite", "batch", "weekly", "mapreduce"],
) as dag:

    calculate_week = PythonOperator(
        task_id="calculate_week_range",
        python_callable=get_week_boundaries,
        op_kwargs={"reference_date": "{{ ds }}"},
    )

    # run_mapreduce.sh lives at /opt/batch/mapreduce/run_mapreduce.sh inside
    # the namenode container (mounted via docker-compose ../batch:/opt/batch:ro)
    tle_drift_analysis = HadoopStreamingDockerOperator(
        task_id="tle_drift_mapreduce",
        script="/opt/batch/mapreduce/run_mapreduce.sh",
        # XCom values are Jinja-templated; operator.template_fields includes script_args
        script_args=[
            "{{ ti.xcom_pull(task_ids='calculate_week_range')['week'] }}",
            "{{ ti.xcom_pull(task_ids='calculate_week_range')['year'] }}",
        ],
    )

    publish_trigger = PythonOperator(
        task_id="publish_batch_trigger",
        python_callable=publish_kafka_trigger,
        op_kwargs={"job_type": "weekly_tle_drift"},
    )

    cache_results = PythonOperator(
        task_id="cache_results_to_redis",
        python_callable=cache_batch_results,
        op_kwargs={
            "redis_key": "batch:weekly:latest",
            "api_path": "/api/reports/drift/{{ ti.xcom_pull(task_ids='calculate_week_range')['week_number'] }}",
        },
    )

    calculate_week >> tle_drift_analysis >> publish_trigger >> cache_results
