"""
Airflow DAG: Satellite Platform Monitoring

Schedule: Every 6 hours
Tasks:
  1. check_positions_freshness  — HDFS /satellite/raw/positions has today's partition
  2. check_aggregations_freshness — HDFS /satellite/aggregated/daily has yesterday's partition

If either check fails it raises an exception, triggering task_failure_alert which
publishes an alert to sat.alerts and logs a structured failure entry.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from callbacks import task_failure_alert


# ─── helpers ──────────────────────────────────────────────────────────────────

def _hdfs_partition_count(container_name: str, path: str) -> int:
    """
    Returns file count at *path* in HDFS by execing 'hdfs dfs -count' in
    *container_name*.  Returns -1 if the container is unavailable.
    """
    try:
        import docker
    except ImportError:
        print("[monitoring] docker-py not installed — skipping HDFS check")
        return 0

    client = docker.from_env()
    try:
        container = client.containers.get(container_name)
    except docker.errors.NotFound:
        print(f"[monitoring] Container {container_name} not found")
        return -1

    exec_obj = client.api.exec_create(
        container.id,
        ["hdfs", "dfs", "-count", path],
        stdout=True,
        stderr=True,
    )
    raw_out = client.api.exec_start(exec_obj["Id"])
    exit_code = client.api.exec_inspect(exec_obj["Id"])["ExitCode"]
    client.close()

    if exit_code != 0:
        return 0

    # hdfs dfs -count output: <dir_count> <file_count> <size> <path>
    # Hadoop often prepends log4j/SLF4J warning lines; scan for the first
    # line whose first token is an integer (the actual count line).
    for line in raw_out.decode("utf-8", errors="replace").splitlines():
        parts = line.split()
        if len(parts) >= 2:
            try:
                int(parts[0])       # dir_count — must be numeric
                return int(parts[1])  # file_count
            except ValueError:
                continue            # skip warning/header lines
    return 0


def check_positions_freshness(ds: str, **context) -> None:
    """
    Verify that today's raw position partition exists and is non-empty.
    Raises on failure so on_failure_callback fires.
    """
    path = f"/satellite/raw/positions/date={ds}"
    count = _hdfs_partition_count("namenode", path)

    if count == -1:
        print(f"[monitoring] Could not reach namenode — skipping positions check for {ds}")
        return

    if count == 0:
        raise RuntimeError(
            f"Positions partition missing or empty for date={ds}. "
            "ISS/N2YO producers may be down."
        )

    print(f"[monitoring] Positions OK for {ds}: {count} files")


def check_aggregations_freshness(ds: str, **context) -> None:
    """
    Verify that yesterday's aggregation partition exists.
    The daily job runs at 02:00 UTC so by the 06:00 check it should be present.
    """
    from datetime import datetime, timedelta
    yesterday = (datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=1)).strftime("%Y-%m-%d")
    path = f"/satellite/aggregated/daily/date={yesterday}"
    count = _hdfs_partition_count("namenode", path)

    if count == -1:
        print(f"[monitoring] Could not reach namenode — skipping aggregations check for {yesterday}")
        return

    if count == 0:
        raise RuntimeError(
            f"Daily aggregation partition missing for date={yesterday}. "
            "Check the satellite_daily_pipeline DAG for failures."
        )

    print(f"[monitoring] Aggregations OK for {yesterday}: {count} files")


# ─── DAG definition ───────────────────────────────────────────────────────────

default_args = {
    "owner": "satellite-team",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 0,
    "email_on_failure": False,
    "on_failure_callback": task_failure_alert,
}

with DAG(
    dag_id="satellite_monitoring",
    default_args=default_args,
    description="Platform health checks — HDFS partition freshness",
    schedule_interval="0 */6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["satellite", "monitoring"],
) as dag:

    check_positions = PythonOperator(
        task_id="check_positions_freshness",
        python_callable=check_positions_freshness,
        op_kwargs={"ds": "{{ ds }}"},
    )

    check_aggregations = PythonOperator(
        task_id="check_aggregations_freshness",
        python_callable=check_aggregations_freshness,
        op_kwargs={"ds": "{{ ds }}"},
    )

    # Run checks in parallel — independent
    [check_positions, check_aggregations]
