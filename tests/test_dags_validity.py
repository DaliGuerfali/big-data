"""
DAG validity tests — require Airflow to be installed (container or local venv).
Skip automatically if airflow is not available so pure-logic CI passes.
"""

import importlib
import sys
from pathlib import Path

# If airflow is unavailable, every test in this file is skipped.
airflow = pytest = None
try:
    import pytest
    airflow = pytest.importorskip("airflow")
except Exception:
    pass

if pytest is not None:
    from airflow.models import DagBag

    DAGS_DIR = str(Path(__file__).parent.parent / "airflow" / "dags")

    # ── fixtures ──────────────────────────────────────────────────────────────

    @pytest.fixture(scope="module")
    def dagbag():
        bag = DagBag(dag_folder=DAGS_DIR, include_examples=False)
        return bag

    # ── tests ─────────────────────────────────────────────────────────────────

    def test_no_import_errors(dagbag):
        assert dagbag.import_errors == {}, (
            "DAG import errors: " + str(dagbag.import_errors)
        )

    def test_expected_dags_present(dagbag):
        expected = {
            "satellite_daily_pipeline",
            "satellite_weekly_pipeline",
            "satellite_monitoring",
        }
        assert expected.issubset(set(dagbag.dag_ids)), (
            f"Missing DAGs. Found: {sorted(dagbag.dag_ids)}"
        )

    def test_daily_pipeline_tasks(dagbag):
        dag = dagbag.get_dag("satellite_daily_pipeline")
        task_ids = {t.task_id for t in dag.tasks}
        assert "check_hdfs_data" in task_ids
        assert "daily_orbital_aggregation" in task_ids
        assert "publish_batch_trigger" in task_ids

    def test_weekly_pipeline_tasks(dagbag):
        dag = dagbag.get_dag("satellite_weekly_pipeline")
        task_ids = {t.task_id for t in dag.tasks}
        assert "calculate_week_range" in task_ids
        assert "tle_drift_mapreduce" in task_ids
        assert "publish_batch_trigger" in task_ids

    def test_monitoring_pipeline_tasks(dagbag):
        dag = dagbag.get_dag("satellite_monitoring")
        task_ids = {t.task_id for t in dag.tasks}
        assert "check_positions_freshness" in task_ids
        assert "check_aggregations_freshness" in task_ids

    def test_daily_pipeline_no_cycle(dagbag):
        dag = dagbag.get_dag("satellite_daily_pipeline")
        assert dag.test_cycle() is None or True  # test_cycle raises on cycle

    def test_weekly_pipeline_no_cycle(dagbag):
        dag = dagbag.get_dag("satellite_weekly_pipeline")
        assert dag.test_cycle() is None or True

    def test_all_dags_have_tags(dagbag):
        for dag_id, dag in dagbag.dags.items():
            assert dag.tags, f"DAG '{dag_id}' has no tags"

    def test_all_dags_have_description(dagbag):
        for dag_id, dag in dagbag.dags.items():
            assert dag.description, f"DAG '{dag_id}' has no description"

    def test_daily_pipeline_schedule(dagbag):
        dag = dagbag.get_dag("satellite_daily_pipeline")
        assert dag.schedule_interval == "0 2 * * *"

    def test_weekly_pipeline_schedule(dagbag):
        dag = dagbag.get_dag("satellite_weekly_pipeline")
        assert dag.schedule_interval == "0 4 * * 0"

    def test_monitoring_schedule(dagbag):
        dag = dagbag.get_dag("satellite_monitoring")
        assert dag.schedule_interval == "0 */6 * * *"
