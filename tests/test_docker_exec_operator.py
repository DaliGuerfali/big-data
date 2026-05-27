"""
Unit tests for the pure command-builder functions in docker_exec_operator.
No Airflow or Docker needed — these functions have no side effects.
"""

import pytest
from operators.docker_exec_operator import (
    build_spark_submit_cmd,
    build_hadoop_streaming_cmd,
)


class TestBuildSparkSubmitCmd:
    def test_minimal(self):
        cmd = build_spark_submit_cmd(application="/opt/spark/jobs/batch/daily.py")
        assert cmd[0] == "/opt/spark/bin/spark-submit"
        assert "--master" in cmd
        assert "spark://spark-master:7077" in cmd
        assert "/opt/spark/jobs/batch/daily.py" in cmd

    def test_custom_master(self):
        cmd = build_spark_submit_cmd(
            application="/app.py", master="local[4]"
        )
        idx = cmd.index("--master")
        assert cmd[idx + 1] == "local[4]"

    def test_conf_flags(self):
        cmd = build_spark_submit_cmd(
            application="/app.py",
            conf={"spark.executor.memory": "2g", "spark.executor.cores": "2"},
        )
        conf_pairs = [cmd[i + 1] for i, x in enumerate(cmd) if x == "--conf"]
        assert "spark.executor.memory=2g" in conf_pairs
        assert "spark.executor.cores=2" in conf_pairs

    def test_packages_flag(self):
        cmd = build_spark_submit_cmd(
            application="/app.py",
            packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
        )
        assert "--packages" in cmd
        idx = cmd.index("--packages")
        assert "kafka" in cmd[idx + 1]

    def test_application_args_appended_after_application(self):
        cmd = build_spark_submit_cmd(
            application="/app.py",
            application_args=["--date", "2024-01-15"],
        )
        app_idx = cmd.index("/app.py")
        assert cmd[app_idx + 1] == "--date"
        assert cmd[app_idx + 2] == "2024-01-15"

    def test_no_conf_no_packages(self):
        cmd = build_spark_submit_cmd(application="/app.py")
        assert "--conf" not in cmd
        assert "--packages" not in cmd

    def test_empty_application_args(self):
        cmd = build_spark_submit_cmd(application="/app.py", application_args=[])
        # application should be the last token
        assert cmd[-1] == "/app.py"


class TestBuildHadoopStreamingCmd:
    def test_minimal(self):
        cmd = build_hadoop_streaming_cmd("/opt/batch/mapreduce/run_mapreduce.sh")
        assert cmd == ["bash", "/opt/batch/mapreduce/run_mapreduce.sh"]

    def test_with_args(self):
        cmd = build_hadoop_streaming_cmd(
            "/opt/batch/mapreduce/run_mapreduce.sh",
            script_args=["03", "2024"],
        )
        assert cmd == ["bash", "/opt/batch/mapreduce/run_mapreduce.sh", "03", "2024"]

    def test_args_are_stringified(self):
        cmd = build_hadoop_streaming_cmd("/script.sh", script_args=[3, 2024])
        assert cmd[2] == "3"
        assert cmd[3] == "2024"
