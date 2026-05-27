"""
Custom Airflow operators that execute commands inside sibling Docker containers
via the Docker socket (docker-py).  This lets Airflow trigger Spark and
Hadoop jobs that live in the spark-master and namenode containers without
requiring spark-submit or hadoop to be installed in the Airflow image itself.

Airflow and docker-py are lazy-imported inside execute() / _exec() so that
the pure command-builder functions can be imported and tested without Airflow.
"""

from __future__ import annotations

import shlex
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from airflow.models import BaseOperator  # noqa: F401 — type hint only


# ─── pure helpers (unit-tested without Airflow or Docker) ────────────────────

def build_spark_submit_cmd(
    application: str,
    master: str = "spark://spark-master:7077",
    application_args: Optional[list] = None,
    conf: Optional[dict] = None,
    packages: Optional[str] = None,
) -> list[str]:
    """Return the spark-submit argument list (no shell quoting needed)."""
    cmd: list[str] = ["/opt/spark/bin/spark-submit", "--master", master]

    if conf:
        for key, value in conf.items():
            cmd += ["--conf", f"{key}={value}"]

    if packages:
        cmd += ["--packages", packages]

    cmd.append(application)

    if application_args:
        cmd.extend(str(a) for a in application_args)

    return cmd


def build_hadoop_streaming_cmd(script: str, script_args: Optional[list] = None) -> list[str]:
    """Return the bash invocation for a Hadoop Streaming shell script."""
    cmd = ["bash", script]
    if script_args:
        cmd.extend(str(a) for a in script_args)
    return cmd


# ─── operators ────────────────────────────────────────────────────────────────

def _get_base_operator():
    from airflow.models import BaseOperator
    return BaseOperator


class _DockerExecBase:
    """
    Mixin: exec a command in a named Docker container and stream its logs to
    the Airflow task log.  Raises AirflowException on non-zero exit code.
    Inherits from BaseOperator lazily to avoid import at module load time.
    """

    def __init_subclass__(cls, **kwargs):
        # Inject BaseOperator as a real base class when Airflow is available.
        # When it's not (e.g. unit tests importing only the helpers), the mixin
        # still exists as a plain class and the subclasses won't be usable as
        # operators — but neither would they be instantiated.
        pass

    def __init__(self, container_name: str, **kwargs):
        super().__init__(**kwargs)
        self.container_name = container_name

    def _exec(self, cmd: list[str]) -> None:
        from airflow.exceptions import AirflowException

        try:
            import docker
        except ImportError as exc:
            raise AirflowException(
                "docker-py is not installed. "
                "Add 'docker==7.1.0' to _PIP_ADDITIONAL_REQUIREMENTS."
            ) from exc

        client = docker.from_env()

        try:
            container = client.containers.get(self.container_name)
        except docker.errors.NotFound:
            raise AirflowException(
                f"Container '{self.container_name}' not found. "
                "Is the Docker stack running?"
            )

        self.log.info("Executing in container %s: %s", self.container_name, shlex.join(cmd))

        exec_obj = client.api.exec_create(
            container.id,
            cmd,
            stdout=True,
            stderr=True,
        )
        output = client.api.exec_start(exec_obj["Id"], stream=True, demux=True)

        for stdout_chunk, stderr_chunk in output:
            if stdout_chunk:
                for line in stdout_chunk.decode("utf-8", errors="replace").splitlines():
                    self.log.info("[%s] %s", self.container_name, line)
            if stderr_chunk:
                for line in stderr_chunk.decode("utf-8", errors="replace").splitlines():
                    self.log.warning("[%s/stderr] %s", self.container_name, line)

        exit_info = client.api.exec_inspect(exec_obj["Id"])
        exit_code = exit_info["ExitCode"]
        client.close()

        if exit_code != 0:
            raise AirflowException(
                f"Command in '{self.container_name}' exited with code {exit_code}: "
                + shlex.join(cmd)
            )


def _make_operator_class(name, base_init, execute_fn, template_fields=()):
    """
    Dynamically build an Airflow BaseOperator subclass.
    Called at import time only when Airflow is available.
    """
    from airflow.models import BaseOperator

    cls = type(name, (_DockerExecBase, BaseOperator), {
        "__init__": base_init,
        "execute": execute_fn,
        "template_fields": template_fields,
    })
    return cls


# ── SparkSubmitDockerOperator ─────────────────────────────────────────────────

def _spark_init(
    self,
    application: str,
    master: str = "spark://spark-master:7077",
    application_args: Optional[list] = None,
    conf: Optional[dict] = None,
    packages: Optional[str] = None,
    container_name: str = "spark-master",
    **kwargs,
):
    _DockerExecBase.__init__(self, container_name=container_name, **kwargs)
    self.application = application
    self.master = master
    self.application_args = application_args or []
    self.conf = conf or {}
    self.packages = packages


def _spark_execute(self, context):
    cmd = build_spark_submit_cmd(
        application=self.application,
        master=self.master,
        application_args=self.application_args,
        conf=self.conf,
        packages=self.packages,
    )
    self._exec(cmd)


# ── HadoopStreamingDockerOperator ─────────────────────────────────────────────

def _hadoop_init(
    self,
    script: str,
    script_args: Optional[list] = None,
    container_name: str = "namenode",
    **kwargs,
):
    _DockerExecBase.__init__(self, container_name=container_name, **kwargs)
    self.script = script
    self.script_args = script_args or []


def _hadoop_execute(self, context):
    cmd = build_hadoop_streaming_cmd(
        script=self.script,
        script_args=self.script_args,
    )
    self._exec(cmd)


# Lazily build operator classes so this module stays importable without Airflow.
try:
    from airflow.models import BaseOperator as _BaseOperator  # noqa: F401

    class SparkSubmitDockerOperator(_DockerExecBase, _BaseOperator):
        """
        Submit a PySpark job via spark-submit inside the 'spark-master' container.

        Parameters
        ----------
        application:      path inside the container
        master:           Spark master URL (default spark://spark-master:7077)
        application_args: list of args passed after the application path
        conf:             dict of --conf key=value pairs
        packages:         Maven coordinates string
        container_name:   name of the Spark master container (default "spark-master")
        """

        template_fields = ("application_args",)

        def __init__(self, application, master="spark://spark-master:7077",
                     application_args=None, conf=None, packages=None,
                     container_name="spark-master", **kwargs):
            _spark_init(self, application=application, master=master,
                        application_args=application_args, conf=conf,
                        packages=packages, container_name=container_name, **kwargs)

        def execute(self, context):
            _spark_execute(self, context)

    class HadoopStreamingDockerOperator(_DockerExecBase, _BaseOperator):
        """
        Run a Hadoop Streaming shell script inside the 'namenode' container.

        Parameters
        ----------
        script:         absolute path inside the container
        script_args:    list of positional args passed to the script
        container_name: name of the Hadoop namenode container (default "namenode")
        """

        template_fields = ("script_args",)

        def __init__(self, script, script_args=None,
                     container_name="namenode", **kwargs):
            _hadoop_init(self, script=script, script_args=script_args,
                         container_name=container_name, **kwargs)

        def execute(self, context):
            _hadoop_execute(self, context)

except ImportError:
    # Airflow not installed — define stub classes so the module imports cleanly.
    # The stubs raise at instantiation if actually called without Airflow.
    class SparkSubmitDockerOperator:  # type: ignore[no-redef]
        def __init__(self, *args, **kwargs):
            raise ImportError("airflow is required to instantiate SparkSubmitDockerOperator")

    class HadoopStreamingDockerOperator:  # type: ignore[no-redef]
        def __init__(self, *args, **kwargs):
            raise ImportError("airflow is required to instantiate HadoopStreamingDockerOperator")
