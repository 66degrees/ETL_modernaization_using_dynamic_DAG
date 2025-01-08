"""Custom DAG implementation for data pipelines."""

# Standard libs
from datetime import datetime
from functools import partial
from typing import Any, Callable, Dict, Optional, TYPE_CHECKING

# Third-party libs
from airflow.models import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.utils.state import State
from airflow.utils.log.logging_mixin import LoggingMixin

# Custom libs
import edw.sys_path  # pylint: disable= unused-import
from edw.gfs.cloud.status import StatusCode, publish_status
from edw.lib.environment.variables import Variables


if TYPE_CHECKING:
    from airflow.models import DagRun
    from logging import Logger


# Utility Methods
def get_airflow_logger(context: Any) -> "Logger":
    """Get an airflow Logger."""
    logging_mixin = LoggingMixin(context=context)
    return logging_mixin.log


def publish_dag_status(service: str, callback: Callable, status: StatusCode, priority: str, context: Dict) -> None:
    """Publish the status of a DAG run.

    Args:
        service: Name of the service associated with the DAG.
        callback: User callback to also be triggered.
        status: Status of the DAG.
        context: Context associated with the DAG.
    """

    def get_timestamp(timestamp: Optional["datetime"]) -> Optional[str]:
        """Format the timestamp into the ISO format.

        Returns:
            ISO-format timestamp, if the timestamp is not None; otherwise None.
        """
        return timestamp.isoformat() if timestamp else None

    log = get_airflow_logger(context=context)
    # pylint: disable=logging-fstring-interpolation
    # TODO: This disable should not be needed once we upgrade PyLint.
    #       [LOGGING] => logging-format-style=new is not being picked up in Jenkins
    log.info(f"DataPipelineDAG -- publish_dag_status => {service}:{priority}:{status}:{datetime.now()}")

    dag = context.get("dag")
    if dag:
        dag = {
            "dag_id": dag.dag_id,
            "task_ids": dag.task_ids,
            "filepath": dag.filepath,
            "start_date": get_timestamp(dag.start_date),
            "end_date": get_timestamp(dag.end_date),
            "priority": "P3" if Variables.get_environment_code().upper() in ("DEV", "TST", "SIT") else priority,
            "current_date": datetime.now().strftime("%Y-%m-%d"),
        }

    task = context.get("task")
    if task:
        task = {
            "dag_id": task.dag_id,
            "task_id": task.task_id,
            "task_type": task.task_type,
            "start_date": get_timestamp(task.start_date),
            "end_date": get_timestamp(task.end_date),
        }

    task_instance = get_task_instance(status=status, context=context)
    task_instance_composed: Dict[str, Any] = {}
    if task_instance:
        task_instance_composed = {
            "dag_id": task_instance.dag_id,
            "task_id": task_instance.task_id,
            "job_id": task_instance.job_id,
            "operator": task_instance.operator,
            "try_number": task_instance.try_number,
            "execution_date": get_timestamp(task_instance.execution_date),
            "start_date": get_timestamp(task_instance.start_date),
            "end_date": get_timestamp(task_instance.end_date),
            "duration": task_instance.duration,
            "log_url": task_instance.log_url,
        }

    dag_run = context.get("dag_run")
    if dag_run:
        dag_run = {
            "dag_id": dag_run.dag_id,
            "id": dag_run.id,
            "run_id": dag_run.run_id,
            "execution_date": get_timestamp(dag_run.execution_date),
            "start_date": get_timestamp(dag_run.start_date),
            "end_date": get_timestamp(dag_run.end_date),
        }

    publish_status(
        env=Variables.get_environment_code(),
        service=service,
        status=status,
        message=context.get("reason"),
        dag=dag,
        task=task,
        task_instance=task_instance_composed,
        dag_run=dag_run,
    )

    # Trigger additional callback
    if context:
        callback(context)


def get_task_instance(status: StatusCode, context: Dict) -> Optional[TaskInstance]:
    """Get the task instance.

    Note: If the dag failed, we need to get the task instance where the actual error happened.

    Arguments:
        status -- Status of the DAG
        context -- Context associated with the DAG

    Returns:
        task_instance of the DAG that either succeeded or failed.
    """
    if status == StatusCode.PROCESS_ERROR:
        # If we have a task failure we need to find the task instance where the actual error happened.
        # To ensure we don't fail when retrieving the dag_run_task_instances,
        #   this code below checks: (Each line below between parenthesis is an ``and`` statement)
        # first: do we have a task_instance in the context.get.
        # second: does the context task_instance have a dag_run.
        # third: does the dag_run have task_instances.
        dag_run_task_instances = (
            context.get("task_instance")
            and context["task_instance"].get_dagrun()
            and context["task_instance"].get_dagrun().get_task_instances()
        )
        for dag_run_task_instance in dag_run_task_instances or []:
            if dag_run_task_instance and dag_run_task_instance.state == State.FAILED:
                return dag_run_task_instance
        return None
    return context.get("task_instance")


class DataPipelineDAG(DAG):
    """Custom DAG for data pipelines.

    Note: Publishes DAG statuses on start, pass, and fail states.
    """

    def __init__(
        self,
        service: str,
        *args,
        priority: Optional[str] = "P3",
        on_execute_callback: Optional[Callable] = None,
        on_success_callback: Optional[Callable] = None,
        on_failure_callback: Optional[Callable] = None,
        **kwargs,
    ):
        """Initialize a data pipeline DAG.

        Args:
            service: Name of the service the DAG is associated with.
            args: All positional arguments are forwarded to the base DAG __init__ method.
            on_execute_callback: Callback that is triggered when the DAG begins execution.
            on_success_callback: Callback that is triggered when the DAG succeeds.
            on_failure_callback: Callback that is triggered when the DAG fails.
            kwargs: All keyword arguments are forwarded to the base DAG __init__ method.
        """
        # TODO: Enable this in Airflow V2. In Airflow V1, this will run every second and pollute the logs
        # self.log.info("DataPipelineDAG -- init --")

        on_success_callback = partial(publish_dag_status, service, on_success_callback, StatusCode.PASSED, priority)
        on_failure_callback = partial(
            publish_dag_status, service, on_failure_callback, StatusCode.PROCESS_ERROR, priority
        )
        super().__init__(  # type: ignore # noqa: misc
            *args,
            on_failure_callback=on_failure_callback,
            on_success_callback=on_success_callback,
            max_active_runs=1,
            **kwargs,
        )
        self.on_execute_callback = partial(
            publish_dag_status, service, on_execute_callback, StatusCode.STARTED, priority
        )

    def create_dagrun(self, *args, **kwargs) -> "DagRun":
        """Create a DAG run from this DAG including the tasks associated with this DAG.

        This method is wraps the base DAG class method to add an "on execute" callback.

        Args:
            args: All positional arguments are forwarded to the base DAG create_dagrun method.
            kwargs: All keyword arguments are forwarded to the base DAG create_dagrun method.
        """
        self.log.info("DataPipelineDAG -- create_dagrun")

        dag_run = super().create_dagrun(*args, **kwargs)

        # Mimics the callback logic built into the DAG class
        # self.log.info('Executing dag callback function: %s', self.on_execute_callback)
        self.on_execute_callback(
            {
                "reason": "dagrun_initialization",
                "dag": self,
                "dag_run": dag_run,
            }
        )

        return dag_run
