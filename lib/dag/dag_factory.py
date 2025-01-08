"""This module is to build the Directional Acyclic Graphs structure based on given configuration."""

# Standard libs
from typing import Any, List, TYPE_CHECKING

# Third-party libs
from airflow import DAG
from airflow.operators.empty import EmptyOperator

# Custom libs
from edw.lib.config.dag_args import DagArgs
from edw.lib.config.edw.project import Project
from edw.lib.dag.dag_tasks import DagTasks
from edw.lib.dag.data_pipeline_dag import DataPipelineDAG
from edw.lib.task_operator import (
    BackupOperator,
    DqPostCheckOperator,
    DeleteEmptyDatasetOperator,
    EDWLogJobOperator,
    ReloadOperator,
)

if TYPE_CHECKING:
    from edw.lib.config.edw.project.dataset import Dataset
    from edw.lib.config.edw.project.dataset.table import Table


class DagFactory:
    """Class that implements and provide methods to build Airflow dynamic Directional Acyclic Graphs."""

    # pylint: disable=pointless-statement
    @staticmethod
    def add_dag_tasks(dag_instance: DAG, table_instance: "Table") -> DAG:
        """Attach the tasks to a DAG.

        Args:
            dag_instance: Unique dag_instance for a data lake table
            table_instance: Instance of table object

        Returns:
            Dag Object with tasks.
        """
        with dag_instance:
            # this ordering is needed to ensure run_has_data_branch can use the correct parameter values
            run_reload = ReloadOperator(task_id=f"{table_instance.target_name}_reload", table_instance=table_instance)
            run_extract = DagTasks.create_extract_task(table_instance=table_instance)
            run_post_extract = DagTasks.create_post_extract_task(table_instance=table_instance)
            run_find_dupes = DagTasks.create_find_dupes_task(table_instance=table_instance)
            run_status_update = DagTasks.create_status_update_task(table_instance=table_instance)
            run_has_data_branch = DagTasks.create_has_data_branch_task(
                table_instance=table_instance,
                yes_data_branch=run_find_dupes.task_id,
                no_data_branch=run_status_update.task_id,
            )
            run_dq_check = DagTasks.create_dq_task(table_instance=table_instance)
            run_staging_scd_farm_fingerprint = DagTasks.create_staging_scd_farm_fingerprint_task(
                table_instance=table_instance
            )
            run_merge = DagTasks.create_merge_task(table_instance=table_instance)
            run_log_consolidation = DagTasks.create_dq_consolidation_log_task(table_instance=table_instance)
            run_find_orphaned_dq_records = DagTasks.create_find_orphaned_dq_records_task(table_instance=table_instance)
            run_dq_post_check = DqPostCheckOperator(
                task_id=f"{table_instance.target_name}_dq_post_check", table_instance=table_instance
            )
            downstream_dag = DagTasks.get_downstream_dependency_tasks(table_instance=table_instance)

            # Get Data tasks
            run_reload >> run_extract >> run_post_extract >> run_has_data_branch
            run_has_data_branch >> [run_find_dupes, run_status_update]
            # Analyze and Process Data tasks
            run_find_dupes >> run_dq_check >> run_staging_scd_farm_fingerprint >> run_merge >> run_log_consolidation
            run_log_consolidation >> run_find_orphaned_dq_records >> run_dq_post_check >> run_status_update
            # Shutdown Dag tasks
            run_status_update >> downstream_dag

        return dag_instance

    # pylint: disable=pointless-statement
    @staticmethod
    def add_dag_tasks_derived_tables(dag_instance: DAG, table_instance: "Table") -> DAG:
        """Attach the tasks to a DAG.

        Args:
            dag_instance: Unique dag_instance for a data lake table.
            table_instance: Instance of table object.

        Returns:
            Dag Object with tasks.
        """
        with dag_instance:
            tasks: List[Any] = DagTasks.create_dynamic_tasks(table_instance=table_instance)
            tasks += DagTasks.get_downstream_dependency_tasks(table_instance=table_instance)
            tasks.append(DagTasks.create_status_update_task(table_instance=table_instance))
            for i in range(0, len(tasks) - 1):
                tasks[i] >> tasks[i + 1]
            # chain(*tasks)

        return dag_instance

    @staticmethod
    def create_backup_dag(dataset_instance: "Dataset") -> "DAG":
        """Add backup task for a dataset.

        Args:
            dataset_instance: Instance of dataset configuration.

        Returns:
            Dag instance with tasks.
        """
        with DataPipelineDAG(
            service=Project.service_name(),
            dag_id=dataset_instance.backup_dag_id,
            schedule_interval="00 20 * * *",
            default_args=DagArgs.get_default_args(),
            catchup=False,
            is_paused_upon_creation=False,
            tags=dataset_instance.backup_tags,
            description=dataset_instance.backup_description,
        ) as dag_instance:
            start_task = EmptyOperator(task_id=dataset_instance.name)
            backup_operator_task = BackupOperator(
                task_id="backup_dataset",
                project_id=dataset_instance.project.name_with_environment_code,
                dataset_id=dataset_instance.name,
            )

            # pylint: disable=pointless-statement
            start_task >> backup_operator_task

            return dag_instance

    @staticmethod
    def create_kick_off_all(dataset_instance: "Dataset") -> "DAG":
        """Build a DAG that will trigger all DAGs belonging to the dataset.

        Args:
            dataset_instance: Instance of dataset configuration.

        Returns:
            Dag instance with tasks.
        """
        with DataPipelineDAG(
            service=Project.service_name(),
            dag_id=dataset_instance.kick_off_all_dag_id,
            schedule_interval=None,
            default_args=DagArgs.get_default_args(),
            catchup=False,
            is_paused_upon_creation=True,
            tags=dataset_instance.kick_off_all_tags,
            description=dataset_instance.kick_off_all_description,
        ) as dag_instance:
            for table in [table for table in dataset_instance.tables or [] if table.prd_schedule]:
                DagTasks.create_trigger_dag_task(dag_name=table.name)

            return dag_instance

    @staticmethod
    def create_delete_empty_datasets_dag(project_instance: "Project") -> "DAG":
        """Build a DAG that will delete empty datasets according to the DAG logic.

        Args:
            project_instance: Instance of a Project.

        Returns:
            DAG instance to execute.
        """
        with DataPipelineDAG(
            service=Project.service_name(),
            dag_id=f"{project_instance.short_name}.002_delete_empty_datasets",
            schedule_interval="0 10 * * *",
            default_args=DagArgs.get_default_args(),
            catchup=False,
            is_paused_upon_creation=False,
            description="Deletes expired and empty datasets matching a dataset label.",
        ) as dag_instance:
            DeleteEmptyDatasetOperator(
                task_id="delete_empty_datasets", project_name=project_instance.name_with_environment_code
            )

            return dag_instance

    @staticmethod
    def create_edw_log_edw_job_dag(table_instance: "Table", dag_id: str) -> "DAG":
        """Build a DAG that will get Bigquery job information for EDW.

        Args:
            table_instance: Instance of table object.

        Returns:
            DAG instance to execute.
        """
        with DataPipelineDAG(
            service=Project.service_name(),
            dag_id=dag_id,
            schedule_interval=table_instance.schedule_interval,
            default_args=DagArgs.get_default_args(),
            catchup=False,
            is_paused_upon_creation=True,
            tags=["edw_log", "edw_job"],
            description="DAG to capture the historical performance and usage data of BigQuery",
        ) as dag_instance:
            EDWLogJobOperator(task_id="get_job_info", table_instance=table_instance)

            return dag_instance
