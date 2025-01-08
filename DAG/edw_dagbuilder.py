"""This module is to build the edw dag dynamically based on given configuration.

This line should never be deleted. It allows the airflow DAG to be discoverable.
"""

# Third-party libs
from airflow import DAG

# Custom libs
# This import has to come first to recognize the gfs.cloud nexus package namespace as edw.gfs.cloud namespace
import edw.sys_path  # pylint: disable=unused-import
from edw.lib.config.dag_args import DagArgs
from edw.lib.config.edw.project import Project
from edw.lib.config.edw.projects import Projects
from edw.lib.config.edw.project.dataset.table import Table
from edw.lib.dag.dag_factory import DagFactory
from edw.lib.dag.data_pipeline_dag import DataPipelineDAG


def create_dag_instance(table_instance: Table) -> DAG:
    """Create an instance of a dag.

    Args:
        table_instance: Instance of a table object we create a dag from. Table is a hierarchy of
            project.dataset.project in the  `edw.lib.config.edw.project.dataset.table` namespace.

    Returns:
        A DAG which inherits from airflow.models.dag
    """
    return DagFactory().add_dag_tasks(
        dag_instance=DataPipelineDAG(
            service=Project.service_name(),
            priority=table_instance.priority,
            dag_id=table_instance.dag_id,
            schedule_interval=table_instance.schedule_interval,
            default_args=DagArgs.get_default_args(),
            catchup=table_instance.catchup,
            is_paused_upon_creation=table_instance.is_paused_upon_creation,
            tags=table_instance.tags,
            description=table_instance.description,
        ),
        table_instance=table_instance,
    )


for project in Projects():
    for dataset in project.datasets:
        for table in dataset.tables:
            globals()[table.dag_id] = create_dag_instance(table_instance=table)
