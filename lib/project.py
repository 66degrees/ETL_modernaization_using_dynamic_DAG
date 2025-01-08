"""This module handles everything around edw config's project."""

# Standard libs
from typing import Optional, TYPE_CHECKING

# Custom libs
from edw.lib.environment.variables import Variables
from edw.lib.general.enum.dag_type import DagType

if TYPE_CHECKING:
    from edw.lib.config.edw.project.datasets import Datasets
    from edw.lib.general.enum.dag_type import DagType


class Project:  # pylint: disable=too-few-public-methods
    """Class representing the project configuration."""

    def __init__(self, project_name: str, source_type: Optional["DagType"] = None) -> None:
        """Initialize the variables that load the project configuration object.

        Args:
            project_name: Name of the project
        """
        self.name = project_name
        self.source_type = source_type
        self._datasets: Optional["Datasets"] = None
        self.reload_bucket_name = f"{project_name}-reload-{Variables.get_environment_code()}"
        self.reload_archive_bucket_name = f"{project_name}-reload-archive-{Variables.get_environment_code()}"

    ###########################################
    ### service meta data #####################
    ###########################################

    @staticmethod
    def service_name() -> str:
        """Name of the service of the alert handler.

        Returns:
            Alert service name for the Alert handler
        """
        # since this is used by the pubsub of the data_pipeline_dag class, it must always be this static value
        return "na_edw_etl"

    ###########################################
    ### Project Properties ####################
    ###########################################

    @property
    def short_name(self) -> str:
        """Project short name.

        Returns:
            Project name without prefix `gcp-gfs` and environment and the last part after the `-`

            Example: gcp-gfs-datalake-core-dev returns core
        """
        if not self.name:
            return "unknown"

        name = self.name.replace(f"-{Variables.get_environment_code()}", "").split("-")[-1:][0].strip()
        return name if len(name) > 0 else "unknown"

    @property
    def staging_dataset_name(self) -> str:
        """Name of the staging dataset in BigQuery.

        Returns:
            BigQuery staging dataset name
        """
        return f"{self.short_name}_stage"

    @property
    def name_with_environment_code(self) -> str:
        """Project name with environment code.

        Returns:
            Project name with Environment Code in the format {name}-{environment_code}
        """
        return f"{self.name}-{Variables.get_environment_code()}"

    ###########################################
    ### Collections ###########################
    ###########################################
    @property
    def datasets(self) -> Optional["Datasets"]:
        """Lazy load the datasets that belong to the project.

        Returns:
            A list of dataset configuration objects belonging to the project.
        """
        # Setting the import statement late in this method (datasets), so we don't get circular reference issues with
        # the edw.lib.config.edw.project import statement in the edw.lib.config.edw.project.datasets module.
        # pylint: disable=import-outside-toplevel
        from edw.lib.config.edw.project.datasets import Datasets

        if not self._datasets:
            self._datasets = Datasets(project=self, source_type=self.source_type)

        return self._datasets
