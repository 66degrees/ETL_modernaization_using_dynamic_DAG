"""This module handles everything around the edw config's datasets."""

# Standard libs
from pathlib import Path
from typing import List, Optional, Tuple, TYPE_CHECKING

# Custom libs
from edw.lib.config.base.custom_directory import CustomDirectory
from edw.lib.config.edw.project.dataset import Dataset
from edw.lib.general.base.custom_list import CustomList
from edw.lib.general.enum.dag_type import DagType

if TYPE_CHECKING:
    from edw.lib.config.edw.project import Project


class Datasets(CustomList):  # pylint: disable=too-few-public-methods
    """Class representing the datasets configuration."""

    def __init__(self, project: "Project", source_type: Optional["DagType"] = None) -> None:
        """Load a list of dataset configuration objects.

        Args:
            project: Instance of the project the datasets belong to
        """
        super().__init__()
        self._list_items, self.config_directory, self.project = Datasets._get_datasets(
            project=project, source_type=source_type
        )

    @staticmethod
    def _get_datasets(
        project: "Project", source_type: Optional["DagType"] = None
    ) -> Tuple[List[Dataset], Path, "Project"]:
        """Get a list of dataset configuration objects.

        Args:
            project: Project object

        Returns:
            list_of_dataset_configuration_objects: List of dataset configuration objects.
            config_directory: The configuration directory.
            project: Project object the dataset belongs to.
        """
        config_directory = Path(CustomDirectory.get_base_dir(source_type=source_type)) / project.name

        assert config_directory.exists(), f"Path {config_directory.absolute()} does not exist."
        return_datasets = [
            Dataset(dataset_name=path_directory.stem, project=project, source_type=source_type)
            for path_directory in config_directory.iterdir()
            if path_directory.is_dir()
        ]

        return return_datasets, config_directory, project
