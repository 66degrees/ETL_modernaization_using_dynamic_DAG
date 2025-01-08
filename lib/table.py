"""This module handles everything around edw config's tables."""

# Standard libs
from pathlib import Path
from typing import List, Optional, Tuple


# Custom libs
from edw.lib.config.base.custom_directory import CustomDirectory
from edw.lib.config.edw.project.dataset.table import Table
from edw.lib.config.edw.project.dataset import Dataset
from edw.lib.general.base.custom_list import CustomList
from edw.lib.general.enum.dag_type import DagType


class Tables(CustomList):  # pylint: disable=too-few-public-methods
    """Tables class handling collection of tables config objects."""

    def __init__(self, dataset: Dataset, source_type: Optional["DagType"] = None) -> None:
        """Initialize variables that load a list of table configuration objects.

        Args:
            dataset: Dataset object the tables belong too.
        """
        super().__init__()
        self._list_items, self.config_directory, self.dataset = Tables._get_tables(
            dataset=dataset, source_type=source_type
        )

    @staticmethod
    def _get_tables(dataset: Dataset, source_type: Optional["DagType"] = None) -> Tuple[List[Table], Path, Dataset]:
        """Get a list of table configuration objects.

        Args:
            dataset: dataset object the table belongs to

        Returns:
            list_of_table_configuration_objects: List of table configuration objects
            config_directory: The configuration directory
            dataset: The dataset the configuration tables belong to
        """
        config_directory = (
            Path(CustomDirectory.get_base_dir(source_type=source_type)) / dataset.project.name / dataset.name
        )

        return_tables = [
            Table(
                table_filename=path_file.name,
                dataset=dataset,
                source_type=source_type,
            )
            for path_file in sorted(config_directory.glob("*.json"))
        ]

        return return_tables, config_directory, dataset
