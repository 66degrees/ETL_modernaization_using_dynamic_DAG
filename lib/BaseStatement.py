"""This module is handles the common functionality for all classes that generate sql statements."""

# Standard libs
from typing import Any, Dict, Optional, TYPE_CHECKING

# Third-party libs
import jinja2

# Custom libs
from edw.lib.environment.variables import Variables
from edw.lib.general.io.file import File
from edw.lib.sql.concat_column import ConcatColumn
from edw.lib.general.extension.ext_str import ExtStr

if TYPE_CHECKING:
    from edw.lib.config.edw.project.dataset.table import Table
    from edw.lib.general.enum.operation_type import OperationType


class BaseStatement:
    """Base Statement Class representing the base for all other statement classes."""

    def __init__(self, table_instance: "Table", operation_type: Optional[str] = None) -> None:
        """Initialize class method.

        Args:
            table_instance: Instance of table object
        """
        self.table_instance = table_instance
        self.operation_type = operation_type
        self.project_template_params = {
            "udf_project": "gcp-gfs-datalake-edw",
        }
        self.dataset_template_params = {
            "sap_source_dataset": "sap__saphanadb_snapshot",
            "rtm": "rtm_views",
            "dataset": f"{table_instance.dataset.name}",
            "dataset_stage": f"{table_instance.dataset.name}_stage",
            "dataset_default_values": f"{table_instance.dataset.name}_default_values",
            "dataset_log": f"{table_instance.dataset.name}_log",
            "edw_stage": "edw_stage",
            "edw": "edw",
            "edw_default_values": "edw_default_values",
            "edw_log": "edw_log",
            "fsm_source_dataset": "fsm__dbo_snapshot",
            "ent__hr_admin": "ent__hr_admin_snapshot",
            "sap_api_s4": "sap__s4__api_snapshot",
            "sap_api_c4": "sap__c4__api_snapshot",
            "ent__wms_ods_admin": "ent__wms_ods_admin_snapshot",
            "ent__wms_replication_admin": "ent__wms_replication_admin_snapshot",
            "sales_forecast_ca": "sales_forecast_predictions__fs__views",
            "samsara_api_source_dataset": "samsara__api_snapshot",
            "hambroker_api_source_dataset": "hambroker__api__views",
            "sales_fs_source_dataset": "sales__fs__views",
            "stars_fs_source_dataset": "stars__fs__views",
            "roadnet_tsdba_source_dataset": "roadnet__tsdba_snapshot",
            "geocoding_source_dataset": "geocoding__views",
            "anaplan_fs_source_dataset": "anaplan__fs_snapshot",
            "anaplan_fs_source_dataset_view": "anaplan__fs__views",
            "ent_crt_admin": "ent__crt_admin_snapshot",
            "ent_crt_admin_view": "ent__crt_admin__views_current",
            "kronos_fs_source_dataset": "kronos__fs_snapshot",
            "workday_api": "workday__api_snapshot",
            "workday_fs": "workday__fs_snapshot",
            "agora_fs": "agora_clc__fs__views",
            "certified_transportation_dataset": "certified_transportation_dataset",
            "transportation_certified_dataset": "transportation_certified_dataset",
            "cma_admin_source_dataset": "ot1__cma_admin_snapshot",
            "samsara_api_source_view": "samsara__api__views",
        }
        self.other_template_params = {
            "env": Variables.get_environment_code(),
            "batch_id": self.table_instance.batch_id,
        }
        self.sql_snippets = {
            "extract_fixed_dq_issues": f"""
            (
            SELECT DISTINCT dqlcv.key_value
            FROM
                `gcp-gfs-datalake-edw-{Variables.get_environment_code()}.edw_log.data_quality_log_current_v` AS
                dqlcv, UNNEST(notes_array) AS na WHERE na.dq_message <> '' AND
                dqlcv.table_name = '{table_instance.name}'
            UNION DISTINCT
            SELECT DISTINCT key_value
            FROM
                `gcp-gfs-datalake-edw-{{{{ params.env }}}}.{{{{ params.edw_stage }}}}.reload_{table_instance.name}`
            )
            """,
            "date_limit": self._compose_date_limit_snippet(self.table_instance.get_date_limit()),
        }

    def _apply_jinja_template(
        self, sql_statement: Optional[str], template_param_overrides: Optional[Dict] = None
    ) -> str:
        """Apply templating rendering to SQL statement.

        Arguments:
            sql_statement: statement to apply rendering to
            template_param_overrides: any value overrides
        """
        template_param_overrides = template_param_overrides or {}
        result = jinja2.Template(source=(sql_statement or "")).render(
            snippets=self.sql_snippets,
            params={
                **self.project_template_params,
                **self.dataset_template_params,
                **self.other_template_params,
                **template_param_overrides,
            },
        )
        result = jinja2.Template(source=(result or "")).render(
            params={
                **self.project_template_params,
                **self.dataset_template_params,
                **self.other_template_params,
                **template_param_overrides,
            },
        )
        return result

    def _get_sql_statement(self) -> Optional[str]:
        """Get a non-formatted SQL statement that performs a function specified by this class's subclasses."""
        raise NotImplementedError()

    def _validate_template_param_overrides(self, template_param_overrides: Dict) -> None:
        """Validate that the overrides are supported keys.

        This method will raise an assertion error if a key is not supported.

        Arguments:
            template_param_overrides: dict of parameter overrides (from default values set)
        Returns:
            None
        """
        _list_keys_valid = [*self.project_template_params, *self.dataset_template_params, *self.other_template_params]
        for key in template_param_overrides:
            if key not in _list_keys_valid:
                raise KeyError(
                    f"Key '{key}' not recognized. "
                    "Make sure there is a default provided in {self.__class__}'s initialization."
                )

    def get_sql_statement(
        self,
        template_param_overrides: Optional[Dict] = None,
        ignore_none_values_in_override: bool = True,
        do_apply_ending_semicolon: bool = True,
    ) -> Optional[str]:
        """Generate full SQL statement with optional key-value overrides.

        Arguments:
            template_param_overrides: dict of parameter overrides (from default values set)
            ignore_none_values_in_override: ignore key-value pair in override if the value is None
            do_apply_ending_semicolon: whether or not a semicolon should be added to the end of the sql_statement

        Returns:
            Generated SQL statement
        """
        template_param_overrides = template_param_overrides or {}

        self._validate_template_param_overrides(template_param_overrides=template_param_overrides)

        if ignore_none_values_in_override:
            template_param_overrides = {k: v for k, v in template_param_overrides.items() if v is not None}
            # ^ Remove key value pairs if the value is None

        sql_statement = self._apply_jinja_template(
            sql_statement=self._get_sql_statement(),
            template_param_overrides=template_param_overrides,
        )

        if do_apply_ending_semicolon:
            sql_statement = BaseStatement.apply_ending_semi_colon(sql_statement=sql_statement) or "\n;"

        return sql_statement

    @property
    def pk_list(self) -> str:
        """Get a list of primary keys.

        Returns:
            a list of primary keys comma separated
        """
        return ", ".join(self.table_instance.pk_list) if self.table_instance.pk_list else ""

    def compose_pk_list(self, alias: Optional[str] = None) -> str:
        """Get the join_keys for the composite keys.

        There is a need to be able to return the join key with an alias prefix

        Args:
            alias: optional table alias

        Returns:
            Concatenated primary keys
        """
        return ConcatColumn.compose_concat_column(column_list=self.table_instance.pk_list, alias=alias)

    @property
    def stage_table_name(self) -> str:
        """Get the name of the staging merge table.

        Returns:
            Name of the staging merge table
        """
        return (
            f"`gcp-gfs-datalake-edw-{{{{ params.env }}}}.{{{{ params.edw_stage }}}}."
            f"stg_{self.table_instance.source_name}`"
        )

    @property
    def type1_concat_column(self) -> Optional[str]:
        """Type 1 column concatenation.

        Some table SQL files will only have one of the table types rather than both table types.
        Therefore, we need to determine here if we have only one or both and create the
        correct return based on the table file contents.  This avoids getting a
        runtime error that would be thrown downstream for passing in an empty concat_column value.

        Returns:
            Concatenated columns
        """
        if len(self.table_instance.type1_columns or []) > 0:
            type1_farm_fingerprint_columns = self.table_instance.type1_columns.copy()
            return ConcatColumn.compose_concat_column(column_list=type1_farm_fingerprint_columns)

        return None

    @property
    def type2_concat_column(self) -> Optional[str]:
        """Type 2 column concatenation.

        Some table SQL files will only have one of the table types rather than both table types.
        Therefore, we need to determine here if we have only one or both and create the
        correct return based on the table file contents.  This avoids getting a
        runtime error that would be thrown downstream for passing in an empty concat_column value.

        Returns:
            Concatenated columns
        """
        if len(self.table_instance.type2_columns or []) > 0:
            return ConcatColumn.compose_concat_column(column_list=self.table_instance.type2_columns)

        return None

    @property
    def edw_log_data_quality_log(self) -> str:
        """Property to determine the data quality log full table name.

        Returns:
            full dataset name in format `project_id.dataset_id.table_name`
        """
        return "`gcp-gfs-datalake-edw-{{ params.env }}.{{ params.edw_log }}.data_quality_log`"

    def _get_query_from_file(self, operation_type: "OperationType", required: Optional[bool] = True) -> Optional[str]:
        """Get the sql from a file.

        Args:
            operation_type: Type of sql operation.
            required: Is the sql_statement required. If the file is not found and required then an error is raised.

        Returns:
            Raw sql statement as in the file.
        """
        file_path = self.table_instance.sql_location / self.table_instance.get_sql_file_name(
            operation_type=operation_type
        )
        return File.read(file_path=file_path, required=required)

    def _get_derived_query_from_file(
        self, operation_type: Optional[str], required: Optional[bool] = True
    ) -> Optional[str]:
        """Get the sql from a file.

        Args:
            operation_type: Type of sql operation.
            required: Is the sql_statement required. If the file is not found and required then an error is raised.

        Returns:
            Raw sql statement as in the file.
        """
        file_path = self.table_instance.derived_sql_location / self.table_instance.get_derived_sql_file_name(
            operation_type=operation_type
        )

        return File.read(file_path=file_path, required=required)

    @staticmethod
    def apply_ending_semi_colon(sql_statement: Optional[str]) -> Optional[str]:
        """Apply semicolon at the end of the sql statement.

        Args:
            sql_statement: sql statement to apply semicolon to.

        Returns:
            sql statement with semicolon.
        """
        sql_statement = BaseStatement.remove_ending_semi_colon(sql_statement=sql_statement)
        if not ExtStr(sql_statement).has_content():
            return sql_statement

        return f"{sql_statement}\n;"

    @staticmethod
    def remove_ending_semi_colon(sql_statement: Optional[str]) -> Optional[str]:
        """Remove the ending semicolon only.

        Note: This will not remove the semicolon, if the semicolon is followed by other sql

        Args:
            sql_statement: sql statement to apply semicolon to.

        Returns:
            sql statement without semicolon.
        """
        if sql_statement is None:
            return sql_statement

        if not ExtStr(sql_statement).has_content() or not ExtStr(sql_statement).r_has_value(";"):
            return sql_statement.strip()

        index = ExtStr(sql_statement).r_value_index(";")
        first_part = sql_statement[:index].strip()
        last_part = sql_statement[index:].strip()
        if last_part == ";":
            return first_part
        return sql_statement.strip()

    @staticmethod
    def _compose_date_limit_snippet(date_limit: Optional[Dict[Any, Any]]) -> str:
        """Get the where condition using date_limit.

        Args:
            date_limit: dictionary of table columns considered for date limit

        Returns:
            Concatenated date limit where condition
        """
        if date_limit is None or date_limit == {}:
            return ""

        return "AND " + " AND ".join(f"{key} >= '{value}'" for key, value in date_limit.items())
