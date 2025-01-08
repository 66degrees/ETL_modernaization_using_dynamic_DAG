"""This module is handles the creation of dynamic merge statements."""

# Standard libs
from typing import List, Optional, TYPE_CHECKING
from google.api_core.exceptions import BadRequest

# Custom libs
from edw.lib.sql.base_statement import BaseStatement
from edw.gfs.cloud.bigquery import get_bq_client

if TYPE_CHECKING:
    from edw.lib.config.edw.project.dataset.table import Table


class MergeStatement(BaseStatement):  # pylint: disable=too-few-public-methods
    """Dynamic Merge Statement Class."""

    audit_columns = [
        "source_last_update_tmstmp",
        "create_tmstmp",
        "last_update_tmstmp",
        "last_update_by_txt",
        "row_status_code",
        "batch_id",
    ]

    type2_columns = [
        "type2_fingerprint",
        "effect_start_date",
        "effect_end_date",
        "active_record_flag",
    ]

    def __init__(self, table_instance: "Table") -> None:
        """Initialize class.

        Args:
            table_instance: Instance of table object
        """
        super().__init__(table_instance=table_instance)

    def _get_join_key(self, alias: Optional[str] = None) -> str:
        """Get the join keys for a table.

        - The join key is based on the primary keys that are defined for a table.
        - Table aliases are supported.
        - If a compound primary key is defined, all columns in the primary key should be concatenated together and
        each column should be cast to a string.
        - If the primary key is not a compound key, then the column is returned with an alias if one has been
        specified but the column is NOT cast to a string. It is returned as is.

        Args:
            alias: optional table alias

        Returns:
            Concatenated keys
        """
        concat_string = ",'-',"

        pk_cols_with_alias = [f"{alias}.{pk_col}" if alias else pk_col for pk_col in self.table_instance.pk_list]

        if not self.table_instance.pk_list:
            raise RuntimeError("No primary key defined. Cannot determine join key for table.")

        if len(self.table_instance.pk_list) == 1:
            ret_val = pk_cols_with_alias[0]
        else:
            ret_val = f"CONCAT({concat_string.join(f'CAST({pk_col} AS STRING)' for pk_col in pk_cols_with_alias)})"

        return ret_val

    @staticmethod
    def _get_join_statement(columns: List[str]) -> str:
        """Get the join statements based on the columns.

        Args:
            columns : list of columns

        Returns:
            string of `targettbl.col = srctbl.col` pair
        """
        return ", ".join(f"targettbl.{column} = srctbl.{column}" for column in columns)

    def _get_stage_table_name(self) -> str:
        """Get the name of the staging merge table.

        Returns:
            Name of the staging merge table
        """
        # NOTE: Double brackets `{{` are transformed to a single bracket `{` inside a python f-string
        return (
            f"`gcp-gfs-datalake-edw-{{{{ params.env }}}}.{{{{ params.edw_stage }}}}."
            f"stg_{self.table_instance.source_name}`"
        )

    def _get_default_value_table_name(self) -> str:
        """Get the name of the default_value table.

        Returns:
            Name of the default_value table
        """
        # NOTE: Double brackets `{{` are transformed to a single bracket `{` inside a python f-string
        return (
            f"`gcp-gfs-datalake-edw-{{{{ params.env }}}}.{{{{ params.edw_default_values }}}}."
            f"{self.table_instance.source_name}_default_values`"
        )

    def _get_target_table_name(self) -> str:
        """Get the name of the target merge table.

        Returns:
            Name of the target merge table
        """
        # NOTE: Double brackets `{{` are transformed to a single bracket `{` inside a python f-string
        return f"`gcp-gfs-datalake-edw-{{{{ params.env }}}}.{{{{ params.edw }}}}.{self.table_instance.source_name}`"

    def _get_table_hk_sql_statement(self, source_table: str = "default_value") -> str:
        """Get the table_hk SQL statement.

        Args:
            source_table : source table to get table_hk

        Returns:
            SQL statement for table hk
        """
        if not self.table_instance.is_scd_type_2:
            return ""

        if source_table == "stg" and not self.table_instance.toggle_sk:
            return f", NULL AS {self.table_instance.table_hk}"

        if source_table == "stg" and self.table_instance.toggle_sk:
            return f", NULL AS {self.table_instance.table_hk}, NULL AS {self.table_instance.table_sk}"

        if self.table_instance.toggle_sk:
            return (
                f", {self.table_instance.table_hk} AS {self.table_instance.table_hk}"
                f", {self.table_instance.table_sk} AS {self.table_instance.table_sk}"
            )

        return f", {self.table_instance.table_hk} AS {self.table_instance.table_hk}"

    def _get_type2_insert_columns(self) -> str:
        """Get the type2_insert_columns SQL statement.

        Returns:
            SQL statement for type2 specific columns
        """
        statement = ""

        if self.table_instance.toggle_sk:
            statement = "".join(
                f",{self.table_instance.table_hk},{self.table_instance.table_sk},"
                f"{','.join(column for column in self.type2_columns)}"
            )

        if not self.table_instance.toggle_sk:
            statement = f",{self.table_instance.table_hk},{','.join(column for column in self.type2_columns)}"

        return statement if self.table_instance.is_scd_type_2 else ""

    def _get_type2_insert_values(self) -> str:
        """Get the type2_insert_values SQL statement.

        Returns:
            SQL statement for type2 specific values
        """
        sql_statement = ""

        if self.table_instance.toggle_sk:
            sql_statement = ", ".join(
                [
                    f", COALESCE(srctbl.{self.table_instance.table_hk}",
                    "generate_uuid())",
                    f"COALESCE(srctbl.{self.table_instance.table_sk}",
                    f"FARM_FINGERPRINT(CONCAT({self.compose_pk_list()}, '-'",
                    "CAST(CASE WHEN srctbl.join_key IS NULL THEN curr_date ELSE "
                    + "`{{ params.udf_project }}-{{ params.env }}.udfs.default_min_date`() END AS STRING))))",
                    "srctbl.type2_fingerprint",
                    "CASE WHEN srctbl.join_key IS NULL THEN curr_date ELSE "
                    + "`{{ params.udf_project }}-{{ params.env }}.udfs.default_min_date`() END",
                    "DATE(9999,12,31)",
                    "'T'",
                ]
            )

        if not self.table_instance.toggle_sk:
            sql_statement = ", ".join(
                [
                    f",COALESCE(srctbl.{self.table_instance.table_hk}",
                    "generate_uuid())",
                    "srctbl.type2_fingerprint",
                    "CASE WHEN srctbl.join_key IS NULL THEN curr_date ELSE "
                    + "`{{ params.udf_project }}-{{ params.env }}.udfs.default_min_date`() END",
                    "DATE(9999,12,31)",
                    "'T'",
                ]
            )

        return sql_statement if self.table_instance.is_scd_type_2 else ""

    def _get_table_type2_fingerprint(self, alias: str = "") -> str:
        """Get the _get_table_type2_fingerprint SQL statement.

        Returns:
            SQL statement for table type2_fingerprint
        """
        table_alias = "" if alias == "" else alias + "."

        return f",{table_alias}type2_fingerprint" if self.table_instance.is_scd_type_2 else ""

    def _get_audit_columns(self) -> List[str]:
        """Get the audit columns for select statement.

        Returns:
            list of audit columns in sorted order
        """
        return sorted(set(self.audit_columns).difference(("create_tmstmp", "last_update_by_txt", "last_update_tmstmp")))

    def _get_stage_select_statement(self) -> str:
        """Get the select statement for stage table.

        Returns:
            string of SQL statement
        """
        select_cols = ", ".join(self.table_instance.type1_columns + self.table_instance.type2_columns)

        return "\n".join(
            [
                "SELECT",
                f"{self._get_join_key()} AS join_key",
                f"{self._get_table_hk_sql_statement(source_table='stg')}",
                f"{self._get_table_type2_fingerprint()}",
                ",type1_fingerprint",
                f",{', '.join(self.table_instance.pk_list)}",
                f",{select_cols}",
                f",{', '.join(self._get_audit_columns())}",
                "FROM",
                f"{self._get_stage_table_name()} ",
                "WHERE key_row_number = 1",
            ]
        )

    def _get_default_values_select_statement(self) -> str:
        """Get the select statement for default value view.

        Returns:
            string of SQL statement
        """
        select_cols = ", ".join(self.table_instance.type1_columns + self.table_instance.type2_columns)

        return "\n".join(
            [
                "SELECT",
                f"{self._get_join_key()} AS join_key",
                f"{self._get_table_hk_sql_statement()}",
                f"{self._get_table_type2_fingerprint()}",
                ",type1_fingerprint",
                f",{', '.join(self.table_instance.pk_list)}",
                f",{select_cols}",
                f",{', '.join(self._get_audit_columns())}",
                f"FROM {self._get_default_value_table_name()}",
            ]
        )

    def _get_dimension_select_statement(self) -> str:
        """Get the select statement for dimension table.

        Returns:
            string of SQL statement
        """
        table_alias = "stg"

        select_cols = ", ".join(
            f"{table_alias}.{column}"
            for column in self.table_instance.type1_columns + self.table_instance.type2_columns
        )

        return "\n".join(
            [
                "SELECT",
                "NULL AS join_key",
                f"{self._get_table_hk_sql_statement(source_table='stg')}",
                f"{self._get_table_type2_fingerprint(alias=table_alias)}",
                ",stg.type1_fingerprint",
                f",{', '.join(f'{table_alias}.{column}' for column in self.table_instance.pk_list)}",
                f",{select_cols}",
                f",{', '.join(f'{table_alias}.{column}' for column in self._get_audit_columns())}",
                "FROM",
                f"{self._get_stage_table_name()} stg",
                f"JOIN {self._get_target_table_name()} dim",
                "ON",
                f"{self._get_join_key(alias='stg')} = {self._get_join_key(alias='dim')}",
                "WHERE",
                "dim.active_record_flag = 'T'",
                "AND dim.effect_start_date <> curr_date",
                "AND stg.type2_fingerprint <> dim.type2_fingerprint",
                "AND stg.key_row_number = 1",
            ]
        )

    def _get_update_audit_columns(self) -> str:
        """Get the SQL update statement for metadata columns.

        Returns:
            string of SQL update statement
        """
        return ", ".join(
            [
                "targettbl.source_last_update_tmstmp = srctbl.source_last_update_tmstmp",
                "targettbl.last_update_tmstmp = CURRENT_TIMESTAMP()",
                "targettbl.last_update_by_txt = SESSION_USER()",
                "targettbl.row_status_code = srctbl.row_status_code",
                "targettbl.batch_id = array_concat(targettbl.batch_id, [srctbl.batch_id])",
            ]
        )

    def _get_insert_audit_columns(self) -> str:
        """Get the SQL insert statement for metadata columns.

        Returns:
            string of SQL statement
        """
        return ", ".join(
            [
                "source_last_update_tmstmp",
                "CURRENT_TIMESTAMP()",
                "CURRENT_TIMESTAMP()",
                "SESSION_USER()",
                "row_status_code",
                "[srctbl.batch_id]",
            ]
        )

    def _get_source_data_select_sql_statement(self) -> str:
        """Get the SQL select statement for the source data.

        Returns:
            string of select statement
        """
        sql_statement = ""

        if self.table_instance.is_scd_type_1:
            sql_statement = self._get_stage_select_statement()

            if self.table_instance.table_type in ("dimension", "lookup"):
                sql_statement += f" UNION ALL {self._get_default_values_select_statement()}"

        if self.table_instance.is_scd_type_2:
            sql_statement = self._get_stage_select_statement()

            if self.table_instance.table_type in ("dimension", "lookup"):
                sql_statement += f" UNION ALL {self._get_default_values_select_statement()}"

            sql_statement += f" UNION ALL {self._get_dimension_select_statement()}"

        return sql_statement

    @staticmethod
    def _apply_exists_check_statement(sql_statement: str) -> str:
        """Apply the SQL to check if data exists in source staging table.

        Args:
            sql_statement: SQL statement we are going to apply the exists check to.

        Returns:
            string of SQL statements.
        """
        return "\n".join(
            [
                "BEGIN",
                "DECLARE curr_date DATE; ",
                "SET curr_date = CURRENT_DATE('America/Detroit'); ",
                sql_statement.rstrip(),
                "END",
            ]
        )

    def default_hk_values(self):
        """Get default hk values for the dimension table from default_values view."""
        bq_client = get_bq_client(project=self.table_instance.dataset.project.name_with_environment_code)
        sql_statement = "".join(
            [
                f"select distinct {self.table_instance.table_hk} from ",
                f"{self.table_instance.dataset.project.name_with_environment_code}",
                f".edw_default_values.{self.table_instance.source_name}_default_values",
            ]
        )

        try:
            result = bq_client.query(query=sql_statement).result()
        except BadRequest as ex:
            print("Expection! ", ex)
            result = [["0"], ["-1"]]

        list_of_default_values = ""
        for row in result:
            list_of_default_values = ",".join([list_of_default_values, "'" + row[0] + "'"])
        return list_of_default_values[1:]

    def default_sk_values(self):
        """Get default sk values for the dimension table from default_values view."""
        bq_client = get_bq_client(project=self.table_instance.dataset.project.name_with_environment_code)
        sql_statement = "".join(
            [
                f"select distinct {self.table_instance.table_sk} from ",
                f"{self.table_instance.dataset.project.name_with_environment_code}",
                f".edw_default_values.{self.table_instance.source_name}_default_values",
            ]
        )

        try:
            result = bq_client.query(query=sql_statement).result()
        except BadRequest as ex:
            print("Expection! ", ex)
            result = [[0], [-1]]

        list_of_default_values = ""
        for row in result:
            list_of_default_values = ",".join([list_of_default_values, str(row[0])])
        return list_of_default_values[1:]

    def _get_update_sql_statement(self) -> str:
        """Get the SQL update statement.

        Returns:
            string of SQL update statement for scd type 1 and 2 accordingly
        """
        sql_statement = ""
        update_audit_columns = ""

        if self.table_instance.is_scd_type_1 or self.table_instance.is_scd_type_2:
            update_audit_columns = self._get_update_audit_columns()

        if self.table_instance.is_scd_type_2:
            hk_values = self.default_hk_values()
            update_join = self._get_join_statement(
                columns=self.table_instance.type1_columns + self.table_instance.type2_columns
            )
            if self.table_instance.toggle_sk:
                sk_values = self.default_sk_values()
                sql_statement = "\n".join(
                    [
                        "WHEN MATCHED",
                        "AND targettbl.active_record_flag = 'T'",
                        "AND targettbl.effect_start_date <> curr_date",
                        "AND COALESCE(targettbl.type2_fingerprint, 0) <> COALESCE(srctbl.type2_fingerprint, 0)",
                        f"AND targettbl.{self.table_instance.table_hk} not in ({hk_values})",
                        f"AND targettbl.{self.table_instance.table_sk} not in ({sk_values})",
                        "THEN",
                        "UPDATE SET",
                        "targettbl.active_record_flag = 'F'",
                        ", targettbl.effect_end_date = DATE_SUB(curr_date, INTERVAL 1 day), ",
                        update_audit_columns,
                        "WHEN MATCHED",
                        "AND targettbl.active_record_flag = 'T'",
                        "AND (targettbl.effect_start_date = curr_date",
                        f"OR targettbl.{self.table_instance.table_hk} in ({hk_values})",
                        f"OR targettbl.{self.table_instance.table_sk} in ({sk_values}))",
                        "AND COALESCE(targettbl.type2_fingerprint, 0) <> COALESCE(srctbl.type2_fingerprint, 0)",
                        "THEN",
                        "UPDATE SET",
                        "targettbl.type2_fingerprint = srctbl.type2_fingerprint",
                        ", targettbl.type1_fingerprint = srctbl.type1_fingerprint, ",
                        update_join,
                        ", ",
                        update_audit_columns,
                    ]
                )

            if not self.table_instance.toggle_sk:
                sql_statement = "\n".join(
                    [
                        "WHEN MATCHED",
                        "AND targettbl.active_record_flag = 'T'",
                        "AND targettbl.effect_start_date <> curr_date",
                        "AND COALESCE(targettbl.type2_fingerprint, 0) <> COALESCE(srctbl.type2_fingerprint, 0)",
                        f"AND targettbl.{self.table_instance.table_hk} not in ({hk_values})",
                        "THEN",
                        "UPDATE SET",
                        "targettbl.active_record_flag = 'F'",
                        ", targettbl.effect_end_date = DATE_SUB(curr_date, INTERVAL 1 day), ",
                        update_audit_columns,
                        "WHEN MATCHED",
                        "AND targettbl.active_record_flag = 'T'",
                        "AND (targettbl.effect_start_date = curr_date",
                        f"OR targettbl.{self.table_instance.table_hk} in ({hk_values}))",
                        "AND COALESCE(targettbl.type2_fingerprint, 0) <> COALESCE(srctbl.type2_fingerprint, 0)",
                        "THEN",
                        "UPDATE SET",
                        "targettbl.type2_fingerprint = srctbl.type2_fingerprint",
                        ", targettbl.type1_fingerprint = srctbl.type1_fingerprint, ",
                        update_join,
                        ", ",
                        update_audit_columns,
                    ]
                )
            update_join = self._get_join_statement(columns=self.table_instance.type1_columns)
            sql_statement += "\n".join(
                [
                    "\n",
                    "WHEN MATCHED",
                    "AND ((COALESCE(targettbl.type1_fingerprint, 0) <> COALESCE(srctbl.type1_fingerprint, 0))",
                    "AND targettbl.active_record_flag = 'T')",
                    "THEN",
                    "UPDATE SET",
                    "targettbl.type1_fingerprint = srctbl.type1_fingerprint",
                    f", {update_join}",
                    f", {update_audit_columns}",
                ]
            )
        if self.table_instance.is_scd_type_1:
            update_join = self._get_join_statement(columns=self.table_instance.type1_columns)
            sql_statement += "\n".join(
                [
                    "\n",
                    "WHEN MATCHED",
                    "AND COALESCE(targettbl.type1_fingerprint, 0) <> COALESCE(srctbl.type1_fingerprint, 0)",
                    "THEN",
                    "UPDATE SET",
                    "targettbl.type1_fingerprint = srctbl.type1_fingerprint",
                    f", {update_join}",
                    f", {update_audit_columns}",
                ]
            )

        return sql_statement

    def _get_insert_sql_statement(self) -> str:
        """Get the SQL insert statement.

        Returns:
            string of SQL insert statement
        """
        all_cols = self.table_instance.pk_list + self.table_instance.type1_columns + self.table_instance.type2_columns
        insert_cols = ", ".join(all_cols)
        values_cols = ", ".join(f"srctbl.{col}" for col in all_cols)

        return "\n".join(
            [
                "WHEN NOT MATCHED THEN",
                "INSERT",
                "(",
                "type1_fingerprint",
                f"{self._get_type2_insert_columns()}",
                f", {insert_cols}",
                f", {', '.join(self.audit_columns)}",
                ")",
                "VALUES",
                "(",
                "srctbl.type1_fingerprint",
                f"{self._get_type2_insert_values()}",
                f",{values_cols}",
                f",{self._get_insert_audit_columns()}",
                ")",
            ]
        )

    def _get_delete_sql_statement(self) -> str:
        """Get the SQL delete statement.

        Returns:
            string of SQL delete statement
        """
        if self.table_instance.table_type == "snapshot_fact":
            return "\n".join(
                [
                    "WHEN NOT MATCHED BY SOURCE",
                    "AND targettbl.snapshot_date = (SELECT MAX(snapshot_date) AS snapshot_date",
                    f"FROM {self._get_stage_table_name()})",
                    "THEN DELETE",
                ]
            )

        return ""

    def _get_merge_sql_statement(self) -> str:
        """Get the SQL merge statement.

        Returns:
            string of SQL merge statement
        """
        target_table_name = self._get_target_table_name()
        source_select = self._get_source_data_select_sql_statement()
        join_key = self._get_join_key(alias="targettbl")
        update_sql = self._get_update_sql_statement()
        delete_sql = self._get_delete_sql_statement()
        insert_sql = self._get_insert_sql_statement()

        return "\n".join(
            [
                f"MERGE INTO {target_table_name} targettbl",
                f"USING ({source_select}) srctbl",
                f"ON {join_key} = srctbl.join_key",
                update_sql,
                insert_sql,
                delete_sql,
                ";",
            ]
        )

    def _get_sql_statement(self) -> str:
        """Get the SQL merge statement.

        Returns:
            String of SQL merge statement.
        """
        return self._apply_exists_check_statement(sql_statement=self._get_merge_sql_statement())
