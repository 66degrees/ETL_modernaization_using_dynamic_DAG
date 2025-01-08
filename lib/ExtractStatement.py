"""This module will generate extract SQL statements."""

# Standard libs
from typing import Optional

# Custom libs
from edw.lib.general.enum.operation_type import OperationType
from edw.lib.sql.base_statement import BaseStatement


class ExtractStatement(BaseStatement):
    """Class to create the extract sql statement."""

    def _apply_create_or_replace_statement(self, sql_statement: Optional[str]) -> Optional[str]:
        """Apply the `create or replace` statement to the extract sql.

        Args:
            sql_statement: sql statement we are going to apply the `create or replace` statement to.

        Returns:
            string of sql statements.
        """
        # Make sure the sql_statement within the `CREATE OR REPLACE` has a semicolon
        _sql_statement_no_semi_colon = BaseStatement.remove_ending_semi_colon(sql_statement=sql_statement)
        _sql_statement = "\n".join(
            [
                "CREATE OR REPLACE TABLE",
                "`gcp-gfs-datalake-edw-{{ params.env }}.{{ params.edw_stage }}" + f".stg_{self.table_instance.name}`",
                "OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 48 HOUR))",
                "AS",
                "(",
                _sql_statement_no_semi_colon or "",
                ")",
            ]
        )
        return BaseStatement.apply_ending_semi_colon(sql_statement=_sql_statement)

    def _get_pre_extract_statement(self) -> Optional[str]:
        """Get the extract sql statement that goes before the `CREATE OR REPLACE` statement in the extract sql.

        Returns:
            Sql statement that needs to be executed before the main extract statement that creates the staging table
        """
        # The pre extract sql statement should have a semicolon because it will go before the `CREATE OR REPLACE`
        # statement.
        return (
            BaseStatement.apply_ending_semi_colon(
                sql_statement=self._get_query_from_file(operation_type=OperationType.pre_extract, required=False)
            )
            or ""
        )

    def _get_extract_statement(self, limit: Optional[int] = None) -> Optional[str]:
        """Get the extract sql statement the creates the staging table.

        Args:
            limit: Number of rows to return. None will return all rows.

        Returns:
            Sql statement that creates the staging table.
        """
        limit_sql = f"\nLIMIT {limit}" if limit is not None else ""

        # Remove the semicolon from the sql statement because if we add the `LIMIT` to it, it should not be there.
        sql_statement = BaseStatement.remove_ending_semi_colon(
            sql_statement=f"{self._get_query_from_file(operation_type=OperationType.extract)}",
        )

        # Join the sql statement with potential `LIMIT` then add the `CREATE OR REPLACE` statement and finish it with
        # a semicolon.
        return self._apply_create_or_replace_statement(sql_statement="".join([sql_statement or "", limit_sql]))

    def _compose_sql_statement(self, limit: Optional[int] = None) -> Optional[str]:
        """Create extract SQL statement with the option to pass in how many rows we want to insert into staging table.

        Args:
            limit: Number of rows to return. By default, set to None which returns all rows.

        Returns:
            SQL statement to extract data into the staging table.
        """
        return "\n".join(
            [
                self._get_pre_extract_statement() or "",
                self._get_extract_statement(limit=limit) or "",
            ]
        )

    def _get_sql_statement(self) -> Optional[str]:
        """Create extract SQL statement.

        Note: This will join the <table>_pre_extract.sql and <table>_extract.sql files together, and wraps it in a
              `CREATE OR REPLACE` statement.

        Returns:
            SQL statement to extract data into the staging table.
        """
        return self._compose_sql_statement()

    def get_sql_statement_returning_zero_rows(self) -> Optional[str]:
        """Create extract sql statement.

        Note: This will return the get_sql_statement and makes sure no new rows are created. This is useful to create
              empty tables.

        Returns:
            SQL statement to extract data into the staging table.
        """
        return self._apply_jinja_template(sql_statement=self._compose_sql_statement(limit=0))
