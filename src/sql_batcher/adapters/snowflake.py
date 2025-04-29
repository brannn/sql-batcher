"""
Snowflake adapter for SQL Batcher.

This module provides a Snowflake-specific adapter for SQL Batcher with
optimizations for Snowflake's features and limitations.
"""

from typing import Any, Dict, List, Optional, Tuple

from sql_batcher.adapters.base import SQLAdapter

try:
    import snowflake.connector

    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False


class SnowflakeAdapter(SQLAdapter):
    """
    Adapter for Snowflake database connections.

    This adapter is optimized for Snowflake's specific features, including:
    - Warehouse management
    - Role-based access control
    - Session management
    - Query history and monitoring

    Args:
        connection_params: Dictionary of connection parameters
        warehouse: Optional warehouse name
        role: Optional role name
        session_parameters: Optional session parameters
        **kwargs: Additional arguments passed to snowflake.connector.connect
    """

    def __init__(
        self,
        connection_params: Dict[str, Any],
        warehouse: Optional[str] = None,
        role: Optional[str] = None,
        session_parameters: Optional[Dict[str, Any]] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the Snowflake adapter."""
        if not SNOWFLAKE_AVAILABLE:
            raise ImportError(
                "snowflake-connector-python package is required for SnowflakeAdapter. "
                "Install it with: pip install snowflake-connector-python"
            )

        # Add warehouse and role if provided
        if warehouse:
            connection_params = connection_params.copy()
            connection_params["warehouse"] = warehouse
        if role:
            connection_params = connection_params.copy()
            connection_params["role"] = role

        # Add session parameters if provided
        if session_parameters:
            connection_params = connection_params.copy()
            connection_params["session_parameters"] = session_parameters

        # Add any additional kwargs
        connection_params.update(kwargs)

        # Create connection
        self._connection = snowflake.connector.connect(**connection_params)
        self._cursor = self._connection.cursor()

    def get_max_query_size(self) -> int:
        """
        Get the maximum query size in bytes.

        Snowflake has a practical limit of around 100MB for query size.

        Returns:
            Maximum query size in bytes (100,000,000)
        """
        return 100_000_000  # 100MB default limit

    def execute(self, sql: str) -> List[Tuple[Any, ...]]:
        """
        Execute a SQL statement and return results.

        Args:
            sql: SQL statement to execute

        Returns:
            List of result rows as tuples
        """
        if self._connection is None:
            raise ValueError("No connection available")

        cursor = self._connection.cursor()
        if cursor is None:
            return []

        cursor.execute(sql)

        if cursor.description is None:
            return []

        result = cursor.fetchall()
        return list(result) if result is not None else []

    def begin_transaction(self) -> None:
        """Begin a transaction."""
        if self._connection is not None:
            self._connection.autocommit = False

    def commit_transaction(self) -> None:
        """Commit the current transaction."""
        if self._connection is not None:
            self._connection.commit()

    def rollback_transaction(self) -> None:
        """Rollback the current transaction."""
        if self._connection is not None:
            self._connection.rollback()

    def close(self) -> None:
        """Close the connection."""
        if hasattr(self, "_cursor") and self._cursor is not None:
            self._cursor.close()
        if hasattr(self, "_connection") and self._connection is not None:
            self._connection.close()

    def set_warehouse(self, warehouse: str) -> None:
        """
        Set the current warehouse.

        Args:
            warehouse: Warehouse name
        """
        if self._connection is not None:
            self._connection.cursor().execute(f"USE WAREHOUSE {warehouse}")

    def set_role(self, role: str) -> None:
        """
        Set the current role.

        Args:
            role: Role name
        """
        if self._connection is not None:
            self._connection.cursor().execute(f"USE ROLE {role}")

    def set_session_parameter(self, name: str, value: Any) -> None:
        """
        Set a session parameter.

        Args:
            name: Parameter name
            value: Parameter value
        """
        if self._connection is not None:
            self._connection.cursor().execute(f"ALTER SESSION SET {name} = {value}")

    def get_warehouses(self) -> List[str]:
        """
        Get available warehouses.

        Returns:
            List of warehouse names
        """
        result = self.execute("SHOW WAREHOUSES")
        return [row[0] for row in result]

    def get_roles(self) -> List[str]:
        """
        Get available roles.

        Returns:
            List of role names
        """
        result = self.execute("SHOW ROLES")
        return [row[0] for row in result]

    def get_databases(self) -> List[str]:
        """
        Get available databases.

        Returns:
            List of database names
        """
        result = self.execute("SHOW DATABASES")
        return [row[0] for row in result]

    def get_schemas(self, database: str) -> List[str]:
        """
        Get available schemas in a database.

        Args:
            database: Database name

        Returns:
            List of schema names
        """
        result = self.execute(f"SHOW SCHEMAS IN DATABASE {database}")
        return [row[0] for row in result]

    def get_tables(self, database: str, schema: str) -> List[str]:
        """
        Get available tables in a schema.

        Args:
            database: Database name
            schema: Schema name

        Returns:
            List of table names
        """
        result = self.execute(f"SHOW TABLES IN {database}.{schema}")
        return [row[0] for row in result]

    def get_columns(self, table: str, database: str, schema: str) -> List[Dict[str, str]]:
        """
        Get column information for a table.

        Args:
            table: Table name
            database: Database name
            schema: Schema name

        Returns:
            List of column information dictionaries
        """
        result = self.execute(f"DESCRIBE TABLE {database}.{schema}.{table}")
        return [{"name": row[0], "type": row[1], "comment": row[2]} for row in result]
