"""
Async Snowflake adapter for SQL Batcher.

This module provides a Snowflake-specific async adapter for SQL Batcher with
optimizations for Snowflake's features and limitations.
"""

from typing import Any, Dict, List, Optional

from sql_batcher.adapters.async_base import AsyncSQLAdapter

try:
    import asyncio
    import snowflake.connector

    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False


class AsyncSnowflakeAdapter(AsyncSQLAdapter):
    """
    Async adapter for Snowflake database connections.

    This adapter is optimized for Snowflake's specific features, including:
    - Multi-statement support
    - Warehouse management
    - Transaction support
    - Snowflake-specific data types

    Note: This adapter uses the synchronous Snowflake connector in an async wrapper
    since there is no official async Snowflake connector yet. Operations are
    executed in a thread pool to avoid blocking the event loop.

    Args:
        account: Snowflake account identifier
        user: Username for authentication
        password: Password for authentication
        warehouse: Default warehouse to use
        database: Default database to use
        schema: Default schema to use
        role: Default role to use
        max_query_size: Optional maximum query size in bytes
        **kwargs: Additional connection parameters
    """

    def __init__(
        self,
        account: str,
        user: str,
        password: str,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        role: Optional[str] = None,
        max_query_size: Optional[int] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the async Snowflake adapter."""
        if not SNOWFLAKE_AVAILABLE:
            raise ImportError(
                "snowflake-connector-python package is required for AsyncSnowflakeAdapter. "
                "Install it with: pip install snowflake-connector-python"
            )

        self._account = account
        self._user = user
        self._password = password
        self._warehouse = warehouse
        self._database = database
        self._schema = schema
        self._role = role
        self._max_query_size = max_query_size or 10_000_000  # Default to 10MB
        self._kwargs = kwargs
        self._connection = None
        self._cursor = None

    async def connect(self) -> None:
        """Connect to the Snowflake server asynchronously."""
        if not self._connection:
            # Execute in a thread pool to avoid blocking
            self._connection = await asyncio.to_thread(
                snowflake.connector.connect,
                account=self._account,
                user=self._user,
                password=self._password,
                warehouse=self._warehouse,
                database=self._database,
                schema=self._schema,
                role=self._role,
                **self._kwargs,
            )
            self._cursor = self._connection.cursor()

    async def get_max_query_size(self) -> int:
        """
        Get the maximum query size in bytes.

        Snowflake has a practical limit of 1MB for SQL text,
        but we use a more conservative default for better performance.

        Returns:
            Maximum query size in bytes
        """
        return self._max_query_size

    async def execute(self, sql: str) -> List[Any]:
        """
        Execute a SQL statement asynchronously and return results.

        Snowflake supports executing multiple statements in a single query
        by separating them with semicolons. This allows SQL Batcher to combine
        multiple statements into a single execution for better performance.

        Args:
            sql: SQL statement(s) to execute

        Returns:
            List of result rows (for SELECT queries) or empty list for others
        """
        if not self._connection:
            await self.connect()

        try:
            # Execute in a thread pool to avoid blocking
            cursor = await asyncio.to_thread(self._connection.cursor)
            result = await asyncio.to_thread(cursor.execute, sql)

            # For SELECT statements, return the results
            if sql.strip().upper().startswith("SELECT"):
                rows = await asyncio.to_thread(result.fetchall)
                return list(rows) if rows is not None else []
            return []
        except Exception as e:
            # Add context to the error
            raise Exception(f"Snowflake Error executing SQL: {str(e)}") from e

    async def begin_transaction(self) -> None:
        """Begin a transaction asynchronously."""
        if not self._connection:
            await self.connect()

        await asyncio.to_thread(self._connection.cursor().execute, "BEGIN")

    async def commit_transaction(self) -> None:
        """Commit the current transaction asynchronously."""
        if not self._connection:
            raise ValueError("No connection to commit")

        await asyncio.to_thread(self._connection.commit)

    async def rollback_transaction(self) -> None:
        """Rollback the current transaction asynchronously."""
        if not self._connection:
            raise ValueError("No connection to rollback")

        await asyncio.to_thread(self._connection.rollback)

    async def create_savepoint(self, name: str) -> None:
        """Create a savepoint with the given name asynchronously."""
        if not self._connection:
            await self.connect()

        await asyncio.to_thread(self._connection.cursor().execute, f"SAVEPOINT {name}")

    async def rollback_to_savepoint(self, name: str) -> None:
        """Rollback to the savepoint with the given name asynchronously."""
        if not self._connection:
            raise ValueError("No connection to rollback to savepoint")

        await asyncio.to_thread(self._connection.cursor().execute, f"ROLLBACK TO SAVEPOINT {name}")

    async def release_savepoint(self, name: str) -> None:
        """Release the savepoint with the given name asynchronously."""
        if not self._connection:
            raise ValueError("No connection to release savepoint")

        await asyncio.to_thread(self._connection.cursor().execute, f"RELEASE SAVEPOINT {name}")

    async def close(self) -> None:
        """Close the connection asynchronously."""
        if self._connection:
            if self._cursor:
                await asyncio.to_thread(self._cursor.close)
                self._cursor = None
            await asyncio.to_thread(self._connection.close)
            self._connection = None

    async def get_databases(self) -> List[str]:
        """
        Get a list of available databases asynchronously.

        Returns:
            List of database names
        """
        if not self._connection:
            await self.connect()

        cursor = await asyncio.to_thread(self._connection.cursor)
        await asyncio.to_thread(cursor.execute, "SHOW DATABASES")
        results = await asyncio.to_thread(cursor.fetchall)
        return [row[1] for row in results]  # Database name is in the second column

    async def get_schemas(self, database: Optional[str] = None) -> List[str]:
        """
        Get a list of available schemas in the specified database asynchronously.

        Args:
            database: Database name (uses default if None)

        Returns:
            List of schema names
        """
        if not self._connection:
            await self.connect()

        db_name = database or self._database
        if not db_name:
            raise ValueError("No database specified and no default database set")

        cursor = await asyncio.to_thread(self._connection.cursor)
        await asyncio.to_thread(cursor.execute, f"SHOW SCHEMAS IN DATABASE {db_name}")
        results = await asyncio.to_thread(cursor.fetchall)
        return [row[1] for row in results]  # Schema name is in the second column

    async def get_tables(self, schema: Optional[str] = None, database: Optional[str] = None) -> List[str]:
        """
        Get a list of available tables in the specified schema asynchronously.

        Args:
            schema: Schema name (uses default if None)
            database: Database name (uses default if None)

        Returns:
            List of table names
        """
        if not self._connection:
            await self.connect()

        db_name = database or self._database
        schema_name = schema or self._schema

        if not db_name:
            raise ValueError("No database specified and no default database set")
        if not schema_name:
            raise ValueError("No schema specified and no default schema set")

        cursor = await asyncio.to_thread(self._connection.cursor)
        await asyncio.to_thread(cursor.execute, f"SHOW TABLES IN {db_name}.{schema_name}")
        results = await asyncio.to_thread(cursor.fetchall)
        return [row[1] for row in results]  # Table name is in the second column

    async def get_table_schema(
        self, table: str, schema: Optional[str] = None, database: Optional[str] = None
    ) -> List[Dict[str, str]]:
        """
        Get the schema of a table asynchronously.

        Args:
            table: Table name
            schema: Schema name (uses default if None)
            database: Database name (uses default if None)

        Returns:
            List of column definitions with name and type
        """
        if not self._connection:
            await self.connect()

        db_name = database or self._database
        schema_name = schema or self._schema

        if not db_name:
            raise ValueError("No database specified and no default database set")
        if not schema_name:
            raise ValueError("No schema specified and no default schema set")

        cursor = await asyncio.to_thread(self._connection.cursor)
        await asyncio.to_thread(cursor.execute, f"DESCRIBE TABLE {db_name}.{schema_name}.{table}")
        results = await asyncio.to_thread(cursor.fetchall)

        columns = []
        for row in results:
            columns.append({"name": row[0], "type": row[1]})

        return columns

    async def execute_batch(self, statements: List[str]) -> int:
        """
        Execute multiple statements as a batch asynchronously.

        Snowflake supports multiple statements in a single query when
        separated by semicolons, so we can combine them for efficiency.

        Args:
            statements: List of SQL statements to execute

        Returns:
            Number of statements executed
        """
        if not statements:
            return 0

        # Combine statements with semicolons
        combined_sql = ";\n".join(statements) + ";"

        # Execute the combined SQL
        await self.execute(combined_sql)

        return len(statements)

    async def set_warehouse(self, warehouse: str) -> None:
        """
        Set the active warehouse asynchronously.

        Args:
            warehouse: Warehouse name
        """
        if not self._connection:
            await self.connect()

        await self.execute(f"USE WAREHOUSE {warehouse}")
        self._warehouse = warehouse

    async def set_database(self, database: str) -> None:
        """
        Set the active database asynchronously.

        Args:
            database: Database name
        """
        if not self._connection:
            await self.connect()

        await self.execute(f"USE DATABASE {database}")
        self._database = database

    async def set_schema(self, schema: str) -> None:
        """
        Set the active schema asynchronously.

        Args:
            schema: Schema name
        """
        if not self._connection:
            await self.connect()

        await self.execute(f"USE SCHEMA {schema}")
        self._schema = schema

    async def set_role(self, role: str) -> None:
        """
        Set the active role asynchronously.

        Args:
            role: Role name
        """
        if not self._connection:
            await self.connect()

        await self.execute(f"USE ROLE {role}")
        self._role = role

    async def get_query_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Get the query history asynchronously.

        Args:
            limit: Maximum number of queries to return

        Returns:
            List of query history entries
        """
        if not self._connection:
            await self.connect()

        result = await self.execute(
            f"""
            SELECT
                QUERY_ID,
                QUERY_TEXT,
                DATABASE_NAME,
                SCHEMA_NAME,
                QUERY_TYPE,
                SESSION_ID,
                USER_NAME,
                ROLE_NAME,
                WAREHOUSE_NAME,
                WAREHOUSE_SIZE,
                EXECUTION_STATUS,
                ERROR_CODE,
                ERROR_MESSAGE,
                START_TIME,
                END_TIME,
                TOTAL_ELAPSED_TIME
            FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
            ORDER BY START_TIME DESC
            LIMIT {limit}
            """
        )

        history = []
        for row in result:
            history.append(
                {
                    "query_id": row[0],
                    "query_text": row[1],
                    "database": row[2],
                    "schema": row[3],
                    "query_type": row[4],
                    "session_id": row[5],
                    "user": row[6],
                    "role": row[7],
                    "warehouse": row[8],
                    "warehouse_size": row[9],
                    "status": row[10],
                    "error_code": row[11],
                    "error_message": row[12],
                    "start_time": row[13],
                    "end_time": row[14],
                    "elapsed_time_ms": row[15],
                }
            )

        return history
