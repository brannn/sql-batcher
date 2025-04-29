"""
Async Trino adapter for SQL Batcher.

This module provides a Trino-specific async adapter for SQL Batcher with
optimizations for Trino's features and limitations.
"""

import json
from typing import Any, Dict, List, Optional, Type

from sql_batcher.adapters.async_base import AsyncSQLAdapter
from sql_batcher.retry import CircuitBreaker, async_retry

try:
    import aiotrino

    AIOTRINO_AVAILABLE = True
except ImportError:
    AIOTRINO_AVAILABLE = False


class AsyncTrinoAdapter(AsyncSQLAdapter):
    """
    Async adapter for Trino database connections using aiotrino.

    This adapter is optimized for Trino's specific features and limitations, including:
    - Query size limits
    - Statement batching
    - Transaction support
    - Catalog and schema management

    Args:
        host: Trino server hostname
        port: Trino server port
        user: Username for authentication
        catalog: Default catalog to use
        schema: Default schema to use
        role: Trino role to use (will be set as 'x-trino-role' HTTP header)
        max_query_size: Optional maximum query size in bytes
        **kwargs: Additional connection parameters
    """

    def __init__(
        self,
        host: str,
        port: int = 8080,
        user: str = "trino",
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        role: Optional[str] = None,
        max_query_size: Optional[int] = None,
        retry_attempts: int = 3,
        retry_delay: float = 0.5,
        retry_max_delay: float = 10.0,
        retry_backoff_factor: float = 2.0,
        retry_jitter: bool = True,
        retry_exceptions: Optional[List[Type[Exception]]] = None,
        circuit_breaker_enabled: bool = True,
        circuit_breaker_failure_threshold: int = 5,
        circuit_breaker_recovery_timeout: float = 30.0,
        **kwargs: Any,
    ) -> None:
        """Initialize the async Trino adapter."""
        if not AIOTRINO_AVAILABLE:
            raise ImportError("aiotrino package is required for AsyncTrinoAdapter. " "Install it with: pip install aiotrino")

        self._host = host
        self._port = port
        self._user = user
        self._catalog = catalog
        self._schema = schema
        self._role = role
        self._max_query_size = max_query_size or 600_000  # Default to 600KB
        self._kwargs = kwargs
        self._connection = None
        self._transaction_id = None

        # Retry configuration
        self._retry_attempts = retry_attempts
        self._retry_delay = retry_delay
        self._retry_max_delay = retry_max_delay
        self._retry_backoff_factor = retry_backoff_factor
        self._retry_jitter = retry_jitter
        self._retry_exceptions = retry_exceptions or [
            # Common Trino exceptions that are retryable
            Exception,  # Placeholder for specific Trino exceptions
        ]

        # Circuit breaker configuration
        self._circuit_breaker_enabled = circuit_breaker_enabled
        if circuit_breaker_enabled:
            self._circuit_breaker = CircuitBreaker(
                failure_threshold=circuit_breaker_failure_threshold,
                recovery_timeout=circuit_breaker_recovery_timeout,
            )

    async def connect(self) -> None:
        """Connect to the Trino server."""
        if not self._connection:
            # Prepare connection parameters
            conn_params = {
                "host": self._host,
                "port": self._port,
                "user": self._user,
                "catalog": self._catalog,
                "schema": self._schema,
            }

            # Handle HTTP headers and role
            if self._role:
                # Get existing headers or create new dict
                headers = self._kwargs.get("http_headers", {}).copy() if "http_headers" in self._kwargs else {}
                # Set the x-trino-role header with the specified role
                headers["x-trino-role"] = f"system=ROLE{{{self._role}}}"
                conn_params["http_headers"] = headers

            # Add any additional kwargs
            conn_params.update(self._kwargs)

            # Create the connection
            self._connection = aiotrino.dbapi.Connection(**conn_params)

    async def get_max_query_size(self) -> int:
        """
        Get the maximum query size in bytes.

        Trino has a practical limit of 1MB for query size.
        We use a 600KB limit to provide a buffer and reduce the likelihood of hitting the limit.

        Returns:
            Maximum query size in bytes
        """
        return self._max_query_size

    async def _execute_with_retry(self, sql: str) -> List[Any]:
        """
        Execute a SQL statement with retry logic.

        This is the internal implementation that will be wrapped with retry logic.

        Args:
            sql: SQL statement(s) to execute

        Returns:
            List of result rows (for SELECT queries) or empty list for others
        """
        if not self._connection:
            await self.connect()

        # Optimization: Check if this is a single statement
        if ";" not in sql or sql.count(";") == 1 and sql.strip().endswith(";"):
            # Single statement case - avoid the split/join overhead
            statement = sql.strip()
            if statement.endswith(";"):
                statement = statement[:-1].strip()

            if not statement:
                return []

            cursor = await self._connection.cursor()
            await cursor.execute(statement)

            # For SELECT statements, return the results
            if statement.upper().startswith("SELECT"):
                rows = await cursor.fetchall()
                return rows
            return []
        else:
            # Multiple statements case
            statements = [s.strip() for s in sql.split(";") if s.strip()]
            results = []

            # Optimization: Use a single cursor for all statements
            cursor = await self._connection.cursor()

            for statement in statements:
                # Skip empty statements
                if not statement:
                    continue

                # Execute the statement
                await cursor.execute(statement)

                # For SELECT statements, return the results
                if statement.upper().startswith("SELECT"):
                    rows = await cursor.fetchall()
                    results.extend(rows)

            return results

    async def execute(self, sql: str) -> List[Any]:
        """
        Execute a SQL statement asynchronously and return results.

        Trino doesn't support executing multiple statements in a single query,
        so we split the statements and execute them individually.

        This method includes retry logic and circuit breaker protection.

        Args:
            sql: SQL statement(s) to execute

        Returns:
            List of result rows (for SELECT queries) or empty list for others
        """
        try:
            # Apply circuit breaker if enabled
            if self._circuit_breaker_enabled:
                execute_func = self._circuit_breaker.async_call(self._execute_with_retry)
            else:
                execute_func = self._execute_with_retry

            # Apply retry logic
            retry_execute = async_retry(
                max_attempts=self._retry_attempts,
                base_delay=self._retry_delay,
                max_delay=self._retry_max_delay,
                backoff_factor=self._retry_backoff_factor,
                jitter=self._retry_jitter,
                retryable_exceptions=self._retry_exceptions,
            )(execute_func)

            # Execute with retry and circuit breaker protection
            return await retry_execute(sql)
        except Exception as e:
            # Add context to the error
            raise Exception(f"Trino Error executing SQL: {str(e)}") from e

    async def begin_transaction(self) -> None:
        """Begin a transaction asynchronously."""
        if not self._connection:
            await self.connect()

        # Trino requires explicit transaction management
        cursor = await self._connection.cursor()
        await cursor.execute("START TRANSACTION")
        self._transaction_id = cursor.transaction_id

    async def commit_transaction(self) -> None:
        """Commit the current transaction asynchronously."""
        if not self._connection or not self._transaction_id:
            raise ValueError("No active transaction to commit")

        cursor = await self._connection.cursor()
        await cursor.execute("COMMIT")
        self._transaction_id = None

    async def rollback_transaction(self) -> None:
        """Rollback the current transaction asynchronously."""
        if not self._connection or not self._transaction_id:
            raise ValueError("No active transaction to rollback")

        cursor = await self._connection.cursor()
        await cursor.execute("ROLLBACK")
        self._transaction_id = None

    async def create_savepoint(self, name: str) -> None:
        """
        Create a savepoint with the given name asynchronously.

        Note: Trino has limited savepoint support. This implementation
        may need to be adjusted based on the specific Trino version.
        """
        if not self._connection or not self._transaction_id:
            raise ValueError("Cannot create savepoint outside of a transaction")

        cursor = await self._connection.cursor()
        await cursor.execute(f"SAVEPOINT {name}")

    async def rollback_to_savepoint(self, name: str) -> None:
        """
        Rollback to the savepoint with the given name asynchronously.

        Note: Trino has limited savepoint support. This implementation
        may need to be adjusted based on the specific Trino version.
        """
        if not self._connection or not self._transaction_id:
            raise ValueError("Cannot rollback to savepoint outside of a transaction")

        cursor = await self._connection.cursor()
        await cursor.execute(f"ROLLBACK TO SAVEPOINT {name}")

    async def release_savepoint(self, name: str) -> None:
        """
        Release the savepoint with the given name asynchronously.

        Note: Trino has limited savepoint support. This implementation
        may need to be adjusted based on the specific Trino version.
        """
        if not self._connection or not self._transaction_id:
            raise ValueError("Cannot release savepoint outside of a transaction")

        cursor = await self._connection.cursor()
        await cursor.execute(f"RELEASE SAVEPOINT {name}")

    async def close(self) -> None:
        """Close the connection asynchronously."""
        if self._connection:
            await self._connection.close()
            self._connection = None
            self._transaction_id = None

    async def get_catalogs(self) -> List[str]:
        """
        Get a list of available catalogs asynchronously.

        Returns:
            List of catalog names
        """
        if not self._connection:
            await self.connect()

        cursor = await self._connection.cursor()
        await cursor.execute("SHOW CATALOGS")
        results = await cursor.fetchall()
        return [row[0] for row in results]

    async def get_schemas(self, catalog: Optional[str] = None) -> List[str]:
        """
        Get a list of available schemas in the specified catalog asynchronously.

        Args:
            catalog: Catalog name (uses default if None)

        Returns:
            List of schema names
        """
        if not self._connection:
            await self.connect()

        catalog_name = catalog or self._catalog
        if not catalog_name:
            raise ValueError("No catalog specified and no default catalog set")

        cursor = await self._connection.cursor()
        await cursor.execute(f"SHOW SCHEMAS FROM {catalog_name}")
        results = await cursor.fetchall()
        return [row[0] for row in results]

    async def get_tables(self, schema: Optional[str] = None, catalog: Optional[str] = None) -> List[str]:
        """
        Get a list of available tables in the specified schema asynchronously.

        Args:
            schema: Schema name (uses default if None)
            catalog: Catalog name (uses default if None)

        Returns:
            List of table names
        """
        if not self._connection:
            await self.connect()

        catalog_name = catalog or self._catalog
        schema_name = schema or self._schema

        if not catalog_name:
            raise ValueError("No catalog specified and no default catalog set")
        if not schema_name:
            raise ValueError("No schema specified and no default schema set")

        cursor = await self._connection.cursor()
        await cursor.execute(f"SHOW TABLES FROM {catalog_name}.{schema_name}")
        results = await cursor.fetchall()
        return [row[0] for row in results]

    async def get_table_schema(
        self, table: str, schema: Optional[str] = None, catalog: Optional[str] = None
    ) -> List[Dict[str, str]]:
        """
        Get the schema of a table asynchronously.

        Args:
            table: Table name
            schema: Schema name (uses default if None)
            catalog: Catalog name (uses default if None)

        Returns:
            List of column definitions with name and type
        """
        if not self._connection:
            await self.connect()

        catalog_name = catalog or self._catalog
        schema_name = schema or self._schema

        if not catalog_name:
            raise ValueError("No catalog specified and no default catalog set")
        if not schema_name:
            raise ValueError("No schema specified and no default schema set")

        cursor = await self._connection.cursor()
        await cursor.execute(f"DESCRIBE {catalog_name}.{schema_name}.{table}")
        results = await cursor.fetchall()

        columns = []
        for row in results:
            columns.append({"name": row[0], "type": row[1]})

        return columns

    async def execute_batch(self, statements: List[str]) -> int:
        """
        Execute multiple statements as a batch asynchronously.

        Trino doesn't support multiple statements in a single query,
        so we execute them individually.

        Args:
            statements: List of SQL statements to execute

        Returns:
            Number of statements executed
        """
        if not statements:
            return 0

        if not self._connection:
            await self.connect()

        count = 0
        for statement in statements:
            if not statement.strip():
                continue

            cursor = await self._connection.cursor()
            await cursor.execute(statement)
            count += 1

        return count

    async def explain(self, sql: str) -> List[str]:
        """
        Get the execution plan for a query asynchronously.

        Args:
            sql: SQL query to explain

        Returns:
            Execution plan as a list of strings
        """
        if not self._connection:
            await self.connect()

        cursor = await self._connection.cursor()
        await cursor.execute(f"EXPLAIN {sql}")
        results = await cursor.fetchall()
        return [row[0] for row in results]

    async def explain_analyze(self, sql: str) -> List[str]:
        """
        Run EXPLAIN ANALYZE on a query asynchronously.

        Args:
            sql: SQL query to analyze

        Returns:
            Execution plan with runtime statistics as a list of strings
        """
        if not self._connection:
            await self.connect()

        cursor = await self._connection.cursor()
        await cursor.execute(f"EXPLAIN ANALYZE {sql}")
        results = await cursor.fetchall()
        return [row[0] for row in results]

    async def get_query_stats(self, query_id: str) -> Dict[str, Any]:
        """
        Get statistics for a specific query asynchronously.

        Args:
            query_id: Query ID to get statistics for

        Returns:
            Dictionary of query statistics
        """
        if not self._connection:
            await self.connect()

        cursor = await self._connection.cursor()
        await cursor.execute(f"SELECT json_format(query) FROM system.runtime.queries WHERE query_id = '{query_id}'")
        result = await cursor.fetchone()
        if not result:
            raise ValueError(f"Query ID {query_id} not found")

        return json.loads(result[0])

    async def cancel_query(self, query_id: str) -> None:
        """
        Cancel a running query asynchronously.

        Args:
            query_id: Query ID to cancel
        """
        if not self._connection:
            await self.connect()

        cursor = await self._connection.cursor()
        await cursor.execute(f"CALL system.runtime.kill_query(query_id => '{query_id}')")

    async def set_session_property(self, name: str, value: str) -> None:
        """
        Set a session property asynchronously.

        Args:
            name: Property name
            value: Property value
        """
        if not self._connection:
            await self.connect()

        cursor = await self._connection.cursor()
        await cursor.execute(f"SET SESSION {name} = {value}")

    async def get_session_properties(self) -> Dict[str, str]:
        """
        Get all session properties asynchronously.

        Returns:
            Dictionary of session properties
        """
        if not self._connection:
            await self.connect()

        cursor = await self._connection.cursor()
        await cursor.execute("SHOW SESSION")
        results = await cursor.fetchall()

        properties = {}
        for row in results:
            properties[row[0]] = row[1]

        return properties
