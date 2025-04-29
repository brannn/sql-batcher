"""
Async PostgreSQL adapter for SQL Batcher.

This module provides a PostgreSQL-specific async adapter for SQL Batcher with
optimizations for PostgreSQL features like COPY commands and transaction management.
"""

from typing import Any, Dict, List, Optional, Tuple, Union, cast

from sql_batcher.adapters.async_base import AsyncSQLAdapter

try:
    import asyncpg

    ASYNCPG_AVAILABLE = True
except ImportError:
    ASYNCPG_AVAILABLE = False


class AsyncPostgreSQLAdapter(AsyncSQLAdapter):
    """
    Async adapter for PostgreSQL database connections using asyncpg.

    This adapter is optimized for PostgreSQL's specific features, including:
    - Multiple statements per query (semicolon-separated)
    - COPY command for bulk data loading
    - Full ACID transaction support
    - JSONB, array, and other PostgreSQL-specific data types

    Args:
        dsn: Connection string
        min_size: Minimum connection pool size
        max_size: Maximum connection pool size
        max_query_size: Optional maximum query size in bytes
        **kwargs: Additional connection parameters
    """

    def __init__(
        self,
        dsn: str,
        min_size: int = 1,
        max_size: int = 10,
        max_query_size: Optional[int] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the async PostgreSQL adapter."""
        if not ASYNCPG_AVAILABLE:
            raise ImportError("asyncpg package is required for AsyncPostgreSQLAdapter. " "Install it with: pip install asyncpg")

        self._dsn = dsn
        self._min_size = min_size
        self._max_size = max_size
        self._max_query_size = max_query_size or 5_000_000
        self._pool = None
        self._kwargs = kwargs

    async def connect(self) -> None:
        """Connect to the database."""
        if not self._pool:
            self._pool = await asyncpg.create_pool(
                dsn=self._dsn,
                min_size=self._min_size,
                max_size=self._max_size,
                **self._kwargs,
            )

    async def get_max_query_size(self) -> int:
        """
        Get the maximum query size in bytes.

        PostgreSQL has a practical limit around 500MB for query size,
        but we use a much more conservative default for better performance.

        Returns:
            Maximum query size in bytes
        """
        return self._max_query_size

    async def execute(self, sql: str) -> List[Any]:
        """
        Execute a SQL statement asynchronously and return results.

        PostgreSQL supports executing multiple statements in a single query
        by separating them with semicolons. This allows SQL Batcher to combine
        multiple statements into a single execution for better performance.

        Args:
            sql: SQL statement(s) to execute

        Returns:
            List of result rows (for SELECT queries) or empty list for others
        """
        if not self._pool:
            await self.connect()

        try:
            # Optimization: Use a connection from the pool directly
            conn = await self._pool.acquire()
            try:
                # Optimization: Check if this is a batch of statements
                if ";" in sql and not sql.strip().upper().startswith("SELECT"):
                    # For batches of non-SELECT statements, execute them all at once
                    await conn.execute(sql)
                    return []
                # For SELECT statements or single statements, use the standard approach
                elif sql.strip().upper().startswith("SELECT"):
                    result = await conn.fetch(sql)
                    return list(result) if result is not None else []
                else:
                    # For other statements, just execute
                    await conn.execute(sql)
                    return []
            finally:
                # Always release the connection back to the pool
                await self._pool.release(conn)
        except Exception as e:
            # Add context to the error
            raise Exception(f"PostgreSQL Error executing SQL: {str(e)}") from e

    async def begin_transaction(self) -> None:
        """Begin a transaction asynchronously."""
        if not self._pool:
            await self.connect()

        async with self._pool.acquire() as conn:
            await conn.execute("BEGIN")

    async def commit_transaction(self) -> None:
        """Commit the current transaction asynchronously."""
        if not self._pool:
            await self.connect()

        async with self._pool.acquire() as conn:
            await conn.execute("COMMIT")

    async def rollback_transaction(self) -> None:
        """Rollback the current transaction asynchronously."""
        if not self._pool:
            await self.connect()

        async with self._pool.acquire() as conn:
            await conn.execute("ROLLBACK")

    async def create_savepoint(self, name: str) -> None:
        """Create a savepoint with the given name asynchronously."""
        if not self._pool:
            await self.connect()

        async with self._pool.acquire() as conn:
            await conn.execute(f"SAVEPOINT {name}")

    async def rollback_to_savepoint(self, name: str) -> None:
        """Rollback to the savepoint with the given name asynchronously."""
        if not self._pool:
            await self.connect()

        async with self._pool.acquire() as conn:
            await conn.execute(f"ROLLBACK TO SAVEPOINT {name}")

    async def release_savepoint(self, name: str) -> None:
        """Release the savepoint with the given name asynchronously."""
        if not self._pool:
            await self.connect()

        async with self._pool.acquire() as conn:
            await conn.execute(f"RELEASE SAVEPOINT {name}")

    async def close(self) -> None:
        """Close the connection pool asynchronously."""
        if self._pool:
            await self._pool.close()
            self._pool = None

    async def explain_analyze(self, sql: str) -> List[Tuple]:
        """
        Run EXPLAIN ANALYZE on a query asynchronously.

        Args:
            sql: SQL query to analyze

        Returns:
            Execution plan as a list of rows
        """
        explain_sql = f"EXPLAIN ANALYZE {sql}"
        return await self.execute(explain_sql)

    async def create_temp_table(self, table_name: str, column_defs: str) -> None:
        """
        Create a temporary table asynchronously.

        Args:
            table_name: Name of the temporary table
            column_defs: Column definitions as a SQL string
        """
        sql = f"CREATE TEMPORARY TABLE {table_name} ({column_defs})"
        await self.execute(sql)

    async def get_server_version(self) -> Tuple[int, int, int]:
        """
        Get the PostgreSQL server version asynchronously.

        Returns:
            Tuple of (major, minor, patch) version numbers
        """
        if not self._pool:
            await self.connect()

        async with self._pool.acquire() as conn:
            version_str = await conn.fetchval("SHOW server_version")
            if not version_str:
                return (0, 0, 0)

            # Parse version string (e.g., "14.5" or "14.5.0")
            parts = version_str.split(".")
            major = int(parts[0]) if len(parts) > 0 else 0
            minor = int(parts[1]) if len(parts) > 1 else 0
            patch = int(parts[2]) if len(parts) > 2 else 0

            return (major, minor, patch)

    async def execute_batch(self, statements: List[str]) -> int:
        """
        Execute multiple statements as a batch asynchronously.

        This is optimized for PostgreSQL which can handle multiple statements
        in a single execution when separated by semicolons.

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

    async def use_copy_for_bulk_insert(
        self,
        table_name: str,
        column_names: List[str],
        data: List[Tuple],
    ) -> int:
        """
        Use PostgreSQL's COPY command for bulk data loading asynchronously.

        This is much faster than individual INSERT statements for large datasets.

        Args:
            table_name: Target table name
            column_names: List of column names
            data: List of data tuples

        Returns:
            Number of rows copied
        """
        if not data:
            return 0

        if not self._pool:
            await self.connect()

        ", ".join(column_names)

        async with self._pool.acquire() as conn:
            # Start a transaction
            async with conn.transaction():
                # Use the COPY protocol
                result = await conn.copy_records_to_table(  # noqa: F841
                    table_name,
                    records=data,
                    columns=column_names,
                )

        return len(data)

    async def create_indices(self, table_name: str, indices: List[Dict[str, Union[str, List[str], bool]]]) -> List[str]:
        """
        Create indices on a table asynchronously.

        Args:
            table_name: Name of the table to create indices on
            indices: List of index definitions, each containing:
                - name: Index name
                - columns: List of column names
                - type: Index type (btree, hash, etc.)
                - unique: Whether the index should be unique (optional)

        Returns:
            List of SQL statements to create the indices
        """
        statements = []
        for index in indices:
            # Extract index properties
            name = str(index["name"])
            columns = index["columns"]
            index_type = str(index.get("type", "btree"))
            unique = bool(index.get("unique", False))

            # Build the column list
            if isinstance(columns, str):
                column_list = columns
            else:
                column_list = ", ".join(cast(List[str], columns))

            # Build the CREATE INDEX statement
            unique_str = "UNIQUE " if unique else ""
            statement = f"CREATE {unique_str}INDEX {name} ON {table_name} " f"USING {index_type} ({column_list})"
            statements.append(statement)

            # Execute the statement
            await self.execute(statement)

        return statements
