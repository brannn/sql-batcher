"""Async PostgreSQL adapter for sql-batcher.

This module provides an async adapter for PostgreSQL databases using asyncpg.
"""

from typing import Any, Awaitable, Optional

import asyncpg

from sql_batcher.adapters.async_base import AsyncSQLAdapter
from sql_batcher.exceptions import AdapterConnectionError, AdapterExecutionError


class AsyncPostgreSQLAdapter(AsyncSQLAdapter):
    """Async adapter for PostgreSQL databases."""

    def __init__(
        self,
        dsn: str,
        min_size: int = 10,
        max_size: int = 10,
        max_queries: int = 50000,
        max_inactive_connection_lifetime: float = 300.0,
        setup: Optional[Awaitable[None]] = None,
        init: Optional[Awaitable[None]] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the async PostgreSQL adapter.

        Args:
            dsn: PostgreSQL connection string
            min_size: Minimum number of connections in the pool
            max_size: Maximum number of connections in the pool
            max_queries: Maximum number of queries per connection
            max_inactive_connection_lifetime: Maximum lifetime of inactive connections
            setup: Optional async setup function
            init: Optional async initialization function
            **kwargs: Additional connection parameters
        """
        self.dsn = dsn
        self.pool: Optional[asyncpg.Pool] = None
        self.pool_kwargs = {
            "min_size": min_size,
            "max_size": max_size,
            "max_queries": max_queries,
            "max_inactive_connection_lifetime": max_inactive_connection_lifetime,
            **kwargs,
        }
        self.setup = setup
        self.init = init

    async def connect(self) -> None:
        """Connect to the PostgreSQL database."""
        try:
            self.pool = await asyncpg.create_pool(self.dsn, **self.pool_kwargs)
            if self.setup:
                await self.setup
            if self.init:
                await self.init
        except Exception as e:
            raise AdapterConnectionError("PostgreSQL", str(e)) from e

    async def disconnect(self) -> None:
        """Disconnect from the PostgreSQL database."""
        if self.pool:
            await self.pool.close()
            self.pool = None

    async def execute(self, query: str) -> Any:
        """Execute a SQL query.

        Args:
            query: SQL query to execute

        Returns:
            Query result

        Raises:
            AdapterExecutionError: If query execution fails
        """
        if not self.pool:
            raise AdapterConnectionError("PostgreSQL", "Not connected to database")

        try:
            async with self.pool.acquire() as conn:
                return await conn.execute(query)
        except Exception as e:
            raise AdapterExecutionError("PostgreSQL", query, str(e)) from e

    async def begin_transaction(self) -> None:
        """Begin a database transaction."""
        if not self.pool:
            raise AdapterConnectionError("PostgreSQL", "Not connected to database")

        try:
            async with self.pool.acquire() as conn:
                await conn.execute("BEGIN")
        except Exception as e:
            raise AdapterExecutionError("PostgreSQL", "BEGIN", str(e)) from e

    async def commit_transaction(self) -> None:
        """Commit the current transaction."""
        if not self.pool:
            raise AdapterConnectionError("PostgreSQL", "Not connected to database")

        try:
            async with self.pool.acquire() as conn:
                await conn.execute("COMMIT")
        except Exception as e:
            raise AdapterExecutionError("PostgreSQL", "COMMIT", str(e)) from e

    async def rollback_transaction(self) -> None:
        """Rollback the current transaction."""
        if not self.pool:
            raise AdapterConnectionError("PostgreSQL", "Not connected to database")

        try:
            async with self.pool.acquire() as conn:
                await conn.execute("ROLLBACK")
        except Exception as e:
            raise AdapterExecutionError("PostgreSQL", "ROLLBACK", str(e)) from e

    async def create_savepoint(self, name: str) -> None:
        """Create a savepoint in the current transaction.

        Args:
            name: Name of the savepoint
        """
        if not self.pool:
            raise AdapterConnectionError("PostgreSQL", "Not connected to database")

        try:
            async with self.pool.acquire() as conn:
                await conn.execute(f"SAVEPOINT {name}")
        except Exception as e:
            raise AdapterExecutionError(
                "PostgreSQL", f"SAVEPOINT {name}", str(e)
            ) from e

    async def rollback_to_savepoint(self, name: str) -> None:
        """Rollback to a previously created savepoint.

        Args:
            name: Name of the savepoint to rollback to
        """
        if not self.pool:
            raise AdapterConnectionError("PostgreSQL", "Not connected to database")

        try:
            async with self.pool.acquire() as conn:
                await conn.execute(f"ROLLBACK TO SAVEPOINT {name}")
        except Exception as e:
            raise AdapterExecutionError(
                "PostgreSQL", f"ROLLBACK TO SAVEPOINT {name}", str(e)
            ) from e

    async def release_savepoint(self, name: str) -> None:
        """Release a previously created savepoint.

        Args:
            name: Name of the savepoint to release
        """
        if not self.pool:
            raise AdapterConnectionError("PostgreSQL", "Not connected to database")

        try:
            async with self.pool.acquire() as conn:
                await conn.execute(f"RELEASE SAVEPOINT {name}")
        except Exception as e:
            raise AdapterExecutionError(
                "PostgreSQL", f"RELEASE SAVEPOINT {name}", str(e)
            ) from e
