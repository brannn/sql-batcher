"""
Async adapter for Trino databases.

This module provides an async adapter for Trino databases.
"""

import asyncio
from typing import Any, Optional

import trino

from sql_batcher.adapters.base import AsyncSQLAdapter
from sql_batcher.exceptions import AdapterConnectionError, AdapterExecutionError


class AsyncTrinoAdapter(AsyncSQLAdapter):
    """Async adapter for Trino databases."""

    def __init__(
        self,
        host: str,
        port: int = 8080,
        user: str = "trino",
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the async Trino adapter.

        Args:
            host: Trino server host
            port: Trino server port
            user: Trino user
            catalog: Optional catalog name
            schema: Optional schema name
            **kwargs: Additional connection parameters
        """
        self.host = host
        self.port = port
        self.user = user
        self.catalog = catalog
        self.schema = schema
        self.conn_kwargs = kwargs
        self.client: Optional[trino.dbapi.Connection] = None

    async def connect(self) -> None:
        """Connect to the Trino database."""
        try:
            loop = asyncio.get_running_loop()
            self.client = await loop.run_in_executor(
                None,
                lambda: trino.dbapi.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    catalog=self.catalog,
                    schema=self.schema,
                    **self.conn_kwargs,
                ),
            )
        except Exception as e:
            raise AdapterConnectionError("Trino", str(e)) from e

    async def disconnect(self) -> None:
        """Disconnect from the Trino database."""
        if self.client:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self.client.close)
            self.client = None

    async def execute(self, query: str) -> Any:
        """Execute a SQL query.

        Args:
            query: SQL query to execute

        Returns:
            Query result

        Raises:
            AdapterExecutionError: If query execution fails
        """
        if not self.client:
            raise AdapterConnectionError("Trino", "Not connected to database")

        try:
            loop = asyncio.get_running_loop()
            cursor = await loop.run_in_executor(None, self.client.cursor)
            await loop.run_in_executor(None, cursor.execute, query)
            return await loop.run_in_executor(None, cursor.fetchall)
        except Exception as e:
            raise AdapterExecutionError("Trino", query, str(e)) from e

    async def begin_transaction(self) -> None:
        """Begin a database transaction."""
        if not self.client:
            raise AdapterConnectionError("Trino", "Not connected to database")

        try:
            loop = asyncio.get_running_loop()
            cursor = await loop.run_in_executor(None, self.client.cursor)
            await loop.run_in_executor(None, cursor.execute, "START TRANSACTION")
        except Exception as e:
            raise AdapterExecutionError("Trino", "START TRANSACTION", str(e)) from e

    async def commit_transaction(self) -> None:
        """Commit the current transaction."""
        if not self.client:
            raise AdapterConnectionError("Trino", "Not connected to database")

        try:
            loop = asyncio.get_running_loop()
            cursor = await loop.run_in_executor(None, self.client.cursor)
            await loop.run_in_executor(None, cursor.execute, "COMMIT")
        except Exception as e:
            raise AdapterExecutionError("Trino", "COMMIT", str(e)) from e

    async def rollback_transaction(self) -> None:
        """Rollback the current transaction."""
        if not self.client:
            raise AdapterConnectionError("Trino", "Not connected to database")

        try:
            loop = asyncio.get_running_loop()
            cursor = await loop.run_in_executor(None, self.client.cursor)
            await loop.run_in_executor(None, cursor.execute, "ROLLBACK")
        except Exception as e:
            raise AdapterExecutionError("Trino", "ROLLBACK", str(e)) from e
