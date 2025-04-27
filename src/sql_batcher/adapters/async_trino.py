"""Async Trino adapter for sql-batcher.

This module provides an async adapter for Trino databases using trino-async.
"""

from typing import Any, Awaitable, Optional

import trino.async_client as trino

from sql_batcher.adapters.async_base import AsyncSQLAdapter
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
        self.client: Optional[trino.TrinoClient] = None

    async def connect(self) -> None:
        """Connect to the Trino database."""
        try:
            self.client = trino.TrinoClient(
                host=self.host,
                port=self.port,
                user=self.user,
                catalog=self.catalog,
                schema=self.schema,
                **self.conn_kwargs,
            )
            await self.client.connect()
        except Exception as e:
            raise AdapterConnectionError("Trino", str(e)) from e

    async def disconnect(self) -> None:
        """Disconnect from the Trino database."""
        if self.client:
            await self.client.close()
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
            return await self.client.execute(query)
        except Exception as e:
            raise AdapterExecutionError("Trino", query, str(e)) from e

    async def begin_transaction(self) -> None:
        """Begin a database transaction."""
        if not self.client:
            raise AdapterConnectionError("Trino", "Not connected to database")

        try:
            await self.client.execute("START TRANSACTION")
        except Exception as e:
            raise AdapterExecutionError("Trino", "START TRANSACTION", str(e)) from e

    async def commit_transaction(self) -> None:
        """Commit the current transaction."""
        if not self.client:
            raise AdapterConnectionError("Trino", "Not connected to database")

        try:
            await self.client.execute("COMMIT")
        except Exception as e:
            raise AdapterExecutionError("Trino", "COMMIT", str(e)) from e

    async def rollback_transaction(self) -> None:
        """Rollback the current transaction."""
        if not self.client:
            raise AdapterConnectionError("Trino", "Not connected to database")

        try:
            await self.client.execute("ROLLBACK")
        except Exception as e:
            raise AdapterExecutionError("Trino", "ROLLBACK", str(e)) from e
