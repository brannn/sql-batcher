"""Async Snowflake adapter for sql-batcher.

This module provides an async adapter for Snowflake databases using snowflake-connector-python.
"""

from typing import Any, Awaitable, Optional

import snowflake.connector.async_connector as snowflake

from sql_batcher.adapters.async_base import AsyncSQLAdapter
from sql_batcher.exceptions import AdapterConnectionError, AdapterExecutionError


class AsyncSnowflakeAdapter(AsyncSQLAdapter):
    """Async adapter for Snowflake databases."""

    def __init__(
        self,
        account: str,
        user: str,
        password: str,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the async Snowflake adapter.

        Args:
            account: Snowflake account identifier
            user: Snowflake user
            password: Snowflake password
            warehouse: Optional warehouse name
            database: Optional database name
            schema: Optional schema name
            **kwargs: Additional connection parameters
        """
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.conn_kwargs = kwargs
        self.conn: Optional[snowflake.SnowflakeConnection] = None

    async def connect(self) -> None:
        """Connect to the Snowflake database."""
        try:
            self.conn = await snowflake.connect(
                account=self.account,
                user=self.user,
                password=self.password,
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema,
                **self.conn_kwargs,
            )
        except Exception as e:
            raise AdapterConnectionError("Snowflake", str(e)) from e

    async def disconnect(self) -> None:
        """Disconnect from the Snowflake database."""
        if self.conn:
            await self.conn.close()
            self.conn = None

    async def execute(self, query: str) -> Any:
        """Execute a SQL query.

        Args:
            query: SQL query to execute

        Returns:
            Query result

        Raises:
            AdapterExecutionError: If query execution fails
        """
        if not self.conn:
            raise AdapterConnectionError("Snowflake", "Not connected to database")

        try:
            cursor = await self.conn.cursor()
            await cursor.execute(query)
            return await cursor.fetchall()
        except Exception as e:
            raise AdapterExecutionError("Snowflake", query, str(e)) from e

    async def begin_transaction(self) -> None:
        """Begin a database transaction."""
        if not self.conn:
            raise AdapterConnectionError("Snowflake", "Not connected to database")

        try:
            cursor = await self.conn.cursor()
            await cursor.execute("BEGIN")
        except Exception as e:
            raise AdapterExecutionError("Snowflake", "BEGIN", str(e)) from e

    async def commit_transaction(self) -> None:
        """Commit the current transaction."""
        if not self.conn:
            raise AdapterConnectionError("Snowflake", "Not connected to database")

        try:
            cursor = await self.conn.cursor()
            await cursor.execute("COMMIT")
        except Exception as e:
            raise AdapterExecutionError("Snowflake", "COMMIT", str(e)) from e

    async def rollback_transaction(self) -> None:
        """Rollback the current transaction."""
        if not self.conn:
            raise AdapterConnectionError("Snowflake", "Not connected to database")

        try:
            cursor = await self.conn.cursor()
            await cursor.execute("ROLLBACK")
        except Exception as e:
            raise AdapterExecutionError("Snowflake", "ROLLBACK", str(e)) from e
