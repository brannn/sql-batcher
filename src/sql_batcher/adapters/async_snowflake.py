"""
Async adapter for Snowflake databases.

This module provides an async adapter for Snowflake databases.
"""

import asyncio
from typing import Any, Optional

import snowflake.connector

from sql_batcher.adapters.base import AsyncSQLAdapter
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
        self.conn: Optional[snowflake.connector.SnowflakeConnection] = None

    async def connect(self) -> None:
        """Connect to the Snowflake database."""
        try:
            loop = asyncio.get_running_loop()
            self.conn = await loop.run_in_executor(
                None,
                lambda: snowflake.connector.connect(
                    account=self.account,
                    user=self.user,
                    password=self.password,
                    warehouse=self.warehouse,
                    database=self.database,
                    schema=self.schema,
                    **self.conn_kwargs,
                ),
            )
        except Exception as e:
            raise AdapterConnectionError("Snowflake", str(e)) from e

    async def disconnect(self) -> None:
        """Disconnect from the Snowflake database."""
        if self.conn:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self.conn.close)
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
            loop = asyncio.get_running_loop()
            cursor = await loop.run_in_executor(None, self.conn.cursor)
            await loop.run_in_executor(None, cursor.execute, query)
            return await loop.run_in_executor(None, cursor.fetchall)
        except Exception as e:
            raise AdapterExecutionError("Snowflake", query, str(e)) from e

    async def begin_transaction(self) -> None:
        """Begin a database transaction."""
        if not self.conn:
            raise AdapterConnectionError("Snowflake", "Not connected to database")

        try:
            loop = asyncio.get_running_loop()
            cursor = await loop.run_in_executor(None, self.conn.cursor)
            await loop.run_in_executor(None, cursor.execute, "BEGIN")
        except Exception as e:
            raise AdapterExecutionError("Snowflake", "BEGIN", str(e)) from e

    async def commit_transaction(self) -> None:
        """Commit the current transaction."""
        if not self.conn:
            raise AdapterConnectionError("Snowflake", "Not connected to database")

        try:
            loop = asyncio.get_running_loop()
            cursor = await loop.run_in_executor(None, self.conn.cursor)
            await loop.run_in_executor(None, cursor.execute, "COMMIT")
        except Exception as e:
            raise AdapterExecutionError("Snowflake", "COMMIT", str(e)) from e

    async def rollback_transaction(self) -> None:
        """Rollback the current transaction."""
        if not self.conn:
            raise AdapterConnectionError("Snowflake", "Not connected to database")

        try:
            loop = asyncio.get_running_loop()
            cursor = await loop.run_in_executor(None, self.conn.cursor)
            await loop.run_in_executor(None, cursor.execute, "ROLLBACK")
        except Exception as e:
            raise AdapterExecutionError("Snowflake", "ROLLBACK", str(e)) from e
