"""
Async adapter for Google BigQuery.

This module provides an async adapter for Google BigQuery databases.
"""

from typing import Any, Optional

from google.cloud import bigquery
from google.cloud.bigquery import Client

from sql_batcher.adapters.base import AsyncSQLAdapter
from sql_batcher.exceptions import AdapterConnectionError, AdapterExecutionError


class AsyncBigQueryAdapter(AsyncSQLAdapter):
    """Async adapter for Google BigQuery."""

    def __init__(
        self,
        project_id: str,
        credentials_path: Optional[str] = None,
        location: Optional[str] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the async BigQuery adapter.

        Args:
            project_id: Google Cloud project ID
            credentials_path: Optional path to credentials file
            location: Optional BigQuery location
            **kwargs: Additional connection parameters
        """
        self.project_id = project_id
        self.credentials_path = credentials_path
        self.location = location
        self.conn_kwargs = kwargs
        self.client: Optional[Client] = None

    async def connect(self) -> None:
        """Connect to BigQuery."""
        try:
            self.client = bigquery.Client(
                project=self.project_id,
                credentials=self.credentials_path,
                location=self.location,
                **self.conn_kwargs,
            )
        except Exception as e:
            raise AdapterConnectionError("BigQuery", str(e)) from e

    async def disconnect(self) -> None:
        """Disconnect from BigQuery."""
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
            raise AdapterConnectionError("BigQuery", "Not connected to database")

        try:
            job = await self.client.query(query)
            return await job.result()
        except Exception as e:
            raise AdapterExecutionError("BigQuery", query, str(e)) from e

    async def begin_transaction(self) -> None:
        """Begin a database transaction."""
        if not self.client:
            raise AdapterConnectionError("BigQuery", "Not connected to database")

        try:
            await self.client.query("BEGIN TRANSACTION")
        except Exception as e:
            raise AdapterExecutionError("BigQuery", "BEGIN TRANSACTION", str(e)) from e

    async def commit_transaction(self) -> None:
        """Commit the current transaction."""
        if not self.client:
            raise AdapterConnectionError("BigQuery", "Not connected to database")

        try:
            await self.client.query("COMMIT TRANSACTION")
        except Exception as e:
            raise AdapterExecutionError("BigQuery", "COMMIT TRANSACTION", str(e)) from e

    async def rollback_transaction(self) -> None:
        """Rollback the current transaction."""
        if not self.client:
            raise AdapterConnectionError("BigQuery", "Not connected to database")

        try:
            await self.client.query("ROLLBACK TRANSACTION")
        except Exception as e:
            raise AdapterExecutionError(
                "BigQuery", "ROLLBACK TRANSACTION", str(e)
            ) from e
