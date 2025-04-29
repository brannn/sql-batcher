"""
Async BigQuery adapter for SQL Batcher.

This module provides a BigQuery-specific async adapter for SQL Batcher with
optimizations for BigQuery's features and limitations.
"""

from typing import Any, Dict, List, Optional

from sql_batcher.adapters.async_base import AsyncSQLAdapter

try:
    import asyncio
    from google.cloud import bigquery

    BIGQUERY_AVAILABLE = True
except ImportError:
    BIGQUERY_AVAILABLE = False


class AsyncBigQueryAdapter(AsyncSQLAdapter):
    """
    Async adapter for BigQuery database connections.

    This adapter is optimized for BigQuery's specific features, including:
    - Batch query execution
    - Query job management
    - BigQuery-specific data types and functions

    Note: This adapter uses the synchronous BigQuery client in an async wrapper
    since the official async BigQuery client is still in development.
    Operations are executed in a thread pool to avoid blocking the event loop.

    Args:
        project_id: Google Cloud project ID
        dataset_id: Default dataset ID
        location: BigQuery location (e.g., 'US', 'EU')
        credentials: Optional Google Cloud credentials
        max_query_size: Optional maximum query size in bytes
        **kwargs: Additional client options
    """

    def __init__(
        self,
        project_id: str,
        dataset_id: Optional[str] = None,
        location: str = "US",
        credentials: Optional[Any] = None,
        max_query_size: Optional[int] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize the async BigQuery adapter."""
        if not BIGQUERY_AVAILABLE:
            raise ImportError(
                "google-cloud-bigquery package is required for AsyncBigQueryAdapter. "
                "Install it with: pip install google-cloud-bigquery"
            )

        self._project_id = project_id
        self._dataset_id = dataset_id
        self._location = location
        self._credentials = credentials
        self._max_query_size = max_query_size or 1_000_000  # Default to 1MB
        self._kwargs = kwargs
        self._client = None
        self._job_config = bigquery.QueryJobConfig(use_legacy_sql=False)

        # Set default dataset if provided
        if self._dataset_id:
            self._job_config.default_dataset = f"{self._project_id}.{self._dataset_id}"

    async def connect(self) -> None:
        """Connect to BigQuery asynchronously."""
        if not self._client:
            # Execute in a thread pool to avoid blocking
            self._client = await asyncio.to_thread(
                bigquery.Client,
                project=self._project_id,
                location=self._location,
                credentials=self._credentials,
                **self._kwargs,
            )

    async def get_max_query_size(self) -> int:
        """
        Get the maximum query size in bytes.

        BigQuery has a practical limit of 1MB for SQL text,
        but we use a conservative default for better performance.

        Returns:
            Maximum query size in bytes
        """
        return self._max_query_size

    async def execute(self, sql: str) -> List[Any]:
        """
        Execute a SQL statement asynchronously and return results.

        BigQuery doesn't support executing multiple statements in a single query,
        so we split the statements and execute them individually.

        Args:
            sql: SQL statement(s) to execute

        Returns:
            List of result rows (for SELECT queries) or empty list for others
        """
        if not self._client:
            await self.connect()

        try:
            # Split statements if multiple are provided
            statements = [s.strip() for s in sql.split(";") if s.strip()]
            results = []

            for statement in statements:
                # Skip empty statements
                if not statement:
                    continue

                # Execute the statement in a thread pool
                query_job = await asyncio.to_thread(
                    self._client.query,
                    statement,
                    job_config=self._job_config,
                    location=self._location,
                )

                # Wait for the job to complete
                await asyncio.to_thread(query_job.result)

                # For SELECT statements, return the results
                if statement.strip().upper().startswith("SELECT"):
                    rows = await asyncio.to_thread(list, query_job.result())
                    results.extend(rows)

            return results
        except Exception as e:
            # Add context to the error
            raise Exception(f"BigQuery Error executing SQL: {str(e)}") from e

    async def begin_transaction(self) -> None:
        """
        Begin a transaction asynchronously.

        Note: BigQuery has limited transaction support. This is a placeholder
        for compatibility with the adapter interface.
        """
        # BigQuery doesn't support traditional transactions

    async def commit_transaction(self) -> None:
        """
        Commit the current transaction asynchronously.

        Note: BigQuery has limited transaction support. This is a placeholder
        for compatibility with the adapter interface.
        """
        # BigQuery doesn't support traditional transactions

    async def rollback_transaction(self) -> None:
        """
        Rollback the current transaction asynchronously.

        Note: BigQuery has limited transaction support. This is a placeholder
        for compatibility with the adapter interface.
        """
        # BigQuery doesn't support traditional transactions

    async def create_savepoint(self, name: str) -> None:
        """
        Create a savepoint with the given name asynchronously.

        Note: BigQuery doesn't support savepoints. This is a placeholder
        for compatibility with the adapter interface.
        """
        # BigQuery doesn't support savepoints

    async def rollback_to_savepoint(self, name: str) -> None:
        """
        Rollback to the savepoint with the given name asynchronously.

        Note: BigQuery doesn't support savepoints. This is a placeholder
        for compatibility with the adapter interface.
        """
        # BigQuery doesn't support savepoints

    async def release_savepoint(self, name: str) -> None:
        """
        Release the savepoint with the given name asynchronously.

        Note: BigQuery doesn't support savepoints. This is a placeholder
        for compatibility with the adapter interface.
        """
        # BigQuery doesn't support savepoints

    async def close(self) -> None:
        """Close the connection asynchronously."""
        if self._client:
            await asyncio.to_thread(self._client.close)
            self._client = None

    async def get_datasets(self) -> List[str]:
        """
        Get a list of available datasets asynchronously.

        Returns:
            List of dataset IDs
        """
        if not self._client:
            await self.connect()

        datasets = await asyncio.to_thread(list, self._client.list_datasets())
        return [dataset.dataset_id for dataset in datasets]

    async def get_tables(self, dataset_id: Optional[str] = None) -> List[str]:
        """
        Get a list of available tables in the specified dataset asynchronously.

        Args:
            dataset_id: Dataset ID (uses default if None)

        Returns:
            List of table IDs
        """
        if not self._client:
            await self.connect()

        dataset = dataset_id or self._dataset_id
        if not dataset:
            raise ValueError("No dataset specified and no default dataset set")

        tables = await asyncio.to_thread(list, self._client.list_tables(f"{self._project_id}.{dataset}"))
        return [table.table_id for table in tables]

    async def get_table_schema(self, table_id: str, dataset_id: Optional[str] = None) -> List[Dict[str, str]]:
        """
        Get the schema of a table asynchronously.

        Args:
            table_id: Table ID
            dataset_id: Dataset ID (uses default if None)

        Returns:
            List of column definitions with name and type
        """
        if not self._client:
            await self.connect()

        dataset = dataset_id or self._dataset_id
        if not dataset:
            raise ValueError("No dataset specified and no default dataset set")

        table_ref = f"{self._project_id}.{dataset}.{table_id}"
        table = await asyncio.to_thread(self._client.get_table, table_ref)

        columns = []
        for field in table.schema:
            columns.append({"name": field.name, "type": field.field_type})

        return columns

    async def execute_batch(self, statements: List[str]) -> int:
        """
        Execute multiple statements as a batch asynchronously.

        BigQuery doesn't support multiple statements in a single query,
        so we execute them individually.

        Args:
            statements: List of SQL statements to execute

        Returns:
            Number of statements executed
        """
        if not statements:
            return 0

        if not self._client:
            await self.connect()

        count = 0
        for statement in statements:
            if not statement.strip():
                continue

            # Execute the statement
            await self.execute(statement)
            count += 1

        return count

    async def create_dataset(self, dataset_id: str) -> None:
        """
        Create a new dataset asynchronously.

        Args:
            dataset_id: Dataset ID to create
        """
        if not self._client:
            await self.connect()

        dataset = bigquery.Dataset(f"{self._project_id}.{dataset_id}")
        dataset.location = self._location
        await asyncio.to_thread(self._client.create_dataset, dataset)

    async def delete_dataset(self, dataset_id: str, delete_contents: bool = False) -> None:
        """
        Delete a dataset asynchronously.

        Args:
            dataset_id: Dataset ID to delete
            delete_contents: Whether to delete the dataset's contents
        """
        if not self._client:
            await self.connect()

        await asyncio.to_thread(
            self._client.delete_dataset,
            f"{self._project_id}.{dataset_id}",
            delete_contents=delete_contents,
        )

    async def create_table(self, table_id: str, schema: List[Dict[str, str]], dataset_id: Optional[str] = None) -> None:
        """
        Create a new table asynchronously.

        Args:
            table_id: Table ID to create
            schema: List of column definitions with name and type
            dataset_id: Dataset ID (uses default if None)
        """
        if not self._client:
            await self.connect()

        dataset = dataset_id or self._dataset_id
        if not dataset:
            raise ValueError("No dataset specified and no default dataset set")

        # Convert schema to BigQuery format
        fields = []
        for column in schema:
            fields.append(
                bigquery.SchemaField(
                    name=column["name"],
                    field_type=column["type"],
                    mode=column.get("mode", "NULLABLE"),
                )
            )

        table = bigquery.Table(f"{self._project_id}.{dataset}.{table_id}", schema=fields)
        await asyncio.to_thread(self._client.create_table, table)

    async def delete_table(self, table_id: str, dataset_id: Optional[str] = None) -> None:
        """
        Delete a table asynchronously.

        Args:
            table_id: Table ID to delete
            dataset_id: Dataset ID (uses default if None)
        """
        if not self._client:
            await self.connect()

        dataset = dataset_id or self._dataset_id
        if not dataset:
            raise ValueError("No dataset specified and no default dataset set")

        await asyncio.to_thread(self._client.delete_table, f"{self._project_id}.{dataset}.{table_id}")

    async def get_job(self, job_id: str) -> Dict[str, Any]:
        """
        Get information about a job asynchronously.

        Args:
            job_id: Job ID to get information for

        Returns:
            Dictionary of job information
        """
        if not self._client:
            await self.connect()

        job = await asyncio.to_thread(self._client.get_job, job_id, location=self._location)
        return {
            "job_id": job.job_id,
            "location": job.location,
            "project": job.project,
            "state": job.state,
            "error_result": job.error_result,
            "created": job.created.isoformat() if job.created else None,
            "started": job.started.isoformat() if job.started else None,
            "ended": job.ended.isoformat() if job.ended else None,
        }

    async def cancel_job(self, job_id: str) -> None:
        """
        Cancel a job asynchronously.

        Args:
            job_id: Job ID to cancel
        """
        if not self._client:
            await self.connect()

        job = await asyncio.to_thread(self._client.get_job, job_id, location=self._location)
        await asyncio.to_thread(job.cancel)

    async def set_query_parameters(self, parameters: Dict[str, Any]) -> None:
        """
        Set query parameters for parameterized queries asynchronously.

        Args:
            parameters: Dictionary of parameter names and values
        """
        if not self._client:
            await self.connect()

        # Convert parameters to BigQuery format
        query_params = []
        for name, value in parameters.items():
            param_type = None
            if isinstance(value, str):
                param_type = "STRING"
            elif isinstance(value, int):
                param_type = "INT64"
            elif isinstance(value, float):
                param_type = "FLOAT64"
            elif isinstance(value, bool):
                param_type = "BOOL"
            elif isinstance(value, (list, tuple)):
                param_type = "ARRAY"
            elif isinstance(value, dict):
                param_type = "STRUCT"

            query_params.append(bigquery.ScalarQueryParameter(name, param_type, value))

        self._job_config.query_parameters = query_params
