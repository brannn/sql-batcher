"""
Async SQL Batcher implementation.

This module contains the AsyncSQLBatcher class, which is the async version of
the main SQLBatcher class for batching SQL statements based on size limits.
"""

import re
from typing import Any, Awaitable, Callable, Dict, List, Optional
import asyncio
import logging

from sql_batcher.adapters.base import AsyncSQLAdapter
from sql_batcher.hooks import Plugin, PluginManager
from sql_batcher.insert_merger import InsertMerger
from sql_batcher.query_collector import QueryCollector
from sql_batcher.batch_manager import BatchManager
from sql_batcher.exceptions import SQLBatcherError
from sql_batcher.hook_manager import HookManager
from sql_batcher.retry_manager import RetryManager

logger = logging.getLogger(__name__)


class SavepointContext:
    """Context manager for savepoints."""

    def __init__(self, batcher: "AsyncSQLBatcher", name: str) -> None:
        """Initialize the savepoint context.

        Args:
            batcher: The batcher instance
            name: Name of the savepoint
        """
        self.batcher = batcher
        self.name = name

    async def __aenter__(self) -> None:
        """Enter the savepoint context."""
        self.batcher._in_savepoint_context = True
        await self.batcher._adapter.create_savepoint(self.name)

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit the savepoint context.

        Args:
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred
            exc_tb: Exception traceback if an exception occurred
        """
        try:
            if exc_type is not None:
                await self.batcher._adapter.rollback_to_savepoint(self.name)
            else:
                await self.batcher._adapter.release_savepoint(self.name)
        finally:
            delattr(self.batcher, "_in_savepoint_context")


class AsyncSQLBatcher:
    """
    Async SQL Batcher for efficiently executing SQL statements in batches.

    This is the async version of SQLBatcher, designed to work with async database
    drivers and async execution contexts. It provides the same batching capabilities
    as SQLBatcher but with async/await support.

    Attributes:
        max_bytes: Maximum batch size in bytes
        delimiter: SQL statement delimiter
        dry_run: Whether to operate in dry run mode (without executing)
        current_batch: Current batch of SQL statements
        current_size: Current size of the batch in bytes
        auto_adjust_for_columns: Whether to dynamically adjust batch size based on column count
        reference_column_count: The reference column count for auto-adjustment (baseline)
        min_adjustment_factor: Minimum adjustment factor for batch size
        max_adjustment_factor: Maximum adjustment factor for batch size
        column_count: Detected column count for INSERT statements
        adjustment_factor: Current adjustment factor based on column count
    """

    def __init__(
        self,
        adapter: AsyncSQLAdapter,
        max_bytes: int = 900_000,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        timeout: Optional[float] = None,
        merge_inserts: bool = False,
    ) -> None:
        """Initialize the AsyncSQLBatcher.

        Args:
            adapter: The SQL adapter to use
            max_bytes: Maximum size in bytes for a batch
            max_retries: Maximum number of retry attempts
            retry_delay: Delay between retries in seconds
            timeout: Operation timeout in seconds
            merge_inserts: Whether to merge compatible INSERT statements
        """
        self._adapter = adapter
        self.batch_manager = BatchManager(max_bytes=max_bytes)
        self.retry_manager = RetryManager(
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
        )
        self.hook_manager = HookManager()
        self.merge_inserts = merge_inserts
        if merge_inserts:
            self.insert_merger = InsertMerger(max_bytes=max_bytes)
        else:
            self.insert_merger = None
        self._collector = QueryCollector()
        self._plugin_manager = PluginManager()

        # Expose public attributes
        self.max_bytes = max_bytes
        self.delimiter = self._collector.get_delimiter()
        self.dry_run = self._collector.is_dry_run()
        self.auto_adjust_for_columns = False
        self.reference_column_count = self._collector.get_reference_column_count()
        self.min_adjustment_factor = self._collector.get_min_adjustment_factor()
        self.max_adjustment_factor = self._collector.get_max_adjustment_factor()
        self.column_count = self._collector.get_column_count()
        self.adjustment_factor = self._collector.get_adjustment_factor()

    def register_plugin(self, plugin: Plugin) -> None:
        """Register a plugin.

        Args:
            plugin: The plugin to register
        """
        self._plugin_manager.register_plugin(plugin)

    def unregister_plugin(self, plugin_name: str) -> None:
        """Unregister a plugin by name.

        Args:
            plugin_name: Name of the plugin to unregister
        """
        self._plugin_manager.unregister_plugin(plugin_name)

    def get_plugins(self) -> List[Plugin]:
        """Get all registered plugins.

        Returns:
            List of registered plugins
        """
        return self._plugin_manager.get_plugins()

    def detect_column_count(self, statement: str) -> Optional[int]:
        """
        Detect the number of columns in an INSERT statement.

        Args:
            statement: SQL statement to analyze

        Returns:
            Number of columns detected, or None if not an INSERT statement or cannot be determined
        """
        # Only process INSERT statements
        if not re.search(r"^\s*INSERT\s+INTO", statement, re.IGNORECASE):
            return None

        # Try to find column count from VALUES clause
        values_pattern = r"VALUES\s*\(([^)]*)\)"
        match = re.search(values_pattern, statement, re.IGNORECASE)
        if match:
            # Count commas in the first VALUES group and add 1
            values_content = match.group(1)
            # Handle nested parentheses in complex expressions
            depth = 0
            comma_count = 0
            for char in values_content:
                if char == "(" or char == "[" or char == "{":
                    depth += 1
                elif char == ")" or char == "]" or char == "}":
                    depth -= 1
                elif char == "," and depth == 0:
                    comma_count += 1
            return comma_count + 1

        # Try to find explicit column list
        columns_pattern = r"INSERT\s+INTO\s+\w+\s*\(([^)]*)\)"
        match = re.search(columns_pattern, statement, re.IGNORECASE)
        if match:
            columns_str = match.group(1)
            # Count commas in the column list and add 1
            comma_count = columns_str.count(",")
            return comma_count + 1

        return None

    def update_adjustment_factor(self, statement: str) -> None:
        """
        Update the adjustment factor based on the column count in the statement.

        Args:
            statement: SQL statement to analyze
        """
        if not self.auto_adjust_for_columns:
            return

        # Only detect columns if we haven't already
        if self._collector.get_column_count() is None:
            detected_count = self.detect_column_count(statement)
            if detected_count is not None:
                self._collector.set_column_count(detected_count)
                self.column_count = detected_count

                # Calculate adjustment factor
                # More columns -> smaller batches (lower adjusted max_bytes)
                # Fewer columns -> larger batches (higher adjusted max_bytes)
                raw_factor = (
                    self._collector.get_reference_column_count() / detected_count
                )

                # Clamp to min/max bounds
                factor = max(
                    self._collector.get_min_adjustment_factor(),
                    min(self._collector.get_max_adjustment_factor(), raw_factor),
                )
                self._collector.set_adjustment_factor(factor)
                self.adjustment_factor = factor

                # Logging for debugging
                logger.debug(
                    f"Column-based adjustment: detected {self._collector.get_column_count()} columns, "
                    f"reference is {self._collector.get_reference_column_count()}, "
                    f"adjustment factor is {self._collector.get_adjustment_factor():.2f}"
                )

    def get_adjusted_max_bytes(self) -> int:
        """
        Get the max_bytes value adjusted for column count.

        Returns:
            Adjusted max_bytes value
        """
        if self.adjustment_factor == 1.0:
            return self.max_bytes

        return int(self.max_bytes * self.adjustment_factor)

    async def add_statement(self, statement: str) -> None:
        """Add a statement to the current batch.

        Args:
            statement: SQL statement to add
        """
        if not statement:
            return

        if self.merge_inserts and statement.upper().strip().startswith("INSERT"):
            merged = self.insert_merger.add_statement(statement)
            if merged:
                self.batch_manager.add_statement(merged)
        else:
            self.batch_manager.add_statement(statement)

    async def process_statements(
        self,
        statements: List[str],
        execute_callback: Callable[[str], None],
    ) -> None:
        """Process a list of SQL statements.

        Args:
            statements: List of SQL statements to process
            execute_callback: Callback to execute SQL statements
        """
        for statement in statements:
            await self.add_statement(statement)

        await self.process_batch(execute_callback)

    async def process_batch(
        self,
        execute_callback: Callable[[str], None],
    ) -> None:
        """Process the current batch of statements.

        Args:
            execute_callback: Callback to execute SQL statements
        """
        batch = self.batch_manager.get_batch()
        if not batch:
            return

        try:
            # Execute pre-batch hooks
            await self.hook_manager.execute_hooks(
                "pre_batch",
                {"statements": batch},
            )

            # Execute the batch
            await execute_callback("; ".join(batch))

            # Execute post-batch hooks
            await self.hook_manager.execute_hooks(
                "post_batch",
                {"statements": batch},
            )
        except Exception as e:
            # Execute error hooks
            await self.hook_manager.execute_hooks(
                "on_error",
                {"statements": batch, "error": e},
            )
            raise e

    async def process_stream(
        self,
        statement_stream: List[str],
        execute_callback: Callable[[str], None],
        chunk_size: int = 1000,
    ) -> None:
        """Process a stream of SQL statements.

        Args:
            statement_stream: Stream of SQL statements
            execute_callback: Callback to execute SQL statements
            chunk_size: Number of statements to process at once
        """
        for i in range(0, len(statement_stream), chunk_size):
            chunk = statement_stream[i:i + chunk_size]
            await self.process_statements(chunk, execute_callback)

    async def process_chunk(
        self,
        chunk: List[str],
        execute_callback: Callable[[str], None],
    ) -> None:
        """Process a chunk of SQL statements.

        Args:
            chunk: Chunk of SQL statements
            execute_callback: Callback to execute SQL statements
        """
        await self.process_statements(chunk, execute_callback)

    async def reset(self) -> None:
        """Reset the batcher state."""
        self.batch_manager.reset()
        if self.insert_merger:
            self.insert_merger = InsertMerger(max_bytes=self.batch_manager.max_bytes)

    async def __aenter__(self) -> "AsyncSQLBatcher":
        """Enter the async context."""
        await self._adapter.begin_transaction()
        return self

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Exit the async context.

        Args:
            exc_type: Exception type if an exception occurred
            exc_val: Exception value if an exception occurred
            exc_tb: Exception traceback if an exception occurred
        """
        if exc_type is not None:
            await self._adapter.rollback_transaction()
            await self.reset()
        else:
            await self._adapter.commit_transaction()

    async def create_savepoint(self, name: str) -> None:
        """Create a savepoint in the current transaction.

        Args:
            name: Name of the savepoint to create
        """
        await self._adapter.create_savepoint(name)

    async def rollback_to_savepoint(self, name: str) -> None:
        """Rollback to a previously created savepoint.

        Args:
            name: Name of the savepoint to rollback to
        """
        await self._adapter.rollback_to_savepoint(name)

    async def release_savepoint(self, name: str) -> None:
        """Release a previously created savepoint.

        Args:
            name: Name of the savepoint to release
        """
        await self._adapter.release_savepoint(name)

    def savepoint(self, name: Optional[str] = None) -> SavepointContext:
        """Create a savepoint context.

        Args:
            name: Optional name for the savepoint. If not provided, a unique name will be generated.

        Returns:
            A savepoint context manager.
        """
        if name is None:
            name = f"sp_{id(self)}_{int(asyncio.get_event_loop().time())}"
        return SavepointContext(self, name)
