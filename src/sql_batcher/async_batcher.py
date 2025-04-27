"""
Async SQL Batcher implementation.

This module contains the AsyncSQLBatcher class, which is the async version of
the main SQLBatcher class for batching SQL statements based on size limits.
"""

import re
from typing import Any, Awaitable, Callable, Dict, List, Optional

from sql_batcher.adapters.base import AsyncSQLAdapter
from sql_batcher.hooks import Plugin, PluginManager
from sql_batcher.insert_merger import InsertMerger
from sql_batcher.query_collector import QueryCollector


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
        max_batch_size: int = 1000,
        max_batch_bytes: int = 900_000,
    ) -> None:
        """Initialize the AsyncSQLBatcher.

        Args:
            adapter: The SQL adapter to use
            max_batch_size: Maximum number of statements in a batch
            max_batch_bytes: Maximum size of a batch in bytes
        """
        self._adapter = adapter
        self.max_batch_size = max_batch_size
        self.max_batch_bytes = max_batch_bytes
        self.current_batch: List[str] = []
        self.current_size = 0
        self._collector = QueryCollector()
        self._plugin_manager = PluginManager()

        # Expose public attributes
        self.max_bytes = 1_000_000  # Default to 1MB if not specified
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
                import logging

                logging.debug(
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

    def add_statement(self, statement: str) -> bool:
        """Add a statement to the current batch.

        Args:
            statement: The SQL statement to add.

        Returns:
            True if the statement should be flushed, False otherwise.
        """
        # Check if adding this statement would exceed the limits
        statement_size = len(statement.encode("utf-8"))
        new_size = self.current_size + statement_size
        new_count = len(self.current_batch) + 1

        # Check if we should flush before adding
        should_flush = (
            new_count >= self.max_batch_size or new_size >= self.max_batch_bytes
        )

        # Add the statement to the current batch
        self.current_batch.append(statement)
        self.current_size = new_size

        return should_flush

    def reset(self) -> None:
        """Reset the current batch."""
        self._collector.reset()
        # Update public attributes
        self.current_batch = self._collector.get_batch()
        self.current_size = self._collector.get_current_size()

    async def flush(
        self,
        execute_callback: Callable[[str], Awaitable[Any]],
        query_collector: Optional[QueryCollector] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[str]:
        """Flush the current batch of statements.

        Args:
            execute_callback: Callback function to execute each statement
            query_collector: Optional QueryCollector for tracking
            metadata: Optional metadata to pass to hooks

        Returns:
            List of processed statements
        """
        if not self.current_batch:
            return []

        # Only create a savepoint if we're not already in a savepoint context
        savepoint_name = f"batch_{id(self)}"
        if not hasattr(self, "_in_savepoint_context"):
            await self._adapter.create_savepoint(savepoint_name)

        try:
            # Execute each statement in the batch
            processed_statements = []
            for statement in self.current_batch:
                try:
                    await execute_callback(statement)
                    processed_statements.append(statement)
                except Exception as e:
                    # Only rollback and reset if it's not a retryable error
                    from sql_batcher.exceptions import AdapterExecutionError

                    if not isinstance(e, AdapterExecutionError):
                        if not hasattr(self, "_in_savepoint_context"):
                            await self._adapter.rollback_to_savepoint(savepoint_name)
                        self.reset()
                    raise e

            # Release the savepoint
            if not hasattr(self, "_in_savepoint_context"):
                await self._adapter.release_savepoint(savepoint_name)

            # Reset the batch
            self.reset()

            return processed_statements
        except Exception as e:
            # Only rollback and reset if it's not a retryable error
            from sql_batcher.exceptions import AdapterExecutionError

            if not isinstance(e, AdapterExecutionError):
                if not hasattr(self, "_in_savepoint_context"):
                    await self._adapter.rollback_to_savepoint(savepoint_name)
                self.reset()
            raise e

    def _merge_insert_statements(self, statements: List[str]) -> List[str]:
        """
        Merge INSERT statements into a single statement where possible.

        Args:
            statements: List of SQL statements to merge

        Returns:
            List of merged SQL statements
        """
        merger = InsertMerger()
        return merger.merge(statements)

    async def process_statements(
        self,
        statements: List[str],
        execute_callback: Callable[[str], Awaitable[Any]],
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[str]:
        """Process a list of statements.

        Args:
            statements: List of SQL statements to process
            execute_callback: Callback function to execute each statement
            metadata: Optional metadata to pass to hooks

        Returns:
            List of processed statements
        """
        query_collector = QueryCollector()
        processed_statements: List[str] = []

        for statement in statements:
            should_flush = self.add_statement(statement)
            if should_flush:
                processed = await self.flush(
                    execute_callback, query_collector, metadata
                )
                processed_statements.extend(processed)

        # Flush any remaining statements
        if self.current_batch:
            processed = await self.flush(execute_callback, query_collector, metadata)
            processed_statements.extend(processed)

        return processed_statements

    async def process_batch(
        self,
        statements: List[str],
        execute_func: Optional[Callable[[str], Awaitable[Any]]] = None,
    ) -> List[Any]:
        """
        Process a batch of statements as a single unit.

        Args:
            statements: List of SQL statements to process
            execute_func: Optional async function to execute SQL statements

        Returns:
            List of results from executing the statements
        """
        if execute_func is None:
            execute_func = self._adapter.execute

        results = []
        for statement in statements:
            result = await execute_func(statement)
            results.append(result)

        return results

    async def process_stream(
        self,
        statements: List[str],
        execute_func: Optional[Callable[[str], Awaitable[Any]]] = None,
    ) -> List[Any]:
        """
        Process statements in a streaming fashion.

        Args:
            statements: List of SQL statements to process
            execute_func: Optional async function to execute SQL statements

        Returns:
            List of results from executing the statements
        """
        if execute_func is None:
            execute_func = self._adapter.execute

        results = []
        for statement in statements:
            result = await execute_func(statement)
            results.append(result)

        return results

    async def process_chunk(
        self,
        statements: List[str],
        execute_func: Optional[Callable[[str], Awaitable[Any]]] = None,
    ) -> List[Any]:
        """
        Process statements in chunks.

        Args:
            statements: List of SQL statements to process
            execute_func: Optional async function to execute SQL statements

        Returns:
            List of results from executing the statements
        """
        if execute_func is None:
            execute_func = self._adapter.execute

        results = []
        for statement in statements:
            result = await execute_func(statement)
            results.append(result)

        return results

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
            name = f"sp_{id(self)}_{int(time.time())}"
        return SavepointContext(self, name)
