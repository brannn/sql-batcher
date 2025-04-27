"""
Async SQL Batcher implementation.

This module contains the AsyncSQLBatcher class, which is the async version of
the main SQLBatcher class for batching SQL statements based on size limits.
"""

import re
from types import TracebackType
from typing import Any, Awaitable, Callable, Dict, List, Optional, Type

from sql_batcher.adapters.base import AsyncSQLAdapter
from sql_batcher.insert_merger import InsertMerger
from sql_batcher.plugins import HookType, Plugin, PluginManager
from sql_batcher.query_collector import QueryCollector


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
        max_bytes: Optional[int] = None,
        batch_mode: bool = True,
        **kwargs: Any,
    ) -> None:
        """Initialize the async SQL batcher."""
        self._adapter = adapter
        self._max_bytes = max_bytes or 1_000_000  # Default to 1MB if not specified
        self._batch_mode = batch_mode
        self._collector = QueryCollector(**kwargs)
        self._plugin_manager = PluginManager()

        # Expose public attributes
        self.max_bytes = self._max_bytes
        self.delimiter = self._collector.get_delimiter()
        self.dry_run = self._collector.is_dry_run()
        self.current_batch = self._collector.get_batch()
        self.current_size = self._collector.get_current_size()
        self.auto_adjust_for_columns = kwargs.get("auto_adjust_for_columns", False)
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
        if not self._batch_mode or not self.auto_adjust_for_columns:
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
        if not self._batch_mode or self._collector.get_adjustment_factor() == 1.0:
            return self._max_bytes

        return int(self._max_bytes * self._collector.get_adjustment_factor())

    def add_statement(self, statement: str) -> bool:
        """
        Add a statement to the current batch.

        Args:
            statement: SQL statement to add

        Returns:
            True if the batch should be flushed, False otherwise
        """
        # Update adjustment factor if needed
        self.update_adjustment_factor(statement)

        # Ensure statement ends with delimiter
        if not statement.strip().endswith(self._collector.get_delimiter()):
            statement = statement.strip() + self._collector.get_delimiter()

        # Add statement to batch
        self._collector.collect(statement)

        # Update size
        statement_size = len(statement.encode("utf-8"))
        self._collector.update_current_size(statement_size)

        # Update public attributes
        self.current_batch = self._collector.get_batch()
        self.current_size = self._collector.get_current_size()

        # Get adjusted max_bytes for comparison
        adjusted_max_bytes = self.get_adjusted_max_bytes()

        # Check if batch should be flushed
        return self._collector.get_current_size() >= adjusted_max_bytes

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
    ) -> int:
        """Flush the current batch of statements.

        Args:
            execute_callback: Async function to execute SQL statements
            query_collector: Optional query collector for tracking
            metadata: Optional metadata for hooks

        Returns:
            Number of statements processed

        Raises:
            Exception: If batch processing fails
        """
        if not self.current_batch:
            return 0

        try:
            # Create a savepoint before processing the batch
            savepoint_name = f"batch_{id(self.current_batch)}"
            await self._adapter.create_savepoint(savepoint_name)

            # Execute pre-batch hooks
            await self._plugin_manager.execute_hooks(
                HookType.PRE_BATCH,
                self.current_batch,
                metadata,
            )

            # Merge INSERT statements if possible
            merged_statements = self._merge_insert_statements(self.current_batch)

            # Execute pre-execute hooks
            await self._plugin_manager.execute_hooks(
                HookType.PRE_EXECUTE,
                merged_statements,
                metadata,
            )

            # Execute statements
            results = []
            for statement in merged_statements:
                try:
                    result = await execute_callback(statement)
                    results.append(result)
                except Exception as e:
                    # Rollback to savepoint on error
                    await self._adapter.rollback_to_savepoint(savepoint_name)
                    # Execute error hooks
                    await self._plugin_manager.execute_hooks(
                        HookType.ON_ERROR,
                        [statement],
                        metadata,
                        e,
                    )
                    raise

            # Execute post-execute hooks
            await self._plugin_manager.execute_hooks(
                HookType.POST_EXECUTE,
                merged_statements,
                metadata,
            )

            # Update collector if provided
            if query_collector:
                query_collector.update_metadata(metadata or {})

            # Execute post-batch hooks
            await self._plugin_manager.execute_hooks(
                HookType.POST_BATCH,
                merged_statements,
                metadata,
            )

            # Release the savepoint
            await self._adapter.release_savepoint(savepoint_name)

            # Reset batch
            self.reset()

            return len(results)

        except Exception as e:
            # Execute error hooks
            await self._plugin_manager.execute_hooks(
                HookType.ON_ERROR,
                self.current_batch,
                metadata,
                e,
            )
            raise

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
        query_collector: Optional[QueryCollector] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> int:
        """
        Process a list of SQL statements in batches.

        Args:
            statements: List of SQL statements to process
            execute_callback: Async function to execute SQL statements
            query_collector: Optional QueryCollector for tracking
            metadata: Optional metadata to pass to the collector

        Returns:
            Number of statements processed
        """
        total_processed = 0

        for statement in statements:
            should_flush = self.add_statement(statement)
            if should_flush:
                processed = await self.flush(
                    execute_callback, query_collector, metadata
                )
                total_processed += processed

        # Flush any remaining statements
        if self.current_batch:
            processed = await self.flush(execute_callback, query_collector, metadata)
            total_processed += processed

        return total_processed

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
        """Enter the async context manager.

        This method is called when entering an async with block. It initializes
        the batcher and executes any plugin initialization hooks.

        Returns:
            The batcher instance for use in the async with block
        """
        # Execute plugin initialization hooks
        await self._plugin_manager.execute_hooks(
            HookType.PRE_BATCH, [], {"context": "initialization"}
        )
        return self

    async def __aexit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        """Exit the async context manager.

        This method is called when exiting an async with block. It ensures that
        any remaining batches are flushed and resources are cleaned up.

        Args:
            exc_type: The type of exception that occurred, if any
            exc_val: The exception instance that occurred, if any
            exc_tb: The traceback for the exception, if any
        """
        try:
            # Flush any remaining batches
            if self.current_batch:
                await self.flush(
                    self._adapter.execute, self._collector, {"context": "cleanup"}
                )
        except Exception as e:
            # Execute error hooks
            await self._plugin_manager.execute_hooks(
                HookType.ON_ERROR, self.current_batch, {"context": "cleanup"}, e
            )
            raise
        finally:
            # Always clean up state
            self.reset()

            # Execute plugin cleanup hooks
            await self._plugin_manager.execute_hooks(
                HookType.POST_BATCH, [], {"context": "cleanup"}
            )
