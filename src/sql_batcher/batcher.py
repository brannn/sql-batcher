"""
Core SQL Batcher implementation.

This module contains the SQLBatcher class, which is the main entry point for
batching SQL statements based on size limits.
"""

import re
from typing import Any, Callable, Dict, List, Optional

from sql_batcher.adapters.base import SQLAdapter
from sql_batcher.insert_merger import InsertMerger
from sql_batcher.query_collector import QueryCollector


class SQLBatcher:
    """
    SQL Batcher for efficiently executing SQL statements in batches.

    SQL Batcher addresses a common challenge in database programming: efficiently
    executing many SQL statements while respecting query size limitations. It's
    especially valuable for systems like Trino and Snowflake that
    have query size or memory constraints.

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
        adapter: SQLAdapter,
        max_bytes: Optional[int] = None,
        batch_mode: bool = True,
        merge_inserts: bool = False,
        **kwargs: Any,
    ) -> None:
        """Initialize the SQL batcher."""
        self._adapter = adapter
        self._max_bytes = max_bytes or 1_000_000  # Default to 1MB if not specified
        self._batch_mode = batch_mode
        self._merge_inserts = merge_inserts
        kwargs["merge_inserts"] = merge_inserts
        self._collector = QueryCollector(**kwargs)

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
        self.merge_inserts = self._merge_inserts

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
                raw_factor = self._collector.get_reference_column_count() / detected_count

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

    def flush(
        self,
        execute_callback: Callable[[str], Any],
        query_collector: Optional[QueryCollector] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> int:
        """
        Flush the current batch of statements.

        Args:
            execute_callback: Function to execute SQL statements
            query_collector: Optional query collector for collecting queries
            metadata: Optional metadata to associate with the batch

        Returns:
            Number of statements flushed
        """
        # Get the current batch count
        count = len(self._collector.get_batch())

        # If batch is empty, return 0
        if count == 0:
            return 0

        # Join statements
        batch_sql = "\n".join(self._collector.get_batch())

        # If in dry run mode, just collect the queries
        if self._collector.is_dry_run():
            if query_collector:
                query_collector.collect(batch_sql, metadata=metadata)
            return 0
        else:
            # Execute the batch
            execute_callback(batch_sql)

            # Optionally collect the query
            if query_collector:
                query_collector.collect(batch_sql, metadata=metadata)

            # Reset the batch
            self.reset()

            # Update public attributes
            self.current_batch = self._collector.get_batch()
            self.current_size = self._collector.get_current_size()

            # Return the count
            return count

    def _merge_insert_statements(self, statements: List[str]) -> List[str]:
        """
        Attempts to merge simple INSERT INTO ... VALUES statements for optimization.

        This method uses InsertMerger to identify and merge compatible INSERT
        statements, reducing the number of database calls while respecting the
        maximum batch size.

        Args:
            statements: List of original SQL statements.

        Returns:
            Optimized list of SQL statements with compatible INSERTs merged.
        """
        merger = InsertMerger(self.get_adjusted_max_bytes())
        merged_statements: List[str] = []

        for statement in statements:
            result = merger.add_statement(statement)
            if result is not None:  # Check for None, as empty strings are valid SQL statements
                merged_statements.append(result)

        # Ensure we get any remaining statements from the merger
        merged_statements.extend(merger.flush_all())
        return merged_statements

    def process_statements(
        self,
        statements: List[str],
        execute_callback: Callable[[str], Any],
        query_collector: Optional[QueryCollector] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> int:
        """
        Process a list of SQL statements.

        Args:
            statements: List of SQL statements to process
            execute_callback: Function to execute SQL statements
            query_collector: Optional query collector for collecting queries
            metadata: Optional metadata to associate with the batch

        Returns:
            Number of statements processed
        """
        # If insert merging is enabled, merge compatible statements
        if self._merge_inserts:
            statements = self._merge_insert_statements(statements)

        results: List[Any] = []
        for statement in statements:
            # Add statement to batch
            if self.add_statement(statement):
                # Batch is full, flush it
                results.append(self.flush(execute_callback, query_collector, metadata))

        # Flush any remaining statements
        if self._collector.get_current_size() > 0:
            results.append(self.flush(execute_callback, query_collector, metadata))

        # Return the total count
        return len(statements)

    def process_batch(self, statements: List[str], execute_func: Optional[Callable[[str], Any]] = None) -> List[Any]:
        """
        Process a batch of statements.

        Args:
            statements: List of SQL statements to process
            execute_func: Optional function to execute SQL statements

        Returns:
            List of results from executed statements
        """
        if not statements:
            return []

        # Use provided execute function or adapter's execute method
        execute_callback = execute_func or self._adapter.execute

        # Process statements and return results
        results: List[Any] = []
        self.process_statements(statements, execute_callback)
        return results

    def process_stream(self, statements: List[str], execute_func: Optional[Callable[[str], Any]] = None) -> List[Any]:
        """
        Process statements in streaming mode.

        Args:
            statements: List of SQL statements to process
            execute_func: Optional function to execute SQL statements

        Returns:
            List of results from executed statements
        """
        if not statements:
            return []

        # Use provided execute function or adapter's execute method
        execute_callback = execute_func or self._adapter.execute

        # Process statements and return results
        results: List[Any] = []
        self.process_statements(statements, execute_callback)
        return results

    def process_chunk(self, statements: List[str], execute_func: Optional[Callable[[str], Any]] = None) -> List[Any]:
        """
        Process statements in chunks.

        Args:
            statements: List of SQL statements to process
            execute_func: Optional function to execute SQL statements

        Returns:
            List of results from executed statements
        """
        if not statements:
            return []

        # Use provided execute function or adapter's execute method
        execute_callback = execute_func or self._adapter.execute

        # Process statements and return results
        results: List[Any] = []
        self.process_statements(statements, execute_callback)
        return results

    def __enter__(self) -> "SQLBatcher":
        """Enter context manager."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Exit context manager, flushing any remaining statements."""
        if self._collector.get_current_size() > 0:
            self.flush(self._adapter.execute)
