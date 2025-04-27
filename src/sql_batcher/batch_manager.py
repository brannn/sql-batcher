"""Batch manager for SQL Batcher.

This module handles batch sizing, merging, and column-aware logic for SQL statements.
"""

from typing import Any, Dict, List, Optional

from sql_batcher.collectors.query_collector import QueryCollector
from sql_batcher.utils.insert_merger import InsertMerger


class BatchManager:
    """Manages batch operations for SQL statements."""

    def __init__(
        self,
        max_bytes: Optional[int] = None,
        batch_mode: bool = True,
        **kwargs: Any,
    ) -> None:
        """Initialize the batch manager.

        Args:
            max_bytes: Maximum batch size in bytes
            batch_mode: Whether to operate in batch mode
            **kwargs: Additional configuration options
        """
        self._max_bytes = max_bytes or 1_000_000  # Default to 1MB if not specified
        self._batch_mode = batch_mode
        self._collector = QueryCollector(**kwargs)
        self._merger = InsertMerger()

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

    def get_adjusted_max_bytes(self) -> int:
        """Get the max_bytes value adjusted for column count.

        Returns:
            Adjusted max_bytes value
        """
        if not self._batch_mode or self._collector.get_adjustment_factor() == 1.0:
            return self._max_bytes

        return int(self._max_bytes * self._collector.get_adjustment_factor())

    def add_statement(self, statement: str) -> bool:
        """Add a statement to the current batch.

        Args:
            statement: SQL statement to add

        Returns:
            True if the batch should be flushed, False otherwise
        """
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

    def merge_insert_statements(self, statements: List[str]) -> List[str]:
        """Merge INSERT statements into a single statement where possible.

        Args:
            statements: List of SQL statements to merge

        Returns:
            List of merged SQL statements
        """
        return self._merger.merge(statements)

    def get_metadata(self) -> Dict[str, Any]:
        """Get current batch metadata.

        Returns:
            Dictionary of batch metadata
        """
        return {
            "batch_size": len(self.current_batch),
            "current_size": self.current_size,
            "max_bytes": self.max_bytes,
            "adjusted_max_bytes": self.get_adjusted_max_bytes(),
            "column_count": self.column_count,
            "adjustment_factor": self.adjustment_factor,
        }
