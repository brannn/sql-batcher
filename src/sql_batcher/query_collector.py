"""
Query collector for SQL Batcher.

This module provides functionality to collect and manage SQL statements for batching.
"""

from typing import Any, Dict, List, Optional


class QueryCollector:
    """
    Collector for SQL statements.

    This class manages the collection and tracking of SQL statements for batching.
    It provides functionality to track statement sizes, manage batches, and handle
    column-based adjustments.
    """

    def __init__(
        self,
        delimiter: str = ";",
        dry_run: bool = False,
        auto_adjust_for_columns: bool = False,
        reference_column_count: int = 10,
        min_adjustment_factor: float = 0.5,
        max_adjustment_factor: float = 2.0,
    ) -> None:
        """Initialize the query collector.

        Args:
            delimiter: SQL statement delimiter
            dry_run: Whether to operate in dry run mode
            auto_adjust_for_columns: Whether to adjust batch size based on column count
            reference_column_count: Reference column count for adjustment
            min_adjustment_factor: Minimum adjustment factor
            max_adjustment_factor: Maximum adjustment factor
        """
        self._delimiter = delimiter
        self._dry_run = dry_run
        self._batch: List[str] = []
        self._current_size = 0
        self._auto_adjust_for_columns = auto_adjust_for_columns
        self._reference_column_count = reference_column_count
        self._min_adjustment_factor = min_adjustment_factor
        self._max_adjustment_factor = max_adjustment_factor
        self._column_count: Optional[int] = None
        self._adjustment_factor = 1.0
        self._metadata: Dict[str, Any] = {}

    def get_delimiter(self) -> str:
        """Get the SQL statement delimiter.

        Returns:
            SQL statement delimiter
        """
        return self._delimiter

    def is_dry_run(self) -> bool:
        """Check if in dry run mode.

        Returns:
            True if in dry run mode, False otherwise
        """
        return self._dry_run

    def get_batch(self) -> List[str]:
        """Get the current batch of statements.

        Returns:
            List of SQL statements in the current batch
        """
        return self._batch

    def get_current_size(self) -> int:
        """Get the current size of the batch in bytes.

        Returns:
            Current batch size in bytes
        """
        return self._current_size

    def get_reference_column_count(self) -> int:
        """Get the reference column count.

        Returns:
            Reference column count
        """
        return self._reference_column_count

    def get_min_adjustment_factor(self) -> float:
        """Get the minimum adjustment factor.

        Returns:
            Minimum adjustment factor
        """
        return self._min_adjustment_factor

    def get_max_adjustment_factor(self) -> float:
        """Get the maximum adjustment factor.

        Returns:
            Maximum adjustment factor
        """
        return self._max_adjustment_factor

    def get_column_count(self) -> Optional[int]:
        """Get the current column count.

        Returns:
            Current column count if set, None otherwise
        """
        return self._column_count

    def get_adjustment_factor(self) -> float:
        """Get the current adjustment factor.

        Returns:
            Current adjustment factor
        """
        return self._adjustment_factor

    def set_column_count(self, count: int) -> None:
        """Set the column count.

        Args:
            count: Column count to set
        """
        self._column_count = count

    def set_adjustment_factor(self, factor: float) -> None:
        """Set the adjustment factor.

        Args:
            factor: Adjustment factor to set
        """
        self._adjustment_factor = factor

    def collect(self, statement: str) -> None:
        """Add a statement to the current batch.

        Args:
            statement: SQL statement to add
        """
        self._batch.append(statement)

    def update_current_size(self, size: int) -> None:
        """Update the current batch size.

        Args:
            size: Size to add to current batch size
        """
        self._current_size += size

    def update_metadata(self, metadata: Dict[str, Any]) -> None:
        """Update the metadata.

        Args:
            metadata: Metadata to update
        """
        self._metadata.update(metadata)

    def get_metadata(self) -> Dict[str, Any]:
        """Get the current metadata.

        Returns:
            Current metadata
        """
        return self._metadata

    def reset(self) -> None:
        """Reset the collector state."""
        self._batch = []
        self._current_size = 0
        self._metadata = {}
