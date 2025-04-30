"""
QueryCollector: A utility class for collecting and tracking SQL queries.
"""

from typing import Any, Dict, List, Optional


class QueryCollector:
    """
    A class that collects SQL queries for inspection and debugging.

    This class provides methods to collect and analyze SQL statements
    executed by SQLBatcher.

    Attributes:
        queries (list): List of collected SQL statements.
        current_size (int): Current size of collected queries in bytes.
        column_count (Optional[int]): Number of columns in INSERT statements.
        reference_column_count (int): Reference column count for batch size adjustment.
        min_adjustment_factor (float): Minimum adjustment factor for batch size.
        max_adjustment_factor (float): Maximum adjustment factor for batch size.
        adjustment_factor (float): Current adjustment factor based on column count.
        delimiter (str): SQL statement delimiter.
        dry_run (bool): Whether to operate in dry run mode.
        auto_adjust_for_columns (bool): Whether to automatically adjust for columns.
        merge_inserts (bool): Whether to merge compatible INSERT statements.
    """

    def __init__(
        self,
        delimiter: str = ";",
        dry_run: bool = False,
        reference_column_count: int = 10,
        min_adjustment_factor: float = 0.5,
        max_adjustment_factor: float = 2.0,
        auto_adjust_for_columns: bool = False,
        merge_inserts: bool = False,
    ) -> None:
        """Initialize a QueryCollector."""
        self.queries: List[Dict[str, Any]] = []
        self.current_size: int = 0
        self.column_count: Optional[int] = None
        self.reference_column_count = reference_column_count
        self.min_adjustment_factor = min_adjustment_factor
        self.max_adjustment_factor = max_adjustment_factor
        self.adjustment_factor: float = 1.0
        self.delimiter = delimiter
        self.dry_run = dry_run
        self.auto_adjust_for_columns = auto_adjust_for_columns
        self.merge_inserts = merge_inserts

    def collect(self, query: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Collect a SQL query.

        Parameters:
            query (str): The SQL query to collect.
            metadata (Optional[Dict[str, Any]]): Optional metadata to associate with the query.
        """
        self.queries.append({"query": query, "metadata": metadata or {}})

    def clear(self) -> None:
        """Clear all collected queries."""
        self.queries = []
        self.current_size = 0

    def get_all(self) -> List[Dict[str, Any]]:
        """
        Get all collected queries.

        Returns:
            list: List of all collected queries with their metadata.
        """
        return self.queries

    def get_count(self) -> int:
        """
        Get the count of collected queries.

        Returns:
            int: Number of collected queries.
        """
        return len(self.queries)

    def get_batch(self) -> List[str]:
        """
        Get the current batch of queries.

        Returns:
            list: Current batch of queries.
        """
        return [q["query"] for q in self.queries]

    def get_current_size(self) -> int:
        """
        Get the current size of collected queries in bytes.

        Returns:
            int: Current size in bytes.
        """
        return self.current_size

    def update_current_size(self, size: int) -> None:
        """
        Update the current size of collected queries.

        Parameters:
            size (int): Size to add to current size.
        """
        self.current_size += size

    def reset(self) -> None:
        """Reset the collector state."""
        self.queries = []
        self.current_size = 0

    def get_column_count(self) -> Optional[int]:
        """
        Get the number of columns in INSERT statements.

        Returns:
            Optional[int]: Number of columns, or None if not set.
        """
        return self.column_count

    def set_column_count(self, count: int) -> None:
        """
        Set the number of columns in INSERT statements.

        Parameters:
            count (int): Number of columns.
        """
        self.column_count = count

    def get_reference_column_count(self) -> int:
        """
        Get the reference column count for batch size adjustment.

        Returns:
            int: Reference column count.
        """
        return self.reference_column_count

    def get_min_adjustment_factor(self) -> float:
        """
        Get the minimum adjustment factor for batch size.

        Returns:
            float: Minimum adjustment factor.
        """
        return self.min_adjustment_factor

    def get_max_adjustment_factor(self) -> float:
        """
        Get the maximum adjustment factor for batch size.

        Returns:
            float: Maximum adjustment factor.
        """
        return self.max_adjustment_factor

    def get_adjustment_factor(self) -> float:
        """
        Get the current adjustment factor.

        Returns:
            float: Current adjustment factor.
        """
        return self.adjustment_factor

    def set_adjustment_factor(self, factor: float) -> None:
        """
        Set the adjustment factor.

        Parameters:
            factor (float): New adjustment factor.
        """
        self.adjustment_factor = factor

    def get_delimiter(self) -> str:
        """
        Get the SQL statement delimiter.

        Returns:
            str: SQL statement delimiter.
        """
        return self.delimiter

    def is_dry_run(self) -> bool:
        """
        Check if dry run mode is enabled.

        Returns:
            bool: True if dry run mode is enabled.
        """
        return self.dry_run

    def should_merge_inserts(self) -> bool:
        """
        Check if insert merging is enabled.

        Returns:
            bool: True if insert merging is enabled.
        """
        return self.merge_inserts


class ListQueryCollector(QueryCollector):
    """
    A specialized QueryCollector that maintains a list of queries.

    This class extends QueryCollector to provide additional functionality
    for managing a list of queries with metadata.
    """

    def get_queries(self) -> List[Dict[str, Any]]:
        """
        Get all collected queries with their metadata.

        Returns:
            List[Dict[str, Any]]: List of queries with metadata.
        """
        return self.queries
