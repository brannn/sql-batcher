"""
AsyncQueryCollector: An async utility class for collecting and tracking SQL queries.
"""

from typing import Any, Dict, List, Optional

from sql_batcher.query_collector import QueryCollector


class AsyncQueryCollector(QueryCollector):
    """
    An async class that collects SQL queries for inspection and debugging.

    This class extends QueryCollector to provide async methods for collecting
    and analyzing SQL statements executed by AsyncSQLBatcher.

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

    async def collect_async(self, query: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Collect a SQL query asynchronously.

        Parameters:
            query (str): The SQL query to collect.
            metadata (Optional[Dict[str, Any]]): Optional metadata to associate with the query.
        """
        # For now, just call the synchronous version
        # This is fine because the collection operation is not I/O bound
        self.collect(query, metadata)

    async def clear_async(self) -> None:
        """Clear all collected queries asynchronously."""
        # For now, just call the synchronous version
        self.clear()

    async def get_all_async(self) -> List[Dict[str, Any]]:
        """
        Get all collected queries asynchronously.

        Returns:
            list: List of all collected queries with their metadata.
        """
        return self.get_all()

    async def get_count_async(self) -> int:
        """
        Get the count of collected queries asynchronously.

        Returns:
            int: Number of collected queries.
        """
        return self.get_count()

    async def get_batch_async(self) -> List[str]:
        """
        Get the current batch of queries asynchronously.

        Returns:
            list: Current batch of queries.
        """
        return self.get_batch()

    async def get_current_size_async(self) -> int:
        """
        Get the current size of collected queries in bytes asynchronously.

        Returns:
            int: Current size in bytes.
        """
        return self.get_current_size()

    async def update_current_size_async(self, size: int) -> None:
        """
        Update the current size of collected queries asynchronously.

        Parameters:
            size (int): Size to add to current size.
        """
        self.update_current_size(size)

    async def reset_async(self) -> None:
        """Reset the collector state asynchronously."""
        self.reset()


class AsyncListQueryCollector(AsyncQueryCollector):
    """
    A specialized AsyncQueryCollector that maintains a list of queries.

    This class extends AsyncQueryCollector to provide additional functionality
    for managing a list of queries with metadata.
    """

    async def get_queries_async(self) -> List[Dict[str, Any]]:
        """
        Get all collected queries with their metadata asynchronously.

        Returns:
            List[Dict[str, Any]]: List of queries with metadata.
        """
        return self.get_queries()
