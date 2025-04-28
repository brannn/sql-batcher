"""
QueryCollector: A tool for collecting and managing SQL queries.

This module provides functionality to collect and manage SQL queries
for batch processing.
"""

from typing import List, Optional

class QueryCollector:
    """Collects and manages SQL queries for batch processing."""

    def __init__(self, max_batch_size: int = 1000) -> None:
        """Initialize the collector.

        Args:
            max_batch_size: Maximum number of queries in a batch
        """
        self.max_batch_size = max_batch_size
        self.queries: List[str] = []
        self.current_batch: List[str] = []
        self.batch_size = 0

    def add_query(self, query: str) -> None:
        """Add a query to the collection.

        Args:
            query: SQL query to add
        """
        if not query:
            return

        self.queries.append(query)
        self.batch_size += 1

    def get_batch(self) -> List[str]:
        """Get the next batch of queries.

        Returns:
            List of queries in the batch
        """
        if not self.queries:
            self.current_batch = []
            return []

        # Get the next batch
        batch = self.queries[:self.max_batch_size]
        self.queries = self.queries[self.max_batch_size:]
        self.current_batch = batch
        self.batch_size = len(batch)
        return batch

    def flush(self) -> List[str]:
        """Flush all remaining queries.

        Returns:
            List of remaining queries
        """
        remaining = self.queries.copy()
        self.queries = []
        self.current_batch = []
        self.batch_size = 0
        return remaining

    def reset(self) -> None:
        """Reset the collector state."""
        self.queries = []
        self.current_batch = []
        self.batch_size = 0
