from typing import List


class QueryCollector:
    """
    A class that collects and manages SQL queries for batch processing.

    This class provides functionality to collect SQL queries, track their execution status,
    and manage batch processing of queries.

    Attributes:
        queries: List of collected SQL queries.
        max_batch_size: Maximum number of queries to process in a single batch.
        current_batch: Current batch of queries being processed.
        batch_size: Number of queries in the current batch.
    """

    def __init__(self, max_batch_size: int = 1000):
        """
        Initialize the QueryCollector with a maximum batch size.

        Args:
            max_batch_size: Maximum number of queries to process in a single batch.
                           Default is 1000 queries.
        """
        self.queries: List[str] = []
        self.max_batch_size = max_batch_size
        self.current_batch: List[str] = []
        self.batch_size = 0

    def add_query(self, query: str) -> None:
        """
        Add a query to the collection.

        Args:
            query: The SQL query to add.
        """
        self.queries.append(query)

    def get_batch(self) -> List[str]:
        """
        Get the next batch of queries to process.

        Returns:
            A list of queries to process in the current batch.
        """
        if not self.queries:
            return []

        # Get the next batch of queries
        batch = self.queries[: self.max_batch_size]
        self.queries = self.queries[self.max_batch_size :]
        self.current_batch = batch
        self.batch_size = len(batch)
        return batch

    def mark_batch_complete(self) -> None:
        """
        Mark the current batch as complete and clear it.
        """
        self.current_batch = []
        self.batch_size = 0

    def has_queries(self) -> bool:
        """
        Check if there are any queries remaining to be processed.

        Returns:
            True if there are queries remaining, False otherwise.
        """
        return len(self.queries) > 0

    def get_remaining_count(self) -> int:
        """
        Get the number of queries remaining to be processed.

        Returns:
            The number of remaining queries.
        """
        return len(self.queries)

    def flush(self) -> List[str]:
        """
        Flush all remaining queries and return them as a list.

        Returns:
            A list of all remaining queries.
        """
        remaining = self.queries
        self.queries = []
        self.current_batch = []
        self.batch_size = 0
        return remaining
