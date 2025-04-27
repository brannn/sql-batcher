"""
QueryCollector: A utility class for collecting and tracking SQL queries.
"""

from typing import List


class QueryCollector:
    """
    A class that collects SQL queries for inspection and debugging.

    This class provides methods to collect and analyze SQL statements
    executed by SQLBatcher.

    Attributes:
        queries (list): List of collected SQL statements.
    """

    def __init__(self) -> None:
        """Initialize an empty QueryCollector."""
        self.queries: List[str] = []

    def collect(self, query: str) -> None:
        """
        Collect a SQL query.

        Parameters:
            query (str): The SQL query to collect.
        """
        self.queries.append(query)

    def clear(self) -> None:
        """Clear all collected queries."""
        self.queries = []

    def get_all(self) -> List[str]:
        """
        Get all collected queries.

        Returns:
            list: List of all collected queries.
        """
        return self.queries

    def get_count(self) -> int:
        """
        Get the count of collected queries.

        Returns:
            int: Number of collected queries.
        """
        return len(self.queries)
