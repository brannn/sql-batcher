"""
Base adapter classes for SQL Batcher.

This module provides abstract base classes for database adapters used by SQL Batcher.
"""

from abc import ABC, abstractmethod
from typing import Any, Callable, List, Optional, Sequence, TypeVar

T = TypeVar("T")


class SQLAdapter(ABC):
    """
    Abstract base class for SQL database adapters.

    This class defines the interface that all database adapters must implement.
    Each adapter provides database-specific functionality while maintaining
    a consistent interface for the SQL Batcher.
    """

    @abstractmethod
    def execute(self, sql: str) -> List[Any]:
        """
        Execute a SQL statement and return results.

        Args:
            sql: SQL statement to execute

        Returns:
            List of result rows
        """

    @abstractmethod
    def get_max_query_size(self) -> int:
        """
        Get the maximum query size in bytes.

        Returns:
            Maximum query size in bytes
        """

    @abstractmethod
    def close(self) -> None:
        """Close the database connection."""

    def begin_transaction(self) -> None:
        """
        Begin a transaction.

        Default implementation does nothing.
        Subclasses should override if the database supports transactions.
        """

    def commit_transaction(self) -> None:
        """
        Commit the current transaction.

        Default implementation does nothing.
        Subclasses should override if the database supports transactions.
        """

    def rollback_transaction(self) -> None:
        """
        Rollback the current transaction.

        Default implementation does nothing.
        Subclasses should override if the database supports transactions.
        """

    def create_savepoint(self, name: str) -> None:
        """
        Create a savepoint with the given name.

        Default implementation does nothing.
        Subclasses should override if the database supports savepoints.

        Args:
            name: Name of the savepoint
        """

    def rollback_to_savepoint(self, name: str) -> None:
        """
        Rollback to the savepoint with the given name.

        Default implementation does nothing.
        Subclasses should override if the database supports savepoints.

        Args:
            name: Name of the savepoint
        """

    def release_savepoint(self, name: str) -> None:
        """
        Release the savepoint with the given name.

        Default implementation does nothing.
        Subclasses should override if the database supports savepoints.

        Args:
            name: Name of the savepoint
        """


class GenericAdapter(SQLAdapter):
    """
    Generic adapter that can work with any database connection.

    This adapter takes connection objects and callback functions
    to interact with any database system. It's useful when a
    specialized adapter is not available.

    Args:
        connection: Database connection object
        execute_func: Optional custom function to execute SQL
        close_func: Optional custom function to close the connection
        max_query_size: Optional maximum query size in bytes
    """

    def __init__(
        self,
        connection: Any,
        execute_func: Optional[Callable[[str], Sequence[Any]]] = None,
        close_func: Optional[Callable[[], None]] = None,
        max_query_size: Optional[int] = None,
    ) -> None:
        """Initialize the generic adapter."""
        self._connection = connection
        self._execute_func = execute_func
        self._close_func = close_func
        self._max_query_size = max_query_size or 500_000  # Default 500KB

    def execute(self, sql: str) -> List[Any]:
        """
        Execute a SQL statement and return results.

        Args:
            sql: SQL statement to execute

        Returns:
            List of result rows
        """
        if self._execute_func:
            # Use the provided execute function
            result = self._execute_func(sql)
            return list(result) if result is not None else []
        elif hasattr(self._connection, "execute"):
            # Try to use connection's execute method directly
            result = self._connection.execute(sql)
            if result is None:
                return []
            if hasattr(result, "fetchall"):
                return list(result.fetchall())
            return list(result)
        elif hasattr(self._connection, "cursor"):
            # Try to get a cursor and use its execute method
            cursor = self._connection.cursor()
            if cursor is None:
                return []
            cursor.execute(sql)
            if cursor.description is not None:
                return list(cursor.fetchall())
            return []
        else:
            raise ValueError(
                "Cannot determine how to execute SQL with the provided connection. " "Please provide an execute_func."
            )

    def get_max_query_size(self) -> int:
        """
        Get the maximum query size in bytes.

        Returns:
            Maximum query size in bytes
        """
        return self._max_query_size

    def close(self) -> None:
        """Close the database connection."""
        if self._close_func:
            # Use the provided close function
            self._close_func()
        elif hasattr(self._connection, "close"):
            # Try to use connection's close method
            self._connection.close()

    def set_max_query_size(self, max_size: int) -> None:
        """
        Set the maximum query size.

        Args:
            max_size: Maximum query size in bytes
        """
        self._max_query_size = max_size
