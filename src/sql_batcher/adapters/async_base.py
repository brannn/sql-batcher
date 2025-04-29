"""
Async base adapter classes for SQL Batcher.

This module provides abstract base classes for async database adapters used by SQL Batcher.
"""

from abc import ABC, abstractmethod
from typing import Any, Awaitable, Callable, List, Optional, Sequence, TypeVar

T = TypeVar("T")


class AsyncSQLAdapter(ABC):
    """
    Abstract base class for async SQL database adapters.

    This class defines the interface that all async database adapters must implement.
    Each adapter provides database-specific functionality while maintaining
    a consistent interface for the AsyncSQLBatcher.
    """

    @abstractmethod
    async def execute(self, sql: str) -> List[Any]:
        """
        Execute a SQL statement asynchronously and return results.

        Args:
            sql: SQL statement to execute

        Returns:
            List of result rows
        """

    @abstractmethod
    async def get_max_query_size(self) -> int:
        """
        Get the maximum query size in bytes.

        Returns:
            Maximum query size in bytes
        """

    @abstractmethod
    async def close(self) -> None:
        """Close the database connection asynchronously."""

    async def begin_transaction(self) -> None:
        """
        Begin a transaction asynchronously.

        Default implementation does nothing.
        Subclasses should override if the database supports transactions.
        """

    async def commit_transaction(self) -> None:
        """
        Commit the current transaction asynchronously.

        Default implementation does nothing.
        Subclasses should override if the database supports transactions.
        """

    async def rollback_transaction(self) -> None:
        """
        Rollback the current transaction asynchronously.

        Default implementation does nothing.
        Subclasses should override if the database supports transactions.
        """

    async def create_savepoint(self, name: str) -> None:
        """
        Create a savepoint with the given name asynchronously.

        Default implementation does nothing.
        Subclasses should override if the database supports savepoints.

        Args:
            name: Name of the savepoint
        """

    async def rollback_to_savepoint(self, name: str) -> None:
        """
        Rollback to the savepoint with the given name asynchronously.

        Default implementation does nothing.
        Subclasses should override if the database supports savepoints.

        Args:
            name: Name of the savepoint
        """

    async def release_savepoint(self, name: str) -> None:
        """
        Release the savepoint with the given name asynchronously.

        Default implementation does nothing.
        Subclasses should override if the database supports savepoints.

        Args:
            name: Name of the savepoint
        """


class AsyncGenericAdapter(AsyncSQLAdapter):
    """
    Generic async adapter that can work with any async database connection.

    This adapter takes async connection objects and callback functions
    to interact with any database system. It's useful when a
    specialized adapter is not available.

    Args:
        connection: Async database connection object
        execute_func: Optional custom async function to execute SQL
        close_func: Optional custom async function to close the connection
        max_query_size: Optional maximum query size in bytes
    """

    def __init__(
        self,
        connection: Any,
        execute_func: Optional[Callable[[str], Awaitable[Sequence[Any]]]] = None,
        close_func: Optional[Callable[[], Awaitable[None]]] = None,
        max_query_size: Optional[int] = None,
    ) -> None:
        """Initialize the async generic adapter."""
        self._connection = connection
        self._execute_func = execute_func
        self._close_func = close_func
        self._max_query_size = max_query_size or 500_000  # Default 500KB

    async def execute(self, sql: str) -> List[Any]:
        """
        Execute a SQL statement asynchronously and return results.

        Args:
            sql: SQL statement to execute

        Returns:
            List of result rows
        """
        if self._execute_func:
            # Use the provided execute function
            result = await self._execute_func(sql)
            return list(result) if result is not None else []
        elif hasattr(self._connection, "execute"):
            # Try to use connection's execute method directly
            result = await self._connection.execute(sql)
            if result is None:
                return []
            if hasattr(result, "fetchall"):
                return list(await result.fetchall())
            return list(result)
        elif hasattr(self._connection, "cursor"):
            # Try to get a cursor and use its execute method
            cursor = await self._connection.cursor()
            if cursor is None:
                return []
            await cursor.execute(sql)
            if cursor.description is not None:
                return list(await cursor.fetchall())
            return []
        else:
            raise ValueError(
                "Cannot determine how to execute SQL with the provided connection. " "Please provide an execute_func."
            )

    async def get_max_query_size(self) -> int:
        """
        Get the maximum query size in bytes.

        Returns:
            Maximum query size in bytes
        """
        return self._max_query_size

    async def close(self) -> None:
        """Close the database connection asynchronously."""
        if self._close_func:
            # Use the provided close function
            await self._close_func()
        elif hasattr(self._connection, "close"):
            # Try to use connection's close method
            await self._connection.close()

    async def set_max_query_size(self, max_size: int) -> None:
        """
        Set the maximum query size.

        Args:
            max_size: Maximum query size in bytes
        """
        self._max_query_size = max_size
