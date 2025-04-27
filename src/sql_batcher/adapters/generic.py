"""
Generic adapter for SQL Batcher.

This module provides a generic adapter that can work with any database connection
by using callback functions for execution and closing.
"""

from typing import Any, Callable, List, Optional, Tuple, Union, cast

from sql_batcher.adapters.base import SQLAdapter


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
        execute_func: Optional[Callable[[str], List[Tuple[Any, ...]]]] = None,
        close_func: Optional[Callable[[], None]] = None,
        max_query_size: Optional[int] = None,
    ) -> None:
        """Initialize the generic adapter."""
        self._connection = connection
        self._execute_func = execute_func
        self._close_func = close_func
        self._max_query_size = max_query_size or 500_000  # Default 500KB
        self._cursor = None

    def get_max_query_size(self) -> int:
        """
        Get the maximum query size in bytes.

        Returns:
            Maximum query size in bytes
        """
        return self._max_query_size

    def execute(self, sql: str) -> List[Tuple[Any, ...]]:
        """
        Execute a SQL statement and return results.

        Args:
            sql: SQL statement to execute

        Returns:
            List of result rows as tuples
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
            if self._cursor is None:
                self._cursor = self._connection.cursor()
            if self._cursor is None:
                return []
            self._cursor.execute(sql)
            if self._cursor.description is not None:
                result = self._cursor.fetchall()
                return list(result) if result is not None else []
            return []
        else:
            raise ValueError(
                "Cannot determine how to execute SQL with the provided connection. "
                "Please provide an execute_func."
            )

    def close(self) -> None:
        """Close the database connection."""
        if self._close_func:
            # Use the provided close function
            self._close_func()
        elif hasattr(self._connection, "close"):
            # Try to use connection's close method
            self._connection.close()
        if self._cursor is not None:
            self._cursor.close()
            self._cursor = None

    def set_max_query_size(self, max_size: int) -> None:
        """
        Set the maximum query size.

        Args:
            max_size: Maximum query size in bytes
        """
        self._max_query_size = max_size

    def begin_transaction(self) -> None:
        """Begin a transaction."""
        # Most DB-API connections automatically start a transaction,
        # but we can explicitly start one for databases that support it
        try:
            if hasattr(self._connection, "begin"):
                self._connection.begin()
        except Exception:
            # Ignore if not supported
            pass

    def commit_transaction(self) -> None:
        """Commit the current transaction."""
        self._connection.commit()

    def rollback_transaction(self) -> None:
        """Rollback the current transaction."""
        self._connection.rollback()
