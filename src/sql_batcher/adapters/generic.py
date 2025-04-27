"""
Generic adapter for SQL Batcher.

This module provides a generic adapter that can work with any database connection
by using callback functions for execution and closing.
"""

from typing import Any, Callable, List, Optional, Tuple

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
        self._in_transaction = False

    def _get_cursor(self) -> Any:
        """Get a valid cursor, creating a new one if needed."""
        if self._cursor is None:
            if hasattr(self._connection, "cursor"):
                self._cursor = self._connection.cursor()
            else:
                raise ValueError("Connection does not support cursor operations")
        return self._cursor

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

        # Get or create cursor
        cursor = self._get_cursor()

        try:
            # Execute the query
            cursor.execute(sql)

            # For SELECT queries, return results
            if cursor.description is not None:
                return list(cursor.fetchall())

            # For non-SELECT queries (INSERT, UPDATE, DELETE)
            return []
        except Exception as e:
            if not self._in_transaction:
                # If not in a transaction, try to rollback any changes
                self.rollback_transaction()
            raise e

    def close(self) -> None:
        """Close the database connection."""
        if self._cursor is not None:
            try:
                self._cursor.close()
            except Exception:
                # Ignore errors when closing cursor
                pass
            finally:
                self._cursor = None

        if self._close_func:
            # Use the provided close function
            try:
                self._close_func()
            except Exception:
                # Ignore errors in custom close function
                pass
        elif hasattr(self._connection, "close"):
            # Try to use connection's close method
            try:
                self._connection.close()
            except Exception:
                # Ignore errors when closing connection
                pass

    def set_max_query_size(self, max_size: int) -> None:
        """
        Set the maximum query size.

        Args:
            max_size: Maximum query size in bytes
        """
        if max_size <= 0:
            raise ValueError("Maximum query size must be positive")
        self._max_query_size = max_size

    def begin_transaction(self) -> None:
        """Begin a transaction."""
        if self._in_transaction:
            raise RuntimeError("Transaction already in progress")

        if hasattr(self._connection, "begin"):
            self._connection.begin()
        elif hasattr(self._connection, "autocommit"):
            self._connection.autocommit = False
        self._in_transaction = True

    def commit_transaction(self) -> None:
        """Commit the current transaction."""
        if not self._in_transaction:
            raise RuntimeError("No transaction in progress")

        if hasattr(self._connection, "commit"):
            try:
                self._connection.commit()
            finally:
                self._in_transaction = False
        else:
            self._in_transaction = False

    def rollback_transaction(self) -> None:
        """Rollback the current transaction."""
        if not self._in_transaction:
            return  # Silently ignore rollback if no transaction

        if hasattr(self._connection, "rollback"):
            try:
                self._connection.rollback()
            finally:
                self._in_transaction = False
        else:
            self._in_transaction = False
