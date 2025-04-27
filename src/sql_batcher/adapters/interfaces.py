"""
Base adapter interfaces for SQL Batcher.

This module provides abstract base classes for database adapters used by SQL Batcher.
"""

from abc import ABC, abstractmethod
from typing import Any, List

__all__ = ["SQLAdapter", "AsyncSQLAdapter"]


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
        Create a savepoint in the current transaction.

        Default implementation does nothing.
        Subclasses should override if the database supports savepoints.

        Args:
            name: Name of the savepoint
        """

    def rollback_to_savepoint(self, name: str) -> None:
        """
        Rollback to a previously created savepoint.

        Default implementation does nothing.
        Subclasses should override if the database supports savepoints.

        Args:
            name: Name of the savepoint to rollback to
        """

    def release_savepoint(self, name: str) -> None:
        """
        Release a previously created savepoint.

        Default implementation does nothing.
        Subclasses should override if the database supports savepoints.

        Args:
            name: Name of the savepoint to release
        """


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
        Execute a SQL statement and return results.

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
        """Close the database connection."""

    async def begin_transaction(self) -> None:
        """
        Begin a transaction.

        Default implementation does nothing.
        Subclasses should override if the database supports transactions.
        """

    async def commit_transaction(self) -> None:
        """
        Commit the current transaction.

        Default implementation does nothing.
        Subclasses should override if the database supports transactions.
        """

    async def rollback_transaction(self) -> None:
        """
        Rollback the current transaction.

        Default implementation does nothing.
        Subclasses should override if the database supports transactions.
        """

    async def create_savepoint(self, name: str) -> None:
        """
        Create a savepoint in the current transaction.

        Default implementation does nothing.
        Subclasses should override if the database supports savepoints.

        Args:
            name: Name of the savepoint
        """

    async def rollback_to_savepoint(self, name: str) -> None:
        """
        Rollback to a previously created savepoint.

        Default implementation does nothing.
        Subclasses should override if the database supports savepoints.

        Args:
            name: Name of the savepoint to rollback to
        """

    async def release_savepoint(self, name: str) -> None:
        """
        Release a previously created savepoint.

        Default implementation does nothing.
        Subclasses should override if the database supports savepoints.

        Args:
            name: Name of the savepoint to release
        """
