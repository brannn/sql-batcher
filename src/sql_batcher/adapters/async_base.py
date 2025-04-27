"""Base async adapter interface for sql-batcher.

This module defines the base interface for async database adapters.
"""

from abc import ABC, abstractmethod
from typing import Any


class AsyncSQLAdapter(ABC):
    """Base class for async database adapters."""

    @abstractmethod
    async def execute(self, query: str) -> Any:
        """Execute a SQL query asynchronously.

        Args:
            query: SQL query to execute

        Returns:
            Result of the query execution
        """

    @abstractmethod
    async def connect(self) -> None:
        """Connect to the database asynchronously."""

    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the database asynchronously."""

    @abstractmethod
    async def begin_transaction(self) -> None:
        """Begin a database transaction asynchronously."""

    @abstractmethod
    async def commit_transaction(self) -> None:
        """Commit the current transaction asynchronously."""

    @abstractmethod
    async def rollback_transaction(self) -> None:
        """Rollback the current transaction asynchronously."""

    @abstractmethod
    async def create_savepoint(self, name: str) -> None:
        """Create a savepoint in the current transaction.

        Args:
            name: Name of the savepoint
        """

    @abstractmethod
    async def rollback_to_savepoint(self, name: str) -> None:
        """Rollback to a previously created savepoint.

        Args:
            name: Name of the savepoint to rollback to
        """

    @abstractmethod
    async def release_savepoint(self, name: str) -> None:
        """Release a previously created savepoint.

        Args:
            name: Name of the savepoint to release
        """
