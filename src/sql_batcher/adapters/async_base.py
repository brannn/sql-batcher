"""Base async adapter interface for sql-batcher.

This module defines the base interface for async database adapters.
"""

from abc import ABC, abstractmethod
from typing import Any, Awaitable


class AsyncSQLAdapter(ABC):
    """Base class for async database adapters."""

    @abstractmethod
    async def execute(self, query: str) -> Awaitable[Any]:
        """Execute a SQL query asynchronously.

        Args:
            query: SQL query to execute

        Returns:
            Awaitable result of the query execution
        """
        pass

    @abstractmethod
    async def connect(self) -> Awaitable[None]:
        """Connect to the database asynchronously."""
        pass

    @abstractmethod
    async def disconnect(self) -> Awaitable[None]:
        """Disconnect from the database asynchronously."""
        pass

    @abstractmethod
    async def begin_transaction(self) -> Awaitable[None]:
        """Begin a database transaction asynchronously."""
        pass

    @abstractmethod
    async def commit_transaction(self) -> Awaitable[None]:
        """Commit the current transaction asynchronously."""
        pass

    @abstractmethod
    async def rollback_transaction(self) -> Awaitable[None]:
        """Rollback the current transaction asynchronously."""
        pass
