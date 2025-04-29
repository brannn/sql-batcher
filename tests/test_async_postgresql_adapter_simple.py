"""Simple tests for the AsyncPostgreSQLAdapter."""

import unittest
from unittest.mock import MagicMock, patch

from sql_batcher.adapters.async_postgresql import AsyncPostgreSQLAdapter


class AsyncMock(MagicMock):
    """Mock class that supports async context managers and awaitable methods."""

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        pass

    def __await__(self):
        future = MagicMock()
        future.__await__ = lambda: (yield from [])
        return future.__await__()


class TestAsyncPostgreSQLAdapter(unittest.IsolatedAsyncioTestCase):
    """Test the AsyncPostgreSQLAdapter with simple mocks."""

    @patch("sql_batcher.adapters.async_postgresql.asyncpg")
    async def test_init(self, mock_asyncpg):
        """Test the initialization of the adapter."""
        # Set up the mock
        mock_pool = AsyncMock()
        mock_asyncpg.create_pool = AsyncMock(return_value=mock_pool)

        # Create the adapter
        adapter = AsyncPostgreSQLAdapter(dsn="postgresql://user:pass@host:5432/db")

        # Verify the adapter was initialized correctly
        self.assertEqual(adapter._dsn, "postgresql://user:pass@host:5432/db")
        self.assertEqual(adapter._min_size, 1)
        self.assertEqual(adapter._max_size, 10)
        self.assertEqual(adapter._max_query_size, 5_000_000)

        # Connect to the database
        await adapter.connect()

        # Verify the pool was created with the correct parameters
        mock_asyncpg.create_pool.assert_called_once_with(dsn="postgresql://user:pass@host:5432/db", min_size=1, max_size=10)

    @patch("sql_batcher.adapters.async_postgresql.ASYNCPG_AVAILABLE", False)
    async def test_missing_asyncpg(self):
        """Test the behavior when the asyncpg package is missing."""
        # Attempt to create an adapter without the asyncpg package
        with self.assertRaises(ImportError):
            AsyncPostgreSQLAdapter(dsn="postgresql://user:pass@host:5432/db")

    @patch("sql_batcher.adapters.async_postgresql.asyncpg")
    async def test_execute_select(self, mock_asyncpg):
        """Test executing a SELECT statement."""
        # Set up the mocks
        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(
            return_value=[
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ]
        )

        # Configure the pool to return the connection
        mock_pool.acquire = AsyncMock(return_value=mock_conn)
        mock_asyncpg.create_pool = AsyncMock(return_value=mock_pool)

        # Create the adapter
        adapter = AsyncPostgreSQLAdapter(dsn="postgresql://user:pass@host:5432/db")

        # Set the pool directly
        adapter._pool = mock_pool

        # Execute a SELECT statement
        result = await adapter.execute("SELECT id, name FROM users")

        # Verify the connection was used correctly
        mock_conn.fetch.assert_called_once_with("SELECT id, name FROM users")

        # Verify the result is correct
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["id"], 1)
        self.assertEqual(result[0]["name"], "Alice")
        self.assertEqual(result[1]["id"], 2)
        self.assertEqual(result[1]["name"], "Bob")

    @patch("sql_batcher.adapters.async_postgresql.asyncpg")
    async def test_execute_insert(self, mock_asyncpg):
        """Test executing an INSERT statement."""
        # Set up the mocks
        mock_pool = AsyncMock()
        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock(return_value="INSERT 0 1")

        # Configure the pool to return the connection
        mock_pool.acquire = AsyncMock(return_value=mock_conn)
        mock_asyncpg.create_pool = AsyncMock(return_value=mock_pool)

        # Create the adapter
        adapter = AsyncPostgreSQLAdapter(dsn="postgresql://user:pass@host:5432/db")

        # Set the pool directly
        adapter._pool = mock_pool

        # Execute an INSERT statement
        result = await adapter.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")

        # Verify the connection was used correctly
        mock_conn.execute.assert_called_once_with("INSERT INTO users (id, name) VALUES (1, 'Alice')")

        # Verify the result is correct
        self.assertEqual(result, [])
