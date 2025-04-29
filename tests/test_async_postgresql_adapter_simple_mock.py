"""Tests for the AsyncPostgreSQLAdapter class with simple mocking."""

import unittest
from unittest.mock import MagicMock, patch

from sql_batcher.adapters.async_postgresql import AsyncPostgreSQLAdapter


class AsyncMock(MagicMock):
    """A mock class that supports async context managers and awaitable methods."""

    async def __aenter__(self):
        return self.return_value

    async def __aexit__(self, *args):
        pass

    def __await__(self):
        future = MagicMock()
        future.__await__ = lambda: (yield from [])
        return future.__await__()


class TestAsyncPostgreSQLAdapter(unittest.IsolatedAsyncioTestCase):
    """Test the AsyncPostgreSQLAdapter with simple mocking."""

    @patch("sql_batcher.adapters.async_postgresql.asyncpg")
    async def test_init(self, mock_asyncpg):
        """Test the initialization of the adapter."""
        # Create the adapter
        adapter = AsyncPostgreSQLAdapter(dsn="postgresql://user:pass@host:5432/db")

        # Verify the adapter was initialized correctly
        self.assertEqual(adapter._dsn, "postgresql://user:pass@host:5432/db")
        self.assertEqual(adapter._min_size, 1)
        self.assertEqual(adapter._max_size, 10)
        self.assertEqual(adapter._max_query_size, 5_000_000)

        # Set up the mock for create_pool
        mock_pool = AsyncMock()
        mock_asyncpg.create_pool = AsyncMock(return_value=mock_pool)

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
    async def test_close(self, mock_asyncpg):
        """Test closing the connection pool."""
        # Create a mock pool
        mock_pool = AsyncMock()

        # Set up the close method to be awaitable
        async def mock_close():
            mock_pool.close.was_called = True

        mock_pool.close = mock_close
        mock_pool.close.was_called = False

        # Make asyncpg.create_pool return our mock pool
        mock_asyncpg.create_pool = AsyncMock(return_value=mock_pool)

        # Create the adapter and connect
        adapter = AsyncPostgreSQLAdapter(dsn="postgresql://user:pass@host:5432/db")
        adapter._pool = mock_pool  # Set the pool directly

        # Close the connection pool
        await adapter.close()

        # Verify the pool was closed
        self.assertTrue(mock_pool.close.was_called, "Pool close method was not called")

    @patch("sql_batcher.adapters.async_postgresql.asyncpg")
    async def test_execute_select(self, mock_asyncpg):
        """Test executing a SELECT statement."""
        # Create a mock connection
        mock_conn = AsyncMock()

        # Set up the fetch method to return results
        async def mock_fetch(sql):
            mock_conn.fetch.was_called = True
            mock_conn.fetch.sql = sql
            return [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ]

        mock_conn.fetch = mock_fetch
        mock_conn.fetch.was_called = False

        # Create a mock pool
        mock_pool = AsyncMock()

        # Set up the acquire method to return our mock connection
        async def mock_acquire():
            return mock_conn

        mock_pool.acquire = mock_acquire

        # Set up the release method
        async def mock_release(conn):
            mock_pool.release.was_called = True
            mock_pool.release.conn = conn

        mock_pool.release = mock_release
        mock_pool.release.was_called = False

        # Make asyncpg.create_pool return our mock pool
        mock_asyncpg.create_pool = AsyncMock(return_value=mock_pool)

        # Create the adapter and connect
        adapter = AsyncPostgreSQLAdapter(dsn="postgresql://user:pass@host:5432/db")
        adapter._pool = mock_pool  # Set the pool directly

        # Execute a SELECT statement
        result = await adapter.execute("SELECT id, name FROM users")

        # Verify the connection was used correctly
        self.assertTrue(mock_conn.fetch.was_called, "Fetch method was not called")
        self.assertEqual(mock_conn.fetch.sql, "SELECT id, name FROM users")

        # Verify the connection was released
        self.assertTrue(mock_pool.release.was_called, "Release method was not called")
        self.assertEqual(mock_pool.release.conn, mock_conn)

        # Verify the result is correct
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["id"], 1)
        self.assertEqual(result[0]["name"], "Alice")
        self.assertEqual(result[1]["id"], 2)
        self.assertEqual(result[1]["name"], "Bob")
