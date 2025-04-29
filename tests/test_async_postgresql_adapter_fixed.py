"""Tests for the AsyncPostgreSQLAdapter class with proper mocking."""

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


class AsyncContextManagerMock:
    """A mock for an async context manager."""

    def __init__(self, return_value):
        self.return_value = return_value

    async def __aenter__(self):
        return self.return_value

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass


class TestAsyncPostgreSQLAdapter(unittest.IsolatedAsyncioTestCase):
    """Test the AsyncPostgreSQLAdapter with proper mocking."""

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
    async def test_execute_select(self, mock_asyncpg):
        """Test executing a SELECT statement."""
        # Create a mock connection
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(
            return_value=[
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ]
        )

        # Create a mock pool
        mock_pool = AsyncMock()

        # Make pool.acquire() return our mock connection
        async def mock_acquire():
            return mock_conn

        mock_pool.acquire = mock_acquire
        # Make pool.release() a coroutine that does nothing
        mock_pool.release = AsyncMock()

        # Make asyncpg.create_pool return our mock pool
        mock_asyncpg.create_pool = AsyncMock(return_value=mock_pool)

        # Create the adapter
        adapter = AsyncPostgreSQLAdapter(dsn="postgresql://user:pass@host:5432/db")

        # Set the pool directly instead of calling connect()
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
        # Create a mock connection
        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock(return_value="INSERT 0 1")

        # Create a mock pool
        mock_pool = AsyncMock()

        # Make pool.acquire() return our mock connection
        async def mock_acquire():
            return mock_conn

        mock_pool.acquire = mock_acquire
        # Make pool.release() a coroutine that does nothing
        mock_pool.release = AsyncMock()

        # Make asyncpg.create_pool return our mock pool
        mock_asyncpg.create_pool = AsyncMock(return_value=mock_pool)

        # Create the adapter
        adapter = AsyncPostgreSQLAdapter(dsn="postgresql://user:pass@host:5432/db")

        # Set the pool directly instead of calling connect()
        adapter._pool = mock_pool

        # Execute an INSERT statement
        result = await adapter.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")

        # Verify the connection was used correctly
        mock_conn.execute.assert_called_once_with("INSERT INTO users (id, name) VALUES (1, 'Alice')")

        # Verify the result is correct
        self.assertEqual(result, [])

    @patch("sql_batcher.adapters.async_postgresql.asyncpg")
    async def test_execute_batch(self, mock_asyncpg):
        """Test executing a batch of statements."""
        # Create a mock connection
        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock(return_value="BATCH EXECUTED")

        # Create a mock pool
        mock_pool = AsyncMock()

        # Make pool.acquire() return our mock connection
        async def mock_acquire():
            return mock_conn

        mock_pool.acquire = mock_acquire
        # Make pool.release() a coroutine that does nothing
        mock_pool.release = AsyncMock()

        # Make asyncpg.create_pool return our mock pool
        mock_asyncpg.create_pool = AsyncMock(return_value=mock_pool)

        # Create the adapter
        adapter = AsyncPostgreSQLAdapter(dsn="postgresql://user:pass@host:5432/db")

        # Set the pool directly instead of calling connect()
        adapter._pool = mock_pool

        # Execute a batch of statements
        batch_sql = """
        INSERT INTO users (id, name) VALUES (1, 'Alice');
        INSERT INTO users (id, name) VALUES (2, 'Bob');
        """
        result = await adapter.execute(batch_sql)

        # Verify the connection was used correctly
        mock_conn.execute.assert_called_once_with(batch_sql)

        # Verify the result is correct
        self.assertEqual(result, [])

    @patch("sql_batcher.adapters.async_postgresql.asyncpg")
    async def test_transaction_methods(self, mock_asyncpg):
        """Test transaction-related methods."""
        # Create a mock connection
        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock()

        # Create a mock pool
        mock_pool = AsyncMock()

        # Make pool.acquire() return our mock connection
        async def mock_acquire():
            return mock_conn

        mock_pool.acquire = mock_acquire
        # Make pool.release() a coroutine that does nothing
        mock_pool.release = AsyncMock()

        # Make asyncpg.create_pool return our mock pool
        mock_asyncpg.create_pool = AsyncMock(return_value=mock_pool)

        # Create the adapter
        adapter = AsyncPostgreSQLAdapter(dsn="postgresql://user:pass@host:5432/db")

        # Set the pool directly instead of calling connect()
        adapter._pool = mock_pool

        # Test begin_transaction
        await adapter.begin_transaction()
        mock_conn.execute.assert_called_with("BEGIN")

        # Test commit_transaction
        mock_conn.execute.reset_mock()
        await adapter.commit_transaction()
        mock_conn.execute.assert_called_with("COMMIT")

        # Test rollback_transaction
        mock_conn.execute.reset_mock()
        await adapter.rollback_transaction()
        mock_conn.execute.assert_called_with("ROLLBACK")

        # Test create_savepoint
        mock_conn.execute.reset_mock()
        await adapter.create_savepoint("sp1")
        mock_conn.execute.assert_called_with("SAVEPOINT sp1")

        # Test rollback_to_savepoint
        mock_conn.execute.reset_mock()
        await adapter.rollback_to_savepoint("sp1")
        mock_conn.execute.assert_called_with("ROLLBACK TO SAVEPOINT sp1")

        # Test release_savepoint
        mock_conn.execute.reset_mock()
        await adapter.release_savepoint("sp1")
        mock_conn.execute.assert_called_with("RELEASE SAVEPOINT sp1")

    @patch("sql_batcher.adapters.async_postgresql.asyncpg")
    async def test_close(self, mock_asyncpg):
        """Test closing the connection pool."""
        # Create a mock pool
        mock_pool = AsyncMock()
        mock_pool.close = AsyncMock()

        # Make asyncpg.create_pool return our mock pool
        mock_asyncpg.create_pool = AsyncMock(return_value=mock_pool)

        # Create the adapter
        adapter = AsyncPostgreSQLAdapter(dsn="postgresql://user:pass@host:5432/db")

        # Set the pool directly instead of calling connect()
        adapter._pool = mock_pool

        # Close the connection pool
        await adapter.close()

        # Verify the pool was closed
        mock_pool.close.assert_called_once()
