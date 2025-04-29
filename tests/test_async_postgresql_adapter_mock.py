"""Tests for the AsyncPostgreSQLAdapter using mocks."""

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


class AsyncContextManagerMock(AsyncMock):
    """Mock class that returns a specific value when used as an async context manager."""

    def __init__(self, return_value=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._return_value = return_value

    async def __aenter__(self):
        return self._return_value


class TestAsyncPostgreSQLAdapterMock(unittest.IsolatedAsyncioTestCase):
    """Test the AsyncPostgreSQLAdapter with mocks."""

    async def asyncSetUp(self):
        """Set up the test."""
        # Create a mock for the asyncpg module
        self.asyncpg_patcher = patch("sql_batcher.adapters.async_postgresql.asyncpg")
        self.mock_asyncpg = self.asyncpg_patcher.start()

        # Create a mock pool and connection
        self.mock_pool = AsyncMock()
        self.mock_connection = AsyncMock()
        self.mock_connection.execute = AsyncMock()
        self.mock_connection.fetch = AsyncMock()
        self.mock_connection.fetchrow = AsyncMock()
        self.mock_connection.close = AsyncMock()

        # Configure the pool to return our mock connection
        self.mock_pool.acquire.return_value.__aenter__.return_value = self.mock_connection

        # Make asyncpg.create_pool return our mock pool
        self.mock_asyncpg.create_pool = AsyncMock(return_value=self.mock_pool)

        # Create the adapter
        self.dsn = "postgresql://mock-user:mock-password@mock-host:5432/mock-database"
        self.adapter = AsyncPostgreSQLAdapter(dsn=self.dsn)

        # Set the pool directly instead of calling connect()
        self.adapter._pool = self.mock_pool

    async def asyncTearDown(self):
        """Tear down the test."""
        self.asyncpg_patcher.stop()
        await self.adapter.close()

    async def test_init(self):
        """Test the initialization of the adapter."""
        # Since we're setting the pool directly, we don't need to check if create_pool was called
        # Just verify the adapter was initialized correctly
        self.assertEqual(self.adapter._dsn, self.dsn)
        self.assertEqual(self.adapter._min_size, 1)
        self.assertEqual(self.adapter._max_size, 10)
        self.assertEqual(self.adapter._max_query_size, 5_000_000)

    async def test_execute_select(self):
        """Test executing a SELECT statement."""
        # Create a mock connection that will be returned by pool.acquire()
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(
            return_value=[
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ]
        )

        # Create a context manager mock that returns our mock connection
        context_manager_mock = AsyncContextManagerMock(return_value=mock_conn)

        # Configure the pool to return our context manager mock
        self.mock_pool.acquire = MagicMock(return_value=context_manager_mock)

        # Execute a SELECT statement
        result = await self.adapter.execute("SELECT id, name FROM users")

        # Verify the connection was used correctly
        mock_conn.fetch.assert_called_once_with("SELECT id, name FROM users")

        # Verify the result is correct
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["id"], 1)
        self.assertEqual(result[0]["name"], "Alice")
        self.assertEqual(result[1]["id"], 2)
        self.assertEqual(result[1]["name"], "Bob")

    async def test_execute_insert(self):
        """Test executing an INSERT statement."""
        # Create a mock connection that will be returned by pool.acquire()
        mock_conn = AsyncMock()
        mock_conn.execute = AsyncMock(return_value="INSERT 0 1")

        # Create a context manager mock that returns our mock connection
        context_manager_mock = AsyncContextManagerMock(return_value=mock_conn)

        # Configure the pool to return our context manager mock
        self.mock_pool.acquire = MagicMock(return_value=context_manager_mock)

        # Execute an INSERT statement
        result = await self.adapter.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")

        # Verify the connection was used correctly
        mock_conn.execute.assert_called_once_with("INSERT INTO users (id, name) VALUES (1, 'Alice')")

        # Verify the result is correct
        self.assertEqual(result, [])

    async def test_begin_transaction(self):
        """Test beginning a transaction."""
        # Begin a transaction
        await self.adapter.begin_transaction()

        # Verify the connection was used correctly
        self.mock_connection.execute.assert_called_once_with("BEGIN")

    async def test_commit_transaction(self):
        """Test committing a transaction."""
        # Commit a transaction
        await self.adapter.commit_transaction()

        # Verify the connection was used correctly
        self.mock_connection.execute.assert_called_once_with("COMMIT")

    async def test_rollback_transaction(self):
        """Test rolling back a transaction."""
        # Rollback a transaction
        await self.adapter.rollback_transaction()

        # Verify the connection was used correctly
        self.mock_connection.execute.assert_called_once_with("ROLLBACK")

    async def test_close(self):
        """Test closing the connection."""
        # Close the connection
        await self.adapter.close()

        # Verify the pool was closed
        self.mock_pool.close.assert_called_once()

    async def test_create_savepoint(self):
        """Test creating a savepoint."""
        # Create a savepoint
        await self.adapter.create_savepoint("sp1")

        # Verify the connection was used correctly
        self.mock_connection.execute.assert_called_once_with("SAVEPOINT sp1")

    async def test_rollback_to_savepoint(self):
        """Test rolling back to a savepoint."""
        # Rollback to a savepoint
        await self.adapter.rollback_to_savepoint("sp1")

        # Verify the connection was used correctly
        self.mock_connection.execute.assert_called_once_with("ROLLBACK TO SAVEPOINT sp1")

    async def test_release_savepoint(self):
        """Test releasing a savepoint."""
        # Release a savepoint
        await self.adapter.release_savepoint("sp1")

        # Verify the connection was used correctly
        self.mock_connection.execute.assert_called_once_with("RELEASE SAVEPOINT sp1")

    @patch("sql_batcher.adapters.async_postgresql.ASYNCPG_AVAILABLE", False)
    async def test_missing_asyncpg(self):
        """Test the behavior when the asyncpg package is missing."""
        # Attempt to create an adapter without the asyncpg package
        with self.assertRaises(ImportError):
            AsyncPostgreSQLAdapter(dsn=self.dsn)
