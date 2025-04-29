"""Tests for the AsyncPostgreSQLAdapter using mocks."""

import unittest
from unittest.mock import MagicMock, patch

import pytest

from sql_batcher.adapters.async_postgresql import AsyncPostgreSQLAdapter


class TestAsyncPostgreSQLAdapterMock(unittest.IsolatedAsyncioTestCase):
    """Test the AsyncPostgreSQLAdapter with mocks."""

    async def asyncSetUp(self):
        """Set up the test."""
        # Create a mock for the asyncpg module
        self.asyncpg_patcher = patch("sql_batcher.adapters.async_postgresql.asyncpg")
        self.mock_asyncpg = self.asyncpg_patcher.start()
        
        # Create a mock connection and cursor
        self.mock_connection = MagicMock()
        self.mock_connection.execute = MagicMock()
        self.mock_connection.fetch = MagicMock()
        self.mock_connection.fetchrow = MagicMock()
        self.mock_connection.close = MagicMock()
        
        # Make asyncpg.connect return our mock connection
        self.mock_asyncpg.connect.return_value = self.mock_connection
        
        # Create the adapter
        self.connection_params = {
            "host": "mock-host",
            "port": 5432,
            "user": "mock-user",
            "password": "mock-password",
            "database": "mock-database",
        }
        self.adapter = await AsyncPostgreSQLAdapter.create(connection_params=self.connection_params)

    async def asyncTearDown(self):
        """Tear down the test."""
        self.asyncpg_patcher.stop()
        await self.adapter.close()

    async def test_init(self):
        """Test the initialization of the adapter."""
        # Verify the connection was created with the correct parameters
        self.mock_asyncpg.connect.assert_called_once_with(**self.connection_params)
        
        # Verify the max_query_size was set correctly
        self.assertEqual(self.adapter._max_query_size, 5_000_000)

    async def test_execute_select(self):
        """Test executing a SELECT statement."""
        # Set up the mock connection to return some data
        self.mock_connection.fetch.return_value = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        
        # Execute a SELECT statement
        result = await self.adapter.execute("SELECT id, name FROM users")
        
        # Verify the connection was used correctly
        self.mock_connection.fetch.assert_called_once_with("SELECT id, name FROM users")
        
        # Verify the result is correct
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["id"], 1)
        self.assertEqual(result[0]["name"], "Alice")
        self.assertEqual(result[1]["id"], 2)
        self.assertEqual(result[1]["name"], "Bob")

    async def test_execute_insert(self):
        """Test executing an INSERT statement."""
        # Set up the mock connection to return no data
        self.mock_connection.execute.return_value = "INSERT 0 1"
        
        # Execute an INSERT statement
        result = await self.adapter.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        
        # Verify the connection was used correctly
        self.mock_connection.execute.assert_called_once_with("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        
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
        
        # Verify the connection was closed
        self.mock_connection.close.assert_called_once()

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
            await AsyncPostgreSQLAdapter.create(connection_params=self.connection_params)
