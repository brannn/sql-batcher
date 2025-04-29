"""Tests for the SQLBatcher class to improve coverage."""

import unittest
from unittest.mock import MagicMock  # noqa: F401

from sql_batcher.batcher import SQLBatcher
from sql_batcher.query_collector import QueryCollector


class TestSQLBatcherCoverage(unittest.TestCase):
    """Test the SQLBatcher class to improve coverage."""

    def setUp(self):
        """Set up the test."""
        # Create a mock adapter
        self.mock_adapter = MagicMock()
        self.batcher = SQLBatcher(adapter=self.mock_adapter)

    def test_get_column_count_from_insert(self):
        """Test getting column count from INSERT statement."""
        # Test with explicit column list
        sql = "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')"
        count = self.batcher.detect_column_count(sql)
        self.assertEqual(count, 3)

        # Test with no column list but with VALUES
        sql = "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')"
        count = self.batcher.detect_column_count(sql)
        self.assertEqual(count, 3)  # It can detect from VALUES clause

        # Test with non-INSERT statement
        sql = "SELECT * FROM users"
        count = self.batcher.detect_column_count(sql)
        self.assertIsNone(count)

    def test_process_batch_empty(self):
        """Test executing an empty batch."""
        # Create a mock adapter
        mock_adapter = MagicMock()  # noqa: F841

        # Execute an empty batch
        result = self.batcher.process_batch([])

        # Verify the adapter was not called
        self.mock_adapter.execute.assert_not_called()
        self.assertEqual(result, [])

    def test_process_statements_with_query_collector(self):
        """Test executing a batch with a query collector."""
        # Create a mock adapter
        mock_adapter = MagicMock()

        # Create a query collector
        query_collector = QueryCollector()

        # Process statements with the query collector
        count = self.batcher.process_statements(["SELECT 1"], mock_adapter.execute, query_collector)

        # Verify the adapter was called
        mock_adapter.execute.assert_called_once()
        self.assertEqual(count, 1)

        # Verify the query was collected
        self.assertEqual(len(query_collector.queries), 1)


class TestAsyncSQLBatcherCoverage(unittest.IsolatedAsyncioTestCase):
    """Test the AsyncSQLBatcher class to improve coverage."""

    def setUp(self):
        """Set up the test."""
        from sql_batcher.async_batcher import AsyncSQLBatcher

        # Create a mock adapter
        self.mock_adapter = MagicMock()
        self.batcher = AsyncSQLBatcher(adapter=self.mock_adapter)

    async def test_get_column_count_from_insert(self):
        """Test getting column count from INSERT statement."""
        # Test with explicit column list
        sql = "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')"
        count = self.batcher.detect_column_count(sql)
        self.assertEqual(count, 3)

        # Test with no column list but with VALUES
        sql = "INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')"
        count = self.batcher.detect_column_count(sql)
        self.assertEqual(count, 3)  # It can detect from VALUES clause

        # Test with non-INSERT statement
        sql = "SELECT * FROM users"
        count = self.batcher.detect_column_count(sql)
        self.assertIsNone(count)

    async def test_process_batch_empty(self):
        """Test executing an empty batch."""
        # Create a mock adapter
        mock_adapter = MagicMock()
        mock_adapter.execute = MagicMock(return_value=[])

        # Execute an empty batch
        result = await self.batcher.process_batch([])

        # Verify the adapter was not called
        self.mock_adapter.execute.assert_not_called()
        self.assertEqual(result, [])

    async def test_process_statements_with_query_collector(self):
        """Test executing a batch with a query collector."""
        # Create a mock adapter with an awaitable execute method
        mock_adapter = MagicMock()
        mock_adapter.execute = MagicMock()

        # Make the execute method awaitable and return an empty list
        async def mock_execute_async(query):
            # Record the call for assertion
            mock_adapter.execute(query)
            return []

        # Use the async function for execution
        mock_execute_async.assert_called_once = mock_adapter.execute.assert_called_once
        mock_execute_async.assert_called_once_with = mock_adapter.execute.assert_called_once_with

        # Create a query collector
        from sql_batcher.async_query_collector import AsyncQueryCollector

        query_collector = AsyncQueryCollector()

        # Process statements with the query collector
        count = await self.batcher.process_statements(["SELECT 1"], mock_execute_async, query_collector)

        # Verify the adapter was called
        mock_adapter.execute.assert_called_once()
        self.assertEqual(count, 1)

        # Verify the query was collected
        self.assertEqual(len(query_collector.queries), 1)
