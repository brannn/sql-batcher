"""Tests for the QueryCollector class to improve coverage."""

import unittest

from sql_batcher.query_collector import QueryCollector


class TestQueryCollectorCoverage(unittest.TestCase):
    """Test the QueryCollector class to improve coverage."""

    def setUp(self):
        """Set up the test."""
        self.collector = QueryCollector()

    def test_clear(self):
        """Test clearing the collector."""
        # Add some queries
        self.collector.collect("SELECT 1")
        self.collector.collect("SELECT 2")

        # Verify the collector has queries
        self.assertEqual(len(self.collector.queries), 2)
        self.assertEqual(self.collector.current_size, 0)

        # Clear the collector
        self.collector.clear()

        # Verify the collector is empty
        self.assertEqual(len(self.collector.queries), 0)
        self.assertEqual(self.collector.current_size, 0)

    def test_get_count(self):
        """Test getting the count of queries."""
        # Initially empty
        self.assertEqual(len(self.collector.queries), 0)

        # Add some queries
        self.collector.collect("SELECT 1")
        self.collector.collect("SELECT 2")

        # Verify the count
        self.assertEqual(len(self.collector.queries), 2)

    def test_merge_inserts(self):
        """Test checking if insert merging is enabled."""
        # Default is False
        self.assertFalse(self.collector.merge_inserts)

        # Create a collector with insert merging enabled
        collector_with_merging = QueryCollector(merge_inserts=True)
        self.assertTrue(collector_with_merging.merge_inserts)


class TestAsyncQueryCollectorCoverage(unittest.IsolatedAsyncioTestCase):
    """Test the AsyncQueryCollector class to improve coverage."""

    def setUp(self):
        """Set up the test."""
        from sql_batcher.async_query_collector import AsyncQueryCollector

        self.collector = AsyncQueryCollector()

    async def test_clear_async(self):
        """Test clearing the collector asynchronously."""
        # Add some queries
        await self.collector.collect_async("SELECT 1")
        await self.collector.collect_async("SELECT 2")

        # Verify the collector has queries
        self.assertEqual(await self.collector.get_count_async(), 2)

        # Clear the collector
        await self.collector.clear_async()

        # Verify the collector is empty
        self.assertEqual(await self.collector.get_count_async(), 0)

    async def test_get_count_async(self):
        """Test getting the count of queries asynchronously."""
        # Initially empty
        self.assertEqual(await self.collector.get_count_async(), 0)

        # Add some queries
        await self.collector.collect_async("SELECT 1")
        await self.collector.collect_async("SELECT 2")

        # Verify the count
        self.assertEqual(await self.collector.get_count_async(), 2)

    async def test_get_queries(self):
        """Test getting all queries with metadata asynchronously."""
        # Add some queries with metadata
        await self.collector.collect_async("SELECT 1", {"id": 1})
        await self.collector.collect_async("SELECT 2", {"id": 2})

        # Get all queries directly from the queries attribute
        queries = self.collector.queries

        # Verify the queries
        self.assertEqual(len(queries), 2)
        self.assertEqual(queries[0]["query"], "SELECT 1")
        self.assertEqual(queries[0]["metadata"]["id"], 1)
        self.assertEqual(queries[1]["query"], "SELECT 2")
        self.assertEqual(queries[1]["metadata"]["id"], 2)
