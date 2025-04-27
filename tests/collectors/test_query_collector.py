"""Tests for the QueryCollector class."""

from typing import Dict, List, Optional

import pytest

from sql_batcher.collectors.query_collector import ListQueryCollector, QueryCollector
from sql_batcher.exceptions import QueryCollectorError


class TestQueryCollector:
    """Test suite for QueryCollector."""

    @pytest.fixture
    def collector(self) -> QueryCollector:
        """Create a QueryCollector instance for testing."""
        return QueryCollector(
            delimiter=";",
            dry_run=False,
            reference_column_count=10,
            min_adjustment_factor=0.5,
            max_adjustment_factor=2.0,
            auto_adjust_for_columns=True,
        )

    def test_initialization(self, collector: QueryCollector) -> None:
        """Test QueryCollector initialization."""
        assert collector.delimiter == ";"
        assert not collector.dry_run
        assert collector.reference_column_count == 10
        assert collector.min_adjustment_factor == 0.5
        assert collector.max_adjustment_factor == 2.0
        assert collector.auto_adjust_for_columns
        assert collector.current_size == 0
        assert collector.column_count is None
        assert collector.adjustment_factor == 1.0

    def test_collect_query(self, collector: QueryCollector) -> None:
        """Test query collection."""
        # Collect a query
        collector.collect("SELECT 1", {"test": "data"})
        assert len(collector.get_all()) == 1
        assert collector.get_count() == 1

        # Collect another query
        collector.collect("SELECT 2", {"test": "data2"})
        assert len(collector.get_all()) == 2
        assert collector.get_count() == 2

    def test_clear(self, collector: QueryCollector) -> None:
        """Test clearing collected queries."""
        # Add some queries
        collector.collect("SELECT 1")
        collector.collect("SELECT 2")

        # Clear queries
        collector.clear()
        assert len(collector.get_all()) == 0
        assert collector.get_count() == 0
        assert collector.current_size == 0

    def test_get_batch(self, collector: QueryCollector) -> None:
        """Test getting current batch."""
        # Add queries
        collector.collect("SELECT 1")
        collector.collect("SELECT 2")

        # Get batch
        batch = collector.get_batch()
        assert len(batch) == 2
        assert "SELECT 1" in batch
        assert "SELECT 2" in batch

    def test_update_current_size(self, collector: QueryCollector) -> None:
        """Test updating current size."""
        # Update size
        collector.update_current_size(100)
        assert collector.current_size == 100

        # Update size again
        collector.update_current_size(50)
        assert collector.current_size == 150

    def test_reset(self, collector: QueryCollector) -> None:
        """Test resetting collector state."""
        # Add queries and update size
        collector.collect("SELECT 1")
        collector.update_current_size(100)

        # Reset
        collector.reset()
        assert len(collector.get_all()) == 0
        assert collector.current_size == 0

    def test_column_count_management(self, collector: QueryCollector) -> None:
        """Test column count management."""
        # Set column count
        collector.set_column_count(5)
        assert collector.get_column_count() == 5

        # Update column count
        collector.set_column_count(10)
        assert collector.get_column_count() == 10

    def test_adjustment_factor_management(self, collector: QueryCollector) -> None:
        """Test adjustment factor management."""
        # Set adjustment factor
        collector.set_adjustment_factor(1.5)
        assert collector.get_adjustment_factor() == 1.5

        # Update adjustment factor
        collector.set_adjustment_factor(0.8)
        assert collector.get_adjustment_factor() == 0.8

    def test_dry_run_mode(self) -> None:
        """Test dry run mode."""
        collector = QueryCollector(dry_run=True)
        assert collector.is_dry_run()

        # Collect queries in dry run mode
        collector.collect("SELECT 1")
        assert len(collector.get_all()) == 1

    def test_metadata_tracking(self, collector: QueryCollector) -> None:
        """Test metadata tracking."""
        # Collect query with metadata
        metadata = {"test": "data", "timestamp": "2024-01-01"}
        collector.collect("SELECT 1", metadata)

        # Get collected query
        queries = collector.get_all()
        assert len(queries) == 1
        assert queries[0]["metadata"] == metadata

    @pytest.mark.parametrize(
        "queries,expected_size",
        [
            (["SELECT 1"], 1),
            (["SELECT 1", "SELECT 2"], 2),
            (["SELECT 1", "SELECT 2", "SELECT 3"], 3),
        ],
    )
    def test_batch_collection(
        self, collector: QueryCollector, queries: List[str], expected_size: int
    ) -> None:
        """Test batch collection with different numbers of queries."""
        for query in queries:
            collector.collect(query)

        assert len(collector.get_all()) == expected_size
        assert collector.get_count() == expected_size


class TestListQueryCollector:
    """Test suite for ListQueryCollector."""

    @pytest.fixture
    def collector(self) -> ListQueryCollector:
        """Create a ListQueryCollector instance for testing."""
        return ListQueryCollector()

    def test_get_queries(self, collector: ListQueryCollector) -> None:
        """Test getting all queries with metadata."""
        # Add queries with metadata
        collector.collect("SELECT 1", {"test": "data1"})
        collector.collect("SELECT 2", {"test": "data2"})

        # Get queries
        queries = collector.get_queries()
        assert len(queries) == 2
        assert queries[0]["query"] == "SELECT 1"
        assert queries[0]["metadata"] == {"test": "data1"}
        assert queries[1]["query"] == "SELECT 2"
        assert queries[1]["metadata"] == {"test": "data2"}

    def test_inheritance(self, collector: ListQueryCollector) -> None:
        """Test that ListQueryCollector inherits from QueryCollector."""
        # Test inherited methods
        collector.collect("SELECT 1")
        assert len(collector.get_all()) == 1
        assert collector.get_count() == 1
        assert len(collector.get_batch()) == 1

        # Test ListQueryCollector specific method
        queries = collector.get_queries()
        assert len(queries) == 1
        assert queries[0]["query"] == "SELECT 1"
