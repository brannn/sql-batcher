import pytest

from sql_batcher.utils.query_collector import QueryCollector


def test_query_collector_initialization():
    """Test QueryCollector initialization with default and custom batch sizes."""
    # Test with default batch size
    collector = QueryCollector()
    assert collector.max_batch_size == 1000
    assert collector.queries == []
    assert collector.current_batch == []
    assert collector.batch_size == 0

    # Test with custom batch size
    collector = QueryCollector(max_batch_size=500)
    assert collector.max_batch_size == 500
    assert collector.queries == []
    assert collector.current_batch == []
    assert collector.batch_size == 0


def test_add_query():
    """Test adding queries to the collector."""
    collector = QueryCollector()

    # Add a single query
    collector.add_query("SELECT * FROM table")
    assert len(collector.queries) == 1
    assert collector.queries[0] == "SELECT * FROM table"

    # Add multiple queries
    collector.add_query("INSERT INTO table VALUES (1)")
    collector.add_query("UPDATE table SET col = 2")
    assert len(collector.queries) == 3


def test_get_batch():
    """Test getting batches of queries."""
    collector = QueryCollector(max_batch_size=2)

    # Add queries
    collector.add_query("SELECT 1")
    collector.add_query("SELECT 2")
    collector.add_query("SELECT 3")

    # Get first batch
    batch = collector.get_batch()
    assert len(batch) == 2
    assert batch == ["SELECT 1", "SELECT 2"]
    assert collector.current_batch == batch
    assert collector.batch_size == 2
    assert len(collector.queries) == 1

    # Get second batch
    batch = collector.get_batch()
    assert len(batch) == 1
    assert batch == ["SELECT 3"]
    assert collector.current_batch == batch
    assert collector.batch_size == 1
    assert len(collector.queries) == 0

    # Get empty batch
    batch = collector.get_batch()
    assert len(batch) == 0
    assert collector.current_batch == []
    assert collector.batch_size == 0


def test_mark_batch_complete():
    """Test marking a batch as complete."""
    collector = QueryCollector()
    collector.add_query("SELECT 1")
    collector.add_query("SELECT 2")

    batch = collector.get_batch()
    assert len(batch) == 2

    collector.mark_batch_complete()
    assert collector.current_batch == []
    assert collector.batch_size == 0


def test_has_queries():
    """Test checking if there are remaining queries."""
    collector = QueryCollector()

    # Initially no queries
    assert not collector.has_queries()

    # Add a query
    collector.add_query("SELECT 1")
    assert collector.has_queries()

    # Get the batch
    collector.get_batch()
    assert not collector.has_queries()


def test_get_remaining_count():
    """Test getting the count of remaining queries."""
    collector = QueryCollector()

    # Initially no queries
    assert collector.get_remaining_count() == 0

    # Add queries
    collector.add_query("SELECT 1")
    collector.add_query("SELECT 2")
    assert collector.get_remaining_count() == 2

    # Get a batch
    collector.get_batch()
    assert collector.get_remaining_count() == 0


def test_flush():
    """Test flushing all remaining queries."""
    collector = QueryCollector()

    # Add queries
    collector.add_query("SELECT 1")
    collector.add_query("SELECT 2")
    collector.add_query("SELECT 3")

    # Get a batch
    collector.get_batch()

    # Flush remaining queries
    remaining = collector.flush()
    assert remaining == ["SELECT 3"]
    assert collector.queries == []
    assert collector.current_batch == []
    assert collector.batch_size == 0
