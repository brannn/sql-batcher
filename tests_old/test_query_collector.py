"""Tests for the QueryCollector class."""

from sql_batcher.query_collector import QueryCollector


def test_query_collector_initialization():
    """Test QueryCollector initialization."""
    collector = QueryCollector()
    assert collector.get_delimiter() == ";"
    assert not collector.is_dry_run()
    assert collector.get_batch() == []
    assert collector.get_current_size() == 0
    assert collector.get_reference_column_count() == 10
    assert collector.get_min_adjustment_factor() == 0.5
    assert collector.get_max_adjustment_factor() == 2.0
    assert collector.get_column_count() is None
    assert collector.get_adjustment_factor() == 1.0


def test_query_collector_custom_initialization():
    """Test QueryCollector initialization with custom values."""
    collector = QueryCollector(
        delimiter="$$",
        dry_run=True,
        auto_adjust_for_columns=True,
        reference_column_count=20,
        min_adjustment_factor=0.1,
        max_adjustment_factor=5.0,
    )
    assert collector.get_delimiter() == "$$"
    assert collector.is_dry_run()
    assert collector.get_reference_column_count() == 20
    assert collector.get_min_adjustment_factor() == 0.1
    assert collector.get_max_adjustment_factor() == 5.0


def test_query_collector_collect():
    """Test collecting statements."""
    collector = QueryCollector()
    collector.collect("SELECT 1")
    collector.collect("SELECT 2")
    assert collector.get_batch() == ["SELECT 1", "SELECT 2"]


def test_query_collector_size_tracking():
    """Test size tracking."""
    collector = QueryCollector()
    collector.collect("SELECT 1")
    collector.update_current_size(10)
    assert collector.get_current_size() == 10


def test_query_collector_column_count():
    """Test column count management."""
    collector = QueryCollector()
    collector.set_column_count(5)
    assert collector.get_column_count() == 5


def test_query_collector_adjustment_factor():
    """Test adjustment factor management."""
    collector = QueryCollector()
    collector.set_adjustment_factor(1.5)
    assert collector.get_adjustment_factor() == 1.5


def test_query_collector_metadata():
    """Test metadata management."""
    collector = QueryCollector()
    collector.update_metadata({"key": "value"})
    assert collector.get_metadata() == {"key": "value"}


def test_query_collector_reset():
    """Test resetting the collector."""
    collector = QueryCollector()
    collector.collect("SELECT 1")
    collector.update_current_size(10)
    collector.update_metadata({"key": "value"})
    collector.reset()
    assert collector.get_batch() == []
    assert collector.get_current_size() == 0
    assert collector.get_metadata() == {}
