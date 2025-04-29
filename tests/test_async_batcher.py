"""
Tests for the AsyncSQLBatcher class.
"""

from typing import Any, Dict, List

import pytest

from sql_batcher import AsyncSQLBatcher
from sql_batcher.adapters.async_base import AsyncSQLAdapter
from sql_batcher.async_query_collector import AsyncQueryCollector


class MockAsyncAdapter(AsyncSQLAdapter):
    """Mock async adapter for testing."""

    def __init__(self, max_query_size: int = 1_000_000) -> None:
        """Initialize the mock async adapter."""
        self.executed_statements: List[str] = []
        self._max_query_size = max_query_size
        self.transaction_started = False
        self.transaction_committed = False
        self.transaction_rolled_back = False
        self.savepoints: Dict[str, bool] = {}  # name -> released

    async def execute(self, sql: str) -> List[Any]:
        """Execute a SQL statement and return results."""
        self.executed_statements.append(sql)
        return []

    async def get_max_query_size(self) -> int:
        """Get the maximum query size in bytes."""
        return self._max_query_size

    async def close(self) -> None:
        """Close the database connection."""

    async def begin_transaction(self) -> None:
        """Begin a transaction."""
        self.transaction_started = True

    async def commit_transaction(self) -> None:
        """Commit the current transaction."""
        self.transaction_committed = True

    async def rollback_transaction(self) -> None:
        """Rollback the current transaction."""
        self.transaction_rolled_back = True

    async def create_savepoint(self, name: str) -> None:
        """Create a savepoint with the given name."""
        self.savepoints[name] = False  # Not released

    async def rollback_to_savepoint(self, name: str) -> None:
        """Rollback to the savepoint with the given name."""
        if name not in self.savepoints:
            raise ValueError(f"Savepoint {name} does not exist")

    async def release_savepoint(self, name: str) -> None:
        """Release the savepoint with the given name."""
        if name not in self.savepoints:
            raise ValueError(f"Savepoint {name} does not exist")
        self.savepoints[name] = True  # Released


@pytest.mark.asyncio
async def test_async_batcher_init() -> None:
    """Test initialization of AsyncSQLBatcher."""
    adapter = MockAsyncAdapter()
    batcher = AsyncSQLBatcher(adapter=adapter)
    assert batcher.max_bytes == 1_000_000
    assert batcher.delimiter == ";"
    assert batcher.dry_run is False
    assert batcher.merge_inserts is False


@pytest.mark.asyncio
async def test_async_batcher_add_statement() -> None:
    """Test adding a statement to the batcher."""
    adapter = MockAsyncAdapter()
    batcher = AsyncSQLBatcher(adapter=adapter, max_bytes=1000)  # Use a smaller max_bytes for testing

    # Add a statement
    should_flush = await batcher.add_statement("SELECT 1")

    # Should not flush yet
    assert not should_flush
    assert len(batcher.current_batch) == 1
    assert batcher.current_size > 0

    # Add more statements until we reach the limit
    for i in range(1000):
        large_statement = f"SELECT * FROM large_table WHERE id = {i} AND name = 'very_long_name_to_make_the_statement_larger';"
        should_flush = await batcher.add_statement(large_statement)
        if should_flush:
            break

    # Should eventually flush
    assert should_flush


@pytest.mark.asyncio
async def test_async_batcher_flush() -> None:
    """Test flushing the batcher."""
    adapter = MockAsyncAdapter()
    batcher = AsyncSQLBatcher(adapter=adapter)

    # Add some statements
    await batcher.add_statement("SELECT 1")
    await batcher.add_statement("SELECT 2")

    # Flush the batcher
    count = await batcher.flush(adapter.execute)

    # Should have flushed 2 statements
    assert count == 2
    assert len(batcher.current_batch) == 0
    assert batcher.current_size == 0
    assert len(adapter.executed_statements) == 1
    assert "SELECT 1" in adapter.executed_statements[0]
    assert "SELECT 2" in adapter.executed_statements[0]


@pytest.mark.asyncio
async def test_async_batcher_process_statements() -> None:
    """Test processing statements with the batcher."""
    adapter = MockAsyncAdapter()
    batcher = AsyncSQLBatcher(adapter=adapter)

    # Process a list of statements
    statements = [
        "INSERT INTO test VALUES (1)",
        "INSERT INTO test VALUES (2)",
        "INSERT INTO test VALUES (3)",
    ]

    count = await batcher.process_statements(statements, adapter.execute)

    # Should have processed all statements
    assert count == 3
    assert len(adapter.executed_statements) == 1
    assert "INSERT INTO test VALUES (1)" in adapter.executed_statements[0]
    assert "INSERT INTO test VALUES (2)" in adapter.executed_statements[0]
    assert "INSERT INTO test VALUES (3)" in adapter.executed_statements[0]


@pytest.mark.asyncio
async def test_async_batcher_merge_inserts() -> None:
    """Test merging INSERT statements."""
    adapter = MockAsyncAdapter()
    batcher = AsyncSQLBatcher(adapter=adapter, merge_inserts=True)

    # Process a list of compatible INSERT statements
    statements = [
        "INSERT INTO test (id, name) VALUES (1, 'one')",
        "INSERT INTO test (id, name) VALUES (2, 'two')",
        "INSERT INTO test (id, name) VALUES (3, 'three')",
    ]

    count = await batcher.process_statements(statements, adapter.execute)

    # Should have processed all statements
    # Note: The count returned is the number of merged statements, not the original count
    # This is the current behavior of the implementation
    assert count == 1
    # With our current implementation, statements are merged in the output
    assert len(adapter.executed_statements) == 1

    # Check that all values were included in the merged statement
    assert (
        "VALUES (1, 'one'), (2, 'two'), (3, 'three')" in adapter.executed_statements[0]
        or "VALUES (1, 'one'), (3, 'three'), (2, 'two')" in adapter.executed_statements[0]
        or "VALUES (2, 'two'), (1, 'one'), (3, 'three')" in adapter.executed_statements[0]
        or "VALUES (2, 'two'), (3, 'three'), (1, 'one')" in adapter.executed_statements[0]
        or "VALUES (3, 'three'), (1, 'one'), (2, 'two')" in adapter.executed_statements[0]
        or "VALUES (3, 'three'), (2, 'two'), (1, 'one')" in adapter.executed_statements[0]
    )


@pytest.mark.asyncio
async def test_async_batcher_context_manager() -> None:
    """Test async context manager support."""
    adapter = MockAsyncAdapter()
    statements = [
        "INSERT INTO test VALUES (1)",
        "INSERT INTO test VALUES (2)",
    ]

    async with AsyncSQLBatcher(adapter=adapter) as batcher:
        for stmt in statements:
            await batcher.add_statement(stmt)

    # Verify statements were flushed on exit
    assert len(adapter.executed_statements) == 1
    assert "INSERT INTO test VALUES (1)" in adapter.executed_statements[0]
    assert "INSERT INTO test VALUES (2)" in adapter.executed_statements[0]


@pytest.mark.asyncio
async def test_async_batcher_with_query_collector() -> None:
    """Test using a query collector with the batcher."""
    adapter = MockAsyncAdapter()
    collector = AsyncQueryCollector()
    batcher = AsyncSQLBatcher(adapter=adapter)

    # Process statements with a collector
    statements = [
        "SELECT 1",
        "SELECT 2",
    ]

    await batcher.process_statements(statements, adapter.execute, collector)

    # Verify collector has the statements
    assert len(collector.get_all()) == 1
    assert "SELECT 1" in collector.get_all()[0]["query"]
    assert "SELECT 2" in collector.get_all()[0]["query"]


@pytest.mark.asyncio
async def test_async_batcher_dry_run() -> None:
    """Test dry run mode."""
    adapter = MockAsyncAdapter()
    batcher = AsyncSQLBatcher(adapter=adapter, dry_run=True)

    # Process statements in dry run mode
    statements = [
        "DELETE FROM important_table",
        "DROP TABLE important_table",
    ]

    await batcher.process_statements(statements, adapter.execute)

    # Verify no statements were executed
    assert len(adapter.executed_statements) == 0


@pytest.mark.asyncio
async def test_async_batcher_column_adjustment() -> None:
    """Test column count-based batch size adjustment."""
    adapter = MockAsyncAdapter()
    batcher = AsyncSQLBatcher(
        adapter=adapter,
        auto_adjust_for_columns=True,
        reference_column_count=10,
        min_adjustment_factor=0.5,
        max_adjustment_factor=2.0,
    )

    # Add a statement with many columns
    await batcher.add_statement(
        "INSERT INTO wide_table (col1, col2, col3, col4, col5, col6, col7, col8, col9, col10, "
        "col11, col12, col13, col14, col15, col16, col17, col18, col19, col20) "
        "VALUES (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20)"
    )

    # Verify adjustment factor was applied
    assert batcher.column_count == 20
    assert batcher.adjustment_factor == 0.5  # 10/20 = 0.5, clamped to min

    # Reset
    await batcher.reset()

    # Add a statement with fewer columns
    await batcher.add_statement("INSERT INTO narrow_table (col1, col2, col3, col4, col5) " "VALUES (1, 2, 3, 4, 5)")

    # Verify adjustment factor was applied
    assert batcher.column_count == 5
    assert batcher.adjustment_factor == 2.0  # 10/5 = 2.0, clamped to max
