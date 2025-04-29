"""
Tests for async savepoint functionality in SQL adapters.
"""

import pytest

from sql_batcher import AsyncSQLBatcher
from sql_batcher.adapters.async_base import AsyncSQLAdapter


class MockAsyncAdapter(AsyncSQLAdapter):
    """Mock async adapter for testing."""

    def __init__(self, max_query_size: int = 1_000_000) -> None:
        """Initialize the mock async adapter."""
        self.executed_statements = []
        self._max_query_size = max_query_size
        self.transaction_started = False
        self.transaction_committed = False
        self.transaction_rolled_back = False
        self.savepoints = {}  # name -> released

    async def execute(self, sql: str) -> list:
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
        self.executed_statements.append(f"SAVEPOINT {name}")

    async def rollback_to_savepoint(self, name: str) -> None:
        """Rollback to the savepoint with the given name."""
        if name not in self.savepoints:
            raise ValueError(f"Savepoint {name} does not exist")
        self.executed_statements.append(f"ROLLBACK TO SAVEPOINT {name}")

    async def release_savepoint(self, name: str) -> None:
        """Release the savepoint with the given name."""
        if name not in self.savepoints:
            raise ValueError(f"Savepoint {name} does not exist")
        self.savepoints[name] = True  # Released
        self.executed_statements.append(f"RELEASE SAVEPOINT {name}")


@pytest.mark.asyncio
async def test_async_adapter_savepoints() -> None:
    """Test savepoint operations in AsyncSQLAdapter."""
    # Create a mock adapter
    adapter = MockAsyncAdapter()

    # Create a savepoint
    await adapter.create_savepoint("test_savepoint")
    assert "SAVEPOINT test_savepoint" in adapter.executed_statements
    assert "test_savepoint" in adapter.savepoints
    assert not adapter.savepoints["test_savepoint"]  # Not released

    # Rollback to the savepoint
    await adapter.rollback_to_savepoint("test_savepoint")
    assert "ROLLBACK TO SAVEPOINT test_savepoint" in adapter.executed_statements

    # Release the savepoint
    await adapter.release_savepoint("test_savepoint")
    assert "RELEASE SAVEPOINT test_savepoint" in adapter.executed_statements
    assert adapter.savepoints["test_savepoint"]  # Released


@pytest.mark.asyncio
async def test_async_savepoint_error_handling() -> None:
    """Test error handling for async savepoint operations."""
    # Create a mock adapter
    adapter = MockAsyncAdapter()

    # Try to rollback to a non-existent savepoint
    with pytest.raises(ValueError):
        await adapter.rollback_to_savepoint("non_existent")

    # Try to release a non-existent savepoint
    with pytest.raises(ValueError):
        await adapter.release_savepoint("non_existent")


@pytest.mark.asyncio
async def test_async_savepoint_with_batcher() -> None:
    """Test using savepoints with AsyncSQLBatcher."""
    # Create a mock adapter
    adapter = MockAsyncAdapter()

    # Create a batcher
    batcher = AsyncSQLBatcher(adapter=adapter)

    # Begin a transaction
    await adapter.begin_transaction()
    assert adapter.transaction_started

    # Create a savepoint
    await adapter.create_savepoint("before_inserts")
    assert "SAVEPOINT before_inserts" in adapter.executed_statements

    # Add some statements
    await batcher.add_statement("INSERT INTO test VALUES (1)")
    await batcher.add_statement("INSERT INTO test VALUES (2)")

    # Flush the batcher
    await batcher.flush(adapter.execute)

    # Verify the statements were executed
    assert any("INSERT INTO test VALUES (1)" in stmt for stmt in adapter.executed_statements)
    assert any("INSERT INTO test VALUES (2)" in stmt for stmt in adapter.executed_statements)

    # Rollback to the savepoint
    await adapter.rollback_to_savepoint("before_inserts")
    assert "ROLLBACK TO SAVEPOINT before_inserts" in adapter.executed_statements

    # Rollback the transaction
    await adapter.rollback_transaction()
    assert adapter.transaction_rolled_back


@pytest.mark.asyncio
async def test_async_savepoint_transaction_flow() -> None:
    """Test a complete transaction flow with savepoints."""
    # Create a mock adapter
    adapter = MockAsyncAdapter()

    # Create a batcher
    batcher = AsyncSQLBatcher(adapter=adapter)

    # Begin a transaction
    await adapter.begin_transaction()

    # Create a savepoint
    await adapter.create_savepoint("step1")

    # Add some statements
    await batcher.add_statement("INSERT INTO test VALUES (1)")
    await batcher.flush(adapter.execute)

    # Create another savepoint
    await adapter.create_savepoint("step2")

    # Add more statements
    await batcher.add_statement("INSERT INTO test VALUES (2)")
    await batcher.flush(adapter.execute)

    # Rollback to the first savepoint
    await adapter.rollback_to_savepoint("step1")

    # Add new statements
    await batcher.add_statement("INSERT INTO test VALUES (3)")
    await batcher.flush(adapter.execute)

    # Commit the transaction
    await adapter.commit_transaction()
    assert adapter.transaction_committed

    # Verify the executed statements
    assert "SAVEPOINT step1" in adapter.executed_statements
    assert "SAVEPOINT step2" in adapter.executed_statements
    assert "ROLLBACK TO SAVEPOINT step1" in adapter.executed_statements
    assert any("INSERT INTO test VALUES (1)" in stmt for stmt in adapter.executed_statements)
    assert any("INSERT INTO test VALUES (2)" in stmt for stmt in adapter.executed_statements)
    assert any("INSERT INTO test VALUES (3)" in stmt for stmt in adapter.executed_statements)
