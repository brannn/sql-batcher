"""Tests for async batcher functionality."""

from typing import Any, List
from unittest.mock import AsyncMock, MagicMock

import pytest

from sql_batcher import AsyncSQLBatcher
from sql_batcher.adapters.async_postgresql import AsyncPostgreSQLAdapter
from sql_batcher.exceptions import AdapterConnectionError, AdapterExecutionError


@pytest.fixture
def mock_asyncpg_pool():
    """Create a mock asyncpg pool."""
    pool = AsyncMock()
    pool.acquire.return_value.__aenter__.return_value = AsyncMock()
    return pool


@pytest.fixture
def mock_adapter():
    """Create a mock async adapter for testing."""
    adapter = AsyncMock()
    adapter.execute = AsyncMock()
    adapter.create_savepoint = AsyncMock()
    adapter.rollback_to_savepoint = AsyncMock()
    adapter.release_savepoint = AsyncMock()
    return adapter


@pytest.fixture
async def postgres_adapter(mock_asyncpg_pool, monkeypatch):
    """Create a PostgreSQL adapter for testing."""
    # Mock asyncpg.create_pool
    monkeypatch.setattr(
        "asyncpg.create_pool", AsyncMock(return_value=mock_asyncpg_pool)
    )

    adapter = AsyncPostgreSQLAdapter(
        dsn="postgresql://user:pass@localhost:5432/dbname",
        min_size=1,
        max_size=1,
    )
    await adapter.connect()
    return adapter


@pytest.fixture
def async_batcher(postgres_adapter):
    """Create an async batcher for testing."""
    return AsyncSQLBatcher(adapter=postgres_adapter, max_bytes=1000)


@pytest.mark.asyncio
async def test_async_batcher_initialization(async_batcher):
    """Test async batcher initialization."""
    assert async_batcher.max_bytes == 1000
    assert async_batcher.delimiter == ";"
    assert not async_batcher.dry_run
    assert async_batcher.current_batch == []
    assert async_batcher.current_size == 0


@pytest.mark.asyncio
async def test_async_batcher_add_statement(async_batcher):
    """Test adding statements to the async batcher."""
    # Add a statement
    should_flush = async_batcher.add_statement("INSERT INTO test VALUES (1)")
    assert not should_flush
    assert len(async_batcher.current_batch) == 1
    assert async_batcher.current_size > 0

    # Add another statement that should trigger a flush
    should_flush = async_batcher.add_statement("INSERT INTO test VALUES (2)")
    assert should_flush


@pytest.mark.asyncio
async def test_async_batcher_process_statements(async_batcher):
    """Test processing statements with the async batcher."""
    statements = [
        "INSERT INTO test VALUES (1)",
        "INSERT INTO test VALUES (2)",
        "INSERT INTO test VALUES (3)",
    ]

    # Mock execute callback
    executed_statements: List[str] = []

    async def execute_callback(sql: str) -> Any:
        executed_statements.append(sql)
        return None

    # Process statements
    processed = await async_batcher.process_statements(statements, execute_callback)
    assert processed == len(statements)
    assert len(executed_statements) == len(statements)


@pytest.mark.asyncio
async def test_async_batcher_process_batch(async_batcher):
    """Test processing a batch of statements."""
    statements = [
        "INSERT INTO test VALUES (1)",
        "INSERT INTO test VALUES (2)",
        "INSERT INTO test VALUES (3)",
    ]

    # Mock execute function
    executed_statements: List[str] = []

    async def execute_func(sql: str) -> Any:
        executed_statements.append(sql)
        return None

    # Process batch
    results = await async_batcher.process_batch(statements, execute_func)
    assert len(results) == len(statements)
    assert len(executed_statements) == len(statements)


@pytest.mark.asyncio
async def test_async_batcher_process_stream(async_batcher):
    """Test processing statements in a streaming fashion."""
    statements = [
        "INSERT INTO test VALUES (1)",
        "INSERT INTO test VALUES (2)",
        "INSERT INTO test VALUES (3)",
    ]

    # Mock execute function
    executed_statements: List[str] = []

    async def execute_func(sql: str) -> Any:
        executed_statements.append(sql)
        return None

    # Process stream
    results = await async_batcher.process_stream(statements, execute_func)
    assert len(results) == len(statements)
    assert len(executed_statements) == len(statements)


@pytest.mark.asyncio
async def test_async_batcher_process_chunk(async_batcher):
    """Test processing statements in chunks."""
    statements = [
        "INSERT INTO test VALUES (1)",
        "INSERT INTO test VALUES (2)",
        "INSERT INTO test VALUES (3)",
    ]

    # Mock execute function
    executed_statements: List[str] = []

    async def execute_func(sql: str) -> Any:
        executed_statements.append(sql)
        return None

    # Process chunk
    results = await async_batcher.process_chunk(statements, execute_func)
    assert len(results) == len(statements)
    assert len(executed_statements) == len(statements)


@pytest.mark.asyncio
async def test_async_batcher_error_handling(async_batcher):
    """Test error handling in the async batcher."""

    # Test with invalid SQL
    async def execute_callback(sql: str) -> Any:
        raise AdapterExecutionError("PostgreSQL", sql, "Invalid SQL")

    with pytest.raises(AdapterExecutionError):
        await async_batcher.process_statements(["INVALID SQL"], execute_callback)


@pytest.mark.asyncio
async def test_async_batcher_transaction_handling(async_batcher, mock_asyncpg_pool):
    """Test transaction handling in the async batcher."""
    # Test transaction methods
    await async_batcher._adapter.begin_transaction()
    mock_asyncpg_pool.acquire.return_value.__aenter__.return_value.execute.assert_called_with(
        "BEGIN"
    )

    await async_batcher._adapter.commit_transaction()
    mock_asyncpg_pool.acquire.return_value.__aenter__.return_value.execute.assert_called_with(
        "COMMIT"
    )

    await async_batcher._adapter.rollback_transaction()
    mock_asyncpg_pool.acquire.return_value.__aenter__.return_value.execute.assert_called_with(
        "ROLLBACK"
    )


@pytest.mark.asyncio
async def test_async_batcher_retry_handling(async_batcher):
    """Test retry handling in the async batcher."""
    from sql_batcher.retry import RetryConfig, with_retry

    # Create a function that fails twice then succeeds
    attempt_count = 0

    @with_retry(RetryConfig(max_retries=2, initial_delay=0.1))
    async def execute_with_retry(sql: str) -> Any:
        nonlocal attempt_count
        attempt_count += 1
        if attempt_count < 3:
            raise AdapterExecutionError("PostgreSQL", sql, "Temporary error")
        return None

    # Process statements with retry
    statements = ["INSERT INTO test VALUES (1)"]
    await async_batcher.process_statements(statements, execute_with_retry)
    assert attempt_count == 3  # Two failures + one success


@pytest.mark.asyncio
async def test_savepoint_functionality(async_batcher, mock_adapter):
    """Test savepoint functionality in batch processing."""
    # Create a batch of statements
    statements = [
        "INSERT INTO users (name) VALUES ('John')",
        "INSERT INTO users (name) VALUES ('Jane')",
        "INSERT INTO users (name) VALUES ('Bob')",
    ]

    # Add statements to batcher
    for stmt in statements:
        async_batcher.add_statement(stmt)

    # Mock adapter to simulate a failure on the second statement
    mock_adapter.execute.side_effect = [
        None,  # First statement succeeds
        Exception("Test error"),  # Second statement fails
        None,  # Third statement (should not be executed)
    ]

    # Process statements and verify savepoint behavior
    with pytest.raises(Exception, match="Test error"):
        await async_batcher.flush(mock_adapter.execute)

    # Verify that savepoint was created and rolled back
    mock_adapter.create_savepoint.assert_called_once()
    mock_adapter.rollback_to_savepoint.assert_called_once()
    mock_adapter.release_savepoint.assert_not_called()

    # Verify that only the first statement was executed
    assert mock_adapter.execute.call_count == 2  # First statement + error


@pytest.mark.asyncio
async def test_savepoint_success(async_batcher, mock_adapter):
    """Test successful batch processing with savepoints."""
    # Create a batch of statements
    statements = [
        "INSERT INTO users (name) VALUES ('John')",
        "INSERT INTO users (name) VALUES ('Jane')",
        "INSERT INTO users (name) VALUES ('Bob')",
    ]

    # Add statements to batcher
    for stmt in statements:
        async_batcher.add_statement(stmt)

    # Process statements
    await async_batcher.flush(mock_adapter.execute)

    # Verify that savepoint was created and released
    mock_adapter.create_savepoint.assert_called_once()
    mock_adapter.release_savepoint.assert_called_once()
    mock_adapter.rollback_to_savepoint.assert_not_called()

    # Verify that all statements were executed
    assert mock_adapter.execute.call_count == len(statements)


@pytest.mark.asyncio
async def test_savepoint_nested_transactions(async_batcher, mock_adapter):
    """Test savepoint behavior with nested transactions."""
    # Create a batch of statements
    statements = [
        "INSERT INTO users (name) VALUES ('John')",
        "INSERT INTO users (name) VALUES ('Jane')",
        "INSERT INTO users (name) VALUES ('Bob')",
    ]

    # Add statements to batcher
    for stmt in statements:
        async_batcher.add_statement(stmt)

    # Mock adapter to simulate a failure in a nested transaction
    mock_adapter.execute.side_effect = [
        None,  # First statement succeeds
        Exception("Nested transaction error"),  # Second statement fails
        None,  # Third statement (should not be executed)
    ]

    # Process statements and verify savepoint behavior
    with pytest.raises(Exception, match="Nested transaction error"):
        await async_batcher.flush(mock_adapter.execute)

    # Verify that savepoint was created and rolled back
    mock_adapter.create_savepoint.assert_called_once()
    mock_adapter.rollback_to_savepoint.assert_called_once()
    mock_adapter.release_savepoint.assert_not_called()

    # Verify that only the first statement was executed
    assert mock_adapter.execute.call_count == 2  # First statement + error


@pytest.mark.asyncio
async def test_savepoint_error_handling(async_batcher, mock_adapter):
    """Test error handling in savepoint operations."""
    # Create a batch of statements
    statements = [
        "INSERT INTO users (name) VALUES ('John')",
        "INSERT INTO users (name) VALUES ('Jane')",
        "INSERT INTO users (name) VALUES ('Bob')",
    ]

    # Add statements to batcher
    for stmt in statements:
        async_batcher.add_statement(stmt)

    # Mock adapter to simulate a failure in savepoint creation
    mock_adapter.create_savepoint.side_effect = Exception("Savepoint creation failed")

    # Process statements and verify error handling
    with pytest.raises(Exception, match="Savepoint creation failed"):
        await async_batcher.flush(mock_adapter.execute)

    # Verify that savepoint creation was attempted
    mock_adapter.create_savepoint.assert_called_once()
    mock_adapter.rollback_to_savepoint.assert_not_called()
    mock_adapter.release_savepoint.assert_not_called()

    # Verify that no statements were executed
    assert mock_adapter.execute.call_count == 0
