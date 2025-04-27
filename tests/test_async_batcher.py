"""Tests for async batcher."""

import pytest

# Try to import optional dependencies
try:
    import asyncpg
except ImportError:
    asyncpg = None

from sql_batcher.async_batcher import AsyncSQLBatcher
from sql_batcher.exceptions import BatchSizeExceededError

@pytest.mark.skipif(asyncpg is None, reason="asyncpg not available")
class TestAsyncSQLBatcher:
    """Test cases for AsyncSQLBatcher class."""

    @pytest.fixture(autouse=True)
    def setup_batcher(self) -> None:
        """Set up test fixtures."""
        self.batcher = AsyncSQLBatcher(max_bytes=1000)
        self.statements = [
            "INSERT INTO test VALUES (1)",
            "INSERT INTO test VALUES (2)",
            "INSERT INTO test VALUES (3)",
        ]

    def test_init_with_defaults(self) -> None:
        """Test initialization with default values."""
        batcher = AsyncSQLBatcher()
        assert batcher.max_bytes == 1_000_000
        assert batcher.delimiter == ";"
        assert batcher.dry_run is False

    def test_init_with_custom_values(self) -> None:
        """Test initialization with custom values."""
        batcher = AsyncSQLBatcher(
            max_bytes=500, delimiter="|", dry_run=True
        )
        assert batcher.max_bytes == 500
        assert batcher.delimiter == "|"
        assert batcher.dry_run is True

    def test_add_statement(self) -> None:
        """Test adding a statement to the batch."""
        # Add a statement
        result = self.batcher.add_statement("INSERT INTO test VALUES (1)")

        # Should not need to flush yet
        assert result is False
        assert len(self.batcher.current_batch) == 1

        # Add more statements until batch is full
        while not result:
            result = self.batcher.add_statement("INSERT INTO test VALUES (2)")

        # Now we should need to flush
        assert result is True

    def test_reset(self) -> None:
        """Test resetting the batch."""
        # Add a statement
        self.batcher.add_statement("INSERT INTO test VALUES (1)")

        # Reset the batch
        self.batcher.reset()

        # Batch should be empty
        assert len(self.batcher.current_batch) == 0
        assert self.batcher.current_size == 0

    def test_process_statements(self) -> None:
        """Test processing statements with the async batcher."""
        processed = self.batcher.process_statements(self.statements, lambda sql: None)
        assert len(processed) == len(self.statements)

    def test_process_batch(self) -> None:
        """Test processing a batch of statements."""
        results = self.batcher.process_batch(self.statements, lambda sql: None)
        assert len(results) == len(self.statements)

    def test_process_stream(self) -> None:
        """Test processing statements in a streaming fashion."""
        results = self.batcher.process_stream(self.statements, lambda sql: None)
        assert len(results) == len(self.statements)

    def test_process_chunk(self) -> None:
        """Test processing statements in chunks."""
        results = self.batcher.process_chunk(self.statements, lambda sql: None)
        assert len(results) == len(self.statements)

    def test_error_handling(self) -> None:
        """Test error handling in the async batcher."""
        with pytest.raises(BatchSizeExceededError):
            self.batcher.add_statement("INSERT INTO test VALUES (4)")

    def test_transaction_handling(self) -> None:
        """Test transaction handling in the async batcher."""
        self.batcher._adapter.begin_transaction()
        assert self.batcher._adapter.pool.acquire.called
        assert self.batcher._adapter.pool.release.called

    def test_retry_handling(self) -> None:
        """Test retry handling in the async batcher."""
        from sql_batcher.retry import RetryConfig, with_retry

        attempt_count = 0

        @with_retry(RetryConfig(max_retries=2, initial_delay=0.1))
        def execute_with_retry(sql: str):
            nonlocal attempt_count
            attempt_count += 1
            if attempt_count < 3:
                raise BatchSizeExceededError("Temporary error")
            return None

        processed = self.batcher.process_statements(self.statements, execute_with_retry)
        assert len(processed) == len(self.statements)
        assert attempt_count == 3  # Two failures + one success

    def test_savepoint_functionality(self) -> None:
        """Test savepoint functionality in batch processing."""
        with pytest.raises(Exception, match="Test error"):
            self.batcher.process_statements(
                self.statements, lambda sql: Exception("Test error")
            )

    def test_savepoint_success(self) -> None:
        """Test successful savepoint creation and release."""
        mock_adapter = self.batcher._adapter
        mock_adapter.execute = lambda sql: None
        mock_adapter.create_savepoint = lambda: None
        mock_adapter.release_savepoint = lambda: None
        mock_adapter.rollback_to_savepoint = lambda: None

        with self.batcher.savepoint("test_savepoint"):
            self.batcher.process_statements(self.statements, mock_adapter.execute)

        assert mock_adapter.create_savepoint.called
        assert mock_adapter.release_savepoint.called
        assert not mock_adapter.rollback_to_savepoint.called

    def test_savepoint_nested_transactions(self) -> None:
        """Test savepoint behavior with nested transactions."""
        with pytest.raises(Exception, match="Nested transaction error"):
            self.batcher.process_statements(
                self.statements, lambda sql: Exception("Nested transaction error")
            )

    def test_savepoint_error_handling(self) -> None:
        """Test error handling in savepoint operations."""
        with pytest.raises(BatchSizeExceededError):
            self.batcher.process_statements(
                self.statements, lambda sql: BatchSizeExceededError("Test error")
            )

    def test_savepoint_rollback(self) -> None:
        """Test savepoint rollback on exception."""
        mock_adapter = self.batcher._adapter
        mock_adapter.execute = lambda sql: Exception("Query failed")
        mock_adapter.create_savepoint = lambda: None
        mock_adapter.release_savepoint = lambda: None
        mock_adapter.rollback_to_savepoint = lambda: None

        with pytest.raises(Exception, match="Query failed"):
            with self.batcher.savepoint("test_savepoint"):
                self.batcher.process_statements(self.statements, mock_adapter.execute)

        assert mock_adapter.create_savepoint.called
        assert mock_adapter.rollback_to_savepoint.called
        assert not mock_adapter.release_savepoint.called
