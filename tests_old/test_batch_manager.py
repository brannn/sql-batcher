"""Tests for the BatchManager class."""

from typing import List

import pytest

from sql_batcher.batch_manager import BatchManager


class TestBatchManager:
    """Test suite for BatchManager."""

    @pytest.fixture
    def batch_manager(self) -> BatchManager:
        """Create a BatchManager instance for testing."""
        return BatchManager(max_bytes=1000)

    def test_initialization(self, batch_manager: BatchManager) -> None:
        """Test BatchManager initialization."""
        assert batch_manager.max_bytes == 1000
        assert batch_manager.current_batch == []
        assert batch_manager.current_size == 0
        assert batch_manager.delimiter == ";"
        assert not batch_manager.dry_run

    def test_add_statement(self, batch_manager: BatchManager) -> None:
        """Test adding statements to the batch."""
        # Add a statement
        should_flush = batch_manager.add_statement("SELECT 1")
        assert not should_flush
        assert len(batch_manager.current_batch) == 1
        assert batch_manager.current_size > 0

        # Add another statement
        should_flush = batch_manager.add_statement("SELECT 2")
        assert not should_flush
        assert len(batch_manager.current_batch) == 2

    def test_batch_size_limit(self, batch_manager: BatchManager) -> None:
        """Test batch size limit handling."""
        # Add a large statement that exceeds the limit
        large_stmt = "SELECT " + "x" * 1000
        should_flush = batch_manager.add_statement(large_stmt)
        assert should_flush
        assert len(batch_manager.current_batch) == 1

    def test_statement_delimiter(self, batch_manager: BatchManager) -> None:
        """Test statement delimiter handling."""
        # Add statement without delimiter
        batch_manager.add_statement("SELECT 1")
        assert batch_manager.current_batch[0] == "SELECT 1;"

        # Add statement with delimiter
        batch_manager.add_statement("SELECT 2;")
        assert batch_manager.current_batch[1] == "SELECT 2;"

    def test_reset(self, batch_manager: BatchManager) -> None:
        """Test batch reset functionality."""
        # Add some statements
        batch_manager.add_statement("SELECT 1")
        batch_manager.add_statement("SELECT 2")

        # Reset the batch
        batch_manager.reset()
        assert batch_manager.current_batch == []
        assert batch_manager.current_size == 0

    def test_merge_insert_statements(self, batch_manager: BatchManager) -> None:
        """Test merging of INSERT statements."""
        statements = [
            "INSERT INTO users (id, name) VALUES (1, 'Alice')",
            "INSERT INTO users (id, name) VALUES (2, 'Bob')",
        ]

        merged = batch_manager.merge_insert_statements(statements)
        assert len(merged) == 1
        assert "VALUES (1, 'Alice'), (2, 'Bob')" in merged[0]

    def test_column_aware_adjustment(self) -> None:
        """Test column-aware batch size adjustment."""
        # Create manager with column-aware adjustment
        manager = BatchManager(
            max_bytes=1000, auto_adjust_for_columns=True, reference_column_count=2
        )

        # Add statement with more columns
        manager.add_statement(
            "INSERT INTO users (id, name, email, age) VALUES (1, 'Alice', 'alice@example.com', 30)"
        )
        assert manager.adjustment_factor < 1.0

        # Add statement with fewer columns
        manager.add_statement("INSERT INTO users (id, name) VALUES (2, 'Bob')")
        assert manager.adjustment_factor > 1.0

    def test_metadata_tracking(self, batch_manager: BatchManager) -> None:
        """Test batch metadata tracking."""
        # Add some statements
        batch_manager.add_statement("SELECT 1")
        batch_manager.add_statement("SELECT 2")

        # Get metadata
        metadata = batch_manager.get_metadata()
        assert metadata["batch_size"] == 2
        assert metadata["current_size"] > 0
        assert metadata["max_bytes"] == 1000
        assert "adjusted_max_bytes" in metadata
        assert "column_count" in metadata
        assert "adjustment_factor" in metadata

    def test_dry_run_mode(self) -> None:
        """Test dry run mode."""
        manager = BatchManager(dry_run=True)
        assert manager.dry_run

        # Add statements in dry run mode
        manager.add_statement("SELECT 1")
        assert len(manager.current_batch) == 1

    @pytest.mark.parametrize(
        "statements,expected_count",
        [
            (["SELECT 1"], 1),
            (["SELECT 1", "SELECT 2"], 2),
            (["SELECT 1", "SELECT 2", "SELECT 3"], 3),
        ],
    )
    def test_batch_collection(
        self, batch_manager: BatchManager, statements: List[str], expected_count: int
    ) -> None:
        """Test batch collection with different numbers of statements."""
        for stmt in statements:
            batch_manager.add_statement(stmt)

        assert len(batch_manager.current_batch) == expected_count
        assert batch_manager.current_size > 0
