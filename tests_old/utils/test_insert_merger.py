"""Tests for the InsertMerger class."""

import os
import time
from typing import List

import psutil
import pytest

from sql_batcher.exceptions import InsertMergerError
from sql_batcher.utils.insert_merger import InsertMerger


class TestInsertMerger:
    """Test suite for InsertMerger."""

    @pytest.fixture
    def merger(self) -> InsertMerger:
        """Create an InsertMerger instance for testing."""
        return InsertMerger(max_bytes=1000)

    def test_basic_merge(self, merger: InsertMerger) -> None:
        """Test basic INSERT statement merging."""
        statements = [
            "INSERT INTO users (id, name) VALUES (1, 'Alice')",
            "INSERT INTO users (id, name) VALUES (2, 'Bob')",
        ]

        merged = merger.merge(statements)
        assert len(merged) == 1
        assert "VALUES (1, 'Alice'), (2, 'Bob')" in merged[0]

    def test_different_tables(self, merger: InsertMerger) -> None:
        """Test handling of different table names."""
        statements = [
            "INSERT INTO users (id) VALUES (1)",
            "INSERT INTO orders (id) VALUES (1)",
        ]

        merged = merger.merge(statements)
        assert len(merged) == 2
        assert "INSERT INTO users" in merged[0]
        assert "INSERT INTO orders" in merged[1]

    def test_column_mismatch(self, merger: InsertMerger) -> None:
        """Test handling of different column lists."""
        statements = [
            "INSERT INTO users (id, name) VALUES (1, 'Alice')",
            "INSERT INTO users (id, email) VALUES (2, 'bob@example.com')",
        ]

        merged = merger.merge(statements)
        assert len(merged) == 2
        assert "name" in merged[0]
        assert "email" in merged[1]

    def test_special_values(self, merger: InsertMerger) -> None:
        """Test handling of special values."""
        statements = [
            "INSERT INTO users (id, name) VALUES (1, NULL)",
            "INSERT INTO users (id, name) VALUES (2, 'O''Brien')",
        ]

        merged = merger.merge(statements)
        assert len(merged) == 1
        assert "NULL" in merged[0]
        assert "O''Brien" in merged[0]

    def test_batch_size_limit(self, merger: InsertMerger) -> None:
        """Test batch size limits."""
        # Create statements that would exceed max_bytes
        statements = [
            f"INSERT INTO users (id, name) VALUES ({i}, 'User {i}')" for i in range(100)
        ]

        merged = merger.merge(statements)
        assert len(merged) > 1  # Should split into multiple batches

    def test_empty_statements(self, merger: InsertMerger) -> None:
        """Test handling of empty statements."""
        statements: List[str] = []
        merged = merger.merge(statements)
        assert len(merged) == 0

    def test_single_statement(self, merger: InsertMerger) -> None:
        """Test handling of single statement."""
        statements = ["INSERT INTO users (id) VALUES (1)"]
        merged = merger.merge(statements)
        assert len(merged) == 1
        assert merged[0] == statements[0]

    def test_mixed_statement_types(self, merger: InsertMerger) -> None:
        """Test handling of mixed statement types."""
        statements = [
            "INSERT INTO users (id) VALUES (1)",
            "SELECT * FROM users",
            "INSERT INTO users (id) VALUES (2)",
        ]

        merged = merger.merge(statements)
        assert len(merged) == 2
        assert "SELECT" in merged[1]

    def test_column_order(self, merger: InsertMerger) -> None:
        """Test handling of different column orders."""
        statements = [
            "INSERT INTO users (id, name) VALUES (1, 'Alice')",
            "INSERT INTO users (name, id) VALUES ('Bob', 2)",
        ]

        merged = merger.merge(statements)
        assert len(merged) == 2  # Should not merge due to different column order

    def test_implicit_columns(self, merger: InsertMerger) -> None:
        """Test handling of implicit column lists."""
        statements = [
            "INSERT INTO users VALUES (1, 'Alice')",
            "INSERT INTO users VALUES (2, 'Bob')",
        ]

        merged = merger.merge(statements)
        assert len(merged) == 1
        assert "VALUES (1, 'Alice'), (2, 'Bob')" in merged[0]

    def test_escaped_values(self, merger: InsertMerger) -> None:
        """Test handling of escaped values."""
        statements = [
            "INSERT INTO users (id, name) VALUES (1, 'O''Brien')",
            "INSERT INTO users (id, name) VALUES (2, 'D''Artagnan')",
        ]

        merged = merger.merge(statements)
        assert len(merged) == 1
        assert "O''Brien" in merged[0]
        assert "D''Artagnan" in merged[0]

    def test_numeric_values(self, merger: InsertMerger) -> None:
        """Test handling of numeric values."""
        statements = [
            "INSERT INTO users (id, age) VALUES (1, 25)",
            "INSERT INTO users (id, age) VALUES (2, 30.5)",
        ]

        merged = merger.merge(statements)
        assert len(merged) == 1
        assert "25" in merged[0]
        assert "30.5" in merged[0]

    def test_invalid_sql(self, merger: InsertMerger) -> None:
        """Test handling of invalid SQL."""
        statements = [
            "INSERT INTO users (id) VALUES (1)",
            "INVALID SQL",
            "INSERT INTO users (id) VALUES (2)",
        ]

        with pytest.raises(InsertMergerError):
            merger.merge(statements)

    def test_max_bytes_adjustment(self) -> None:
        """Test max_bytes adjustment."""
        # Create merger with small max_bytes
        merger = InsertMerger(max_bytes=50)
        statements = [
            "INSERT INTO users (id, name) VALUES (1, 'Alice')",
            "INSERT INTO users (id, name) VALUES (2, 'Bob')",
        ]

        merged = merger.merge(statements)
        assert len(merged) == 2  # Should not merge due to size limit

    def test_complex_values(self, merger: InsertMerger) -> None:
        """Test handling of complex values."""
        statements = [
            "INSERT INTO users (id, name, data) VALUES (1, 'Alice', '{\"key\": \"value\"}')",
            "INSERT INTO users (id, name, data) VALUES (2, 'Bob', '{\"key\": \"value2\"}')",
        ]

        merged = merger.merge(statements)
        assert len(merged) == 1
        assert '{"key": "value"}' in merged[0]
        assert '{"key": "value2"}' in merged[0]

    @pytest.mark.parametrize(
        "statements,expected_count",
        [
            (["INSERT INTO users (id) VALUES (1)"], 1),
            (
                [
                    "INSERT INTO users (id) VALUES (1)",
                    "INSERT INTO users (id) VALUES (2)",
                ],
                1,
            ),
            (
                [
                    "INSERT INTO users (id) VALUES (1)",
                    "INSERT INTO orders (id) VALUES (1)",
                ],
                2,
            ),
        ],
    )
    def test_merge_scenarios(
        self, merger: InsertMerger, statements: List[str], expected_count: int
    ) -> None:
        """Test various merge scenarios."""
        merged = merger.merge(statements)
        assert len(merged) == expected_count

    def test_malformed_sql(self, merger: InsertMerger) -> None:
        """Test handling of malformed SQL."""
        test_cases = [
            [
                "INSERT INTO users (id) VALUES (1",
                "INSERT INTO users (id) VALUES (2)",
            ],  # Missing closing parenthesis
            [
                "INSERT INTO users (id) VALUES '1'",
                "INSERT INTO users (id) VALUES '2'",
            ],  # Missing parentheses
            [
                "INSERT INTO users (id) VALUES (1, 2)",
                "INSERT INTO users (id) VALUES (3)",
            ],  # Value count mismatch
        ]

        for statements in test_cases:
            with pytest.raises(InsertMergerError):
                merger.merge(statements)

    def test_invalid_column_names(self, merger: InsertMerger) -> None:
        """Test handling of invalid column names."""
        statements = [
            "INSERT INTO users (id, `invalid name`) VALUES (1, 'value')",
            "INSERT INTO users (id, `invalid name`) VALUES (2, 'value')",
        ]

        with pytest.raises(InsertMergerError):
            merger.merge(statements)

    def test_duplicate_columns(self, merger: InsertMerger) -> None:
        """Test handling of duplicate column names."""
        statements = [
            "INSERT INTO users (id, id) VALUES (1, 2)",
            "INSERT INTO users (id, id) VALUES (3, 4)",
        ]

        with pytest.raises(InsertMergerError):
            merger.merge(statements)

    def test_empty_column_list(self, merger: InsertMerger) -> None:
        """Test handling of empty column lists."""
        statements = [
            "INSERT INTO users () VALUES (1)",
            "INSERT INTO users () VALUES (2)",
        ]

        with pytest.raises(InsertMergerError):
            merger.merge(statements)

    def test_empty_value_list(self, merger: InsertMerger) -> None:
        """Test handling of empty value lists."""
        statements = [
            "INSERT INTO users (id) VALUES ()",
            "INSERT INTO users (id) VALUES ()",
        ]

        with pytest.raises(InsertMergerError):
            merger.merge(statements)

    def test_whitespace_handling(self, merger: InsertMerger) -> None:
        """Test handling of different whitespace patterns."""
        statements = [
            "INSERT  INTO  users  (id)  VALUES  (1)",
            "INSERT INTO users(id)VALUES(2)",
        ]

        merged = merger.merge(statements)
        assert len(merged) == 1
        assert "VALUES (1), (2)" in merged[0]

    def test_sql_comments(self, merger: InsertMerger) -> None:
        """Test handling of SQL comments."""
        statements = [
            "INSERT INTO users (id) VALUES (1) -- Comment",
            "INSERT INTO users (id) VALUES (2) /* Multi-line\ncomment */",
        ]

        merged = merger.merge(statements)
        assert len(merged) == 1
        assert "VALUES (1), (2)" in merged[0]

    def test_case_sensitivity(self, merger: InsertMerger) -> None:
        """Test case sensitivity handling."""
        statements = [
            "INSERT INTO users (ID) VALUES (1)",
            "INSERT INTO users (id) VALUES (2)",
        ]

        merged = merger.merge(statements)
        assert len(merged) == 1
        assert "VALUES (1), (2)" in merged[0]

    def test_schema_qualified_tables(self, merger: InsertMerger) -> None:
        """Test handling of schema-qualified table names."""
        statements = [
            "INSERT INTO public.users (id) VALUES (1)",
            "INSERT INTO public.users (id) VALUES (2)",
        ]

        merged = merger.merge(statements)
        assert len(merged) == 1
        assert "VALUES (1), (2)" in merged[0]

    def test_large_batch_performance(self, merger: InsertMerger) -> None:
        """Test performance with large batches."""
        # Create a large batch of statements
        statements = [
            f"INSERT INTO users (id, name) VALUES ({i}, 'User {i}')"
            for i in range(1000)
        ]

        start_time = time.time()
        merged = merger.merge(statements)
        end_time = time.time()

        # Performance assertions
        assert end_time - start_time < 1.0  # Should complete within 1 second
        assert len(merged) > 1  # Should split into multiple batches

    def test_memory_usage(self, merger: InsertMerger) -> None:
        """Test memory usage with large batches."""
        process = psutil.Process(os.getpid())
        initial_memory = process.memory_info().rss

        # Create a large batch of statements
        statements = [
            f"INSERT INTO users (id, name) VALUES ({i}, 'User {i}')"
            for i in range(1000)
        ]

        merger.merge(statements)
        final_memory = process.memory_info().rss

        # Memory usage assertions
        memory_increase = final_memory - initial_memory
        assert memory_increase < 50 * 1024 * 1024  # Should use less than 50MB

    def test_dynamic_batch_size(self) -> None:
        """Test dynamic batch size adjustment."""
        merger = InsertMerger(max_bytes=1000)
        statements = [
            f"INSERT INTO users (id, name) VALUES ({i}, 'User {i}')" for i in range(100)
        ]

        # First merge
        merged1 = merger.merge(statements)
        initial_batch_count = len(merged1)

        # Adjust max_bytes
        merger.max_bytes = 500
        merged2 = merger.merge(statements)
        adjusted_batch_count = len(merged2)

        # Should create more batches with smaller max_bytes
        assert adjusted_batch_count > initial_batch_count

    def test_batch_size_optimization(self, merger: InsertMerger) -> None:
        """Test batch size optimization."""
        statements = [
            f"INSERT INTO users (id, name) VALUES ({i}, 'User {i}')" for i in range(100)
        ]

        # Test different max_bytes values
        test_sizes = [100, 500, 1000, 2000]
        batch_counts = []

        for size in test_sizes:
            merger.max_bytes = size
            merged = merger.merge(statements)
            batch_counts.append(len(merged))

        # Verify that batch count decreases as max_bytes increases
        assert all(
            batch_counts[i] >= batch_counts[i + 1] for i in range(len(batch_counts) - 1)
        )

    def test_merge_insert_statements_with_different_tables(self):
        """Test merging insert statements for different tables."""
        merger = InsertMerger()

        # Add statements for different tables
        merger.add_statement("INSERT INTO table1 (col1) VALUES (1)")
        merger.add_statement("INSERT INTO table2 (col1) VALUES (2)")

        # Get merged statements
        merged_statements = merger.get_merged_statements()

        # Verify that statements for different tables are not merged
        assert len(merged_statements) == 2
        assert merged_statements[0] == "INSERT INTO table1 (col1) VALUES (1)"
        assert merged_statements[1] == "INSERT INTO table2 (col1) VALUES (2)"


def test_insert_merger_initialization():
    """Test InsertMerger initialization with default and custom max bytes."""
    # Test with default max bytes
    merger = InsertMerger()
    assert merger.max_bytes == 900_000
    assert merger.table_maps == {}

    # Test with custom max bytes
    merger = InsertMerger(max_bytes=500_000)
    assert merger.max_bytes == 500_000
    assert merger.table_maps == {}


def test_add_statement_simple_insert():
    """Test adding simple INSERT statements."""
    merger = InsertMerger()

    # Add a simple INSERT statement
    result = merger.add_statement("INSERT INTO table VALUES (1, 2, 3)")
    assert result is None
    assert "table" in merger.table_maps
    assert len(merger.table_maps["table"]["values"]) == 1

    # Add another compatible INSERT
    result = merger.add_statement("INSERT INTO table VALUES (4, 5, 6)")
    assert result is None
    assert len(merger.table_maps["table"]["values"]) == 2


def test_add_statement_with_columns():
    """Test adding INSERT statements with column specifications."""
    merger = InsertMerger()

    # Add an INSERT with columns
    result = merger.add_statement("INSERT INTO table (col1, col2) VALUES (1, 2)")
    assert result is None
    assert "table" in merger.table_maps
    assert merger.table_maps["table"]["columns"] == "(col1, col2)"

    # Add another compatible INSERT
    result = merger.add_statement("INSERT INTO table (col1, col2) VALUES (3, 4)")
    assert result is None
    assert len(merger.table_maps["table"]["values"]) == 2


def test_add_statement_incompatible_columns():
    """Test adding INSERT statements with incompatible column specifications."""
    merger = InsertMerger()

    # Add first INSERT
    result = merger.add_statement("INSERT INTO table (col1, col2) VALUES (1, 2)")
    assert result is None

    # Add incompatible INSERT
    result = merger.add_statement("INSERT INTO table (col1, col3) VALUES (3, 4)")
    assert result == "INSERT INTO table (col1, col3) VALUES (3, 4)"
    assert len(merger.table_maps["table"]["values"]) == 1


def test_add_statement_different_tables():
    """Test adding INSERT statements for different tables."""
    merger = InsertMerger()

    # Add INSERT for first table
    result = merger.add_statement("INSERT INTO table1 VALUES (1)")
    assert result is None
    assert "table1" in merger.table_maps

    # Add INSERT for second table
    result = merger.add_statement("INSERT INTO table2 VALUES (2)")
    assert result is None
    assert "table2" in merger.table_maps
    assert len(merger.table_maps["table1"]["values"]) == 1
    assert len(merger.table_maps["table2"]["values"]) == 1


def test_add_statement_non_insert():
    """Test adding non-INSERT statements."""
    merger = InsertMerger()

    # Add a SELECT statement
    result = merger.add_statement("SELECT * FROM table")
    assert result == "SELECT * FROM table"
    assert merger.table_maps == {}

    # Add an UPDATE statement
    result = merger.add_statement("UPDATE table SET col = 1")
    assert result == "UPDATE table SET col = 1"
    assert merger.table_maps == {}


def test_flush_all():
    """Test flushing all pending INSERT statements."""
    merger = InsertMerger()

    # Add INSERT statements
    merger.add_statement("INSERT INTO table1 VALUES (1)")
    merger.add_statement("INSERT INTO table1 VALUES (2)")
    merger.add_statement("INSERT INTO table2 VALUES (3)")

    # Flush all statements
    results = merger.flush_all()
    assert len(results) == 2
    assert "INSERT INTO table1 VALUES (1), (2)" in results
    assert "INSERT INTO table2 VALUES (3)" in results
    assert merger.table_maps == {}


def test_max_bytes_limit():
    """Test that statements are flushed when max_bytes is exceeded."""
    merger = InsertMerger(max_bytes=50)

    # Add first INSERT
    result = merger.add_statement("INSERT INTO table VALUES (1, 2, 3)")
    assert result is None

    # Add second INSERT that would exceed max_bytes
    result = merger.add_statement("INSERT INTO table VALUES (4, 5, 6)")
    assert result is not None
    assert result.startswith("INSERT INTO table VALUES")
    assert len(merger.table_maps["table"]["values"]) == 1
