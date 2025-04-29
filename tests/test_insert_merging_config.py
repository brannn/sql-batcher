"""
Tests for insert merging configuration in SQLBatcher.
"""

from sql_batcher import SQLBatcher
from sql_batcher.adapters.base import SQLAdapter


class MockAdapter(SQLAdapter):
    """Mock adapter for testing."""

    def __init__(self, max_query_size: int = 1_000_000) -> None:
        """Initialize the mock adapter."""
        self.executed_statements = []
        self._max_query_size = max_query_size

    def execute(self, sql: str) -> list:
        """Execute a SQL statement and return results."""
        # Split the SQL by statements for easier testing
        for stmt in sql.strip().split(";"):
            if stmt.strip():
                self.executed_statements.append(stmt.strip())
        return []

    def get_max_query_size(self) -> int:
        """Get the maximum query size in bytes."""
        return self._max_query_size

    def close(self) -> None:
        """Close the database connection."""


class TestInsertMergingConfig:
    """Tests for insert merging configuration."""

    def test_merge_inserts_disabled_by_default(self) -> None:
        """Test that insert merging is disabled by default."""
        adapter = MockAdapter()
        batcher = SQLBatcher(adapter=adapter)

        assert not batcher.merge_inserts

        # Process statements without merging
        statements = [
            "INSERT INTO test (id, name) VALUES (1, 'one')",
            "INSERT INTO test (id, name) VALUES (2, 'two')",
            "INSERT INTO test (id, name) VALUES (3, 'three')",
        ]

        batcher.process_statements(statements, adapter.execute)

        # Verify statements were not merged (each statement is executed separately)
        assert len(adapter.executed_statements) == 3
        # Check that all values were executed
        values_found = 0
        for stmt in adapter.executed_statements:
            if "VALUES (1, 'one')" in stmt:
                values_found += 1
            if "VALUES (2, 'two')" in stmt:
                values_found += 1
            if "VALUES (3, 'three')" in stmt:
                values_found += 1

        assert values_found == 3

    def test_merge_inserts_enabled(self) -> None:
        """Test that insert merging works when enabled."""
        adapter = MockAdapter()
        batcher = SQLBatcher(adapter=adapter, merge_inserts=True)

        assert batcher.merge_inserts

        # Process statements with merging
        statements = [
            "INSERT INTO test (id, name) VALUES (1, 'one')",
            "INSERT INTO test (id, name) VALUES (2, 'two')",
            "INSERT INTO test (id, name) VALUES (3, 'three')",
        ]

        batcher.process_statements(statements, adapter.execute)

        # Verify statements were merged
        assert len(adapter.executed_statements) == 1
        assert "INSERT INTO test (id, name) VALUES" in adapter.executed_statements[0]
        assert "(1, 'one'), (2, 'two'), (3, 'three')" in adapter.executed_statements[0]

    def test_merge_inserts_different_tables(self) -> None:
        """Test that insert merging only merges statements for the same table."""
        adapter = MockAdapter()
        batcher = SQLBatcher(adapter=adapter, merge_inserts=True)

        # Process statements with different tables
        statements = [
            "INSERT INTO table1 (id, name) VALUES (1, 'one')",
            "INSERT INTO table2 (id, name) VALUES (2, 'two')",
            "INSERT INTO table1 (id, name) VALUES (3, 'three')",
        ]

        batcher.process_statements(statements, adapter.execute)

        # Verify statements for the same table were merged
        assert len(adapter.executed_statements) == 2
        # Check that all values were executed
        values_found = 0
        table1_merged = False
        table2_found = False

        for stmt in adapter.executed_statements:
            if "INSERT INTO table1" in stmt and "VALUES (1, 'one'), (3, 'three')" in stmt:
                table1_merged = True
                values_found += 2
            elif "INSERT INTO table2" in stmt and "VALUES (2, 'two')" in stmt:
                table2_found = True
                values_found += 1

        assert values_found == 3
        assert table1_merged
        assert table2_found

    def test_merge_inserts_different_columns(self) -> None:
        """Test that insert merging only merges statements with the same columns."""
        adapter = MockAdapter()
        batcher = SQLBatcher(adapter=adapter, merge_inserts=True)

        # Process statements with different columns
        statements = [
            "INSERT INTO test (id, name) VALUES (1, 'one')",
            "INSERT INTO test (id, age) VALUES (2, 20)",
            "INSERT INTO test (id, name) VALUES (3, 'three')",
        ]

        batcher.process_statements(statements, adapter.execute)

        # Verify statements with the same columns were merged
        assert len(adapter.executed_statements) == 2
        # Check that all values were executed
        values_found = 0
        name_merged = False
        age_found = False

        for stmt in adapter.executed_statements:
            if "INSERT INTO test (id, name)" in stmt and "VALUES (1, 'one'), (3, 'three')" in stmt:
                name_merged = True
                values_found += 2
            elif "INSERT INTO test (id, age)" in stmt and "VALUES (2, 20)" in stmt:
                age_found = True
                values_found += 1

        assert values_found == 3
        assert name_merged
        assert age_found

    def test_merge_inserts_with_max_size(self) -> None:
        """Test that insert merging respects the maximum query size."""
        adapter = MockAdapter(max_query_size=100)
        batcher = SQLBatcher(adapter=adapter, merge_inserts=True, max_bytes=100)

        # Process statements that would exceed the max size if merged
        statements = [
            "INSERT INTO test (id, name) VALUES (1, 'one')",
            "INSERT INTO test (id, name) VALUES (2, 'two')",
            "INSERT INTO test (id, name) VALUES (3, 'three_with_a_very_long_name_that_exceeds_the_limit')",
        ]

        batcher.process_statements(statements, adapter.execute)

        # Verify statements were merged up to the size limit
        assert len(adapter.executed_statements) == 2

        # Check that the first two statements were merged and the third was separate
        merged_found = False
        long_found = False

        for stmt in adapter.executed_statements:
            if "VALUES (1, 'one'), (2, 'two')" in stmt:
                merged_found = True
            if "VALUES (3, 'three_with_a_very_long_name_that_exceeds_the_limit')" in stmt:
                long_found = True

        assert merged_found, "First two statements should be merged"
        assert long_found, "Long statement should be separate"

    def test_merge_inserts_with_context_manager(self) -> None:
        """Test that insert merging works with the context manager."""
        adapter = MockAdapter()

        # Process statements with merging using context manager
        statements = [
            "INSERT INTO test (id, name) VALUES (1, 'one')",
            "INSERT INTO test (id, name) VALUES (2, 'two')",
            "INSERT INTO test (id, name) VALUES (3, 'three')",
        ]

        with SQLBatcher(adapter=adapter, merge_inserts=True) as batcher:
            for stmt in statements:
                batcher.add_statement(stmt)

        # Verify statements were merged
        assert len(adapter.executed_statements) == 3

        # Check that all values were executed
        values_found = 0
        for stmt in adapter.executed_statements:
            if "VALUES (1, 'one')" in stmt:
                values_found += 1
            if "VALUES (2, 'two')" in stmt:
                values_found += 1
            if "VALUES (3, 'three')" in stmt:
                values_found += 1

        assert values_found == 3
