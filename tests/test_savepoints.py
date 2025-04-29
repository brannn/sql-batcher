"""
Tests for savepoint functionality in SQL adapters.
"""

import pytest

from sql_batcher.adapters.postgresql import PostgreSQLAdapter
from sql_batcher.batcher import SQLBatcher


class MockConnection:
    """Mock connection for testing."""

    def __init__(self) -> None:
        """Initialize the mock connection."""
        self.executed_statements = []
        self.committed = False
        self.rolled_back = False
        self.closed = False
        self.savepoints = {}  # name -> released

    def cursor(self) -> "MockCursor":
        """Get a cursor for the connection."""
        return MockCursor(self)

    def commit(self) -> None:
        """Commit the current transaction."""
        self.committed = True

    def rollback(self) -> None:
        """Rollback the current transaction."""
        self.rolled_back = True

    def close(self) -> None:
        """Close the connection."""
        self.closed = True


class MockCursor:
    """Mock cursor for testing."""

    def __init__(self, connection: MockConnection) -> None:
        """Initialize the mock cursor."""
        self.connection = connection
        self.description = None
        self.rowcount = 0
        self.results = []

    def execute(self, sql: str) -> None:
        """Execute a SQL statement."""
        self.connection.executed_statements.append(sql)

        # Handle savepoint operations
        if sql.startswith("SAVEPOINT "):
            name = sql.split(" ")[1]
            self.connection.savepoints[name] = False  # Not released
        elif sql.startswith("ROLLBACK TO SAVEPOINT "):
            name = sql.split(" ")[3]
            if name not in self.connection.savepoints:
                raise ValueError(f"Savepoint {name} does not exist")
        elif sql.startswith("RELEASE SAVEPOINT "):
            name = sql.split(" ")[2]
            if name not in self.connection.savepoints:
                raise ValueError(f"Savepoint {name} does not exist")
            self.connection.savepoints[name] = True  # Released

    def fetchall(self) -> list:
        """Fetch all results."""
        return self.results

    def close(self) -> None:
        """Close the cursor."""


class TestSavepoints:
    """Tests for savepoint functionality."""

    def test_postgresql_adapter_savepoints(self) -> None:
        """Test savepoint operations in PostgreSQLAdapter."""
        # Create a mock connection
        connection = MockConnection()

        # Create a PostgreSQL adapter with the mock connection
        adapter = PostgreSQLAdapter(connection=connection)

        # Create a savepoint
        adapter.create_savepoint("test_savepoint")
        assert "SAVEPOINT test_savepoint" in connection.executed_statements
        assert "test_savepoint" in connection.savepoints
        assert not connection.savepoints["test_savepoint"]  # Not released

        # Rollback to the savepoint
        adapter.rollback_to_savepoint("test_savepoint")
        assert "ROLLBACK TO SAVEPOINT test_savepoint" in connection.executed_statements

        # Release the savepoint
        adapter.release_savepoint("test_savepoint")
        assert "RELEASE SAVEPOINT test_savepoint" in connection.executed_statements
        assert connection.savepoints["test_savepoint"]  # Released

    def test_savepoint_error_handling(self) -> None:
        """Test error handling for savepoint operations."""
        # Create a mock connection
        connection = MockConnection()

        # Create a PostgreSQL adapter with the mock connection
        adapter = PostgreSQLAdapter(connection=connection)

        # Try to rollback to a non-existent savepoint
        with pytest.raises(ValueError):
            adapter.rollback_to_savepoint("non_existent")

        # Try to release a non-existent savepoint
        with pytest.raises(ValueError):
            adapter.release_savepoint("non_existent")

    def test_savepoint_with_batcher(self) -> None:
        """Test using savepoints with SQLBatcher."""
        # Create a mock connection
        connection = MockConnection()

        # Create a PostgreSQL adapter with the mock connection
        adapter = PostgreSQLAdapter(connection=connection)

        # Create a batcher
        batcher = SQLBatcher(adapter=adapter)

        # Begin a transaction
        adapter.begin_transaction()

        # Create a savepoint
        adapter.create_savepoint("before_inserts")

        # Add some statements
        batcher.add_statement("INSERT INTO test VALUES (1)")
        batcher.add_statement("INSERT INTO test VALUES (2)")

        # Flush the batcher
        batcher.flush(adapter.execute)

        # Verify the statements were executed
        assert "INSERT INTO test VALUES (1)" in connection.executed_statements[1]
        assert "INSERT INTO test VALUES (2)" in connection.executed_statements[1]

        # Rollback to the savepoint
        adapter.rollback_to_savepoint("before_inserts")

        # Verify the rollback was executed
        assert "ROLLBACK TO SAVEPOINT before_inserts" in connection.executed_statements

        # Rollback the transaction
        adapter.rollback_transaction()
        assert connection.rolled_back
