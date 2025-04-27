"""
Tests for SQL adapter base classes.
"""

from typing import Any, List, Optional, Protocol, Tuple, TypeVar
from unittest.mock import MagicMock, call

import pytest

from sql_batcher.adapters.base import SQLAdapter
from sql_batcher.adapters.generic import GenericAdapter

T = TypeVar("T")


class TestAdapter(Protocol):
    """Test adapter protocol."""

    def execute(self, sql: str) -> List[Tuple[Any, ...]]:
        """Execute a SQL statement."""
        ...

    def get_max_query_size(self) -> int:
        """Get maximum query size."""
        ...

    def close(self) -> None:
        """Close the connection."""
        ...

    def begin_transaction(self) -> None:
        """Begin a transaction."""
        ...

    def commit_transaction(self) -> None:
        """Commit a transaction."""
        ...

    def rollback_transaction(self) -> None:
        """Rollback a transaction."""
        ...


class TestAdapterImpl(SQLAdapter):
    """Test implementation of SQLAdapter."""

    def __init__(self, max_query_size: Optional[int] = None):
        """Initialize test adapter."""
        self.max_query_size = max_query_size or 500_000
        self.cursor = MagicMock()
        self.connection = MagicMock()
        self.connection.cursor.return_value = self.cursor
        self.closed = False
        self._executed_statements: List[str] = []

    def get_max_query_size(self) -> int:
        """Get maximum query size."""
        return self.max_query_size

    def execute(self, sql: str) -> List[Tuple[Any, ...]]:
        """Execute SQL statement."""
        self._executed_statements.append(sql)
        self.cursor.execute(sql)
        if self.cursor.description is not None:
            return self.cursor.fetchall()
        return []

    def close(self) -> None:
        """Close connection."""
        self.closed = True
        self.cursor.close()
        self.connection.close()

    def begin_transaction(self) -> None:
        """Begin transaction."""
        self.connection.begin()

    def commit_transaction(self) -> None:
        """Commit transaction."""
        self.connection.commit()

    def rollback_transaction(self) -> None:
        """Rollback transaction."""
        self.connection.rollback()

    def get_executed_statements(self) -> List[str]:
        """Get list of executed SQL statements."""
        return self._executed_statements


def test_adapter_execute() -> None:
    """Test adapter execute method."""
    adapter = TestAdapterImpl()
    adapter.execute("SELECT 1")
    assert adapter.get_executed_statements() == ["SELECT 1"]


def test_adapter_max_query_size() -> None:
    """Test adapter max query size."""
    adapter = TestAdapterImpl()
    assert adapter.get_max_query_size() == 500_000  # Fixed to match default value


def test_adapter_transaction() -> None:
    """Test adapter transaction methods."""
    adapter = TestAdapterImpl()

    # Test basic transaction flow
    adapter.begin_transaction()
    adapter.execute("INSERT INTO test VALUES (1)")
    adapter.commit_transaction()

    # Verify calls were made in correct order
    assert adapter.connection.begin.call_count == 1
    assert adapter.cursor.execute.call_count == 1
    assert adapter.connection.commit.call_count == 1

    # Test rollback
    adapter.begin_transaction()
    adapter.execute("INSERT INTO test VALUES (2)")
    adapter.rollback_transaction()

    assert adapter.connection.rollback.call_count == 1


@pytest.fixture
def adapter() -> TestAdapterImpl:
    """Create a test adapter."""
    adapter = TestAdapterImpl()
    yield adapter
    adapter.close()


def test_adapter_with_fixture(adapter: TestAdapterImpl) -> None:
    """Test adapter using fixture."""
    adapter.execute("SELECT 1")
    assert adapter.get_executed_statements() == ["SELECT 1"]


@pytest.mark.core
class TestSQLAdapter:
    """Test cases for abstract SQLAdapter class."""

    def test_abstract_methods(self) -> None:
        """Test that SQLAdapter requires implementing abstract methods."""
        # Should not be able to instantiate the abstract class
        with pytest.raises(TypeError):
            SQLAdapter()  # type: ignore

        # Create a minimal implementation
        class MinimalAdapter(SQLAdapter):
            def execute(self, sql: str) -> List[Any]:
                return []

            def get_max_query_size(self) -> int:
                return 1000

            def close(self) -> None:
                pass

        # Should be able to instantiate the minimal implementation
        adapter = MinimalAdapter()
        assert adapter is not None

        # Default transaction methods should not raise exceptions
        adapter.begin_transaction()
        adapter.commit_transaction()
        adapter.rollback_transaction()


@pytest.mark.core
class TestGenericAdapter:
    """Test cases for GenericAdapter."""

    @pytest.fixture(autouse=True)
    def setup_adapter(self) -> None:
        """Set up test fixtures."""
        # Create a mock connection and cursor
        self.connection = MagicMock()
        self.cursor = MagicMock()

        # Configure cursor behavior
        self.cursor.description = [
            ["id", "INT", None, None, None, None, None],
            ["name", "VARCHAR", None, None, None, None, None],
        ]
        self.cursor.execute.return_value = None
        self.cursor.fetchone.return_value = (1, "Test")
        self.cursor.fetchmany.return_value = [(1, "Test")]
        self.cursor.fetchall.return_value = [(1, "Test")]

        # Configure cursor to handle execute calls
        def execute_side_effect(sql: str) -> None:
            if sql.strip().upper().startswith("SELECT"):
                self.cursor.description = [
                    ["id", "INT", None, None, None, None, None],
                    ["name", "VARCHAR", None, None, None, None, None],
                ]
                self.cursor.fetchall.return_value = [(1, "Test")]
            else:
                self.cursor.description = None
                self.cursor.fetchall.return_value = []
            return None

        self.cursor.execute.side_effect = execute_side_effect

        # Configure connection to return cursor
        self.connection.cursor.return_value = self.cursor

        # Create the adapter
        self.adapter = GenericAdapter(connection=self.connection, max_query_size=1000)

        yield

        # Clean up
        self.adapter.close()

    def test_init(self) -> None:
        """Test initialization."""
        assert self.adapter._max_query_size == 1000

    def test_get_max_query_size(self) -> None:
        """Test get_max_query_size method."""
        assert self.adapter.get_max_query_size() == 1000

    def test_execute_select(self) -> None:
        """Test executing a SELECT statement."""
        # Execute the query
        results = self.adapter.execute("SELECT * FROM test ORDER BY id")

        # Should return results (mocked in fixture)
        assert len(results) == 1
        assert results[0][0] == 1
        assert results[0][1] == "Test"

        # Verify cursor was used correctly
        self.cursor.execute.assert_called_once_with("SELECT * FROM test ORDER BY id")
        self.cursor.fetchall.assert_called_once()

    def test_execute_insert(self) -> None:
        """Test executing an INSERT statement."""
        # Execute the query
        results = self.adapter.execute("INSERT INTO test VALUES (3, 'Test 3')")

        # Should not return results for INSERT
        assert len(results) == 0

        # Verify cursor was used correctly
        self.cursor.execute.assert_called_once_with(
            "INSERT INTO test VALUES (3, 'Test 3')"
        )

    def test_transactions(self) -> None:
        """Test transaction behavior."""
        # Test successful transaction
        self.adapter.begin_transaction()
        self.adapter.execute("INSERT INTO test VALUES (1)")
        self.adapter.execute("UPDATE test SET value = 2")
        self.adapter.commit_transaction()

        assert self.connection.begin.call_count == 1
        assert self.cursor.execute.call_count == 2
        assert self.connection.commit.call_count == 1

        # Test rollback
        self.adapter.begin_transaction()
        self.adapter.execute("INSERT INTO test VALUES (3)")
        self.adapter.rollback_transaction()

        assert self.connection.rollback.call_count == 1

        # Reset mock call counts
        self.connection.reset_mock()
        self.cursor.reset_mock()

        # Test nested transaction (should raise an error)
        self.adapter.begin_transaction()
        with pytest.raises(Exception):
            self.adapter.begin_transaction()
        self.adapter.rollback_transaction()

    def test_transaction_error_handling(self) -> None:
        """Test transaction error scenarios."""
        # Test commit without begin
        with pytest.raises(RuntimeError, match="No transaction in progress"):
            self.adapter.commit_transaction()

        # Test rollback without begin
        with pytest.raises(RuntimeError, match="No transaction in progress"):
            self.adapter.rollback_transaction()

        # Test transaction with connection error
        self.connection.begin.side_effect = Exception("Connection error")
        with pytest.raises(Exception, match="Connection error"):
            self.adapter.begin_transaction()

        # Reset side effect
        self.connection.begin.side_effect = None

        # Test commit error
        self.adapter.begin_transaction()
        self.connection.commit.side_effect = Exception("Commit error")
        with pytest.raises(Exception, match="Commit error"):
            self.adapter.commit_transaction()
