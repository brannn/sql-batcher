"""
Tests for SQL adapter base classes.
"""

from typing import Any, List, Optional, Protocol, Tuple, TypeVar
from unittest.mock import MagicMock

import pytest

from sql_batcher.adapters.base import SQLAdapter
from sql_batcher.adapters.generic import GenericAdapter

T = TypeVar("T")


class AdapterProtocol(Protocol):
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


class MockAdapter(SQLAdapter):
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
            return list(self.cursor.fetchall())
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
    adapter = MockAdapter()
    adapter.execute("SELECT 1")
    assert adapter.get_executed_statements() == ["SELECT 1"]


def test_adapter_max_query_size() -> None:
    """Test adapter max query size."""
    adapter = MockAdapter()
    assert adapter.get_max_query_size() == 500_000


def test_adapter_transaction() -> None:
    """Test adapter transaction methods."""
    adapter = MockAdapter()

    # Test basic transaction flow
    adapter.begin_transaction()
    adapter.execute("INSERT INTO test VALUES (1)")
    adapter.commit_transaction()

    assert adapter.connection.begin.call_count == 1
    assert adapter.cursor.execute.call_count == 1
    assert adapter.connection.commit.call_count == 1


@pytest.fixture
def adapter() -> MockAdapter:
    """Create a test adapter."""
    adapter = MockAdapter()
    yield adapter
    adapter.close()


def test_adapter_with_fixture(adapter: MockAdapter) -> None:
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
        results = self.adapter.execute("SELECT * FROM test")
        assert len(results) == 1
        assert results[0][0] == 1
        assert results[0][1] == "Test"

    def test_execute_insert(self) -> None:
        """Test executing an INSERT statement."""
        results = self.adapter.execute("INSERT INTO test VALUES (3, 'Test 3')")
        assert len(results) == 0

    def test_transactions(self) -> None:
        """Test transaction behavior."""
        self.adapter.begin_transaction()
        self.adapter.execute("INSERT INTO test VALUES (1)")
        self.adapter.commit_transaction()

        assert self.connection.begin.call_count == 1
        assert self.cursor.execute.call_count == 1
        assert self.connection.commit.call_count == 1
