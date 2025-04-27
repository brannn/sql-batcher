"""
Tests for SQL adapter base classes.
"""

from typing import Any, Generator, List

import pytest

from sql_batcher.adapters.base import SQLAdapter
from sql_batcher.adapters.generic import GenericAdapter


class TestAdapter(SQLAdapter):
    """Test adapter implementation."""

    def __init__(self) -> None:
        self._max_query_size = 1000
        self._results: List[Any] = []

    def execute(self, sql: str) -> List[Any]:
        """Execute a SQL statement."""
        self._results.append(sql)
        return []

    def get_max_query_size(self) -> int:
        """Get maximum query size."""
        return self._max_query_size

    def close(self) -> None:
        """Close the connection."""
        pass

    def get_results(self) -> List[str]:
        """Get executed statements."""
        return self._results


def test_adapter_execute() -> None:
    """Test basic execution functionality."""
    adapter = TestAdapter()
    adapter.execute("SELECT 1")
    assert adapter.get_results() == ["SELECT 1"]


def test_adapter_max_query_size() -> None:
    """Test max query size functionality."""
    adapter = TestAdapter()
    assert adapter.get_max_query_size() == 1000


def test_adapter_transaction() -> None:
    """Test transaction management."""
    adapter = TestAdapter()

    # Default implementations should not raise errors
    adapter.begin_transaction()
    adapter.commit_transaction()
    adapter.rollback_transaction()


@pytest.fixture
def adapter() -> Generator[TestAdapter, None, None]:
    """Fixture providing a test adapter instance."""
    adapter = TestAdapter()
    yield adapter
    adapter.close()


def test_adapter_with_fixture(adapter: TestAdapter) -> None:
    """Test adapter using fixture."""
    adapter.execute("SELECT 1")
    assert adapter.get_results() == ["SELECT 1"]


@pytest.mark.core
class TestSQLAdapter:
    """Test cases for abstract SQLAdapter class."""

    def test_abstract_methods(self) -> None:
        """Test that SQLAdapter requires implementing abstract methods."""
        # Should not be able to instantiate the abstract class
        with pytest.raises(TypeError):
            SQLAdapter()

        # Create a minimal implementation
        class MinimalAdapter(SQLAdapter):
            def execute(self, sql):
                return []

            def get_max_query_size(self):
                return 1000

            def close(self):
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
    def setup_adapter(self, mock_db_connection) -> None:
        """Set up test fixtures."""
        # Use the mocked database connection from conftest.py
        self.connection = mock_db_connection

        # Create the adapter
        self.adapter = GenericAdapter(connection=self.connection, max_query_size=1000)

        yield

        # Clean up
        self.adapter.close()

    def test_init(self) -> None:
        """Test initialization."""
        assert self.adapter.max_query_size == 1000
        assert self.adapter.fetch_results is True

    def test_get_max_query_size(self) -> None:
        """Test get_max_query_size method."""
        assert self.adapter.get_max_query_size() == 1000

    def test_execute_select(self) -> None:
        """Test executing a SELECT statement."""
        results = self.adapter.execute("SELECT * FROM test ORDER BY id")

        # Should return results (mocked in fixture)
        assert len(results) == 1
        assert results[0][0] == 1
        assert results[0][1] == "Test"

    def test_execute_insert(self) -> None:
        """Test executing an INSERT statement."""
        results = self.adapter.execute("INSERT INTO test VALUES (3, 'Test 3')")

        # Should not return results for INSERT
        assert len(results) == 0

    def test_execute_with_fetch_results_false(self) -> None:
        """Test executing with fetch_results=False."""
        # Create an adapter with fetch_results=False
        adapter = GenericAdapter(connection=self.connection, fetch_results=False)

        # Execute a SELECT statement
        results = adapter.execute("SELECT * FROM test")

        # Should not return results when fetch_results is False
        assert len(results) == 0

    def test_transactions(self) -> None:
        """Test transaction methods."""
        # Begin a transaction
        self.adapter.begin_transaction()

        # Insert a row
        self.adapter.execute("INSERT INTO test VALUES (4, 'Test 4')")

        # Commit the transaction
        self.adapter.commit_transaction()

        # Test rollback
        self.adapter.begin_transaction()
        self.adapter.execute("INSERT INTO test VALUES (5, 'Test 5')")
        self.adapter.rollback_transaction()

        # Verify connection methods were called
        assert self.connection.commit.call_count >= 1
        assert self.connection.rollback.call_count >= 1
