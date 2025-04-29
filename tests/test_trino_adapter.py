from typing import Any, Tuple
from unittest.mock import MagicMock

import pytest

from sql_batcher.adapters.trino import TrinoAdapter

# Mark all tests in this file as using trino-specific functionality
pytestmark = [pytest.mark.db, pytest.mark.trino]


def setup_mock_trino_connection(mocker: Any) -> Tuple[Any, Any]:
    """Set up mock Trino connection and cursor."""
    mock_cursor = mocker.Mock()
    mock_connection = mocker.Mock()
    mock_connection.cursor.return_value = mock_cursor
    mocker.patch("trino.dbapi.connect", return_value=mock_connection)
    return mock_connection, mock_cursor


@pytest.fixture
def mock_trino(mocker: Any) -> Tuple[TrinoAdapter, Any, Any]:
    """Create a mock Trino adapter."""
    # Set up mock connection and cursor
    mock_connection, mock_cursor = setup_mock_trino_connection(mocker)

    # Create the adapter
    adapter = TrinoAdapter(
        host="localhost",
        port=8080,
        user="test_user",
        catalog="test_catalog",
        schema="test_schema",
    )

    # Replace the connection with our mock
    adapter._connection = mock_connection
    adapter._cursor = mock_cursor

    return adapter, mock_cursor, mock_connection


def test_trino_execute(mock_trino: Tuple[TrinoAdapter, Any, Any]) -> None:
    """Test basic execution functionality."""
    adapter, cursor, _ = mock_trino

    # Configure mock cursor
    cursor.description = [("id",), ("name",)]
    cursor.fetchall.return_value = [(1, "Test"), (2, "Another")]

    # Execute a query
    result = adapter.execute("SELECT * FROM test")

    # Verify the query was executed
    cursor.execute.assert_called_once_with("SELECT * FROM test")

    # Check the results
    assert len(result) == 2
    assert result[0] == (1, "Test")
    assert result[1] == (2, "Another")


def test_trino_execute_no_results(mock_trino: Tuple[TrinoAdapter, Any, Any]) -> None:
    """Test execution with no results."""
    adapter, cursor, _ = mock_trino

    # Configure mock cursor for no results
    cursor.description = None
    cursor.fetchall.return_value = []

    # Execute a query
    result = adapter.execute("INSERT INTO test VALUES (1, 'Test')")

    # Verify the query was executed
    cursor.execute.assert_called_once_with("INSERT INTO test VALUES (1, 'Test')")

    # Check that no results were returned
    assert len(result) == 0


def test_trino_multiple_statements(mocker: Any) -> None:
    """Test handling of multiple statements."""
    # Create a mock connection
    connection, cursor = setup_mock_trino_connection(mocker)

    # Create adapter
    adapter = TrinoAdapter(host="localhost", port=8080, user="test")

    # Try to execute multiple statements
    with pytest.raises(ValueError) as exc_info:
        adapter.execute("SELECT 1; SELECT 2")

    assert "multiple statements" in str(exc_info.value)


def test_trino_session_properties(mocker: Any) -> None:
    """Test setting session properties."""
    # Create a mock connection
    connection, cursor = setup_mock_trino_connection(mocker)

    # Create adapter with session properties
    TrinoAdapter(
        host="localhost",
        port=8080,
        user="test",
        session_properties={
            "query_max_memory": "1GB",
            "query_max_run_time": "1h",
        },
    )

    # Verify that session properties were set during initialization
    cursor.execute.assert_any_call("SET SESSION query_max_memory = '1GB'")
    cursor.execute.assert_any_call("SET SESSION query_max_run_time = '1h'")


class TestTrinoAdapter:
    """Test cases for TrinoAdapter class."""

    @pytest.fixture(autouse=True)
    def setup(self, monkeypatch: Any) -> None:
        """Set up test fixtures."""
        # Mock the trino.dbapi module
        self.mock_trino = MagicMock()
        self.mock_connection = MagicMock()
        self.mock_cursor = MagicMock()

        # Configure the mocks
        self.mock_connection.cursor.return_value = self.mock_cursor
        self.mock_trino.connect.return_value = self.mock_connection

        # Patch the trino.dbapi module
        monkeypatch.setattr("sql_batcher.adapters.trino.trino.dbapi", self.mock_trino)

        # Create the adapter
        self.adapter = TrinoAdapter(
            host="localhost",
            port=8080,
            user="test_user",
            catalog="test_catalog",
            schema="test_schema",
        )

        # Set up mock session properties for testing
        self.adapter._session_properties = {
            "query_max_run_time": "2h",
            "distributed_join": "true",
        }

    def test_init(self) -> None:
        """Test initialization."""
        # Check that the connection was created with the correct parameters
        self.mock_trino.connect.assert_called_once()
        call_kwargs = self.mock_trino.connect.call_args.kwargs

        assert call_kwargs["host"] == "localhost"
        assert call_kwargs["port"] == 8080
        assert call_kwargs["user"] == "test_user"
        assert call_kwargs["catalog"] == "test_catalog"
        assert call_kwargs["schema"] == "test_schema"

        # Check that the adapter has the correct properties
        assert self.adapter._connection == self.mock_connection
        assert self.adapter._cursor == self.mock_cursor

    def test_get_max_query_size(self) -> None:
        """Test get_max_query_size method."""
        # Trino has a default max query size of 1MB (1,000,000 bytes)
        assert self.adapter.get_max_query_size() == 1_000_000

    def test_execute_select(self) -> None:
        """Test executing a SELECT statement."""
        # Configure the mock cursor to return test data
        self.mock_cursor.description = [("id",), ("name",)]
        self.mock_cursor.fetchall.return_value = [(1, "Test User"), (2, "Another User")]

        # Execute a SELECT statement
        result = self.adapter.execute("SELECT id, name FROM users")

        # Verify the query was executed with the correct SQL
        self.mock_cursor.execute.assert_called_once_with("SELECT id, name FROM users")

        # Verify the result contains the expected data
        assert result == [(1, "Test User"), (2, "Another User")]

    def test_execute_insert(self) -> None:
        """Test executing an INSERT statement."""
        # Configure the mock cursor for an INSERT
        self.mock_cursor.description = None
        self.mock_cursor.rowcount = 1

        # Execute an INSERT statement
        result = self.adapter.execute("INSERT INTO users VALUES (3, 'New User')")

        # Verify the query was executed with the correct SQL
        self.mock_cursor.execute.assert_called_once_with("INSERT INTO users VALUES (3, 'New User')")

        # Verify the result is empty for non-SELECT statements
        assert result == []

    def test_execute_with_session_properties(self) -> None:
        """Test execution with session properties."""
        # Configure the mock cursor
        self.mock_cursor.description = None

        # Execute a statement
        self.adapter.execute("CREATE TABLE test (id INT, name VARCHAR)")

        # Verify session properties were set
        assert self.mock_cursor.execute.call_count == 3

        # First call should set query_max_run_time
        self.mock_cursor.execute.assert_any_call("SET SESSION query_max_run_time = '2h'")

        # Second call should set distributed_join
        self.mock_cursor.execute.assert_any_call("SET SESSION distributed_join = 'true'")

        # Third call should execute the actual statement
        self.mock_cursor.execute.assert_any_call("CREATE TABLE test (id INT, name VARCHAR)")

    def test_begin_transaction(self) -> None:
        """Test beginning a transaction."""
        # Run the method
        self.adapter.begin_transaction()

        # Verify a start transaction statement was executed
        self.mock_cursor.execute.assert_called_once_with("START TRANSACTION")

    def test_commit_transaction(self) -> None:
        """Test committing a transaction."""
        # Run the method
        self.adapter.commit_transaction()

        # Verify a commit statement was executed
        self.mock_cursor.execute.assert_called_once_with("COMMIT")

    def test_rollback_transaction(self) -> None:
        """Test rolling back a transaction."""
        # Run the method
        self.adapter.rollback_transaction()

        # Verify a rollback statement was executed
        self.mock_cursor.execute.assert_called_once_with("ROLLBACK")

    def test_close(self) -> None:
        """Test closing the connection."""
        # Run the method
        self.adapter.close()

        # Verify the cursor and connection were closed
        self.mock_cursor.close.assert_called_once()
        self.mock_connection.close.assert_called_once()

    def test_get_catalogs(self) -> None:
        """Test getting available catalogs."""
        # Configure the mock cursor
        self.mock_cursor.description = [("Catalog",)]
        self.mock_cursor.fetchall.return_value = [("catalog1",), ("catalog2",)]

        # Get catalogs
        result = self.adapter.get_catalogs()

        # Verify the query was executed
        self.mock_cursor.execute.assert_called_once_with("SHOW CATALOGS")

        # Verify the result
        assert result == ["catalog1", "catalog2"]

    def test_get_schemas(self) -> None:
        """Test getting available schemas."""
        # Configure the mock cursor
        self.mock_cursor.description = [("Schema",)]
        self.mock_cursor.fetchall.return_value = [("schema1",), ("schema2",)]

        # Get schemas
        result = self.adapter.get_schemas("catalog1")

        # Verify the query was executed
        self.mock_cursor.execute.assert_called_once_with("SHOW SCHEMAS FROM catalog1")

        # Verify the result
        assert result == ["schema1", "schema2"]

    def test_get_tables(self) -> None:
        """Test getting available tables."""
        # Configure the mock cursor
        self.mock_cursor.description = [("Table",)]
        self.mock_cursor.fetchall.return_value = [("table1",), ("table2",)]

        # Get tables
        result = self.adapter.get_tables("catalog1", "schema1")

        # Verify the query was executed
        self.mock_cursor.execute.assert_called_once_with("SHOW TABLES FROM catalog1.schema1")

        # Verify the result
        assert result == ["table1", "table2"]

    def test_get_columns(self) -> None:
        """Test getting column information."""
        # Configure the mock cursor
        self.mock_cursor.description = [
            ("Column", "Type", "Extra"),
            ("id", "INTEGER", ""),
            ("name", "VARCHAR", ""),
        ]
        self.mock_cursor.fetchall.return_value = [
            ("id", "INTEGER", ""),
            ("name", "VARCHAR", ""),
        ]

        # Get columns
        result = self.adapter.get_columns("catalog1", "schema1", "table1")

        # Verify the query was executed
        self.mock_cursor.execute.assert_called_once_with("SHOW COLUMNS FROM catalog1.schema1.table1")

        # Verify the result
        assert result == [
            {"name": "id", "type": "INTEGER", "extra": ""},
            {"name": "name", "type": "VARCHAR", "extra": ""},
        ]

    def test_set_session_property(self) -> None:
        """Test setting a session property."""
        # Set a property
        self.adapter.set_session_property("query_max_memory", "2GB")

        # Verify the property was added to the session properties
        assert self.adapter._session_properties["query_max_memory"] == "2GB"

        # Verify the property was set in the session
        self.mock_cursor.execute.assert_called_once_with("SET SESSION query_max_memory = '2GB'")

    def test_execute_with_http_headers(self) -> None:
        """Test executing a query with HTTP headers."""
        # Configure the mock cursor
        self.mock_cursor.description = None

        # Execute with headers
        self.adapter.execute(
            "SELECT * FROM test",
            extra_headers={
                "X-Trino-User": "test_user",
                "X-Trino-Schema": "test_schema",
            },
        )

        # Verify the headers were set
        assert self.mock_cursor.execute.call_count == 3
        self.mock_cursor.execute.assert_any_call("SET SESSION query_max_run_time = '2h'")
        self.mock_cursor.execute.assert_any_call("SET SESSION distributed_join = 'true'")
        self.mock_cursor.execute.assert_any_call("SELECT * FROM test")

    def test_missing_trino_package(self, monkeypatch: Any) -> None:
        """Test behavior when trino package is not installed."""
        # Remove the trino module
        monkeypatch.setattr("sql_batcher.adapters.trino.trino", None)

        # Attempt to create the adapter
        with pytest.raises(ImportError) as exc_info:
            TrinoAdapter(
                host="localhost",
                port=8080,
                user="test_user",
                catalog="test_catalog",
                schema="test_schema",
            )

        assert "trino package is required" in str(exc_info.value)
