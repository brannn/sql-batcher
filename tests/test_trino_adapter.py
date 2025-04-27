from typing import Any, Tuple
from unittest.mock import MagicMock

import pytest

# Mark all tests in this file as using trino-specific functionality
pytestmark = [pytest.mark.db, pytest.mark.trino]

from sql_batcher.adapters.trino import TrinoAdapter


def setup_mock_trino_connection(mocker: Any) -> Tuple[Any, Any]:
    """Set up mock Trino connection and cursor."""
    mock_cursor = mocker.Mock()
    mock_connection = mocker.Mock()
    mock_connection.cursor.return_value = mock_cursor
    mocker.patch("trino.dbapi.connect", return_value=mock_connection)
    return mock_connection, mock_cursor


@pytest.fixture
def mock_trino(mocker: Any) -> Tuple[TrinoAdapter, Any, Any]:
    """Create a mock Trino adapter with mocked connection and cursor."""
    connection, cursor = setup_mock_trino_connection(mocker)
    adapter = TrinoAdapter(
        host="localhost",
        port=8080,
        user="test",
        catalog="test_catalog",
        schema="test_schema",
    )
    return adapter, connection, cursor


def test_trino_execute(mock_trino: Tuple[TrinoAdapter, Any, Any]) -> None:
    """Test executing a query with Trino adapter."""
    adapter, _, cursor = mock_trino
    cursor.description = [("column1",)]
    cursor.fetchall.return_value = [(1,), (2,), (3,)]

    result = adapter.execute("SELECT * FROM test_table")
    assert result == [(1,), (2,), (3,)]
    cursor.execute.assert_called_once_with("SELECT * FROM test_table")


def test_trino_execute_no_results(mock_trino: Tuple[TrinoAdapter, Any, Any]) -> None:
    """Test executing a non-SELECT query with Trino adapter."""
    adapter, _, cursor = mock_trino
    cursor.description = None

    result = adapter.execute("CREATE TABLE test_table (id INT)")
    assert result == []
    cursor.execute.assert_called_once_with("CREATE TABLE test_table (id INT)")


def test_trino_multiple_statements(mocker: Any) -> None:
    """Test that multiple statements in a single query raise an error."""
    connection, _ = setup_mock_trino_connection(mocker)
    TrinoAdapter(
        host="localhost",
        port=8080,
        user="test",
        catalog="test_catalog",
        schema="test_schema",
    )

    with pytest.raises(ValueError) as exc_info:
        connection.cursor().execute("SELECT 1; SELECT 2")
    assert "multiple statements" in str(exc_info.value).lower()


def test_trino_session_properties(mocker: Any) -> None:
    """Test setting session properties in Trino adapter."""
    connection, cursor = setup_mock_trino_connection(mocker)
    session_properties = {"query_max_memory": "1GB", "query_max_run_time": "1h"}

    TrinoAdapter(
        host="localhost", port=8080, user="test", session_properties=session_properties
    )

    # Verify that session properties were set during initialization
    expected_calls = [
        mocker.call("SET SESSION query_max_memory = '1GB'"),
        mocker.call("SET SESSION query_max_run_time = '1h'"),
    ]
    cursor.execute.assert_has_calls(expected_calls, any_order=True)


class TestTrinoAdapter:
    """Test cases for TrinoAdapter class."""

    @pytest.fixture(autouse=True)
    def setup(self, monkeypatch) -> None:
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
        self.mock_cursor.execute.assert_called_once_with(
            "INSERT INTO users VALUES (3, 'New User')"
        )

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
        self.mock_cursor.execute.assert_any_call(
            "SET SESSION query_max_run_time = '2h'"
        )

        # Second call should set distributed_join
        self.mock_cursor.execute.assert_any_call(
            "SET SESSION distributed_join = 'true'"
        )

        # Third call should execute the actual statement
        self.mock_cursor.execute.assert_any_call(
            "CREATE TABLE test (id INT, name VARCHAR)"
        )

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
        self.mock_cursor.execute.assert_called_once_with(
            "SHOW TABLES FROM catalog1.schema1"
        )

        # Verify the result
        assert result == ["table1", "table2"]

    def test_get_columns(self) -> None:
        """Test getting column information."""
        # Configure the mock cursor
        self.mock_cursor.description = [
            ("Column", "Type", "Extra", "Comment"),
            ("Column", "Type", "Extra", "Comment"),
        ]
        self.mock_cursor.fetchall.return_value = [
            ("id", "integer", "", "Primary key"),
            ("name", "varchar", "", "User name"),
        ]

        # Get columns
        result = self.adapter.get_columns("table1", "catalog1", "schema1")

        # Verify the query was executed
        self.mock_cursor.execute.assert_called_once_with(
            "SHOW COLUMNS FROM catalog1.schema1.table1"
        )

        # Verify the result
        assert len(result) == 2
        assert result[0]["name"] == "id"
        assert result[0]["type"] == "integer"
        assert result[1]["name"] == "name"
        assert result[1]["type"] == "varchar"

    def test_set_session_property(self) -> None:
        """Test setting a session property."""
        # Set a session property
        self.adapter.set_session_property("query_max_memory", "2GB")

        # Verify the property was set
        assert self.adapter._session_properties["query_max_memory"] == "2GB"

        # Verify the statement was executed
        self.mock_cursor.execute.assert_called_once_with(
            "SET SESSION query_max_memory = '2GB'"
        )

    def test_execute_with_http_headers(self) -> None:
        """Test execution with HTTP headers."""
        # Create an adapter with HTTP headers
        adapter = TrinoAdapter(
            host="localhost",
            port=8080,
            user="test",
            http_headers={"X-Trino-User": "test_user"},
        )

        # Execute a statement
        adapter.execute("SELECT 1")

        # Verify the headers were used
        assert adapter._connection.http_headers == {"X-Trino-User": "test_user"}

    def test_missing_trino_package(self, monkeypatch) -> None:
        """Test behavior when trino package is not installed."""
        # Simulate the trino package not being installed
        monkeypatch.setattr("sql_batcher.adapters.trino.trino", None)

        # Attempting to create the adapter should raise an ImportError
        with pytest.raises(ImportError) as excinfo:
            TrinoAdapter(host="localhost", port=8080, user="test")

        # Verify the error message
        assert "trino package is required" in str(excinfo.value)
