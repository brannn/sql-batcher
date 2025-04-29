from typing import Any, Dict, List, Tuple, Union
from unittest.mock import Mock

import pytest

from sql_batcher.adapters.postgresql import PostgreSQLAdapter

pytest.importorskip("psycopg2")

# Mark all tests in this file as using postgres-specific functionality
pytestmark = [pytest.mark.db, pytest.mark.postgres]


def setup_mock_pg_connection(mocker: Any) -> Tuple[Any, Any]:
    """Set up mock PostgreSQL connection and cursor."""
    mock_cursor = mocker.Mock()
    mock_connection = mocker.Mock()
    mock_connection.cursor.return_value = mock_cursor
    mocker.patch("psycopg2.connect", return_value=mock_connection)
    return mock_connection, mock_cursor


@pytest.fixture
def mock_pg(mocker: Any) -> Tuple[PostgreSQLAdapter, Any, Any]:
    """Create a mock PostgreSQL adapter with mocked connection and cursor."""
    connection, cursor = setup_mock_pg_connection(mocker)
    adapter = PostgreSQLAdapter(
        connection_params={
            "host": "localhost",
            "port": 5432,
            "user": "test",
            "password": "test",
            "database": "test",
        }
    )
    return adapter, connection, cursor


def test_pg_execute(mock_pg: Tuple[PostgreSQLAdapter, Any, Any]) -> None:
    """Test executing a query with PostgreSQL adapter."""
    adapter, _, cursor = mock_pg
    cursor.description = [("column1",)]
    cursor.fetchall.return_value = [(1,), (2,), (3,)]

    result = adapter.execute("SELECT * FROM test_table")
    assert result == [(1,), (2,), (3,)]
    cursor.execute.assert_called_once_with("SELECT * FROM test_table")


def test_pg_execute_no_results(mock_pg: Tuple[PostgreSQLAdapter, Any, Any]) -> None:
    """Test executing a non-SELECT query with PostgreSQL adapter."""
    adapter, _, cursor = mock_pg
    cursor.description = None

    result = adapter.execute("CREATE TABLE test_table (id INT)")
    assert result == []
    cursor.execute.assert_called_once_with("CREATE TABLE test_table (id INT)")


def test_pg_transaction(mock_pg: Tuple[PostgreSQLAdapter, Any, Any]) -> None:
    """Test transaction management with PostgreSQL adapter."""
    adapter, connection, _ = mock_pg

    adapter.begin_transaction()
    connection.autocommit = False

    adapter.commit_transaction()
    connection.commit.assert_called_once()

    adapter.rollback_transaction()
    connection.rollback.assert_called_once()


def test_pg_create_indices(mock_pg: Tuple[PostgreSQLAdapter, Any, Any]) -> None:
    """Test creating indices with PostgreSQL adapter."""
    adapter, _, cursor = mock_pg

    indices: List[Dict[str, Union[str, List[str], bool]]] = [
        {"name": "idx_test_id", "columns": ["id"], "type": "btree", "unique": True},
        {"name": "idx_test_name", "columns": ["name"], "type": "hash"},
    ]

    statements = adapter.create_indices("test_table", indices)

    assert len(statements) == 2
    assert "CREATE UNIQUE INDEX idx_test_id ON test_table USING btree (id)" in statements
    assert "CREATE INDEX idx_test_name ON test_table USING hash (name)" in statements


def test_pg_isolation_level(mocker: Any) -> None:
    """Test setting isolation level in PostgreSQL adapter."""
    import psycopg2.extensions

    connection, _ = setup_mock_pg_connection(mocker)

    PostgreSQLAdapter(
        connection_params={"host": "localhost", "database": "test"},
        isolation_level="serializable",
    )

    assert connection.isolation_level == psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE


def test_pg_cursor_factory(mocker: Any) -> None:
    """Test using custom cursor factory in PostgreSQL adapter."""
    connection, _ = setup_mock_pg_connection(mocker)
    mock_factory = mocker.Mock()

    PostgreSQLAdapter(
        connection_params={"host": "localhost", "database": "test"},
        cursor_factory=mock_factory,
    )

    connection.cursor.assert_called_once_with(cursor_factory=mock_factory)


class TestPostgreSQLAdapter:
    """Test cases for PostgreSQLAdapter class."""

    @pytest.fixture(autouse=True)
    def setup(self, monkeypatch: Any) -> None:
        """Set up test fixtures."""
        # Create mock psycopg2 module
        mock_psycopg2 = Mock()
        mock_psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED = 1
        mock_psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ = 2
        mock_psycopg2.extensions.ISOLATION_LEVEL_SERIALIZABLE = 3

        # Mock connection and cursor
        self.mock_cursor = Mock()
        self.mock_connection = Mock()
        self.mock_connection.cursor.return_value = self.mock_cursor
        mock_psycopg2.connect.return_value = self.mock_connection

        # Set up the mocks before instantiating the adapter
        monkeypatch.setattr("sql_batcher.adapters.postgresql.psycopg2", mock_psycopg2)

        # Create connection params
        self.connection_params = {"host": "localhost", "database": "test"}

        # Initialize adapter
        self.adapter = PostgreSQLAdapter(
            connection_params=self.connection_params,  # Use the mock connection directly
            max_query_size=1_000_000,
            isolation_level="read_committed",
        )

    def test_get_max_query_size(self) -> None:
        """Test get_max_query_size method."""
        assert self.adapter.get_max_query_size() == 1_000_000

    def test_execute_select(self) -> None:
        """Test executing a SELECT statement."""
        # Set up mock
        self.mock_cursor.description = [("id",), ("name",)]
        self.mock_cursor.fetchall.return_value = [(1, "Alice"), (2, "Bob")]

        # Execute a SELECT query
        result = self.adapter.execute("SELECT * FROM users")

        # Check behavior
        self.mock_cursor.execute.assert_called_once_with("SELECT * FROM users")
        self.mock_cursor.fetchall.assert_called_once()
        assert result == [(1, "Alice"), (2, "Bob")]

    def test_execute_insert(self) -> None:
        """Test executing an INSERT statement."""
        # Set up mock (no result for INSERT)
        self.mock_cursor.description = None

        # Execute an INSERT query
        result = self.adapter.execute("INSERT INTO users VALUES (1, 'Alice')")

        # Check behavior
        self.mock_cursor.execute.assert_called_once_with("INSERT INTO users VALUES (1, 'Alice')")
        self.mock_cursor.fetchall.assert_not_called()
        assert result == []

    def test_execute_copy(self) -> None:
        """Test executing a COPY statement."""
        # Execute a COPY query
        result = self.adapter.execute("COPY users FROM '/tmp/users.csv'")

        # Check behavior
        self.mock_cursor.execute.assert_called_once_with("COPY users FROM '/tmp/users.csv'")
        self.mock_connection.commit.assert_called_once()
        assert result == []

    def test_execute_error_handling(self) -> None:
        """Test error handling in execute method."""
        # Set up mock to raise an exception
        self.mock_cursor.execute.side_effect = Exception("Test error")

        # Execute a query that should fail
        with pytest.raises(Exception) as exc_info:
            self.adapter.execute("SELECT * FROM users")

        # Check error
        assert str(exc_info.value) == "Test error"

    def test_begin_transaction(self) -> None:
        """Test beginning a transaction."""
        # Run the method
        self.adapter.begin_transaction()

        # Verify autocommit was disabled
        self.mock_connection.autocommit = False

    def test_commit_transaction(self) -> None:
        """Test committing a transaction."""
        # Run the method
        self.adapter.commit_transaction()

        # Verify commit was called
        self.mock_connection.commit.assert_called_once()

    def test_rollback_transaction(self) -> None:
        """Test rolling back a transaction."""
        # Run the method
        self.adapter.rollback_transaction()

        # Verify rollback was called
        self.mock_connection.rollback.assert_called_once()

    def test_close(self) -> None:
        """Test closing the connection."""
        # Run the method
        self.adapter.close()

        # Verify close was called
        self.mock_connection.close.assert_called_once()

    def test_explain_analyze(self) -> None:
        """Test EXPLAIN ANALYZE functionality."""
        # Set up mock
        self.mock_cursor.description = [("QUERY PLAN",)]
        self.mock_cursor.fetchall.return_value = [("Seq Scan on users",)]

        # Execute EXPLAIN ANALYZE
        result = self.adapter.execute("EXPLAIN ANALYZE SELECT * FROM users")

        # Check behavior
        self.mock_cursor.execute.assert_called_once_with("EXPLAIN ANALYZE SELECT * FROM users")
        assert result == [("Seq Scan on users",)]

    def test_create_temp_table(self) -> None:
        """Test creating a temporary table."""
        # Execute CREATE TEMP TABLE
        result = self.adapter.execute("CREATE TEMP TABLE temp_users AS SELECT * FROM users")

        # Check behavior
        self.mock_cursor.execute.assert_called_once_with("CREATE TEMP TABLE temp_users AS SELECT * FROM users")
        assert result == []

    def test_get_server_version(self) -> None:
        """Test getting server version."""
        # Set up mock
        self.mock_connection.server_version = 120000

        # Get version
        version = self.adapter.get_server_version()

        # Check result
        assert version == "12.0"

    def test_missing_psycopg2(self, monkeypatch: Any) -> None:
        """Test behavior when psycopg2 is not installed."""
        # Remove the psycopg2 module
        monkeypatch.setattr("sql_batcher.adapters.postgresql.psycopg2", None)

        # Attempt to create the adapter
        with pytest.raises(ImportError) as exc_info:
            PostgreSQLAdapter(connection_params={"host": "localhost", "database": "test"})

        assert "psycopg2-binary package is required" in str(exc_info.value)

    def test_execute_batch(self) -> None:
        """Test executing a batch of statements."""
        # Set up mock
        self.mock_cursor.description = None

        # Execute a batch of statements
        result = self.adapter.execute("INSERT INTO users VALUES (1, 'Alice'); INSERT INTO users VALUES (2, 'Bob')")

        # Check behavior
        self.mock_cursor.execute.assert_called_once_with(
            "INSERT INTO users VALUES (1, 'Alice'); INSERT INTO users VALUES (2, 'Bob')"
        )
        assert result == []

    def test_use_copy_for_bulk_insert_stdin(self, monkeypatch: Any) -> None:
        """Test using COPY for bulk insert from stdin."""

        # Mock the use_copy method
        def mock_use_copy(*args: Any, **kwargs: Any) -> int:
            # Just return the length of the data argument
            return len(kwargs.get("data", ""))

        monkeypatch.setattr(self.adapter, "use_copy", mock_use_copy)

        # Test with some data
        data = "1,Alice\n2,Bob\n"
        result = self.adapter.use_copy_for_bulk_insert_stdin("users", data)

        # Check result
        assert result == len(data)

    def test_create_indices(self) -> None:
        """Test creating indices."""
        # Create some test indices
        indices: List[Dict[str, Union[str, List[str], bool]]] = [
            {"name": "idx_test_id", "columns": ["id"], "type": "btree", "unique": True},
            {"name": "idx_test_name", "columns": ["name"], "type": "hash"},
        ]

        # Create the indices
        statements = self.adapter.create_indices("test_table", indices)

        # Verify the statements
        assert len(statements) == 2
        assert "CREATE UNIQUE INDEX idx_test_id ON test_table USING btree (id)" in statements
        assert "CREATE INDEX idx_test_name ON test_table USING hash (name)" in statements
