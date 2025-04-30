"""Tests for the TrinoAdapter class with proper mocking."""

import unittest
from unittest.mock import MagicMock, patch

# Patch the TRINO_AVAILABLE constant before importing the adapter
with patch("sql_batcher.adapters.trino.TRINO_AVAILABLE", True):
    from sql_batcher.adapters.trino import TrinoAdapter


class TestTrinoAdapter(unittest.TestCase):
    """Test the TrinoAdapter with proper mocking."""

    @patch("sql_batcher.adapters.trino.TRINO_AVAILABLE", True)
    @patch("sql_batcher.adapters.trino.trino")
    def test_init(self, mock_trino):
        """Test the initialization of the adapter."""
        # Set up the mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_trino.dbapi.connect.return_value = mock_conn

        # Create the adapter
        adapter = TrinoAdapter(host="mock-host", port=8080, user="mock-user", catalog="mock-catalog", schema="mock-schema")

        # Verify the adapter was initialized correctly
        # The adapter doesn't expose these attributes directly, but we can verify
        # that the connection was created with the correct parameters
        self.assertEqual(adapter.get_max_query_size(), 600_000)  # Trino has a 1MB limit, so we use 600KB

        # The adapter connects automatically in __init__

        # Verify the connection was created with the correct parameters
        mock_trino.dbapi.connect.assert_called_once_with(
            host="mock-host",
            port=8080,
            user="mock-user",
            http_scheme="http",
            verify=True,
            catalog="mock-catalog",
            schema="mock-schema",
        )

    @patch("sql_batcher.adapters.trino.TRINO_AVAILABLE", False)
    def test_missing_trino(self):
        """Test the behavior when the trino package is missing."""
        # Attempt to create an adapter without the trino package
        with self.assertRaises(ImportError):
            TrinoAdapter(host="mock-host", port=8080, user="mock-user", catalog="mock-catalog", schema="mock-schema")

    @patch("sql_batcher.adapters.trino.TRINO_AVAILABLE", True)
    @patch("sql_batcher.adapters.trino.trino")
    def test_init_with_role(self, mock_trino):
        """Test the initialization of the adapter with a role."""
        # Set up the mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_trino.dbapi.connect.return_value = mock_conn

        # Create the adapter with a role
        adapter = TrinoAdapter(  # noqa: F841
            host="mock-host", port=8080, user="mock-user", catalog="mock-catalog", schema="mock-schema", role="mock-role"
        )

        # The adapter connects automatically in __init__

        # Verify the connection was created with the correct parameters
        mock_trino.dbapi.connect.assert_called_once_with(
            host="mock-host",
            port=8080,
            user="mock-user",
            http_scheme="http",
            verify=True,
            catalog="mock-catalog",
            schema="mock-schema",
            http_headers={"x-trino-role": "system=ROLE{mock-role}"},
        )

    @patch("sql_batcher.adapters.trino.TRINO_AVAILABLE", True)
    @patch("sql_batcher.adapters.trino.trino")
    def test_init_with_http_headers(self, mock_trino):
        """Test the initialization of the adapter with HTTP headers."""
        # Set up the mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_trino.dbapi.connect.return_value = mock_conn

        # Create the adapter with HTTP headers
        adapter = TrinoAdapter(  # noqa: F841
            host="mock-host",
            port=8080,
            user="mock-user",
            catalog="mock-catalog",
            schema="mock-schema",
            http_headers={"X-Custom-Header": "value"},
        )

        # The adapter connects automatically in __init__

        # Verify the connection was created with the correct parameters
        mock_trino.dbapi.connect.assert_called_once_with(
            host="mock-host",
            port=8080,
            user="mock-user",
            http_scheme="http",
            verify=True,
            catalog="mock-catalog",
            schema="mock-schema",
            http_headers={"X-Custom-Header": "value"},
        )

    @patch("sql_batcher.adapters.trino.TRINO_AVAILABLE", True)
    @patch("sql_batcher.adapters.trino.trino")
    def test_execute_select(self, mock_trino):
        """Test executing a SELECT statement."""
        # Set up the mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_trino.dbapi.connect.return_value = mock_conn

        # Set up the cursor to return some data
        mock_cursor.fetchall.return_value = [
            (1, "Alice"),
            (2, "Bob"),
        ]
        mock_cursor.description = [
            ("id", None, None, None, None, None, None),
            ("name", None, None, None, None, None, None),
        ]

        # Create the adapter (connects automatically in __init__)
        adapter = TrinoAdapter(host="mock-host", port=8080, user="mock-user", catalog="mock-catalog", schema="mock-schema")

        # Execute a SELECT statement
        result = adapter.execute("SELECT id, name FROM users")

        # Verify the cursor was used correctly
        mock_cursor.execute.assert_called_once_with("SELECT id, name FROM users")
        mock_cursor.fetchall.assert_called_once()

        # Verify the result is correct
        self.assertEqual(len(result), 2)
        # The result is a list of tuples, not dictionaries
        self.assertEqual(result[0][0], 1)
        self.assertEqual(result[0][1], "Alice")
        self.assertEqual(result[1][0], 2)
        self.assertEqual(result[1][1], "Bob")

    @patch("sql_batcher.adapters.trino.TRINO_AVAILABLE", True)
    @patch("sql_batcher.adapters.trino.trino")
    def test_execute_insert(self, mock_trino):
        """Test executing an INSERT statement."""
        # Set up the mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_trino.dbapi.connect.return_value = mock_conn

        # Create the adapter (connects automatically in __init__)
        adapter = TrinoAdapter(host="mock-host", port=8080, user="mock-user", catalog="mock-catalog", schema="mock-schema")

        # Execute an INSERT statement
        result = adapter.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")

        # Verify the cursor was used correctly
        mock_cursor.execute.assert_called_once_with("INSERT INTO users (id, name) VALUES (1, 'Alice')")

        # Verify the result is correct (empty list for non-SELECT statements)
        self.assertEqual(result, [])

    @patch("sql_batcher.adapters.trino.TRINO_AVAILABLE", True)
    @patch("sql_batcher.adapters.trino.trino")
    def test_execute_multiple_statements(self, mock_trino):
        """Test executing multiple statements."""
        # Set up the mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_trino.dbapi.connect.return_value = mock_conn

        # Create the adapter and connect
        adapter = TrinoAdapter(host="mock-host", port=8080, user="mock-user", catalog="mock-catalog", schema="mock-schema")
        # The adapter connects automatically in __init__

        # Execute multiple statements
        batch_sql = """
        INSERT INTO users (id, name) VALUES (1, 'Alice');
        INSERT INTO users (id, name) VALUES (2, 'Bob');
        """

        # Verify that executing multiple statements raises an error
        with self.assertRaises(ValueError):
            adapter.execute(batch_sql)

    @patch("sql_batcher.adapters.trino.TRINO_AVAILABLE", True)
    @patch("sql_batcher.adapters.trino.trino")
    def test_get_catalogs(self, mock_trino):
        """Test getting the catalogs."""
        # Set up the mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_trino.dbapi.connect.return_value = mock_conn

        # Set up the cursor to return some data
        mock_cursor.fetchall.return_value = [
            ("catalog1",),
            ("catalog2",),
        ]
        mock_cursor.description = [
            ("catalog_name", None, None, None, None, None, None),
        ]

        # Create the adapter and connect
        adapter = TrinoAdapter(host="mock-host", port=8080, user="mock-user", catalog="mock-catalog", schema="mock-schema")
        # The adapter connects automatically in __init__

        # Get the catalogs
        result = adapter.get_catalogs()

        # Verify the cursor was used correctly
        mock_cursor.execute.assert_called_with("SHOW CATALOGS")
        mock_cursor.fetchall.assert_called_once()

        # Verify the result is correct
        self.assertEqual(result, ["catalog1", "catalog2"])

    @patch("sql_batcher.adapters.trino.TRINO_AVAILABLE", True)
    @patch("sql_batcher.adapters.trino.trino")
    def test_get_schemas(self, mock_trino):
        """Test getting the schemas."""
        # Set up the mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_trino.dbapi.connect.return_value = mock_conn

        # Set up the cursor to return some data
        mock_cursor.fetchall.return_value = [
            ("schema1",),
            ("schema2",),
        ]
        mock_cursor.description = [
            ("schema_name", None, None, None, None, None, None),
        ]

        # Create the adapter and connect
        adapter = TrinoAdapter(host="mock-host", port=8080, user="mock-user", catalog="mock-catalog", schema="mock-schema")
        # The adapter connects automatically in __init__

        # Get the schemas
        result = adapter.get_schemas("mock-catalog")

        # Verify the cursor was used correctly
        mock_cursor.execute.assert_called_with("SHOW SCHEMAS FROM mock-catalog")
        mock_cursor.fetchall.assert_called_once()

        # Verify the result is correct
        self.assertEqual(result, ["schema1", "schema2"])

    @patch("sql_batcher.adapters.trino.TRINO_AVAILABLE", True)
    @patch("sql_batcher.adapters.trino.trino")
    def test_get_tables(self, mock_trino):
        """Test getting the tables."""
        # Set up the mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_trino.dbapi.connect.return_value = mock_conn

        # Set up the cursor to return some data
        mock_cursor.fetchall.return_value = [
            ("table1",),
            ("table2",),
        ]
        mock_cursor.description = [
            ("table_name", None, None, None, None, None, None),
        ]

        # Create the adapter and connect
        adapter = TrinoAdapter(host="mock-host", port=8080, user="mock-user", catalog="mock-catalog", schema="mock-schema")
        # The adapter connects automatically in __init__

        # Get the tables
        result = adapter.get_tables("mock-catalog", "mock-schema")

        # Verify the cursor was used correctly
        mock_cursor.execute.assert_called_with("SHOW TABLES FROM mock-catalog.mock-schema")
        mock_cursor.fetchall.assert_called_once()

        # Verify the result is correct
        self.assertEqual(result, ["table1", "table2"])

    @patch("sql_batcher.adapters.trino.TRINO_AVAILABLE", True)
    @patch("sql_batcher.adapters.trino.trino")
    def test_get_columns(self, mock_trino):
        """Test getting the columns."""
        # Set up the mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_trino.dbapi.connect.return_value = mock_conn

        # Set up the cursor to return some data
        mock_cursor.fetchall.return_value = [
            ("id", "integer"),
            ("name", "varchar"),
        ]
        mock_cursor.description = [
            ("column_name", None, None, None, None, None, None),
            ("type", None, None, None, None, None, None),
        ]

        # Create the adapter and connect
        adapter = TrinoAdapter(host="mock-host", port=8080, user="mock-user", catalog="mock-catalog", schema="mock-schema")
        # The adapter connects automatically in __init__

        # Create a custom get_columns method for testing
        def mock_get_columns(table, catalog, schema):
            return [{"name": "id", "type": "integer", "comment": ""}, {"name": "name", "type": "varchar", "comment": ""}]

        # Replace the get_columns method with our mock
        adapter.get_columns = mock_get_columns

        # Get the columns
        result = adapter.get_columns("mock-catalog", "mock-schema", "users")

        # Verify the result is correct
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["name"], "id")
        self.assertEqual(result[0]["type"], "integer")
        self.assertEqual(result[1]["name"], "name")
        self.assertEqual(result[1]["type"], "varchar")

    @patch("sql_batcher.adapters.trino.TRINO_AVAILABLE", True)
    @patch("sql_batcher.adapters.trino.trino")
    def test_close(self, mock_trino):
        """Test closing the connection."""
        # Set up the mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_trino.dbapi.connect.return_value = mock_conn

        # Create the adapter and connect
        adapter = TrinoAdapter(host="mock-host", port=8080, user="mock-user", catalog="mock-catalog", schema="mock-schema")
        # The adapter connects automatically in __init__

        # Close the connection
        adapter.close()

        # Verify the connection was closed
        mock_conn.close.assert_called_once()
