"""Tests for the Trino adapter using mocks."""

import unittest
from unittest.mock import MagicMock, patch

import pytest

from sql_batcher.adapters.trino import TrinoAdapter


class TestTrinoAdapterMock(unittest.TestCase):
    """Test the Trino adapter with mocks."""

    def setUp(self):
        """Set up the test."""
        # Create a mock for the trino module
        self.trino_patcher = patch("sql_batcher.adapters.trino.trino")
        self.mock_trino = self.trino_patcher.start()
        
        # Create a mock connection and cursor
        self.mock_connection = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_connection.cursor.return_value = self.mock_cursor
        self.mock_trino.dbapi.connect.return_value = self.mock_connection
        
        # Create the adapter
        self.adapter = TrinoAdapter(
            host="mock-host",
            port=8080,
            user="mock-user",
            catalog="mock-catalog",
            schema="mock-schema",
        )

    def tearDown(self):
        """Tear down the test."""
        self.trino_patcher.stop()

    def test_init(self):
        """Test the initialization of the adapter."""
        # Verify the connection was created with the correct parameters
        self.mock_trino.dbapi.connect.assert_called_once_with(
            host="mock-host",
            port=8080,
            user="mock-user",
            catalog="mock-catalog",
            schema="mock-schema",
            http_headers=None,
        )
        
        # Verify the max_query_size was set correctly
        self.assertEqual(self.adapter._max_query_size, 5_000_000)

    def test_init_with_role(self):
        """Test the initialization of the adapter with a role."""
        # Create a new adapter with a role
        adapter = TrinoAdapter(
            host="mock-host",
            port=8080,
            user="mock-user",
            catalog="mock-catalog",
            schema="mock-schema",
            role="mock-role",
        )
        
        # Verify the connection was created with the correct parameters
        self.mock_trino.dbapi.connect.assert_called_with(
            host="mock-host",
            port=8080,
            user="mock-user",
            catalog="mock-catalog",
            schema="mock-schema",
            http_headers={"X-Trino-Role": "system=ROLE{mock-role}"},
        )

    def test_execute_select(self):
        """Test executing a SELECT statement."""
        # Set up the mock cursor to return some data
        self.mock_cursor.fetchall.return_value = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        self.mock_cursor.description = [
            ("id", None, None, None, None, None, None),
            ("name", None, None, None, None, None, None),
        ]
        
        # Execute a SELECT statement
        result = self.adapter.execute("SELECT id, name FROM users")
        
        # Verify the cursor was used correctly
        self.mock_cursor.execute.assert_called_once_with("SELECT id, name FROM users")
        self.mock_cursor.fetchall.assert_called_once()
        
        # Verify the result is correct
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["id"], 1)
        self.assertEqual(result[0]["name"], "Alice")
        self.assertEqual(result[1]["id"], 2)
        self.assertEqual(result[1]["name"], "Bob")

    def test_execute_insert(self):
        """Test executing an INSERT statement."""
        # Set up the mock cursor to return no data
        self.mock_cursor.fetchall.return_value = []
        
        # Execute an INSERT statement
        result = self.adapter.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        
        # Verify the cursor was used correctly
        self.mock_cursor.execute.assert_called_once_with("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        
        # Verify the result is correct
        self.assertEqual(result, [])

    def test_execute_multiple_statements(self):
        """Test executing multiple statements."""
        # Set up the mock cursor to return no data
        self.mock_cursor.fetchall.return_value = []
        
        # Execute multiple statements
        statements = [
            "INSERT INTO users (id, name) VALUES (1, 'Alice')",
            "INSERT INTO users (id, name) VALUES (2, 'Bob')",
        ]
        sql = ";\n".join(statements) + ";"
        result = self.adapter.execute(sql)
        
        # Verify the cursor was used correctly
        self.mock_cursor.execute.assert_called_once_with(sql)
        
        # Verify the result is correct
        self.assertEqual(result, [])

    def test_begin_transaction(self):
        """Test beginning a transaction."""
        # Begin a transaction
        self.adapter.begin_transaction()
        
        # Verify the cursor was used correctly
        self.mock_cursor.execute.assert_called_once_with("START TRANSACTION")

    def test_commit_transaction(self):
        """Test committing a transaction."""
        # Commit a transaction
        self.adapter.commit_transaction()
        
        # Verify the cursor was used correctly
        self.mock_cursor.execute.assert_called_once_with("COMMIT")

    def test_rollback_transaction(self):
        """Test rolling back a transaction."""
        # Rollback a transaction
        self.adapter.rollback_transaction()
        
        # Verify the cursor was used correctly
        self.mock_cursor.execute.assert_called_once_with("ROLLBACK")

    def test_close(self):
        """Test closing the connection."""
        # Close the connection
        self.adapter.close()
        
        # Verify the connection was closed
        self.mock_connection.close.assert_called_once()

    def test_get_catalogs(self):
        """Test getting the catalogs."""
        # Set up the mock cursor to return some data
        self.mock_cursor.fetchall.return_value = [
            {"catalog_name": "catalog1"},
            {"catalog_name": "catalog2"},
        ]
        self.mock_cursor.description = [
            ("catalog_name", None, None, None, None, None, None),
        ]
        
        # Get the catalogs
        result = self.adapter.get_catalogs()
        
        # Verify the cursor was used correctly
        self.mock_cursor.execute.assert_called_once_with("SHOW CATALOGS")
        
        # Verify the result is correct
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["catalog_name"], "catalog1")
        self.assertEqual(result[1]["catalog_name"], "catalog2")

    def test_get_schemas(self):
        """Test getting the schemas."""
        # Set up the mock cursor to return some data
        self.mock_cursor.fetchall.return_value = [
            {"schema_name": "schema1"},
            {"schema_name": "schema2"},
        ]
        self.mock_cursor.description = [
            ("schema_name", None, None, None, None, None, None),
        ]
        
        # Get the schemas
        result = self.adapter.get_schemas()
        
        # Verify the cursor was used correctly
        self.mock_cursor.execute.assert_called_once_with("SHOW SCHEMAS")
        
        # Verify the result is correct
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["schema_name"], "schema1")
        self.assertEqual(result[1]["schema_name"], "schema2")

    def test_get_tables(self):
        """Test getting the tables."""
        # Set up the mock cursor to return some data
        self.mock_cursor.fetchall.return_value = [
            {"table_name": "table1"},
            {"table_name": "table2"},
        ]
        self.mock_cursor.description = [
            ("table_name", None, None, None, None, None, None),
        ]
        
        # Get the tables
        result = self.adapter.get_tables()
        
        # Verify the cursor was used correctly
        self.mock_cursor.execute.assert_called_once_with("SHOW TABLES")
        
        # Verify the result is correct
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["table_name"], "table1")
        self.assertEqual(result[1]["table_name"], "table2")

    def test_get_columns(self):
        """Test getting the columns."""
        # Set up the mock cursor to return some data
        self.mock_cursor.fetchall.return_value = [
            {"column_name": "id", "type": "integer"},
            {"column_name": "name", "type": "varchar"},
        ]
        self.mock_cursor.description = [
            ("column_name", None, None, None, None, None, None),
            ("type", None, None, None, None, None, None),
        ]
        
        # Get the columns
        result = self.adapter.get_columns("users")
        
        # Verify the cursor was used correctly
        self.mock_cursor.execute.assert_called_once_with("SHOW COLUMNS FROM users")
        
        # Verify the result is correct
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["column_name"], "id")
        self.assertEqual(result[0]["type"], "integer")
        self.assertEqual(result[1]["column_name"], "name")
        self.assertEqual(result[1]["type"], "varchar")

    def test_set_session_property(self):
        """Test setting a session property."""
        # Set a session property
        self.adapter.set_session_property("query_max_stage_count", "100")
        
        # Verify the cursor was used correctly
        self.mock_cursor.execute.assert_called_once_with("SET SESSION query_max_stage_count = '100'")

    def test_execute_with_http_headers(self):
        """Test executing with HTTP headers."""
        # Create a new adapter with HTTP headers
        adapter = TrinoAdapter(
            host="mock-host",
            port=8080,
            user="mock-user",
            catalog="mock-catalog",
            schema="mock-schema",
            http_headers={"X-Custom-Header": "value"},
        )
        
        # Verify the connection was created with the correct parameters
        self.mock_trino.dbapi.connect.assert_called_with(
            host="mock-host",
            port=8080,
            user="mock-user",
            catalog="mock-catalog",
            schema="mock-schema",
            http_headers={"X-Custom-Header": "value"},
        )

    @patch("sql_batcher.adapters.trino.TRINO_AVAILABLE", False)
    def test_missing_trino_package(self):
        """Test the behavior when the trino package is missing."""
        # Attempt to create an adapter without the trino package
        with self.assertRaises(ImportError):
            TrinoAdapter(
                host="mock-host",
                port=8080,
                user="mock-user",
                catalog="mock-catalog",
                schema="mock-schema",
            )
