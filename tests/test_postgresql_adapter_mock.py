"""Tests for the PostgreSQL adapter using mocks."""

import unittest
from unittest.mock import MagicMock, patch

import pytest

from sql_batcher.adapters.postgresql import PostgreSQLAdapter


class TestPostgreSQLAdapterMock(unittest.TestCase):
    """Test the PostgreSQL adapter with mocks."""

    def setUp(self):
        """Set up the test."""
        # Create a mock for the psycopg2 module
        self.psycopg2_patcher = patch("sql_batcher.adapters.postgresql.psycopg2")
        self.mock_psycopg2 = self.psycopg2_patcher.start()
        
        # Create a mock connection and cursor
        self.mock_connection = MagicMock()
        self.mock_cursor = MagicMock()
        self.mock_connection.cursor.return_value = self.mock_cursor
        self.mock_psycopg2.connect.return_value = self.mock_connection
        
        # Create the adapter
        self.connection_params = {
            "host": "mock-host",
            "port": 5432,
            "user": "mock-user",
            "password": "mock-password",
            "database": "mock-database",
        }
        self.adapter = PostgreSQLAdapter(connection_params=self.connection_params)

    def tearDown(self):
        """Tear down the test."""
        self.psycopg2_patcher.stop()

    def test_init(self):
        """Test the initialization of the adapter."""
        # Verify the connection was created with the correct parameters
        self.mock_psycopg2.connect.assert_called_once_with(**self.connection_params)
        
        # Verify the max_query_size was set correctly
        self.assertEqual(self.adapter._max_query_size, 5_000_000)

    def test_init_with_connection(self):
        """Test the initialization of the adapter with a connection."""
        # Create a mock connection
        mock_connection = MagicMock()
        
        # Create a new adapter with the connection
        adapter = PostgreSQLAdapter(connection=mock_connection)
        
        # Verify the connection was used
        self.assertEqual(adapter._connection, mock_connection)
        
        # Verify connect was not called
        self.mock_psycopg2.connect.assert_called_once()  # Only from setUp

    def test_init_with_application_name(self):
        """Test the initialization of the adapter with an application name."""
        # Create a new adapter with an application name
        adapter = PostgreSQLAdapter(
            connection_params=self.connection_params,
            application_name="mock-app",
        )
        
        # Verify the connection was created with the correct parameters
        expected_params = self.connection_params.copy()
        expected_params["application_name"] = "mock-app"
        self.mock_psycopg2.connect.assert_called_with(**expected_params)

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
        self.mock_cursor.execute.assert_called_once_with("BEGIN")

    def test_commit_transaction(self):
        """Test committing a transaction."""
        # Commit a transaction
        self.adapter.commit_transaction()
        
        # Verify the connection was used correctly
        self.mock_connection.commit.assert_called_once()

    def test_rollback_transaction(self):
        """Test rolling back a transaction."""
        # Rollback a transaction
        self.adapter.rollback_transaction()
        
        # Verify the connection was used correctly
        self.mock_connection.rollback.assert_called_once()

    def test_close(self):
        """Test closing the connection."""
        # Close the connection
        self.adapter.close()
        
        # Verify the connection was closed
        self.mock_connection.close.assert_called_once()

    def test_execute_copy(self):
        """Test executing a COPY statement."""
        # Set up the mock cursor to return no data
        self.mock_cursor.fetchall.return_value = []
        
        # Execute a COPY statement
        data = "1\tAlice\n2\tBob"
        result = self.adapter.execute_copy("COPY users (id, name) FROM STDIN", data)
        
        # Verify the cursor was used correctly
        self.mock_cursor.copy_expert.assert_called_once_with("COPY users (id, name) FROM STDIN", data)
        
        # Verify the result is correct
        self.assertEqual(result, [])

    def test_execute_batch(self):
        """Test executing a batch of statements."""
        # Set up the mock cursor to return no data
        self.mock_cursor.fetchall.return_value = []
        
        # Execute a batch of statements
        statements = [
            "INSERT INTO users (id, name) VALUES (1, 'Alice')",
            "INSERT INTO users (id, name) VALUES (2, 'Bob')",
        ]
        result = self.adapter.execute_batch(statements)
        
        # Verify the cursor was used correctly
        self.mock_psycopg2.extras.execute_batch.assert_called_once_with(
            self.mock_cursor, "%s", statements
        )
        
        # Verify the result is correct
        self.assertEqual(result, [])

    def test_create_indices(self):
        """Test creating indices."""
        # Set up the mock cursor to return no data
        self.mock_cursor.fetchall.return_value = []
        
        # Create indices
        indices = [
            "CREATE INDEX idx_users_id ON users (id)",
            "CREATE INDEX idx_users_name ON users (name)",
        ]
        result = self.adapter.create_indices(indices)
        
        # Verify the cursor was used correctly
        for index in indices:
            self.mock_cursor.execute.assert_any_call(index)
        
        # Verify the result is correct
        self.assertEqual(result, [])

    def test_explain_analyze(self):
        """Test explaining and analyzing a query."""
        # Set up the mock cursor to return some data
        self.mock_cursor.fetchall.return_value = [
            {"QUERY PLAN": "Seq Scan on users"},
            {"QUERY PLAN": "Filter: (id = 1)"},
        ]
        self.mock_cursor.description = [
            ("QUERY PLAN", None, None, None, None, None, None),
        ]
        
        # Explain and analyze a query
        result = self.adapter.explain_analyze("SELECT * FROM users WHERE id = 1")
        
        # Verify the cursor was used correctly
        self.mock_cursor.execute.assert_called_once_with(
            "EXPLAIN ANALYZE SELECT * FROM users WHERE id = 1"
        )
        
        # Verify the result is correct
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["QUERY PLAN"], "Seq Scan on users")
        self.assertEqual(result[1]["QUERY PLAN"], "Filter: (id = 1)")

    def test_create_temp_table(self):
        """Test creating a temporary table."""
        # Set up the mock cursor to return no data
        self.mock_cursor.fetchall.return_value = []
        
        # Create a temporary table
        result = self.adapter.create_temp_table(
            "temp_users",
            ["id INTEGER", "name TEXT"],
        )
        
        # Verify the cursor was used correctly
        self.mock_cursor.execute.assert_called_once_with(
            "CREATE TEMPORARY TABLE temp_users (id INTEGER, name TEXT)"
        )
        
        # Verify the result is correct
        self.assertEqual(result, [])

    def test_get_server_version(self):
        """Test getting the server version."""
        # Set up the mock connection to return a version
        self.mock_connection.server_version = 120004
        
        # Get the server version
        result = self.adapter.get_server_version()
        
        # Verify the result is correct
        self.assertEqual(result, 120004)

    @patch("sql_batcher.adapters.postgresql.PSYCOPG2_AVAILABLE", False)
    def test_missing_psycopg2(self):
        """Test the behavior when the psycopg2 package is missing."""
        # Attempt to create an adapter without the psycopg2 package
        with self.assertRaises(ImportError):
            PostgreSQLAdapter(connection_params=self.connection_params)

    def test_create_savepoint(self):
        """Test creating a savepoint."""
        # Create a savepoint
        self.adapter.create_savepoint("sp1")
        
        # Verify the cursor was used correctly
        self.mock_cursor.execute.assert_called_once_with("SAVEPOINT sp1")

    def test_rollback_to_savepoint(self):
        """Test rolling back to a savepoint."""
        # Rollback to a savepoint
        self.adapter.rollback_to_savepoint("sp1")
        
        # Verify the cursor was used correctly
        self.mock_cursor.execute.assert_called_once_with("ROLLBACK TO SAVEPOINT sp1")

    def test_release_savepoint(self):
        """Test releasing a savepoint."""
        # Release a savepoint
        self.adapter.release_savepoint("sp1")
        
        # Verify the cursor was used correctly
        self.mock_cursor.execute.assert_called_once_with("RELEASE SAVEPOINT sp1")
