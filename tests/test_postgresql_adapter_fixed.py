"""Tests for the PostgreSQLAdapter class with proper mocking."""

import unittest
from unittest.mock import MagicMock, patch

import pytest

from sql_batcher.adapters.postgresql import PostgreSQLAdapter


class TestPostgreSQLAdapter(unittest.TestCase):
    """Test the PostgreSQLAdapter with proper mocking."""

    @patch("sql_batcher.adapters.postgresql.psycopg2")
    def test_init(self, mock_psycopg2):
        """Test the initialization of the adapter."""
        # Set up the mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_psycopg2.connect.return_value = mock_conn

        # Create the adapter
        adapter = PostgreSQLAdapter(
            host="mock-host",
            port=5432,
            user="mock-user",
            password="mock-password",
            database="mock-database"
        )
        
        # Verify the adapter was initialized correctly
        self.assertEqual(adapter._host, "mock-host")
        self.assertEqual(adapter._port, 5432)
        self.assertEqual(adapter._user, "mock-user")
        self.assertEqual(adapter._password, "mock-password")
        self.assertEqual(adapter._database, "mock-database")
        self.assertEqual(adapter._max_query_size, 5_000_000)
        
        # Connect to the database
        adapter.connect()
        
        # Verify the connection was created with the correct parameters
        mock_psycopg2.connect.assert_called_once_with(
            host="mock-host",
            port=5432,
            user="mock-user",
            password="mock-password",
            database="mock-database"
        )

    @patch("sql_batcher.adapters.postgresql.PSYCOPG2_AVAILABLE", False)
    def test_missing_psycopg2(self):
        """Test the behavior when the psycopg2 package is missing."""
        # Attempt to create an adapter without the psycopg2 package
        with self.assertRaises(ImportError):
            PostgreSQLAdapter(
                host="mock-host",
                port=5432,
                user="mock-user",
                password="mock-password",
                database="mock-database"
            )

    @patch("sql_batcher.adapters.postgresql.psycopg2")
    def test_execute_select(self, mock_psycopg2):
        """Test executing a SELECT statement."""
        # Set up the mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_psycopg2.connect.return_value = mock_conn
        
        # Set up the cursor to return some data
        mock_cursor.fetchall.return_value = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        mock_cursor.description = [
            ("id", None, None, None, None, None, None),
            ("name", None, None, None, None, None, None),
        ]
        
        # Create the adapter and connect
        adapter = PostgreSQLAdapter(
            host="mock-host",
            port=5432,
            user="mock-user",
            password="mock-password",
            database="mock-database"
        )
        adapter.connect()
        
        # Execute a SELECT statement
        result = adapter.execute("SELECT id, name FROM users")
        
        # Verify the cursor was used correctly
        mock_cursor.execute.assert_called_once_with("SELECT id, name FROM users")
        mock_cursor.fetchall.assert_called_once()
        
        # Verify the result is correct
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["id"], 1)
        self.assertEqual(result[0]["name"], "Alice")
        self.assertEqual(result[1]["id"], 2)
        self.assertEqual(result[1]["name"], "Bob")

    @patch("sql_batcher.adapters.postgresql.psycopg2")
    def test_execute_insert(self, mock_psycopg2):
        """Test executing an INSERT statement."""
        # Set up the mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_psycopg2.connect.return_value = mock_conn
        
        # Create the adapter and connect
        adapter = PostgreSQLAdapter(
            host="mock-host",
            port=5432,
            user="mock-user",
            password="mock-password",
            database="mock-database"
        )
        adapter.connect()
        
        # Execute an INSERT statement
        result = adapter.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        
        # Verify the cursor was used correctly
        mock_cursor.execute.assert_called_once_with("INSERT INTO users (id, name) VALUES (1, 'Alice')")
        
        # Verify the result is correct (empty list for non-SELECT statements)
        self.assertEqual(result, [])

    @patch("sql_batcher.adapters.postgresql.psycopg2")
    def test_execute_batch(self, mock_psycopg2):
        """Test executing a batch of statements."""
        # Set up the mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_psycopg2.connect.return_value = mock_conn
        
        # Create the adapter and connect
        adapter = PostgreSQLAdapter(
            host="mock-host",
            port=5432,
            user="mock-user",
            password="mock-password",
            database="mock-database"
        )
        adapter.connect()
        
        # Execute a batch of statements
        batch_sql = """
        INSERT INTO users (id, name) VALUES (1, 'Alice');
        INSERT INTO users (id, name) VALUES (2, 'Bob');
        """
        result = adapter.execute(batch_sql)
        
        # Verify the cursor was used correctly
        mock_cursor.execute.assert_called_once_with(batch_sql)
        
        # Verify the result is correct (empty list for non-SELECT statements)
        self.assertEqual(result, [])

    @patch("sql_batcher.adapters.postgresql.psycopg2")
    def test_transaction_methods(self, mock_psycopg2):
        """Test transaction-related methods."""
        # Set up the mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_psycopg2.connect.return_value = mock_conn
        
        # Create the adapter and connect
        adapter = PostgreSQLAdapter(
            host="mock-host",
            port=5432,
            user="mock-user",
            password="mock-password",
            database="mock-database"
        )
        adapter.connect()
        
        # Test begin_transaction
        adapter.begin_transaction()
        mock_cursor.execute.assert_called_with("BEGIN")
        
        # Test commit_transaction
        mock_cursor.reset_mock()
        adapter.commit_transaction()
        mock_cursor.execute.assert_called_with("COMMIT")
        
        # Test rollback_transaction
        mock_cursor.reset_mock()
        adapter.rollback_transaction()
        mock_cursor.execute.assert_called_with("ROLLBACK")
        
        # Test create_savepoint
        mock_cursor.reset_mock()
        adapter.create_savepoint("sp1")
        mock_cursor.execute.assert_called_with("SAVEPOINT sp1")
        
        # Test rollback_to_savepoint
        mock_cursor.reset_mock()
        adapter.rollback_to_savepoint("sp1")
        mock_cursor.execute.assert_called_with("ROLLBACK TO SAVEPOINT sp1")
        
        # Test release_savepoint
        mock_cursor.reset_mock()
        adapter.release_savepoint("sp1")
        mock_cursor.execute.assert_called_with("RELEASE SAVEPOINT sp1")

    @patch("sql_batcher.adapters.postgresql.psycopg2")
    def test_close(self, mock_psycopg2):
        """Test closing the connection."""
        # Set up the mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_psycopg2.connect.return_value = mock_conn
        
        # Create the adapter and connect
        adapter = PostgreSQLAdapter(
            host="mock-host",
            port=5432,
            user="mock-user",
            password="mock-password",
            database="mock-database"
        )
        adapter.connect()
        
        # Close the connection
        adapter.close()
        
        # Verify the connection was closed
        mock_conn.close.assert_called_once()

    @patch("sql_batcher.adapters.postgresql.psycopg2")
    def test_get_server_version(self, mock_psycopg2):
        """Test getting the server version."""
        # Set up the mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_psycopg2.connect.return_value = mock_conn
        
        # Set up the cursor to return a version
        mock_cursor.fetchone.return_value = (120004,)
        
        # Create the adapter and connect
        adapter = PostgreSQLAdapter(
            host="mock-host",
            port=5432,
            user="mock-user",
            password="mock-password",
            database="mock-database"
        )
        adapter.connect()
        
        # Get the server version
        version = adapter.get_server_version()
        
        # Verify the cursor was used correctly
        mock_cursor.execute.assert_called_with("SHOW server_version_num")
        mock_cursor.fetchone.assert_called_once()
        
        # Verify the version is correct
        self.assertEqual(version, (12, 0, 4))

    @patch("sql_batcher.adapters.postgresql.psycopg2")
    def test_create_temp_table(self, mock_psycopg2):
        """Test creating a temporary table."""
        # Set up the mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_psycopg2.connect.return_value = mock_conn
        
        # Create the adapter and connect
        adapter = PostgreSQLAdapter(
            host="mock-host",
            port=5432,
            user="mock-user",
            password="mock-password",
            database="mock-database"
        )
        adapter.connect()
        
        # Create a temporary table
        adapter.create_temp_table("temp_users", "id INTEGER, name TEXT")
        
        # Verify the cursor was used correctly
        mock_cursor.execute.assert_called_with("CREATE TEMPORARY TABLE temp_users (id INTEGER, name TEXT)")

    @patch("sql_batcher.adapters.postgresql.psycopg2")
    def test_create_indices(self, mock_psycopg2):
        """Test creating indices."""
        # Set up the mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_psycopg2.connect.return_value = mock_conn
        
        # Create the adapter and connect
        adapter = PostgreSQLAdapter(
            host="mock-host",
            port=5432,
            user="mock-user",
            password="mock-password",
            database="mock-database"
        )
        adapter.connect()
        
        # Create indices
        indices = [
            {
                "name": "idx_users_id",
                "columns": ["id"],
                "type": "btree",
                "unique": True
            },
            {
                "name": "idx_users_name",
                "columns": "name",
                "type": "hash"
            }
        ]
        statements = adapter.create_indices("users", indices)
        
        # Verify the statements are correct
        self.assertEqual(len(statements), 2)
        self.assertIn("CREATE UNIQUE INDEX idx_users_id ON users USING btree (id)", statements)
        self.assertIn("CREATE INDEX idx_users_name ON users USING hash (name)", statements)
        
        # Verify the cursor was used correctly
        self.assertEqual(mock_cursor.execute.call_count, 2)

    @patch("sql_batcher.adapters.postgresql.psycopg2")
    def test_use_copy_for_bulk_insert(self, mock_psycopg2):
        """Test using COPY for bulk insert."""
        # Set up the mock connection and cursor
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_psycopg2.connect.return_value = mock_conn
        
        # Create the adapter and connect
        adapter = PostgreSQLAdapter(
            host="mock-host",
            port=5432,
            user="mock-user",
            password="mock-password",
            database="mock-database"
        )
        adapter.connect()
        
        # Use COPY for bulk insert
        data = [
            (1, "Alice"),
            (2, "Bob")
        ]
        count = adapter.use_copy_for_bulk_insert("users", ["id", "name"], data)
        
        # Verify the cursor was used correctly
        mock_cursor.copy_from.assert_called_once()
        
        # Verify the count is correct
        self.assertEqual(count, 2)
