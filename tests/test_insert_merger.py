"""
Tests for the InsertMerger class.
"""

import unittest

from sql_batcher.insert_merger import InsertMerger


class TestInsertMerger(unittest.TestCase):
    """Tests for the InsertMerger class."""

    def test_init(self) -> None:
        """Test InsertMerger initialization."""
        merger = InsertMerger()
        self.assertEqual(merger.max_bytes, 900_000)
        self.assertEqual(merger.table_maps, {})

        merger = InsertMerger(max_bytes=1000)
        self.assertEqual(merger.max_bytes, 1000)

    def test_add_non_insert_statement(self) -> None:
        """Test adding a non-INSERT statement."""
        merger = InsertMerger()
        statement = "SELECT * FROM test"
        result = merger.add_statement(statement)

        # Should return the statement unchanged
        self.assertEqual(result, statement)
        self.assertEqual(merger.table_maps, {})

    def test_add_insert_statement(self) -> None:
        """Test adding a single INSERT statement."""
        merger = InsertMerger()
        statement = "INSERT INTO test VALUES (1)"
        result = merger.add_statement(statement)

        # Should buffer the statement but not return anything yet
        self.assertIsNone(result)
        self.assertIn("test", merger.table_maps)
        self.assertEqual(merger.table_maps["test"]["values"], ["(1)"])

    def test_add_multiple_insert_statements_same_table(self) -> None:
        """Test adding multiple INSERT statements for the same table."""
        merger = InsertMerger()

        # Add first statement
        statement1 = "INSERT INTO test VALUES (1)"
        result1 = merger.add_statement(statement1)
        self.assertIsNone(result1)

        # Add second statement
        statement2 = "INSERT INTO test VALUES (2)"
        result2 = merger.add_statement(statement2)
        self.assertIsNone(result2)

        # Add third statement
        statement3 = "INSERT INTO test VALUES (3)"
        result3 = merger.add_statement(statement3)
        self.assertIsNone(result3)

        # Check internal state
        self.assertIn("test", merger.table_maps)
        self.assertEqual(merger.table_maps["test"]["values"], ["(1)", "(2)", "(3)"])

        # Flush and check the result
        results = merger.flush_all()
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], "INSERT INTO test VALUES (1), (2), (3)")

    def test_add_insert_statements_different_tables(self) -> None:
        """Test adding INSERT statements for different tables."""
        merger = InsertMerger()

        # Add statements for different tables
        statement1 = "INSERT INTO table1 VALUES (1)"
        statement2 = "INSERT INTO table2 VALUES (2)"

        result1 = merger.add_statement(statement1)
        result2 = merger.add_statement(statement2)

        self.assertIsNone(result1)
        self.assertIsNone(result2)

        # Check internal state
        self.assertIn("table1", merger.table_maps)
        self.assertIn("table2", merger.table_maps)
        self.assertEqual(merger.table_maps["table1"]["values"], ["(1)"])
        self.assertEqual(merger.table_maps["table2"]["values"], ["(2)"])

        # Flush and check the results
        results = merger.flush_all()
        self.assertEqual(len(results), 2)

        # Results may be in any order, so check both possibilities
        if results[0].startswith("INSERT INTO table1"):
            self.assertEqual(results[0], "INSERT INTO table1 VALUES (1)")
            self.assertEqual(results[1], "INSERT INTO table2 VALUES (2)")
        else:
            self.assertEqual(results[0], "INSERT INTO table2 VALUES (2)")
            self.assertEqual(results[1], "INSERT INTO table1 VALUES (1)")

    def test_add_insert_statements_with_columns(self) -> None:
        """Test adding INSERT statements that specify columns."""
        merger = InsertMerger()

        # Add statements with explicit columns
        statement1 = "INSERT INTO test (id, name) VALUES (1, 'Alice')"
        statement2 = "INSERT INTO test (id, name) VALUES (2, 'Bob')"

        result1 = merger.add_statement(statement1)
        result2 = merger.add_statement(statement2)

        self.assertIsNone(result1)
        self.assertIsNone(result2)

        # Check internal state
        self.assertIn("test", merger.table_maps)
        self.assertEqual(merger.table_maps["test"]["columns"], "(id, name)")
        self.assertEqual(merger.table_maps["test"]["values"], ["(1, 'Alice')", "(2, 'Bob')"])

        # Flush and check the result
        results = merger.flush_all()
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], "INSERT INTO test (id, name) VALUES (1, 'Alice'), (2, 'Bob')")

    def test_incompatible_columns(self) -> None:
        """Test handling INSERT statements with incompatible columns."""
        merger = InsertMerger()

        # Add statement with one set of columns
        statement1 = "INSERT INTO test (id, name) VALUES (1, 'Alice')"
        result1 = merger.add_statement(statement1)
        self.assertIsNone(result1)

        # Add statement with different columns for same table
        statement2 = "INSERT INTO test (id, age) VALUES (2, 30)"
        result2 = merger.add_statement(statement2)

        # Should not be merged, returned as is
        self.assertEqual(result2, statement2)

        # Check internal state - only the first statement is buffered
        self.assertIn("test", merger.table_maps)
        self.assertEqual(merger.table_maps["test"]["columns"], "(id, name)")
        self.assertEqual(merger.table_maps["test"]["values"], ["(1, 'Alice')"])

        # Flush and check the result
        results = merger.flush_all()
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], "INSERT INTO test (id, name) VALUES (1, 'Alice')")

    def test_max_bytes_limit(self) -> None:
        """Test that merging respects the max_bytes limit."""
        # Small max_bytes to trigger flush after 2 statements
        merger = InsertMerger(max_bytes=50)

        # Add first statement (should be buffered)
        statement1 = "INSERT INTO test VALUES (1)"
        result1 = merger.add_statement(statement1)
        self.assertIsNone(result1)

        # Add second statement (should be buffered)
        statement2 = "INSERT INTO test VALUES (2)"
        result2 = merger.add_statement(statement2)
        self.assertIsNone(result2)

        # Add third statement (should trigger a flush of previous statements)
        statement3 = "INSERT INTO test VALUES (3)"
        result3 = merger.add_statement(statement3)

        # Should return the merged statement for the first two
        self.assertEqual(result3, "INSERT INTO test VALUES (1), (2)")

        # Internal state should now contain only the third statement
        self.assertIn("test", merger.table_maps)
        self.assertEqual(merger.table_maps["test"]["values"], ["(3)"])

        # Flush and check the result
        results = merger.flush_all()
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0], "INSERT INTO test VALUES (3)")


if __name__ == "__main__":
    unittest.main()
