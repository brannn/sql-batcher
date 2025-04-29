#!/usr/bin/env python
"""
Example demonstrating insert merging with statistics using SQLite.

This script:
1. Creates a SQLite database in memory
2. Generates a large number of INSERT statements
3. Processes them with and without insert merging
4. Shows statistics on how many database operations were reduced

No external database is required - this example uses SQLite in-memory database.
"""

import sqlite3
import time
from typing import List, Dict, Any, Tuple

from sql_batcher import SQLBatcher
from sql_batcher.adapters.base import SQLAdapter
from sql_batcher.query_collector import ListQueryCollector


class SQLiteAdapter(SQLAdapter):
    """SQLite adapter for synchronous operations."""

    def __init__(self, db_path: str = ":memory:"):
        """Initialize the SQLite adapter."""
        self.db_path = db_path
        self.connection = sqlite3.connect(db_path)
        self.connection.row_factory = sqlite3.Row
        self.executed_statements = []

    def execute(self, sql: str) -> List[Dict[str, Any]]:
        """Execute a SQL statement and return results."""
        # Store the executed SQL for analysis
        self.executed_statements.append(sql)

        cursor = self.connection.cursor()
        try:
            cursor.executescript(sql)
            self.connection.commit()

            # For SELECT statements, return the results
            if sql.strip().upper().startswith("SELECT"):
                return [dict(row) for row in cursor.fetchall()]
            return []
        except Exception as e:
            print(f"Error executing SQL: {e}")
            return []

    def get_max_query_size(self) -> int:
        """Get the maximum query size in bytes."""
        return 1_000_000  # 1MB default limit

    def close(self) -> None:
        """Close the database connection."""
        if self.connection:
            self.connection.close()


def generate_insert_statements(count: int, tables: int = 1) -> List[str]:
    """
    Generate a list of INSERT statements.

    Args:
        count: Number of statements to generate
        tables: Number of different tables to use

    Returns:
        List of INSERT statements
    """
    statements = []
    for i in range(count):
        table_idx = i % tables
        table_name = f"users{table_idx}" if tables > 1 else "users"
        statements.append(f"INSERT INTO {table_name} (name, email) VALUES ('User {i}', 'user{i}@example.com')")
    return statements


def count_insert_statements(sql: str) -> int:
    """
    Count the number of INSERT statements in a SQL string.

    Args:
        sql: SQL string to analyze

    Returns:
        Number of INSERT statements
    """
    # Count VALUES clauses in the SQL
    return sql.upper().count("VALUES")


def process_with_insert_merging(
    statements: List[str],
    merge_enabled: bool = True
) -> Tuple[float, int, int]:
    """
    Process statements with or without insert merging.

    Args:
        statements: List of SQL statements to process
        merge_enabled: Whether to enable insert merging

    Returns:
        Tuple of (execution_time, db_operations, total_inserts)
    """
    # Create adapter and batcher
    adapter = SQLiteAdapter()
    collector = ListQueryCollector()

    # Create the tables
    for i in range(10):  # Support up to 10 tables
        adapter.execute(f"CREATE TABLE IF NOT EXISTS users{i} (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")
    adapter.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")

    # Clear the executed statements list
    adapter.executed_statements = []

    # Create batcher with or without insert merging
    batcher = SQLBatcher(
        adapter=adapter,
        max_bytes=100_000,
        merge_inserts=merge_enabled
    )

    # Measure execution time
    start_time = time.time()
    batcher.process_statements(statements, adapter.execute, collector)
    end_time = time.time()

    # Count the total number of INSERT operations
    db_operations = len(adapter.executed_statements)

    # Count the total number of INSERT statements executed
    total_inserts = 0
    for sql in adapter.executed_statements:
        total_inserts += count_insert_statements(sql)

    # Close the connection
    adapter.close()

    return end_time - start_time, db_operations, total_inserts


def main():
    """Run the example."""
    print("SQL Batcher - Insert Merging Statistics Example (SQLite)")
    print("======================================================")
    print()

    # Configuration
    num_statements = 1000
    num_tables = 5

    print(f"Generating {num_statements} INSERT statements across {num_tables} tables...")
    statements = generate_insert_statements(num_statements, num_tables)
    print()

    # Process without insert merging
    print("Processing WITHOUT insert merging...")
    time_without_merging, ops_without_merging, inserts_without_merging = process_with_insert_merging(
        statements, merge_enabled=False
    )
    print(f"Execution time: {time_without_merging:.4f} seconds")
    print(f"Database operations: {ops_without_merging}")
    print(f"Total INSERT statements: {inserts_without_merging}")
    print()

    # Process with insert merging
    print("Processing WITH insert merging...")
    time_with_merging, ops_with_merging, inserts_with_merging = process_with_insert_merging(
        statements, merge_enabled=True
    )
    print(f"Execution time: {time_with_merging:.4f} seconds")
    print(f"Database operations: {ops_with_merging}")
    print(f"Total INSERT statements: {inserts_with_merging}")
    print()

    # Calculate statistics
    ops_reduction = ops_without_merging - ops_with_merging
    ops_reduction_pct = (ops_reduction / ops_without_merging) * 100

    time_reduction = time_without_merging - time_with_merging
    time_reduction_pct = (time_reduction / time_without_merging) * 100

    print("Statistics:")
    print(f"Database operations reduced: {ops_reduction} ({ops_reduction_pct:.2f}%)")
    print(f"Execution time reduced: {time_reduction:.4f} seconds ({time_reduction_pct:.2f}%)")
    print()

    print("Insert Merging Analysis:")
    print(f"Original INSERT statements: {num_statements}")
    print(f"Merged INSERT operations: {ops_with_merging}")
    print(f"Average INSERTs per operation: {inserts_with_merging / ops_with_merging:.2f}")
    print()

    print("Note: Insert merging combines compatible INSERT statements into a single operation,")
    print("reducing the number of database calls and improving performance.")
    print("The effectiveness depends on the similarity of the INSERT statements and the")
    print("maximum batch size. In this example, statements for the same table with the")
    print("same column structure were merged together.")


if __name__ == "__main__":
    main()
