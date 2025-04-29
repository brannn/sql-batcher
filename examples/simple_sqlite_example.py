#!/usr/bin/env python
"""
Simple SQLite example demonstrating SQL Batcher functionality.

This script:
1. Creates a SQLite database in memory
2. Demonstrates basic SQL Batcher functionality
3. Shows insert merging with statistics

No external database is required - this example uses SQLite in-memory database.
"""

import sqlite3
import time
from typing import List, Dict, Any

from sql_batcher import SQLBatcher
from sql_batcher.adapters.base import SQLAdapter
from sql_batcher.query_collector import ListQueryCollector


class SQLiteAdapter(SQLAdapter):
    """SQLite adapter for SQL Batcher."""

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
            # Split statements if multiple are provided
            statements = [s.strip() for s in sql.split(";") if s.strip()]
            results = []

            for statement in statements:
                if not statement:
                    continue

                cursor.execute(statement)
                self.connection.commit()

                # For SELECT statements, return the results
                if statement.strip().upper().startswith("SELECT"):
                    rows = cursor.fetchall()
                    results.extend([dict(row) for row in rows])

            return results
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


def generate_insert_statements(count: int) -> List[str]:
    """Generate a list of INSERT statements."""
    statements = []
    for i in range(count):
        statements.append(f"INSERT INTO users (name, email) VALUES ('User {i}', 'user{i}@example.com')")
    return statements


def main():
    """Run the example."""
    print("SQL Batcher - Simple SQLite Example")
    print("===================================")
    print()

    # Create adapter and collector
    adapter = SQLiteAdapter()
    collector = ListQueryCollector()

    # Create the table
    adapter.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")

    # Generate statements
    num_statements = 100
    print(f"Generating {num_statements} INSERT statements...")
    statements = generate_insert_statements(num_statements)
    print()

    # First, process without insert merging
    print("Processing WITHOUT insert merging...")
    adapter.executed_statements = []  # Clear executed statements

    batcher = SQLBatcher(
        adapter=adapter,
        max_bytes=100_000
        # merge_inserts is False by default
    )

    start_time = time.time()
    batcher.process_statements(statements, adapter.execute, collector)
    end_time = time.time()

    time_without_merging = end_time - start_time
    ops_without_merging = len(adapter.executed_statements)

    print(f"Execution time: {time_without_merging:.4f} seconds")
    print(f"Database operations: {ops_without_merging}")
    print()

    # Verify the results
    result = adapter.execute("SELECT COUNT(*) as count FROM users")
    count = result[0]['count'] if result else 0
    print(f"Inserted {count} records")
    print()

    # Clear the table
    adapter.execute("DELETE FROM users")

    # Now, process with insert merging
    print("Processing WITH insert merging...")
    adapter.executed_statements = []  # Clear executed statements
    collector = ListQueryCollector()  # Create a new collector

    # Create a batcher with merge_inserts=True
    batcher = SQLBatcher(
        adapter=adapter,
        max_bytes=100_000,
        merge_inserts=True,
        dry_run=False
    )

    start_time = time.time()
    batcher.process_statements(statements, adapter.execute, collector)
    end_time = time.time()

    time_with_merging = end_time - start_time
    ops_with_merging = len(adapter.executed_statements)

    print(f"Execution time: {time_with_merging:.4f} seconds")
    print(f"Database operations: {ops_with_merging}")
    print()

    # Verify the results
    result = adapter.execute("SELECT COUNT(*) as count FROM users")
    count = result[0]['count'] if result else 0
    print(f"Inserted {count} records")
    print()

    # Calculate statistics
    ops_reduction = ops_without_merging - ops_with_merging
    ops_reduction_pct = (ops_reduction / ops_without_merging) * 100 if ops_without_merging > 0 else 0

    time_reduction = time_without_merging - time_with_merging
    time_reduction_pct = (time_reduction / time_without_merging) * 100 if time_without_merging > 0 else 0

    print("Statistics:")
    print(f"Database operations reduced: {ops_reduction} ({ops_reduction_pct:.2f}%)")
    print(f"Execution time reduced: {time_reduction:.4f} seconds ({time_reduction_pct:.2f}%)")
    print()

    # Show query collector information
    queries = collector.get_all()
    print(f"Collected {len(queries)} query batches")

    if queries:
        print("\nExample merged query:")
        print("-" * 80)
        print(queries[0]["query"][:500] + "..." if len(queries[0]["query"]) > 500 else queries[0]["query"])
        # Calculate the size of the query
        query_size = len(queries[0]['query'].encode('utf-8'))
        print(f"Size: {query_size} bytes")
        print("-" * 80)

    # Close the connection
    adapter.close()

    print("\nNote: Insert merging combines compatible INSERT statements into a single operation,")
    print("reducing the number of database calls and improving performance.")


if __name__ == "__main__":
    main()
