"""
SQLite demonstration of SQL Batcher features.

This example demonstrates:
1. Using SQL Batcher with SQLite
2. Insert merging functionality
3. Query collection and statistics
4. Performance comparison between merged and unmerged inserts
"""

import os
import sqlite3
import time
from typing import Any, List, Tuple

from sql_batcher import SQLBatcher
from sql_batcher.adapters.base import SQLAdapter
from sql_batcher.query_collector import QueryCollector


class SQLiteAdapter(SQLAdapter):
    """SQLite adapter for SQL Batcher."""

    def __init__(self, db_path: str):
        """Initialize SQLite adapter."""
        self.db_path = db_path
        self.connection = None
        self.cursor = None

    def execute(self, sql: str) -> List[Tuple[Any, ...]]:
        """Execute SQL statement."""
        if not self.connection:
            self.connection = sqlite3.connect(self.db_path)
            self.cursor = self.connection.cursor()

        self.cursor.execute(sql)
        if self.cursor.description:
            return self.cursor.fetchall()
        return []

    def get_max_query_size(self) -> int:
        """Get maximum query size."""
        return 1_000_000  # 1MB limit for SQLite

    def close(self) -> None:
        """Close connection."""
        if self.connection:
            self.connection.close()
            self.connection = None
            self.cursor = None


def create_test_table(adapter: SQLiteAdapter) -> None:
    """Create test table."""
    adapter.execute(
        """
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            email TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )


def generate_insert_statements(count: int) -> List[str]:
    """Generate test INSERT statements."""
    statements = []
    for i in range(count):
        statements.append(
            f"INSERT INTO users (id, name, email) "
            f"VALUES ({i}, 'User {i}', 'user{i}@example.com')"
        )
    return statements


def run_demo() -> None:
    """Run the SQLite demonstration."""
    # Create database file
    db_path = "demo.db"
    if os.path.exists(db_path):
        os.remove(db_path)

    # Create adapter
    adapter = SQLiteAdapter(db_path)

    # Create test table
    create_test_table(adapter)

    # Generate test data
    statements = generate_insert_statements(1000)

    # Test 1: Without insert merging
    print("\nTest 1: Without insert merging")
    collector = QueryCollector()
    batcher = SQLBatcher(
        adapter=adapter, max_bytes=1_000_000, batch_mode=True, collector=collector
    )

    start_time = time.time()
    batcher.process_statements(statements, adapter.execute)
    end_time = time.time()

    stats = collector.get_stats()
    print(f"Total statements: {len(statements)}")
    print(f"Total queries executed: {stats['count']}")
    print(f"Average batch size: {stats['avg_batch_size']}")
    print(f"Total execution time: {end_time - start_time:.2f} seconds")

    # Clear the table
    adapter.execute("DELETE FROM users")

    # Test 2: With insert merging
    print("\nTest 2: With insert merging")
    collector = QueryCollector()  # Reset collector
    batcher = SQLBatcher(
        adapter=adapter, max_bytes=1_000_000, batch_mode=True, collector=collector
    )

    start_time = time.time()
    batcher.process_statements(statements, adapter.execute)
    end_time = time.time()

    stats = collector.get_stats()
    print(f"Total statements: {len(statements)}")
    print(f"Total queries executed: {stats['count']}")
    print(f"Average batch size: {stats['avg_batch_size']}")
    print(f"Total execution time: {end_time - start_time:.2f} seconds")

    # Print detailed query information
    print("\nQuery Details:")
    for query in collector.get_queries():
        print(f"\nQuery: {query.sql[:100]}...")  # Show first 100 chars
        print(f"Execution time: {query.execution_time:.3f} seconds")
        print(f"Batch size: {query.batch_size}")

    # Clean up
    adapter.close()
    if os.path.exists(db_path):
        os.remove(db_path)


if __name__ == "__main__":
    run_demo()
