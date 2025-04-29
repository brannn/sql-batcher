#!/usr/bin/env python
"""
Example demonstrating the query collector functionality using SQLite.

This script:
1. Creates a SQLite database in memory
2. Processes SQL statements with a query collector
3. Shows how to use the collected query information for analysis and debugging

No external database is required - this example uses SQLite in-memory database.
"""

import sqlite3
import time
from typing import List, Dict, Any

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

    def execute(self, sql: str) -> List[Dict[str, Any]]:
        """Execute a SQL statement and return results."""
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


def generate_mixed_statements(count: int) -> List[str]:
    """
    Generate a mix of different SQL statements.

    Args:
        count: Number of statements to generate

    Returns:
        List of SQL statements
    """
    statements = []

    # Create tables
    statements.append("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, created_at TIMESTAMP)")
    statements.append("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL, status TEXT, created_at TIMESTAMP)")

    # Generate INSERT statements for users
    for i in range(count // 2):
        statements.append(f"INSERT INTO users (name, email, created_at) VALUES ('User {i}', 'user{i}@example.com', datetime('now', '-{i} hours'))")

    # Generate INSERT statements for orders
    for i in range(count // 2):
        user_id = (i % (count // 4)) + 1  # Create multiple orders per user
        amount = 10.0 + (i * 1.5)
        status = "completed" if i % 3 == 0 else "pending"
        statements.append(f"INSERT INTO orders (user_id, amount, status, created_at) VALUES ({user_id}, {amount}, '{status}', datetime('now', '-{i} minutes'))")

    # Add some SELECT statements
    statements.append("SELECT * FROM users LIMIT 5")
    statements.append("SELECT * FROM orders WHERE status = 'completed' LIMIT 5")
    statements.append("SELECT u.name, COUNT(o.id) as order_count, SUM(o.amount) as total_spent FROM users u JOIN orders o ON u.id = o.user_id GROUP BY u.id ORDER BY total_spent DESC LIMIT 10")

    # Add some UPDATE statements
    statements.append("UPDATE users SET name = 'Updated User' WHERE id = 1")
    statements.append("UPDATE orders SET status = 'shipped' WHERE status = 'pending' AND id < 10")

    return statements


def main():
    """Run the example."""
    print("SQL Batcher - Query Collector Example (SQLite)")
    print("=============================================")
    print()

    # Configuration
    num_statements = 100

    print(f"Generating {num_statements} mixed SQL statements...")
    statements = generate_mixed_statements(num_statements)
    print()

    # Create adapter and collector
    adapter = SQLiteAdapter()
    collector = ListQueryCollector()

    # Create batcher with insert merging enabled
    batcher = SQLBatcher(
        adapter=adapter,
        max_bytes=100_000,
        merge_inserts=True
    )

    # Process statements with query collection
    print("Processing statements with query collection...")
    start_time = time.time()
    batcher.process_statements(statements, adapter.execute, collector)
    end_time = time.time()
    print(f"Execution time: {end_time - start_time:.4f} seconds")
    print()

    # Analyze the collected queries
    queries = collector.get_all()
    print(f"Collected {len(queries)} query batches")

    # Calculate statistics
    total_size = sum(len(q["query"].encode("utf-8")) for q in queries)
    avg_size = total_size / len(queries) if queries else 0
    max_size = max(len(q["query"].encode("utf-8")) for q in queries) if queries else 0
    min_size = min(len(q["query"].encode("utf-8")) for q in queries) if queries else 0

    print("Query Statistics:")
    print(f"Total size: {total_size} bytes")
    print(f"Average size: {avg_size:.2f} bytes")
    print(f"Maximum size: {max_size} bytes")
    print(f"Minimum size: {min_size} bytes")
    print()

    # Categorize queries
    create_queries = []
    insert_queries = []
    select_queries = []
    update_queries = []
    other_queries = []

    for query in queries:
        sql = query["query"].strip().upper()
        if sql.startswith("CREATE"):
            create_queries.append(query)
        elif sql.startswith("INSERT"):
            insert_queries.append(query)
        elif sql.startswith("SELECT"):
            select_queries.append(query)
        elif sql.startswith("UPDATE"):
            update_queries.append(query)
        else:
            other_queries.append(query)

    print("Query Categories:")
    print(f"CREATE statements: {len(create_queries)}")
    print(f"INSERT statements: {len(insert_queries)}")
    print(f"SELECT statements: {len(select_queries)}")
    print(f"UPDATE statements: {len(update_queries)}")
    print(f"Other statements: {len(other_queries)}")
    print()

    # Show some example queries
    print("Example Queries:")

    if insert_queries:
        print("\nExample INSERT batch:")
        print("-" * 80)
        print(insert_queries[0]["query"][:500] + "..." if len(insert_queries[0]["query"]) > 500 else insert_queries[0]["query"])
        print(f"Size: {len(insert_queries[0]['query'].encode('utf-8'))} bytes")
        print("-" * 80)

    if select_queries:
        print("\nExample SELECT query:")
        print("-" * 80)
        print(select_queries[0]["query"])
        print(f"Size: {len(select_queries[0]['query'].encode('utf-8'))} bytes")
        print("-" * 80)

    # Demonstrate adding metadata to queries
    print("\nDemonstrating query metadata:")
    collector.clear()

    # Add a query with metadata
    collector.collect(
        "SELECT * FROM users WHERE id = 1",
        metadata={
            "type": "user_lookup",
            "user_id": 1,
            "timestamp": time.time(),
            "source": "example_script"
        }
    )

    # Get the query with metadata
    queries_with_metadata = collector.get_all()
    if queries_with_metadata:
        print("-" * 80)
        print(f"Query: {queries_with_metadata[0]['query']}")
        print(f"Size: {len(queries_with_metadata[0]['query'].encode('utf-8'))} bytes")
        print("Metadata:")
        for key, value in queries_with_metadata[0]["metadata"].items():
            print(f"  {key}: {value}")
        print("-" * 80)

    # Close the connection
    adapter.close()

    print("\nNote: The query collector is a powerful tool for debugging, logging, and analyzing")
    print("SQL operations. It can be used to track query sizes, monitor performance, and")
    print("associate custom metadata with queries for better traceability.")


if __name__ == "__main__":
    main()
