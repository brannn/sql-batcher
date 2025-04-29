#!/usr/bin/env python
"""
Basic SQLite example for SQL Batcher.

This script demonstrates the basic functionality of SQL Batcher using SQLite.
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
    print("SQL Batcher - Basic SQLite Example")
    print("==================================")
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
    
    # Create batcher
    batcher = SQLBatcher(
        adapter=adapter,
        max_bytes=100_000
    )
    
    # Process statements
    print("Processing statements...")
    start_time = time.time()
    batcher.process_statements(statements, adapter.execute, collector)
    end_time = time.time()
    
    # Verify the results
    result = adapter.execute("SELECT COUNT(*) as count FROM users")
    count = result[0]['count'] if result else 0
    print(f"Inserted {count} records")
    print(f"Execution time: {end_time - start_time:.4f} seconds")
    print(f"Database operations: {len(adapter.executed_statements)}")
    print()
    
    # Show query collector information
    queries = collector.get_all()
    print(f"Collected {len(queries)} query batches")
    
    if queries:
        print("\nExample query:")
        print("-" * 80)
        print(queries[0]["query"][:500] + "..." if len(queries[0]["query"]) > 500 else queries[0]["query"])
        print("-" * 80)
    
    # Close the connection
    adapter.close()
    
    print("\nNote: SQL Batcher automatically batches statements to optimize database operations.")
    print("      This example demonstrates the basic functionality using SQLite.")


if __name__ == "__main__":
    main()
