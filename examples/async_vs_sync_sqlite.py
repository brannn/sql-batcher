#!/usr/bin/env python
"""
Example demonstrating the difference between async and synchronous operations using SQLite.

This script:
1. Creates a SQLite database in memory
2. Performs the same operations using both synchronous and async approaches
3. Compares the performance of both approaches

No external database is required - this example uses SQLite in-memory database.
"""

import asyncio
import sqlite3
import time
from typing import List, Dict, Any

from sql_batcher import SQLBatcher, AsyncSQLBatcher
from sql_batcher.adapters.base import SQLAdapter
from sql_batcher.adapters.async_base import AsyncSQLAdapter


class SQLiteAdapter(SQLAdapter):
    """SQLite adapter for synchronous operations."""

    def __init__(self, db_path: str = ":memory:"):
        """Initialize the SQLite adapter."""
        self.db_path = db_path
        self.connection = sqlite3.connect(db_path, uri=":memory:" not in db_path)
        self.connection.row_factory = sqlite3.Row

    def execute(self, sql: str) -> List[Dict[str, Any]]:
        """Execute a SQL statement and return results."""
        cursor = self.connection.cursor()
        cursor.executescript(sql)
        self.connection.commit()

        # For SELECT statements, return the results
        if sql.strip().upper().startswith("SELECT"):
            return [dict(row) for row in cursor.fetchall()]
        return []

    def get_max_query_size(self) -> int:
        """Get the maximum query size in bytes."""
        return 1_000_000  # 1MB default limit

    def close(self) -> None:
        """Close the database connection."""
        if self.connection:
            self.connection.close()


class AsyncSQLiteAdapter(AsyncSQLAdapter):
    """SQLite adapter for asynchronous operations."""

    def __init__(self, db_path: str = ":memory:"):
        """Initialize the async SQLite adapter."""
        self.db_path = db_path
        # Don't create the connection here, create it in each thread
        self.connection = None

    async def connect(self) -> None:
        """Connect to the database."""
        # Connection will be created in each thread as needed
        pass

    async def disconnect(self) -> None:
        """Disconnect from the database."""
        if self.connection:
            self.connection.close()
            self.connection = None

    async def execute(self, sql: str) -> List[Dict[str, Any]]:
        """Execute a SQL statement asynchronously and return results."""
        # Create a new connection for each execution to avoid thread issues
        connection = sqlite3.connect(self.db_path, uri=":memory:" not in self.db_path)
        connection.row_factory = sqlite3.Row

        try:
            cursor = connection.cursor()
            cursor.executescript(sql)
            connection.commit()

            # For SELECT statements, return the results
            if sql.strip().upper().startswith("SELECT"):
                rows = cursor.fetchall()
                result = [dict(row) for row in rows]
                return result
            return []
        except Exception as e:
            print(f"Error executing SQL: {e}")
            return []
        finally:
            connection.close()

    async def get_max_query_size(self) -> int:
        """Get the maximum query size in bytes."""
        return 1_000_000  # 1MB default limit

    async def close(self) -> None:
        """Close the database connection."""
        # No persistent connection to close
        pass

    async def begin_transaction(self) -> None:
        """Begin a transaction."""
        await self.execute("BEGIN TRANSACTION")

    async def commit_transaction(self) -> None:
        """Commit the current transaction."""
        await self.execute("COMMIT")

    async def rollback_transaction(self) -> None:
        """Rollback the current transaction."""
        await self.execute("ROLLBACK")

    async def create_savepoint(self, name: str) -> None:
        """Create a savepoint with the given name."""
        await self.execute(f"SAVEPOINT {name}")

    async def rollback_to_savepoint(self, name: str) -> None:
        """Rollback to the savepoint with the given name."""
        await self.execute(f"ROLLBACK TO SAVEPOINT {name}")

    async def release_savepoint(self, name: str) -> None:
        """Release the savepoint with the given name."""
        await self.execute(f"RELEASE SAVEPOINT {name}")


def generate_insert_statements(count: int) -> List[str]:
    """Generate a list of INSERT statements."""
    statements = []
    for i in range(count):
        statements.append(f"INSERT INTO users (name, email) VALUES ('User {i}', 'user{i}@example.com')")
    return statements


def run_sync_example(num_statements: int) -> float:
    """Run the synchronous example and return the execution time."""
    # Create adapter and batcher
    adapter = SQLiteAdapter("file:memdb1?mode=memory&cache=shared")
    batcher = SQLBatcher(adapter=adapter, max_bytes=100_000)

    # Create the table
    adapter.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")

    # Generate statements
    statements = generate_insert_statements(num_statements)

    # Measure execution time
    start_time = time.time()
    batcher.process_statements(statements, adapter.execute)
    end_time = time.time()

    # Verify the results
    result = adapter.execute("SELECT COUNT(*) as count FROM users")
    count = result[0]['count'] if result else 0
    print(f"Synchronous: Inserted {count} records")

    # Close the connection
    adapter.close()

    return end_time - start_time


async def run_async_example(num_statements: int) -> float:
    """Run the asynchronous example and return the execution time."""
    # Create adapter and batcher
    adapter = AsyncSQLiteAdapter("file:memdb1?mode=memory&cache=shared")
    batcher = AsyncSQLBatcher(adapter=adapter, max_bytes=100_000)

    # Create the table
    await adapter.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)")

    # Generate statements
    statements = generate_insert_statements(num_statements)

    # Measure execution time
    start_time = time.time()
    await batcher.process_statements(statements, adapter.execute)
    end_time = time.time()

    # Verify the results
    result = await adapter.execute("SELECT COUNT(*) as count FROM users")
    count = result[0]['count'] if result else 0
    print(f"Asynchronous: Inserted {count} records")

    # Close the connection
    await adapter.close()

    return end_time - start_time


def main():
    """Run the example."""
    print("SQL Batcher - Async vs. Sync Example (SQLite)")
    print("============================================")
    print()

    # Number of statements to process
    num_statements = 1000
    print(f"Processing {num_statements} INSERT statements...")
    print()

    # Run synchronous example
    sync_time = run_sync_example(num_statements)
    print(f"Synchronous execution time: {sync_time:.4f} seconds")
    print()

    # Run asynchronous example
    async_time = asyncio.run(run_async_example(num_statements))
    print(f"Asynchronous execution time: {async_time:.4f} seconds")
    print()

    # Compare results
    if sync_time > async_time:
        speedup = sync_time / async_time
        print(f"Async is {speedup:.2f}x faster than sync")
    else:
        slowdown = async_time / sync_time
        print(f"Sync is {slowdown:.2f}x faster than async")

    print()
    print("Note: Since SQLite is a file-based database and doesn't have true async support,")
    print("the async example simulates async execution by running in a separate thread.")
    print("In a real-world scenario with a network database like PostgreSQL or Trino,")
    print("the async approach would typically show more significant performance benefits,")
    print("especially when dealing with network latency and concurrent operations.")


if __name__ == "__main__":
    main()
