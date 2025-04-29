"""
Benchmark script to compare the performance of async vs. sync implementations.

This script measures the performance of the AsyncSQLBatcher vs. SQLBatcher
for various operations and database adapters.
"""

import asyncio
import time
from typing import Any, Dict, List, Optional, Tuple

from sql_batcher import AsyncSQLBatcher, SQLBatcher
from sql_batcher.adapters.async_postgresql import AsyncPostgreSQLAdapter
from sql_batcher.adapters.postgresql import PostgreSQLAdapter


def generate_insert_statements(num_statements: int) -> List[str]:
    """Generate a list of INSERT statements for benchmarking."""
    statements = []
    for i in range(num_statements):
        statements.append(f"INSERT INTO benchmark_table (id, name) VALUES ({i}, 'test_{i}')")
    return statements


def generate_select_statements(num_statements: int) -> List[str]:
    """Generate a list of SELECT statements for benchmarking."""
    statements = []
    for i in range(num_statements):
        statements.append(f"SELECT * FROM benchmark_table WHERE id = {i}")
    return statements


def setup_database(connection_params: Dict[str, Any]) -> None:
    """Set up the database for benchmarking."""
    import psycopg2

    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()
    
    # Drop the table if it exists
    cursor.execute("DROP TABLE IF EXISTS benchmark_table")
    
    # Create the table
    cursor.execute(
        """
        CREATE TABLE benchmark_table (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255) NOT NULL
        )
        """
    )
    
    conn.commit()
    cursor.close()
    conn.close()


def teardown_database(connection_params: Dict[str, Any]) -> None:
    """Clean up the database after benchmarking."""
    import psycopg2

    conn = psycopg2.connect(**connection_params)
    cursor = conn.cursor()
    
    # Drop the table
    cursor.execute("DROP TABLE IF EXISTS benchmark_table")
    
    conn.commit()
    cursor.close()
    conn.close()


def benchmark_sync(
    connection_params: Dict[str, Any],
    statements: List[str],
    batch_size: int,
    merge_inserts: bool = False,
) -> float:
    """Benchmark the synchronous SQLBatcher."""
    adapter = PostgreSQLAdapter(**connection_params)
    batcher = SQLBatcher(
        adapter=adapter,
        max_bytes=batch_size,
        merge_inserts=merge_inserts,
    )
    
    start_time = time.time()
    batcher.process_statements(statements, adapter.execute)
    end_time = time.time()
    
    adapter.close()
    return end_time - start_time


async def benchmark_async(
    connection_params: Dict[str, Any],
    statements: List[str],
    batch_size: int,
    merge_inserts: bool = False,
) -> float:
    """Benchmark the asynchronous AsyncSQLBatcher."""
    dsn = f"postgresql://{connection_params['user']}:{connection_params['password']}@{connection_params['host']}:{connection_params['port']}/{connection_params['database']}"
    adapter = AsyncPostgreSQLAdapter(dsn=dsn)
    batcher = AsyncSQLBatcher(
        adapter=adapter,
        max_bytes=batch_size,
        merge_inserts=merge_inserts,
    )
    
    start_time = time.time()
    await batcher.process_statements(statements, adapter.execute)
    end_time = time.time()
    
    await adapter.close()
    return end_time - start_time


def run_benchmark(
    connection_params: Dict[str, Any],
    num_statements: int,
    batch_size: int,
    statement_type: str = "insert",
    merge_inserts: bool = False,
) -> Tuple[float, float]:
    """Run both sync and async benchmarks and return the results."""
    # Generate statements
    if statement_type == "insert":
        statements = generate_insert_statements(num_statements)
    else:
        statements = generate_select_statements(num_statements)
    
    # Run sync benchmark
    sync_time = benchmark_sync(connection_params, statements, batch_size, merge_inserts)
    
    # Run async benchmark
    async_time = asyncio.run(benchmark_async(connection_params, statements, batch_size, merge_inserts))
    
    return sync_time, async_time


def main() -> None:
    """Run the benchmark suite."""
    # Connection parameters
    connection_params = {
        "host": "localhost",
        "port": 5432,
        "user": "postgres",
        "password": "postgres",
        "database": "postgres",
    }
    
    # Set up the database
    setup_database(connection_params)
    
    try:
        print("SQL Batcher Performance Benchmark")
        print("=================================")
        print()
        
        # Benchmark parameters
        batch_sizes = [10_000, 100_000, 1_000_000]
        statement_counts = [100, 1000, 10000]
        
        # Run benchmarks for INSERT statements
        print("INSERT Statement Benchmarks")
        print("--------------------------")
        print(f"{'Count':<10} {'Batch Size':<15} {'Sync Time (s)':<15} {'Async Time (s)':<15} {'Speedup':<10}")
        print("-" * 65)
        
        for count in statement_counts:
            for batch_size in batch_sizes:
                sync_time, async_time = run_benchmark(
                    connection_params, count, batch_size, "insert", merge_inserts=False
                )
                speedup = sync_time / async_time if async_time > 0 else 0
                print(f"{count:<10} {batch_size:<15} {sync_time:<15.4f} {async_time:<15.4f} {speedup:<10.2f}x")
        
        print()
        
        # Run benchmarks for INSERT statements with merging
        print("INSERT Statement Benchmarks (with merging)")
        print("----------------------------------------")
        print(f"{'Count':<10} {'Batch Size':<15} {'Sync Time (s)':<15} {'Async Time (s)':<15} {'Speedup':<10}")
        print("-" * 65)
        
        for count in statement_counts:
            for batch_size in batch_sizes:
                sync_time, async_time = run_benchmark(
                    connection_params, count, batch_size, "insert", merge_inserts=True
                )
                speedup = sync_time / async_time if async_time > 0 else 0
                print(f"{count:<10} {batch_size:<15} {sync_time:<15.4f} {async_time:<15.4f} {speedup:<10.2f}x")
        
        print()
        
        # Run benchmarks for SELECT statements
        print("SELECT Statement Benchmarks")
        print("--------------------------")
        print(f"{'Count':<10} {'Batch Size':<15} {'Sync Time (s)':<15} {'Async Time (s)':<15} {'Speedup':<10}")
        print("-" * 65)
        
        for count in statement_counts:
            for batch_size in batch_sizes:
                sync_time, async_time = run_benchmark(
                    connection_params, count, batch_size, "select"
                )
                speedup = sync_time / async_time if async_time > 0 else 0
                print(f"{count:<10} {batch_size:<15} {sync_time:<15.4f} {async_time:<15.4f} {speedup:<10.2f}x")
    
    finally:
        # Clean up the database
        teardown_database(connection_params)


if __name__ == "__main__":
    main()
