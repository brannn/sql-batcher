#!/usr/bin/env python3
"""
Simplified PostgreSQL Insert Merging Example for SQL Batcher.

This example demonstrates how to use SQL Batcher's insert merging feature
to reduce the number of database calls when performing bulk inserts.

Requirements:
- PostgreSQL database
- psycopg2-binary package: pip install psycopg2-binary
- sql-batcher: pip install sql-batcher
"""

import os
import random
import time
from typing import Dict, List

import psycopg2

from sql_batcher import SQLBatcher


def get_connection_params() -> Dict[str, str]:
    """
    Get PostgreSQL connection parameters from environment or defaults.

    Returns:
        Dictionary of connection parameters
    """
    return {
        "host": os.environ.get("PGHOST", "localhost"),
        "port": os.environ.get("PGPORT", "5432"),
        "user": os.environ.get("PGUSER", "postgres"),
        "password": os.environ.get("PGPASSWORD", "postgres"),
        "database": os.environ.get("PGDATABASE", "postgres"),
    }


def setup_database(conn) -> None:
    """
    Set up test table for the example.

    Args:
        conn: PostgreSQL connection object
    """
    with conn.cursor() as cursor:
        # Drop table if it exists
        cursor.execute("DROP TABLE IF EXISTS example_data")

        # Create table
        cursor.execute(
            """
            CREATE TABLE example_data (
                id SERIAL PRIMARY KEY,
                name VARCHAR(100),
                value NUMERIC(10, 2),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
        )
        conn.commit()


def generate_insert_statements(count: int) -> List[str]:
    """
    Generate sample INSERT statements.

    Args:
        count: Number of statements to generate

    Returns:
        List of SQL INSERT statements
    """
    names = ["Alpha", "Beta", "Gamma", "Delta", "Epsilon", "Zeta", "Eta", "Theta"]
    statements = []

    for i in range(count):
        name = random.choice(names)
        value = round(random.uniform(10.0, 1000.0), 2)
        stmt = f"INSERT INTO example_data (name, value) VALUES ('{name}', {value})"
        statements.append(stmt)

    return statements


def main() -> None:
    """Run examples to compare with and without insert merging."""
    conn_params = get_connection_params()

    try:
        # Connect to PostgreSQL
        print(f"Connecting to PostgreSQL at {conn_params['host']}:{conn_params['port']}...")
        # Use dictionary unpacking to avoid LSP type issues
        conn = psycopg2.connect(
            host=conn_params["host"],
            port=conn_params["port"],
            user=conn_params["user"],
            password=conn_params["password"],
            database=conn_params["database"],
        )
        conn.autocommit = False

        # Set up test database
        print("Setting up test database...")
        setup_database(conn)

        # Generate sample INSERT statements
        num_statements = 50
        statements = generate_insert_statements(num_statements)
        print(f"Generated {len(statements)} INSERT statements")

        # Function to execute SQL statements
        def execute_fn(sql):
            with conn.cursor() as cursor:
                cursor.execute(sql)
            conn.commit()

        # Example without merging
        print("\nRunning without insert merging...")
        conn.rollback()
        with conn.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE example_data")
        conn.commit()

        batcher_no_merge = SQLBatcher(merge_inserts=False)
        start_time = time.time()
        count_no_merge = batcher_no_merge.process_statements(statements, execute_fn)
        duration_no_merge = time.time() - start_time

        # Count rows
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM example_data")
            row_count_no_merge = cursor.fetchone()[0]

        # Example with merging
        print("\nRunning with insert merging...")
        conn.rollback()
        with conn.cursor() as cursor:
            cursor.execute("TRUNCATE TABLE example_data")
        conn.commit()

        batcher_with_merge = SQLBatcher(merge_inserts=True)
        start_time = time.time()
        count_with_merge = batcher_with_merge.process_statements(statements, execute_fn)
        duration_with_merge = time.time() - start_time

        # Count rows
        with conn.cursor() as cursor:
            cursor.execute("SELECT COUNT(*) FROM example_data")
            row_count_with_merge = cursor.fetchone()[0]

        # Print results
        print("\nResults:")
        print("=========")
        print(f"Number of statements: {num_statements}")
        print(f"Without merging: {count_no_merge} SQL calls, {row_count_no_merge} rows, {duration_no_merge:.6f} seconds")
        print(f"With merging: {count_with_merge} SQL calls, {row_count_with_merge} rows, {duration_with_merge:.6f} seconds")

        if duration_no_merge > 0:
            speedup = (duration_no_merge - duration_with_merge) / duration_no_merge * 100
            print(f"Performance improvement: {speedup:.2f}%")

    except Exception as e:
        print(f"Error: {e}")
    finally:
        if "conn" in locals() and conn:
            conn.close()


if __name__ == "__main__":
    main()
