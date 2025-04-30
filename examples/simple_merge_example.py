#!/usr/bin/env python3
"""
Simple Insert Merging Example for SQL Batcher.

This example demonstrates the basic usage of SQL Batcher's insert merging
feature without requiring a database connection.
"""

from sql_batcher import SQLBatcher


def main():
    """Demonstrate insert merging with a simple example."""
    # Initialize SQLBatcher with insert merging enabled
    batcher = SQLBatcher(merge_inserts=True)

    # Define a list of SQL statements
    statements = [
        "INSERT INTO users VALUES (1, 'Alice', 30)",
        "INSERT INTO users VALUES (2, 'Bob', 25)",
        "INSERT INTO users VALUES (3, 'Charlie', 35)",
        "INSERT INTO users VALUES (4, 'David', 28)",
        # Different table
        "INSERT INTO products VALUES (101, 'Widget', 19.99)",
        "INSERT INTO products VALUES (102, 'Gadget', 29.99)",
        # Different columns - should not be merged with above
        "INSERT INTO users (id, age) VALUES (5, 40)",
        # Back to users with all columns
        "INSERT INTO users VALUES (6, 'Eve', 31)",
    ]

    # Count to keep track of how many SQL statements are executed
    executed_count = 0

    # Function to execute SQL (in a real scenario, this would send SQL to a database)
    def execute_fn(sql):
        nonlocal executed_count
        executed_count += 1
        print(f"Executed SQL ({executed_count}): {sql}")

    # Without merging (for comparison)
    print("WITHOUT MERGING:")
    no_merge_batcher = SQLBatcher(merge_inserts=False)
    executed_count = 0
    no_merge_batcher.process_statements(statements, execute_fn)
    print(f"\nTotal SQL statements executed without merging: {executed_count}\n")

    # With merging
    print("\nWITH MERGING:")
    executed_count = 0
    batcher.process_statements(statements, execute_fn)
    print(f"\nTotal SQL statements executed with merging: {executed_count}")


if __name__ == "__main__":
    main()
