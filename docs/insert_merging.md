# Insert Merging

SQL Batcher provides insert merging functionality to optimize database operations by combining compatible INSERT statements into a single statement.

## Why Use Insert Merging?

- **Performance**: Reduce the number of database calls by merging multiple INSERT statements
- **Network Efficiency**: Minimize network overhead by sending fewer, larger statements
- **Database Load**: Reduce the load on the database server by processing fewer statements
- **Transaction Efficiency**: Improve transaction throughput by reducing the number of operations

## Key Features

- **Automatic Detection**: Automatically detects compatible INSERT statements
- **Size Awareness**: Respects maximum query size limits when merging
- **Table Awareness**: Only merges statements for the same table
- **Column Awareness**: Only merges statements with the same column structure
- **Execution Order Preservation**: Preserves the order of execution for non-INSERT statements
- **Configuration**: Enable/disable insert merging as needed

## Basic Usage

```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters import TrinoAdapter

# Create adapter and batcher with insert merging enabled
adapter = TrinoAdapter(
    host="trino.example.com",
    port=8080,
    user="trino"
)
batcher = SQLBatcher(
    adapter=adapter,
    merge_inserts=True  # Enable insert merging
)

# Process statements with insert merging
statements = [
    "INSERT INTO users (id, name) VALUES (1, 'John')",
    "INSERT INTO users (id, name) VALUES (2, 'Jane')",
    "INSERT INTO users (id, name) VALUES (3, 'Bob')",
]

# These will be merged into a single statement:
# INSERT INTO users (id, name) VALUES (1, 'John'), (2, 'Jane'), (3, 'Bob')
batcher.process_statements(statements, adapter.execute)
```

## How It Works

Insert merging works by:

1. Analyzing each INSERT statement to extract the table name, column list, and values
2. Grouping compatible statements (same table and columns)
3. Merging the VALUES clauses of compatible statements
4. Respecting the maximum query size limit when merging
5. Executing the merged statements

### Preserving Execution Order

An important feature of insert merging is that it preserves the execution order of non-INSERT statements. Here's how it works:

1. When a non-INSERT statement is encountered, it's immediately returned as-is, preserving its position in the execution order.
2. Only INSERT statements are considered for merging, and they're only merged with other compatible INSERT statements.
3. The `process_statements` method applies insert merging first, then processes the resulting statements in order.

This ensures that any SELECT, UPDATE, DELETE, or other statements are executed in the exact order they were provided, even when insert merging is enabled.

For example, these statements:

```sql
INSERT INTO users (id, name) VALUES (1, 'John');
INSERT INTO users (id, name) VALUES (2, 'Jane');
INSERT INTO users (id, name) VALUES (3, 'Bob');
```

Would be merged into:

```sql
INSERT INTO users (id, name) VALUES (1, 'John'), (2, 'Jane'), (3, 'Bob');
```

## Configuration

Insert merging is disabled by default. To enable it, set the `merge_inserts` parameter to `True` when creating the batcher:

```python
batcher = SQLBatcher(
    adapter=adapter,
    merge_inserts=True  # Enable insert merging
)
```

## Limitations

Insert merging has some limitations:

- Only works with simple INSERT statements with VALUES clauses
- Doesn't merge INSERT statements with different tables or column structures
- Doesn't merge INSERT statements with complex expressions or subqueries
- Respects the maximum query size limit, so very large batches may still be split

## Async Support

Insert merging is also supported with AsyncSQLBatcher:

```python
from sql_batcher import AsyncSQLBatcher
from sql_batcher.adapters.async_trino import AsyncTrinoAdapter

# Create async adapter and batcher with insert merging enabled
adapter = AsyncTrinoAdapter(
    host="trino.example.com",
    port=8080,
    user="trino"
)
batcher = AsyncSQLBatcher(
    adapter=adapter,
    merge_inserts=True  # Enable insert merging
)

# Process statements with insert merging
statements = [
    "INSERT INTO users (id, name) VALUES (1, 'John')",
    "INSERT INTO users (id, name) VALUES (2, 'Jane')",
    "INSERT INTO users (id, name) VALUES (3, 'Bob')",
]

# These will be merged into a single statement
await batcher.process_statements(statements, adapter.execute)
```
