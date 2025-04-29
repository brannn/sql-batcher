# Query Collector

The `QueryCollector` class is a utility for collecting and tracking SQL queries executed by SQL Batcher. It's useful for debugging, logging, and monitoring database operations.

## Why Use Query Collector?

- **Debugging**: Capture all SQL statements for troubleshooting
- **Logging**: Record all database operations for audit trails
- **Performance Monitoring**: Track query sizes and execution patterns
- **Testing**: Verify that the expected queries are being executed

## Key Features

- **Query Collection**: Collects all SQL statements executed
- **Metadata Support**: Associates custom metadata with queries
- **Size Tracking**: Monitors the size of collected queries
- **Batch Management**: Manages batches of queries
- **Column Count Detection**: Detects column counts in INSERT statements

## Basic Usage

```python
from sql_batcher import SQLBatcher
from sql_batcher.query_collector import ListQueryCollector
from sql_batcher.adapters import TrinoAdapter

# Create adapter and batcher
adapter = TrinoAdapter(
    host="trino.example.com",
    port=8080,
    user="trino"
)

# Create a query collector
collector = ListQueryCollector()

# Create batcher
batcher = SQLBatcher(adapter=adapter)

# Process statements with query collection
statements = [
    "INSERT INTO table1 VALUES (1, 'a')",
    "INSERT INTO table1 VALUES (2, 'b')",
]

batcher.process_statements(statements, adapter.execute, collector)

# Get collected queries
queries = collector.get_all()
for query in queries:
    print(f"Query: {query['query']}")
    print(f"Size: {query['size']} bytes")
    if 'metadata' in query:
        print(f"Metadata: {query['metadata']}")
```

## Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `delimiter` | `str` | `;` | SQL statement delimiter |
| `dry_run` | `bool` | `False` | Whether to operate in dry run mode |
| `reference_column_count` | `int` | 10 | Reference column count for batch size adjustment |
| `min_adjustment_factor` | `float` | 0.5 | Minimum adjustment factor for batch size |
| `max_adjustment_factor` | `float` | 2.0 | Maximum adjustment factor for batch size |
| `auto_adjust_for_columns` | `bool` | `False` | Whether to adjust batch size based on column count |
| `merge_inserts` | `bool` | `False` | Whether to merge compatible INSERT statements |

## Methods

### `collect(query: str, metadata: Optional[Dict[str, Any]] = None) -> None`

Collect a SQL query with optional metadata.

### `clear() -> None`

Clear all collected queries.

### `get_all() -> List[Dict[str, Any]]`

Get all collected queries with their metadata.

### `get_count() -> int`

Get the count of collected queries.

### `get_batch() -> List[str]`

Get the current batch of queries.

### `get_current_size() -> int`

Get the current size of collected queries in bytes.

## Specialized Query Collectors

### `ListQueryCollector`

A specialized QueryCollector that maintains a list of queries with their metadata.

```python
from sql_batcher.query_collector import ListQueryCollector

collector = ListQueryCollector()
collector.collect("SELECT * FROM table1", metadata={"type": "select"})

# Get all queries
queries = collector.get_queries()
```

### `AsyncQueryCollector`

An async version of QueryCollector for use with AsyncSQLBatcher.

```python
from sql_batcher.async_query_collector import AsyncListQueryCollector

collector = AsyncListQueryCollector()
await collector.collect_async("SELECT * FROM table1", metadata={"type": "select"})

# Get all queries
queries = collector.get_all()
```
