# SQL Batcher

The `SQLBatcher` class is the core component of the library, responsible for efficiently batching SQL statements based on size limits and other constraints.

## Why Use SQL Batcher?

SQL Batcher addresses a common challenge in database programming: efficiently executing many SQL statements while respecting query size limitations. This is especially valuable for systems like Trino (our first-class query engine) that have query size or memory constraints.

## Key Features

- **Smart Batching**: Automatically groups SQL statements into optimal batches based on size limits
- **Size Limit Awareness**: Respects database-specific query size limits
- **Column-Based Adjustment**: Dynamically adjusts batch sizes based on column count
- **Memory Efficiency**: Minimizes memory usage by processing statements in batches
- **Network Optimization**: Reduces network overhead by minimizing the number of database calls

## Basic Usage

```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters import TrinoAdapter

# Create adapter and batcher
adapter = TrinoAdapter(
    host="trino.example.com",
    port=8080,
    user="trino",
    catalog="hive",
    schema="default",
    role="admin",  # Trino role (sets 'x-trino-role' HTTP header)
    max_query_size=600_000  # 600KB limit to provide buffer for Trino's 1MB limit
)

batcher = SQLBatcher(
    adapter=adapter,
    max_bytes=500_000,  # 500KB limit
    batch_mode=True,
    auto_adjust_for_columns=True  # Adjust batch size based on column count
)

# Process statements
statements = [
    "INSERT INTO table1 VALUES (1, 'a')",
    "INSERT INTO table1 VALUES (2, 'b')",
    # ... many more statements
]

# Process all statements in batches
batcher.process_statements(statements, adapter.execute)
```

## Configuration Options

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `adapter` | `SQLAdapter` | Required | Database adapter to use |
| `max_bytes` | `int` | 1,000,000 | Maximum batch size in bytes |
| `batch_mode` | `bool` | `True` | Whether to operate in batch mode |
| `merge_inserts` | `bool` | `False` | Whether to merge compatible INSERT statements |
| `auto_adjust_for_columns` | `bool` | `False` | Whether to adjust batch size based on column count |
| `reference_column_count` | `int` | 10 | Reference column count for batch size adjustment |
| `min_adjustment_factor` | `float` | 0.5 | Minimum adjustment factor for batch size |
| `max_adjustment_factor` | `float` | 2.0 | Maximum adjustment factor for batch size |
| `delimiter` | `str` | `;` | SQL statement delimiter |
| `dry_run` | `bool` | `False` | Whether to operate in dry run mode |

## Methods

### `add_statement(statement: str) -> bool`

Add a statement to the current batch. Returns `True` if the batch should be flushed.

### `flush(execute_callback: Callable[[str], Any], query_collector: Optional[QueryCollector] = None, metadata: Optional[Dict[str, Any]] = None) -> int`

Flush the current batch of statements. Returns the number of statements flushed.

### `process_statements(statements: List[str], execute_callback: Callable[[str], Any], query_collector: Optional[QueryCollector] = None, metadata: Optional[Dict[str, Any]] = None) -> int`

Process a list of SQL statements. Returns the number of statements processed.

### `process_chunk(statements: List[str], execute_func: Optional[Callable[[str], Any]] = None) -> List[Any]`

Process statements in chunks. Returns a list of results from executed statements.

## Advanced Usage

For more advanced usage, including context managers, transaction management, and savepoints, see the respective documentation sections.
