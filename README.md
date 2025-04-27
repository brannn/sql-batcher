# SQL Batcher

[![Python Version](https://img.shields.io/pypi/pyversions/sql-batcher.svg)](https://pypi.org/project/sql-batcher)
[![PyPI Version](https://img.shields.io/pypi/v/sql-batcher.svg)](https://pypi.org/project/sql-batcher)
[![License](https://img.shields.io/pypi/l/sql-batcher.svg)](https://github.com/sql-batcher/sql-batcher/blob/main/LICENSE)

A Python library for batching SQL statements based on size limits, with optional INSERT statement merging to significantly reduce database calls and improve performance.

SQL Batcher helps manage large volumes of SQL statements when working with databases that have query size and memory constraints. It groups statements into appropriately sized batches and offers database-specific adapters to improve performance across different database systems.

## Features

- 🚀 **High Performance**: Optimize database operations by batching multiple SQL statements
- 🔄 **Insert Merging**: Intelligently combine INSERT statements to reduce database calls by up to 98%
- 🧩 **Modularity**: Easily swap between different database adapters (Trino, Snowflake, Spark, etc.)
- 📏 **Smart Sizing**: Automatic batch size adjustment based on column count 
- 🔍 **Transparency**: Dry run mode to inspect generated SQL without execution
- 📊 **Monitoring**: Collect and analyze batched queries
- 🔗 **Extensibility**: Create custom adapters for any database system
- 🛡️ **Type Safety**: Full type annotations for better IDE support

## Installation

```bash
pip install sql-batcher
```

## Development Setup

### Git Hooks

This repository includes git hooks to ensure code quality. To set up the hooks:

```bash
# Run the setup script
./setup-hooks.sh
```

The hooks will:
- Format code using black and isort before each commit
- Automatically add formatted files to the commit

## Basic Usage

```python
from sql_batcher import SQLBatcher

# Create statements
statements = [
    "INSERT INTO users (name) VALUES ('Alice')",
    "INSERT INTO users (name) VALUES ('Bob')",
    "INSERT INTO users (name) VALUES ('Charlie')",
    # ... more statements
]

# Define execution function
def execute_fn(sql):
    # Your database execution logic here
    cursor.execute(sql)

# Create batcher and process statements
batcher = SQLBatcher(max_bytes=900_000)
batcher.process_statements(statements, execute_fn)
```

## Insert Merging

The library can optionally merge compatible INSERT statements to reduce the number of database calls:

```python
# Enable insert merging
batcher = SQLBatcher(max_bytes=900_000, merge_inserts=True)

# Process statements with merging
batcher.process_statements(statements, execute_fn)
```

When `merge_inserts` is enabled, statements like:

```sql
INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com')
INSERT INTO users (name, email) VALUES ('Bob', 'bob@example.com')
INSERT INTO users (name, email) VALUES ('Charlie', 'charlie@example.com')
```

Will be merged into:

```sql
INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com'), ('Bob', 'bob@example.com'), ('Charlie', 'charlie@example.com')
```

This can significantly reduce the number of database calls, especially when processing large batches of inserts.

### Merging Conditions

Statements will only be merged if they:

1. Are INSERT statements with the same table name
2. Have the same column specifications
3. Don't exceed the `max_bytes` limit when merged
4. Follow the `VALUES` syntax pattern

Non-mergeable statements (like SELECT, UPDATE) will be processed normally.

## Query Tracking

You can track executed queries using the `QueryCollector`:

```python
from sql_batcher import SQLBatcher
from sql_batcher.query_collector import QueryCollector

# Create collector
collector = QueryCollector()

# Pass to SQLBatcher
batcher = SQLBatcher(max_bytes=900_000, merge_inserts=True)
batcher.process_statements(statements, execute_fn, collector)

# Get count and queries
count = collector.get_count()
queries = collector.get_all()
print(f"Executed {count} queries")
```

## Examples

See the `examples` directory for more detailed examples:

- `simple_merge_example.py`: Basic demonstration of merge functionality
- `insert_merging_example.py`: PostgreSQL example with performance comparison

## License

MIT

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.