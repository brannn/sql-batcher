# SQL Batcher

[![Python Version](https://img.shields.io/pypi/pyversions/sql-batcher.svg)](https://pypi.org/project/sql-batcher)
[![PyPI Version](https://img.shields.io/pypi/v/sql-batcher.svg)](https://pypi.org/project/sql-batcher)
[![License](https://img.shields.io/pypi/l/sql-batcher.svg)](https://github.com/sql-batcher/sql-batcher/blob/main/LICENSE)

A Python library for managing large volumes of SQL statements by batching them according to size limits. SQL Batcher helps handle database operations that exceed query size or memory constraints by grouping statements into appropriately sized batches.

## Database Support

| Database | Query Size Limit | Dedicated Adapter | Notes |
|----------|-----------------|-------------------|-------|
| PostgreSQL | 1GB | ✅ | Default limit can be increased |
| Snowflake | 100MB | ✅ | Varies by warehouse size |
| Trino | 1GB | ✅ | Configurable via `query.max-size` |
| BigQuery | 1MB (interactive)<br>20MB (batch) | ✅ | Different limits for interactive/batch |
| Spark | 1GB | ✅ | Configurable via `spark.sql.maxQuerySize` |
| MySQL | 1GB | ❌ | Configurable via `max_allowed_packet` |
| SQLite | 1GB | ❌ | Limited by available memory |
| Oracle | 2GB | ❌ | Configurable via `MAX_STRING_SIZE` |
| SQL Server | 2GB | ❌ | Configurable via `max text repl size` |

## What SQL Batcher Does

SQL Batcher provides:
- Batching of SQL statements based on size limits
- Support for multiple database systems (PostgreSQL, Snowflake, Trino, etc.)
- Optional merging of compatible INSERT statements
- Transaction management for batched operations
- Query tracking and monitoring

## When to Use SQL Batcher

Use SQL Batcher when:
- You need to process large datasets in chunks
- Your database has query size limits
- You want to manage memory usage during large operations
- You need to work with multiple database types
- You want to track and monitor batched queries

## How It Works

1. **Query Collection**: SQL statements are collected and grouped
2. **Size Calculation**: Each statement's size is calculated
3. **Batching**: Statements are grouped into batches that fit within size limits
4. **Execution**: Batches are executed in sequence
5. **Transaction Management**: Batches can be executed within transactions
6. **Error Handling**: Failed batches can be retried or rolled back

## Installation

```bash
pip install sql-batcher
```

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

## Database Adapters

SQL Batcher supports multiple database systems through adapters:

```python
from sql_batcher.adapters import PostgreSQLAdapter, SnowflakeAdapter, TrinoAdapter

# PostgreSQL
adapter = PostgreSQLAdapter(
    host="localhost",
    port=5432,
    database="mydb",
    user="user",
    password="password"
)

# Snowflake
adapter = SnowflakeAdapter(
    account="your_account",
    user="user",
    password="password",
    database="mydb",
    schema="public"
)

# Trino
adapter = TrinoAdapter(
    host="localhost",
    port=8080,
    user="user",
    catalog="hive",
    schema="default"
)
```

## Insert Merging

SQL Batcher can merge compatible INSERT statements to reduce the number of database calls:

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

### Merging Conditions

Statements will only be merged if they:
1. Are INSERT statements with the same table name
2. Have the same column specifications
3. Don't exceed the `max_bytes` limit when merged
4. Follow the `VALUES` syntax pattern

## Query Tracking

Track executed queries using the `QueryCollector`:

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

## License

MIT

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.