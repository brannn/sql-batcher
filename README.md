# SQL Batcher

[![Python Version](https://img.shields.io/pypi/pyversions/sql-batcher.svg)](https://pypi.org/project/sql-batcher)
[![PyPI Version](https://img.shields.io/pypi/v/sql-batcher.svg)](https://pypi.org/project/sql-batcher)
[![License](https://img.shields.io/pypi/l/sql-batcher.svg)](https://github.com/sql-batcher/sql-batcher/blob/main/LICENSE)

SQL Batcher is a powerful Python library designed to optimize large-scale SQL operations through intelligent batching, statement merging, and database-specific optimizations. It's particularly valuable for data engineers and application developers working with large datasets, complex data pipelines, or multiple database systems.

## Why SQL Batcher?

When working with large datasets, you often encounter challenges like:
- Database query size limits
- Memory constraints
- Network overhead
- Transaction management
- Performance optimization

SQL Batcher addresses these challenges by providing:
1. Intelligent statement batching
2. Automatic INSERT statement merging
3. Database-specific optimizations
4. Comprehensive transaction management
5. Detailed performance monitoring

## Key Features

### 1. Smart Batching
SQL Batcher intelligently groups SQL statements into optimal batches based on:
- Database-specific size limits
- Column count adjustments
- Memory constraints
- Network considerations

Example:
```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters import PostgreSQLAdapter

# Create adapter and batcher
adapter = PostgreSQLAdapter(connection_params)
batcher = SQLBatcher(
    adapter=adapter,
    max_bytes=1_000_000,  # 1MB limit
    batch_mode=True,
    auto_adjust_for_columns=True  # Adjust batch size based on column count
)

# Process large dataset
batcher.process_statements(statements, adapter.execute)
```

### 2. Insert Merging
The library can combine compatible INSERT statements into single statements, significantly reducing database calls:

```python
# Without merging (1000 separate calls):
for i in range(1000):
    cursor.execute(f"INSERT INTO users VALUES ({i}, 'User {i}')")

# With SQL Batcher (1 call):
batcher.process_statements(statements, adapter.execute)
```

### 3. Column-Aware Batching
SQL Batcher automatically adjusts batch sizes based on the number of columns in your statements:

```python
batcher = SQLBatcher(
    adapter=adapter,
    max_bytes=1_000_000,
    auto_adjust_for_columns=True,
    reference_column_count=10  # Base column count for adjustment
)
```

### 4. Query Collection and Monitoring
Track and analyze your SQL operations with detailed metrics:

```python
from sql_batcher.query_collector import QueryCollector

collector = QueryCollector()
batcher.process_statements(statements, adapter.execute, collector)

# Get detailed statistics
stats = collector.get_stats()
print(f"Total queries: {stats['count']}")
print(f"Average batch size: {stats['avg_batch_size']}")
print(f"Total execution time: {stats['total_time']}")
```

### 5. Database Adapters
SQL Batcher provides optimized adapters for popular databases:

#### PostgreSQL
```python
from sql_batcher.adapters import PostgreSQLAdapter

adapter = PostgreSQLAdapter(
    host="localhost",
    port=5432,
    database="mydb",
    user="user",
    password="password"
)
```

#### Snowflake
```python
from sql_batcher.adapters import SnowflakeAdapter

adapter = SnowflakeAdapter(
    account="your_account",
    user="your_user",
    password="your_password",
    warehouse="your_warehouse",
    database="your_database"
)
```

#### BigQuery
```python
from sql_batcher.adapters import BigQueryAdapter

adapter = BigQueryAdapter(
    project_id="your_project",
    credentials_path="path/to/credentials.json"
)
```

#### Generic Adapter
For any database that supports SQL:
```python
from sql_batcher.adapters import GenericAdapter

adapter = GenericAdapter(
    connection=your_connection,
    max_query_size=1_000_000
)
```

## Real-World Use Cases

### 1. Data Pipeline Optimization
```python
# ETL pipeline with optimized batching
def process_large_dataset(source_data):
    batcher = SQLBatcher(
        adapter=target_adapter,
        max_bytes=1_000_000,
        batch_mode=True
    )
    
    # Process in chunks to manage memory
    for chunk in source_data.chunks(10000):
        statements = generate_insert_statements(chunk)
        batcher.process_statements(statements, target_adapter.execute)
```

### 2. Multi-Database Operations
```python
# Migrate data between different databases
def migrate_data(source_adapter, target_adapter):
    batcher = SQLBatcher(
        adapter=target_adapter,
        max_bytes=1_000_000
    )
    
    # Read from source, write to target
    for batch in source_adapter.read_in_batches():
        statements = convert_to_insert_statements(batch)
        batcher.process_statements(statements, target_adapter.execute)
```

### 3. Real-time Data Processing
```python
# Process streaming data with batching
def process_stream(stream_data):
    batcher = SQLBatcher(
        adapter=adapter,
        max_bytes=100_000,  # Smaller batches for real-time
        batch_mode=True
    )
    
    collector = QueryCollector()
    
    for data in stream_data:
        statements = generate_statements(data)
        batcher.process_statements(statements, adapter.execute, collector)
        
        # Monitor performance
        if collector.get_stats()['avg_batch_size'] > 1000:
            adjust_batch_size()
```

## Installation

```bash
pip install sql-batcher
```

For development installation:
```bash
pip install -e ".[dev]"
```

## Contributing

We welcome contributions! Please see our [Contributing Guide](CONTRIBUTING.md) for details.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.