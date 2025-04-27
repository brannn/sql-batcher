# SQL Batcher

[![Python Version](https://img.shields.io/pypi/pyversions/sql-batcher.svg)](https://pypi.org/project/sql-batcher)
[![PyPI Version](https://img.shields.io/pypi/v/sql-batcher.svg)](https://pypi.org/project/sql-batcher)
[![License](https://img.shields.io/pypi/l/sql-batcher.svg)](https://github.com/sql-batcher/sql-batcher/blob/main/LICENSE)

SQL Batcher is a Python library that helps developers manage large SQL operations by intelligently batching statements based on database-specific size limits. It combines compatible INSERT statements to reduce database calls and provides dedicated adapters for popular databases like PostgreSQL, Snowflake, and BigQuery. The library is particularly useful when you need to process large datasets in chunks, handle database-specific query size limits, or work with multiple database types. Whether you're building data pipelines, migrating data between systems, or processing large-scale analytics, SQL Batcher helps ensure reliable and efficient database operations.

## Table of Contents
- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Core Concepts](#core-concepts)
- [Database Adapters](#database-adapters)
- [Basic Usage](#basic-usage)
- [Advanced Features](#advanced-features)
- [Examples](#examples)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)

## Features

SQL Batcher provides several key features to help manage large SQL operations:

### Query Batching
SQL Batcher automatically groups SQL statements into batches based on size limits, preventing database query size constraints from being exceeded. It manages memory usage during large operations and supports different batching strategies for different database systems. The batching process is transparent and configurable, allowing you to optimize for your specific use case.

### Insert Merging
The library can combine compatible INSERT statements into single statements, reducing the number of database calls while maintaining transaction integrity. This feature is particularly useful when inserting large datasets, as it can significantly reduce the overhead of multiple database round trips. The merging process is configurable and respects database-specific limitations.

### Database Adapters
SQL Batcher includes dedicated adapters for popular database systems, providing a consistent interface across different databases. Each adapter includes database-specific optimizations and follows best practices for that particular system. The adapter system is extensible, allowing you to create custom adapters for any database that supports SQL.

### Transaction Management
The library provides comprehensive transaction management, including automatic transaction handling, configurable commit strategies, and error recovery with rollback support. It supports nested transactions and ensures data consistency across batch operations. This makes it suitable for complex operations that require atomicity.

### Query Collector
The Query Collector is a powerful tool for monitoring and analyzing SQL operations. It tracks detailed information about each executed query, including:

- Query text and parameters
- Execution time and batch size
- Success/failure status
- Transaction boundaries
- Memory usage

The collector provides both real-time monitoring and post-execution analysis capabilities. You can use it to:
- Monitor query execution in real-time
- Generate performance reports
- Debug issues with specific queries
- Optimize batch sizes and transaction boundaries
- Track memory usage and identify potential bottlenecks

Example usage:
```python
from sql_batcher.query_collector import QueryCollector

# Create a collector
collector = QueryCollector()

# Process statements with tracking
batcher.process_statements(statements, adapter.execute, collector)

# Get detailed statistics
stats = collector.get_stats()
print(f"Total queries: {stats['count']}")
print(f"Average batch size: {stats['avg_batch_size']}")
print(f"Total execution time: {stats['total_time']}")
print(f"Memory usage: {stats['memory_usage']}")

# Get individual query details
for query in collector.get_queries():
    print(f"Query: {query.sql}")
    print(f"Execution time: {query.execution_time}")
    print(f"Batch size: {query.batch_size}")
```

The Query Collector is particularly useful when:
- Debugging performance issues
- Optimizing batch sizes
- Monitoring long-running operations
- Generating execution reports
- Identifying problematic queries

## Installation

```bash
pip install sql-batcher
```

For development installation:
```bash
pip install -e ".[dev]"
```

## Quick Start

```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters import PostgreSQLAdapter
from sql_batcher.query_collector import QueryCollector
import time

# Create a PostgreSQL adapter
adapter = PostgreSQLAdapter(
    host="localhost",
    port=5432,
    database="mydb",
    user="user",
    password="password"
)

# Generate a large number of INSERT statements
statements = []
for i in range(10000):
    statements.append(
        f"INSERT INTO users (id, name, email, created_at) "
        f"VALUES ({i}, 'User {i}', 'user{i}@example.com', NOW())"
    )

# Create a query collector to track performance
collector = QueryCollector()

# Create batcher with 1MB limit and insert merging enabled
batcher = SQLBatcher(
    max_bytes=1_000_000,
    merge_inserts=True,
    transaction_size=1000
)

# Process statements with tracking
start_time = time.time()
batcher.process_statements(statements, adapter.execute, collector)
end_time = time.time()

# Print performance statistics
stats = collector.get_stats()
print(f"Total statements: {len(statements)}")
print(f"Total queries executed: {stats['count']}")
print(f"Average batch size: {stats['avg_batch_size']}")
print(f"Total execution time: {end_time - start_time:.2f} seconds")
```

This example demonstrates how SQL Batcher can efficiently handle large datasets by:
1. Batching statements to stay within database limits
2. Merging compatible INSERT statements to reduce database calls
3. Managing transactions for data consistency
4. Tracking performance metrics

The output might look like:
```
Total statements: 10000
Total queries executed: 10
Average batch size: 1000
Total execution time: 2.34 seconds
```

Instead of executing 10,000 individual INSERT statements, SQL Batcher merged them into 10 efficient batch operations.

## Core Concepts

### Query Batching
SQL Batcher processes SQL statements in batches to manage memory usage and prevent exceeding database limits. The batching process:

1. Calculates the size of each statement
2. Groups statements into batches that fit within the size limit
3. Executes batches in sequence
4. Manages transactions for each batch

Example:
```python
# Create batcher with 1MB limit
batcher = SQLBatcher(max_bytes=1_000_000)

# Process statements in batches
batcher.process_statements(statements, adapter.execute)
```

### Size Limits
Each database has different query size limits. SQL Batcher helps manage these limits by:

1. Respecting database-specific limits
2. Configuring batch sizes appropriately
3. Handling large statements that exceed limits
4. Providing size calculation utilities

Example:
```python
# Configure batch size for PostgreSQL
batcher = SQLBatcher(max_bytes=1_000_000)  # 1MB limit

# Configure batch size for Snowflake
batcher = SQLBatcher(max_bytes=100_000_000)  # 100MB limit
```

### Transaction Management
SQL Batcher provides transaction management to ensure data consistency:

1. Automatic transaction handling
2. Configurable commit strategies
3. Error recovery and rollback
4. Nested transaction support

Example:
```python
# Process statements in a transaction
with adapter.transaction():
    batcher.process_statements(statements, adapter.execute)
```

### Error Handling
SQL Batcher includes robust error handling:

1. Automatic retry for transient errors
2. Transaction rollback on failure
3. Detailed error reporting
4. Custom error handling strategies

Example:
```python
# Configure error handling
batcher = SQLBatcher(
    max_bytes=1_000_000,
    max_retries=3,
    retry_delay=1.0
)

# Process statements with error handling
try:
    batcher.process_statements(statements, adapter.execute)
except Exception as e:
    print(f"Error processing statements: {e}")
```

### Query Tracking
SQL Batcher provides query tracking capabilities:

1. Monitor executed queries
2. Track batch sizes and execution times
3. Debug and optimize performance
4. Generate execution reports

Example:
```python
from sql_batcher.query_collector import QueryCollector

# Create collector
collector = QueryCollector()

# Process statements with tracking
batcher.process_statements(statements, adapter.execute, collector)

# Get execution statistics
stats = collector.get_stats()
print(f"Executed {stats['count']} queries")
print(f"Average batch size: {stats['avg_batch_size']}")
```

## Database Adapters

SQL Batcher supports multiple database systems through adapters:

### PostgreSQL
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

### Snowflake
```python
from sql_batcher.adapters import SnowflakeAdapter

adapter = SnowflakeAdapter(
    account="your_account",
    user="user",
    password="password",
    database="mydb",
    schema="public"
)
```

### Trino
```python
from sql_batcher.adapters import TrinoAdapter

adapter = TrinoAdapter(
    host="localhost",
    port=8080,
    user="user",
    catalog="hive",
    schema="default"
)
```

### BigQuery
```python
from sql_batcher.adapters import BigQueryAdapter

adapter = BigQueryAdapter(
    project_id="your-project",
    dataset_id="your_dataset",
    location="US"
)
```

### Spark
```python
from sql_batcher.adapters import SparkAdapter

adapter = SparkAdapter(
    app_name="SQLBatcher",
    master="local[*]"
)
```

### Custom Adapters
You can create custom adapters by implementing the `SQLAdapter` interface:

```python
from sql_batcher.adapters.base import SQLAdapter

class CustomAdapter(SQLAdapter):
    def execute(self, sql: str) -> List[Tuple[Any, ...]]:
        # Implement execution logic
        pass

    def get_max_query_size(self) -> int:
        # Return maximum query size
        return 1_000_000

    def close(self) -> None:
        # Implement cleanup logic
        pass
```

## Basic Usage

### Simple Batching
```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters import PostgreSQLAdapter

# Create adapter
adapter = PostgreSQLAdapter(
    host="localhost",
    port=5432,
    database="mydb",
    user="user",
    password="password"
)

# Create statements
statements = [
    "INSERT INTO users (name) VALUES ('Alice')",
    "INSERT INTO users (name) VALUES ('Bob')",
    "INSERT INTO users (name) VALUES ('Charlie')",
]

# Create batcher
batcher = SQLBatcher(max_bytes=1_000_000)

# Process statements
batcher.process_statements(statements, adapter.execute)
```

### Insert Merging
```python
# Enable insert merging
batcher = SQLBatcher(max_bytes=1_000_000, merge_inserts=True)

# Process statements with merging
batcher.process_statements(statements, adapter.execute)
```

### Query Tracking
```python
from sql_batcher.query_collector import QueryCollector

# Create collector
collector = QueryCollector()

# Process statements with tracking
batcher.process_statements(statements, adapter.execute, collector)

# Get execution statistics
stats = collector.get_stats()
print(f"Executed {stats['count']} queries")
```

## Advanced Features

### Custom Batch Sizing
```python
from sql_batcher.batcher import BatchSizeCalculator

class CustomBatchSizeCalculator(BatchSizeCalculator):
    def calculate_batch_size(self, statements: List[str]) -> int:
        # Implement custom batch size calculation
        return min(len(statements), 1000)

# Use custom calculator
batcher = SQLBatcher(
    max_bytes=1_000_000,
    batch_size_calculator=CustomBatchSizeCalculator()
)
```

### Transaction Management
```python
# Process statements in a transaction
with adapter.transaction():
    batcher.process_statements(statements, adapter.execute)

# Configure transaction behavior
batcher = SQLBatcher(
    max_bytes=1_000_000,
    transaction_size=1000,  # Commit every 1000 statements
    auto_commit=True
)
```

### Error Handling
```python
# Configure error handling
batcher = SQLBatcher(
    max_bytes=1_000_000,
    max_retries=3,
    retry_delay=1.0,
    retry_on_errors=[TimeoutError, ConnectionError]
)

# Process statements with error handling
try:
    batcher.process_statements(statements, adapter.execute)
except Exception as e:
    print(f"Error processing statements: {e}")
```

### Query Collectors
```python
from sql_batcher.query_collector import QueryCollector, QueryStats

class CustomQueryCollector(QueryCollector):
    def on_query_executed(self, sql: str, stats: QueryStats) -> None:
        # Implement custom query tracking
        print(f"Executed query: {sql}")
        print(f"Execution time: {stats.execution_time}")

# Use custom collector
collector = CustomQueryCollector()
batcher.process_statements(statements, adapter.execute, collector)
```

## Examples

### Basic Examples

#### Simple Batching
```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters import PostgreSQLAdapter

# Create adapter
adapter = PostgreSQLAdapter(
    host="localhost",
    port=5432,
    database="mydb",
    user="user",
    password="password"
)

# Create statements
statements = [
    "INSERT INTO users (name) VALUES ('Alice')",
    "INSERT INTO users (name) VALUES ('Bob')",
    "INSERT INTO users (name) VALUES ('Charlie')",
]

# Create batcher
batcher = SQLBatcher(max_bytes=1_000_000)

# Process statements
batcher.process_statements(statements, adapter.execute)
```

#### Insert Merging
```python
# Enable insert merging
batcher = SQLBatcher(max_bytes=1_000_000, merge_inserts=True)

# Process statements with merging
batcher.process_statements(statements, adapter.execute)
```

#### Query Tracking
```python
from sql_batcher.query_collector import QueryCollector

# Create collector
collector = QueryCollector()

# Process statements with tracking
batcher.process_statements(statements, adapter.execute, collector)

# Get execution statistics
stats = collector.get_stats()
print(f"Executed {stats['count']} queries")
```

### Database-Specific Examples

#### PostgreSQL
```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters import PostgreSQLAdapter

# Create PostgreSQL adapter
adapter = PostgreSQLAdapter(
    host="localhost",
    port=5432,
    database="mydb",
    user="user",
    password="password"
)

# Create batcher with PostgreSQL-specific settings
batcher = SQLBatcher(
    max_bytes=1_000_000,
    merge_inserts=True,
    transaction_size=1000
)

# Process statements
batcher.process_statements(statements, adapter.execute)
```

#### Snowflake
```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters import SnowflakeAdapter

# Create Snowflake adapter
adapter = SnowflakeAdapter(
    account="your_account",
    user="user",
    password="password",
    database="mydb",
    schema="public"
)

# Create batcher with Snowflake-specific settings
batcher = SQLBatcher(
    max_bytes=100_000_000,
    merge_inserts=True,
    transaction_size=1000
)

# Process statements
batcher.process_statements(statements, adapter.execute)
```

#### Trino
```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters import TrinoAdapter

# Create Trino adapter
adapter = TrinoAdapter(
    host="localhost",
    port=8080,
    user="user",
    catalog="hive",
    schema="default"
)

# Create batcher with Trino-specific settings
batcher = SQLBatcher(
    max_bytes=1_000_000_000,
    merge_inserts=True,
    transaction_size=1000
)

# Process statements
batcher.process_statements(statements, adapter.execute)
```

### Advanced Examples

#### Custom Adapter
```python
from sql_batcher.adapters.base import SQLAdapter
from typing import Any, List, Tuple

class CustomDatabaseAdapter(SQLAdapter):
    def __init__(self, connection_string: str):
        self.connection_string = connection_string
        self.connection = None
        self.cursor = None

    def execute(self, sql: str) -> List[Tuple[Any, ...]]:
        if not self.connection:
            self.connection = self._create_connection()
            self.cursor = self.connection.cursor()
        
        self.cursor.execute(sql)
        if self.cursor.description:
            return self.cursor.fetchall()
        return []

    def get_max_query_size(self) -> int:
        return 1_000_000

    def close(self) -> None:
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()

    def _create_connection(self):
        # Implement connection creation
        pass

# Use custom adapter
adapter = CustomDatabaseAdapter("connection_string")
batcher = SQLBatcher(max_bytes=1_000_000)
batcher.process_statements(statements, adapter.execute)
```

#### Complex Transactions
```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters import PostgreSQLAdapter

# Create adapter
adapter = PostgreSQLAdapter(
    host="localhost",
    port=5432,
    database="mydb",
    user="user",
    password="password"
)

# Create batcher
batcher = SQLBatcher(
    max_bytes=1_000_000,
    merge_inserts=True,
    transaction_size=1000
)

# Process statements in a transaction
with adapter.transaction():
    # First batch
    batcher.process_statements(statements1, adapter.execute)
    
    # Second batch
    batcher.process_statements(statements2, adapter.execute)
    
    # Third batch
    batcher.process_statements(statements3, adapter.execute)
```

#### Error Handling
```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters import PostgreSQLAdapter

# Create adapter
adapter = PostgreSQLAdapter(
    host="localhost",
    port=5432,
    database="mydb",
    user="user",
    password="password"
)

# Create batcher with error handling
batcher = SQLBatcher(
    max_bytes=1_000_000,
    max_retries=3,
    retry_delay=1.0,
    retry_on_errors=[TimeoutError, ConnectionError]
)

# Process statements with error handling
try:
    batcher.process_statements(statements, adapter.execute)
except Exception as e:
    print(f"Error processing statements: {e}")
    # Handle error
```

#### Performance Optimization
```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters import PostgreSQLAdapter
from sql_batcher.query_collector import QueryCollector

# Create adapter
adapter = PostgreSQLAdapter(
    host="localhost",
    port=5432,
    database="mydb",
    user="user",
    password="password"
)

# Create collector
collector = QueryCollector()

# Create batcher with performance settings
batcher = SQLBatcher(
    max_bytes=1_000_000,
    merge_inserts=True,
    transaction_size=1000,
    batch_size=100
)

# Process statements with tracking
batcher.process_statements(statements, adapter.execute, collector)

# Analyze performance
stats = collector.get_stats()
print(f"Total queries: {stats['count']}")
print(f"Average batch size: {stats['avg_batch_size']}")
print(f"Total execution time: {stats['total_time']}")
```

## Development

### Git Hooks

This repository includes git hooks to ensure code quality. To set up the hooks:

```bash
# Run the setup script
./setup-hooks.sh
```

The hooks will:
- Format code using black and isort before each commit
- Automatically add formatted files to the commit

### Running Tests

```bash
# Run all tests
pytest

# Run specific test file
pytest tests/test_adapters.py

# Run with coverage
pytest --cov=src/sql_batcher
```

### Code Style

The project uses:
- black for code formatting
- isort for import sorting
- flake8 for linting
- mypy for type checking

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## License

MIT