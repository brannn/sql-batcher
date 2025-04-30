# SQL Batcher

[![PyPI version](https://img.shields.io/pypi/v/sql-batcher.svg)](https://pypi.org/project/sql-batcher/)
[![Python Versions](https://img.shields.io/pypi/pyversions/sql-batcher.svg)](https://pypi.org/project/sql-batcher/)
[![Python CI](https://github.com/brannn/sql-batcher/actions/workflows/python-ci.yml/badge.svg)](https://github.com/brannn/sql-batcher/actions/workflows/python-ci.yml)
[![License](https://img.shields.io/github/license/brannn/sql-batcher.svg)](https://github.com/brannn/sql-batcher/blob/main/LICENSE)

## Why SQL Batcher?

Data engineers and developers face significant challenges when working with large-scale database operations: performance bottlenecks, memory constraints, network overhead, and the complexity of managing transactions across different database systems.

SQL Batcher attempts to address these pain points with:

- **Performance Optimization**: Intelligently batch and merge SQL statements to reduce round-trips to the database, dramatically improving throughput for large-scale operations
- **Memory Efficiency**: Control memory usage with configurable batch sizes and smart statement merging, preventing out-of-memory errors during massive data operations
- **Database Adaptability**: Leverage database-specific optimizations with adapters for PostgreSQL, Trino, Snowflake, and more
- **Transaction Management**: Simplify complex transaction handling with built-in savepoints, retries, and error recovery
- **Developer Experience**: Write clean, maintainable code with an intuitive API that abstracts away the complexities of efficient database interactions

SQL Batcher is particularly valuable in data engineering workflows, ETL pipelines, large dataset ingestion, and any scenario requiring high-performance database operations.

## Key Features

SQL Batcher provides a comprehensive set of features for efficient SQL statement execution:

### [SQL Batcher](docs/batcher.md)

Efficiently batch SQL statements based on size limits and other constraints. The core component that handles:

- Smart batching based on database-specific size limits
- Dynamic batch size adjustment based on column count
- Memory and network optimization
- [Learn more about SQL Batcher →](docs/batcher.md)

### [Query Collector](docs/query_collector.md)

Collect and track SQL queries for debugging, logging, and monitoring:

- Query collection with metadata support
- Size tracking and batch management
- Column count detection for INSERT statements
- [Learn more about Query Collector →](docs/query_collector.md)

### [Insert Merging](docs/insert_merging.md)

Optimize database operations by combining compatible INSERT statements:

- Automatic detection of compatible statements
- Size-aware merging respecting query limits
- Table and column structure awareness
- Preserves execution order of non-INSERT statements
- [Learn more about Insert Merging →](docs/insert_merging.md)

### [Database Adapters](docs/adapters.md)

Optimized adapters for popular databases:

- Database-specific optimizations
- Consistent interface across databases
- Connection and resource management
- [Learn more about Database Adapters →](docs/adapters.md)

### [Async Support](docs/async.md)

Comprehensive async support for modern Python applications:

- Async batching and execution
- Async adapters for all supported databases
- Async context managers and transaction management
- [Learn more about Async Support →](docs/async.md)

### [Context Manager](docs/context_manager.md)

Clean resource management and automatic flushing of batched statements:

- Automatic flushing when exiting the context
- Proper resource cleanup and error handling
- Support for both synchronous and asynchronous operations
- Seamless integration with transaction management
- [Learn more about Context Manager →](docs/context_manager.md)

### [Transaction Management](docs/transactions.md)

Control transaction boundaries and ensure data consistency:

- Begin, commit, and rollback transactions
- Error handling and recovery
- Integration with context managers
- [Learn more about Transaction Management →](docs/transactions.md)

### [Savepoint Support](docs/savepoints.md)

Create intermediate points within a transaction for partial rollbacks:

- Create, rollback to, and release savepoints
- Error recovery within transactions
- Support for complex transaction workflows
- [Learn more about Savepoint Support →](docs/savepoints.md)

## Installation

Install SQL Batcher using pip:

```bash
pip install sql-batcher
```

With database-specific dependencies:

```bash
# For Trino support
pip install "sql-batcher[trino]"

# For PostgreSQL support
pip install "sql-batcher[postgresql]"

# For Snowflake support
pip install "sql-batcher[snowflake]"

# For BigQuery support
pip install "sql-batcher[bigquery]"

# For all supported databases
pip install "sql-batcher[all]"

# For development (includes testing and linting tools)
pip install "sql-batcher[dev]"
```

## Quick Start

Here's a simple example to get you started with SQL Batcher:

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
    role="admin",  # Trino role (sets 'x-trino-role' HTTP header as 'system=ROLE{role}')
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

For async usage:

```python
import asyncio
from sql_batcher import AsyncSQLBatcher
from sql_batcher.adapters.async_trino import AsyncTrinoAdapter

async def main():
    # Create async adapter and batcher
    adapter = AsyncTrinoAdapter(
        host="trino.example.com",
        port=8080,
        user="trino",
        catalog="hive",
        schema="default",
        role="admin",  # Trino role (sets 'x-trino-role' HTTP header as 'system=ROLE{role}')
        max_query_size=600_000  # 600KB limit to provide buffer for Trino's 1MB limit
    )

    batcher = AsyncSQLBatcher(
        adapter=adapter,
        max_bytes=500_000,  # 500KB limit
        batch_mode=True,
        auto_adjust_for_columns=True  # Adjust batch size based on column count
    )

    # Process statements asynchronously
    statements = [
        "INSERT INTO table1 VALUES (1, 'a')",
        "INSERT INTO table1 VALUES (2, 'b')",
        # ... many more statements
    ]

    await batcher.process_statements(statements, adapter.execute)

    # Close the connection
    await adapter.close()

# Run the async function
asyncio.run(main())
```

## Documentation

For more detailed documentation, see the following pages:

- [SQL Batcher](docs/batcher.md) - Core batching functionality
- [Query Collector](docs/query_collector.md) - Query collection and tracking
- [Database Adapters](docs/adapters.md) - Database-specific adapters
- [Async Support](docs/async.md) - Async functionality
- [Context Manager](docs/context_manager.md) - Clean resource management
- [Transaction Management](docs/transactions.md) - Transaction control
- [Savepoint Support](docs/savepoints.md) - Savepoint functionality
- [Insert Merging](docs/insert_merging.md) - INSERT statement optimization
- [Usage Examples](docs/examples.md) - Collection of usage examples
- [Testing](docs/testing.md) - Testing guidelines and examples
- [Code Style Guide](CODE_STYLE.md) - Code style guidelines

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Install pre-commit hooks (`pip install pre-commit && pre-commit install`)
4. Make your changes (the pre-commit hooks will automatically format your code)
5. Commit your changes (`git commit -m 'Add some amazing feature'`)
6. Push to the branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

The project uses pre-commit hooks to ensure code quality:
- **black** for code formatting
- **isort** for import sorting
- **flake8** for code linting
- **autoflake** for removing unused imports and variables

## License

This project is licensed under the MIT License - see the LICENSE file for details.
