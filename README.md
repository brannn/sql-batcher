# SQL Batcher

SQL Batcher is a Python library designed to optimize large-scale SQL operations by batching SQL statements, intelligently merging inserts, managing transaction size, and applying database-specific optimizations. It is particularly valuable in data engineering, ETL pipelines, and large dataset ingestion.

[![PyPI version](https://badge.fury.io/py/sql-batcher.svg)](https://badge.fury.io/py/sql-batcher)
[![Python Versions](https://img.shields.io/pypi/pyversions/sql-batcher.svg)](https://pypi.org/project/sql-batcher/)
[![License](https://img.shields.io/github/license/yourusername/sql-batcher.svg)](https://github.com/yourusername/sql-batcher/blob/main/LICENSE)

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

Optimized adapters for popular databases, with Trino as our first-class query engine:

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

### [Insert Merging](docs/insert_merging.md)

Optimize database operations by combining compatible INSERT statements:

- Automatic detection of compatible statements
- Size-aware merging respecting query limits
- Table and column structure awareness
- [Learn more about Insert Merging →](docs/insert_merging.md)

## Installation

Install SQL Batcher using pip:

```bash
pip install sql-batcher
```

With database-specific dependencies:

```bash
# For Trino support (our first-class query engine)
pip install "sql-batcher[trino]"

# For PostgreSQL support
pip install "sql-batcher[postgresql]"

# For Snowflake support
pip install "sql-batcher[snowflake]"

# For BigQuery support
pip install "sql-batcher[bigquery]"

# For all supported databases
pip install "sql-batcher[all]"
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
        role="admin",  # Trino role (sets 'x-trino-role' HTTP header)
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
- [Transaction Management](docs/transactions.md) - Transaction control
- [Savepoint Support](docs/savepoints.md) - Savepoint functionality
- [Insert Merging](docs/insert_merging.md) - INSERT statement optimization
- [Usage Examples](docs/examples.md) - Collection of usage examples
- [Code Style Guide](CODE_STYLE.md) - Code style guidelines

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Format your code (`./format_code.sh`)
4. Commit your changes (`git commit -m 'Add some amazing feature'`)
5. Push to the branch (`git push origin feature/amazing-feature`)
6. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.
