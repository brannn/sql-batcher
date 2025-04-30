# Async Support

SQL Batcher provides comprehensive async support through the `AsyncSQLBatcher` class and async adapters for all supported databases.

## Why Use Async Support?

- **Performance**: Async I/O can significantly improve performance for I/O-bound operations
- **Scalability**: Handle more concurrent operations with fewer resources
- **Integration**: Seamlessly integrate with async web frameworks like FastAPI, Starlette, or Quart
- **Modern Codebases**: Align with modern Python's emphasis on async/await syntax

## Key Features

- **Async Batching**: Efficiently batch SQL statements asynchronously
- **Async Adapters**: Async versions of all database adapters
- **Async Query Collection**: Track queries asynchronously
- **Async Context Managers**: Clean resource management with async context managers
- **Async Transaction Management**: Manage transactions asynchronously
- **Async Savepoints**: Create and manage savepoints asynchronously

## Basic Usage

```python
import asyncio
from sql_batcher import AsyncSQLBatcher
from sql_batcher.adapters.async_trino import AsyncTrinoAdapter

async def main():
    # Create async adapter
    adapter = AsyncTrinoAdapter(
        host="trino.example.com",
        port=8080,
        user="trino",
        catalog="hive",
        schema="default",
        role="admin",  # Trino role (sets 'x-trino-role' HTTP header as 'system=ROLE{role}')
        max_query_size=600_000  # 600KB limit to provide buffer for Trino's 1MB limit
    )

    # Create async batcher
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

## Async Context Manager

```python
import asyncio
from sql_batcher import AsyncSQLBatcher
from sql_batcher.adapters.async_trino import AsyncTrinoAdapter

async def main():
    # Create async adapter
    adapter = AsyncTrinoAdapter(
        host="trino.example.com",
        port=8080,
        user="trino"
    )

    # Use async context manager
    async with AsyncSQLBatcher(adapter=adapter) as batcher:
        # Add statements asynchronously
        await batcher.add_statement("INSERT INTO users (name) VALUES ('John')")
        await batcher.add_statement("INSERT INTO users (name) VALUES ('Jane')")

        # Batches are automatically flushed on exit
        # Resources are automatically cleaned up

# Run the async function
asyncio.run(main())
```

## Supported Async Adapters

SQL Batcher provides async adapters for all supported databases:

### Trino

```python
from sql_batcher.adapters.async_trino import AsyncTrinoAdapter

adapter = AsyncTrinoAdapter(
    host="trino.example.com",
    port=8080,
    user="trino",
    catalog="hive",
    schema="default",
    role="admin",  # Trino role (sets 'x-trino-role' HTTP header as 'system=ROLE{role}')
    max_query_size=600_000,  # 600KB limit to provide buffer for Trino's 1MB limit
    retry_attempts=3,  # With retry mechanism
    circuit_breaker_enabled=True  # With circuit breaker protection
)
```

### PostgreSQL

```python
from sql_batcher.adapters.async_postgresql import AsyncPostgreSQLAdapter

adapter = AsyncPostgreSQLAdapter(
    dsn="postgresql://user:pass@localhost:5432/dbname",
    min_size=5,
    max_size=10
)
```

### Snowflake

```python
from sql_batcher.adapters.async_snowflake import AsyncSnowflakeAdapter

adapter = AsyncSnowflakeAdapter(
    account="myaccount",
    user="myuser",
    password="mypassword",
    warehouse="mywarehouse",
    database="mydatabase",
    schema="myschema"
)
```

### BigQuery

```python
from sql_batcher.adapters.async_bigquery import AsyncBigQueryAdapter

adapter = AsyncBigQueryAdapter(
    project_id="my-project",
    dataset_id="my_dataset",
    location="US"
)
```

### Generic Async Adapter

```python
from sql_batcher.adapters.async_base import AsyncGenericAdapter

adapter = AsyncGenericAdapter(
    connection=my_async_connection,
    execute_func=my_async_execute_function,
    close_func=my_async_close_function
)
```

## Async Methods

### `async def add_statement(statement: str) -> bool`

Add a statement to the current batch asynchronously. Returns `True` if the batch should be flushed.

### `async def flush(execute_callback: Callable[[str], Awaitable[Any]], query_collector: Optional[AsyncQueryCollector] = None, metadata: Optional[Dict[str, Any]] = None) -> int`

Flush the current batch of statements asynchronously. Returns the number of statements flushed.

### `async def process_statements(statements: List[str], execute_callback: Callable[[str], Awaitable[Any]], query_collector: Optional[AsyncQueryCollector] = None, metadata: Optional[Dict[str, Any]] = None) -> int`

Process a list of SQL statements asynchronously. Returns the number of statements processed.

### `async def process_batch(statements: List[str], execute_func: Optional[Callable[[str], Awaitable[Any]]] = None) -> List[Any]`

Process statements in batches asynchronously. Returns a list of results from executed statements.
