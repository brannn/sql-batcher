# Context Manager

SQL Batcher provides a context manager interface for both synchronous and asynchronous operations, allowing for clean resource management and automatic flushing of batched statements.

## Why Use Context Managers?

- **Clean Resource Management**: Automatically handles resource cleanup
- **Automatic Flushing**: Ensures all batched statements are executed when exiting the context
- **Error Handling**: Provides built-in error handling with proper cleanup
- **Simplified Code**: Makes your code more concise and readable
- **Consistent Pattern**: Follows Python's standard context manager pattern

## Key Features

- **Automatic Flushing**: All pending statements are flushed when exiting the context
- **Resource Cleanup**: Ensures proper cleanup of resources
- **Exception Handling**: Properly handles exceptions within the context
- **Both Sync and Async**: Supports both synchronous and asynchronous operations
- **Transaction Integration**: Works seamlessly with transaction management

## Basic Usage

### Synchronous Context Manager

```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters import TrinoAdapter

# Create adapter
adapter = TrinoAdapter(
    host="trino.example.com",
    port=8080,
    user="trino"
)

# Use context manager
with SQLBatcher(adapter=adapter) as batcher:
    # Add statements
    batcher.add_statement("INSERT INTO users (name) VALUES ('John')")
    batcher.add_statement("INSERT INTO users (name) VALUES ('Jane')")

    # Batches are automatically flushed on exit
```

### Asynchronous Context Manager

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

    # Close the connection
    await adapter.close()

# Run the async function
asyncio.run(main())
```

## With Transaction Management

Context managers work seamlessly with transaction management:

```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters import PostgreSQLAdapter

# Create adapter
connection_params = {
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password": "password",
    "database": "mydb"
}

adapter = PostgreSQLAdapter(connection_params=connection_params)

# Begin a transaction
adapter.begin_transaction()

try:
    # Use context manager
    with SQLBatcher(adapter=adapter) as batcher:
        # Add statements
        batcher.add_statement("INSERT INTO users (name) VALUES ('John')")
        batcher.add_statement("INSERT INTO users (name) VALUES ('Jane')")

        # Batches are automatically flushed on exit

    # Commit the transaction
    adapter.commit_transaction()
except Exception as e:
    # Rollback on error
    adapter.rollback_transaction()
    raise
```

## Implementation Details

The context manager implementation ensures that:

1. All pending statements are flushed when exiting the context
2. Resources are properly cleaned up
3. Exceptions are properly propagated
4. The adapter's connection is maintained (not closed)

### Synchronous Implementation

```python
def __enter__(self) -> 'SQLBatcher':
    """Enter the context manager."""
    return self

def __exit__(self, exc_type, exc_val, exc_tb) -> None:
    """Exit the context manager, flushing any pending statements."""
    if exc_type is None:
        # No exception occurred, flush any pending statements
        self.flush(self._adapter.execute)
```

### Asynchronous Implementation

```python
async def __aenter__(self) -> 'AsyncSQLBatcher':
    """Enter the async context manager."""
    return self

async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
    """Exit the async context manager, flushing any pending statements."""
    if exc_type is None:
        # No exception occurred, flush any pending statements
        await self.flush(self._adapter.execute)
```
