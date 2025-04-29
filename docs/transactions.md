# Transaction Management

SQL Batcher provides comprehensive transaction management capabilities through its adapter interface, allowing you to control transaction boundaries and ensure data consistency.

## Why Use Transaction Management?

- **Data Consistency**: Ensure that related operations succeed or fail as a unit
- **Error Recovery**: Roll back changes if errors occur
- **Isolation**: Control the visibility of changes to other transactions
- **Performance**: Optimize performance by batching operations within a transaction

## Key Features

- **Transaction Control**: Begin, commit, and rollback transactions
- **Adapter Integration**: Transaction support for all database adapters
- **Error Handling**: Automatic rollback on errors when using context managers
- **Savepoint Support**: Create savepoints for partial rollbacks (see [Savepoints](savepoints.md))

## Basic Usage

```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters import TrinoAdapter

# Create adapter and batcher
adapter = TrinoAdapter(
    host="trino.example.com",
    port=8080,
    user="trino"
)
batcher = SQLBatcher(adapter=adapter)

# Begin a transaction
adapter.begin_transaction()

try:
    # Process statements within the transaction
    statements = [
        "INSERT INTO users (name) VALUES ('John')",
        "INSERT INTO users (name) VALUES ('Jane')",
    ]
    batcher.process_statements(statements, adapter.execute)
    
    # Commit the transaction
    adapter.commit_transaction()
except Exception as e:
    # Rollback on error
    adapter.rollback_transaction()
    raise
```

## With Context Manager

```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters import TrinoAdapter

# Create adapter and batcher
adapter = TrinoAdapter(
    host="trino.example.com",
    port=8080,
    user="trino"
)

# Begin a transaction
adapter.begin_transaction()

try:
    # Use context manager for clean resource management
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

## Async Transaction Management

```python
import asyncio
from sql_batcher import AsyncSQLBatcher
from sql_batcher.adapters.async_trino import AsyncTrinoAdapter

async def main():
    # Create async adapter and batcher
    adapter = AsyncTrinoAdapter(
        host="trino.example.com",
        port=8080,
        user="trino"
    )
    
    # Begin a transaction
    await adapter.begin_transaction()
    
    try:
        # Use async context manager
        async with AsyncSQLBatcher(adapter=adapter) as batcher:
            # Add statements asynchronously
            await batcher.add_statement("INSERT INTO users (name) VALUES ('John')")
            await batcher.add_statement("INSERT INTO users (name) VALUES ('Jane')")
            
            # Batches are automatically flushed on exit
        
        # Commit the transaction
        await adapter.commit_transaction()
    except Exception as e:
        # Rollback on error
        await adapter.rollback_transaction()
        raise
    finally:
        # Close the connection
        await adapter.close()

# Run the async function
asyncio.run(main())
```

## Transaction Methods

### `begin_transaction() -> None`

Begin a transaction.

### `commit_transaction() -> None`

Commit the current transaction.

### `rollback_transaction() -> None`

Rollback the current transaction.

## Database Support

Transaction support varies by database. Here's a summary of transaction support for each adapter:

| Adapter | Transaction Support | Notes |
|---------|---------------------|-------|
| Trino | ✅ | Supports standard transactions |
| PostgreSQL | ✅ | Full ACID transaction support |
| Snowflake | ✅ | Full transaction support |
| BigQuery | ⚠️ | Limited transaction support |
| Generic | ⚠️ | Depends on the underlying database |

For more advanced transaction management, including savepoints, see the [Savepoints](savepoints.md) documentation.
