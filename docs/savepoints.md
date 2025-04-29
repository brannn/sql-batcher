# Savepoint Support

SQL Batcher provides savepoint support through its adapter interface, allowing you to create intermediate points within a transaction that you can roll back to without aborting the entire transaction.

## Why Use Savepoints?

- **Partial Rollbacks**: Roll back to a specific point in a transaction without aborting the entire transaction
- **Error Recovery**: Recover from errors in specific parts of a transaction
- **Complex Workflows**: Manage complex transaction workflows with multiple steps
- **Performance**: Avoid restarting entire transactions when only part of the work needs to be redone

## Key Features

- **Savepoint Creation**: Create named savepoints within a transaction
- **Rollback to Savepoint**: Roll back to a specific savepoint
- **Savepoint Release**: Release savepoints when they're no longer needed
- **Adapter Integration**: Savepoint support for compatible database adapters

## Basic Usage

```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters import PostgreSQLAdapter

# Create adapter and batcher
adapter = PostgreSQLAdapter(connection_params)
batcher = SQLBatcher(adapter=adapter)

# Begin a transaction
adapter.begin_transaction()

try:
    # Create a savepoint
    adapter.create_savepoint("batch1")
    
    # Process statements
    with batcher as b:
        b.add_statement("INSERT INTO users (name) VALUES ('John')")
        b.add_statement("INSERT INTO users (name) VALUES ('Jane')")
    
    # Create another savepoint
    adapter.create_savepoint("batch2")
    
    # Process more statements
    with batcher as b:
        b.add_statement("INSERT INTO orders (user_id) VALUES (1)")
    
    # Commit the transaction
    adapter.commit_transaction()
    
except Exception as e:
    # Rollback to the appropriate savepoint
    adapter.rollback_to_savepoint("batch1")
    # Or rollback the entire transaction
    adapter.rollback_transaction()
```

## Async Usage

```python
from sql_batcher import AsyncSQLBatcher
from sql_batcher.adapters.async_postgresql import AsyncPostgreSQLAdapter

# Create adapter and batcher
adapter = AsyncPostgreSQLAdapter(dsn="postgresql://user:pass@localhost:5432/db")
batcher = AsyncSQLBatcher(adapter=adapter)

# Begin a transaction
await adapter.begin_transaction()

try:
    # Create a savepoint
    await adapter.create_savepoint("batch1")
    
    # Process statements
    async with batcher as b:
        await b.add_statement("INSERT INTO users (name) VALUES ('John')")
        await b.add_statement("INSERT INTO users (name) VALUES ('Jane')")
    
    # Create another savepoint
    await adapter.create_savepoint("batch2")
    
    # Process more statements
    async with batcher as b:
        await b.add_statement("INSERT INTO orders (user_id) VALUES (1)")
    
    # Commit the transaction
    await adapter.commit_transaction()
    
except Exception as e:
    # Rollback to the appropriate savepoint
    await adapter.rollback_to_savepoint("batch1")
    # Or rollback the entire transaction
    await adapter.rollback_transaction()
```

## Savepoint Methods

### `create_savepoint(name: str) -> None`

Create a savepoint with the given name.

### `rollback_to_savepoint(name: str) -> None`

Rollback to the savepoint with the given name.

### `release_savepoint(name: str) -> None`

Release the savepoint with the given name.

## Database Support

Savepoint support varies by database. Here's a summary of savepoint support for each adapter:

| Adapter | Savepoint Support | Notes |
|---------|-------------------|-------|
| Trino | ✅ | Supports standard savepoints |
| PostgreSQL | ✅ | Full savepoint support |
| Snowflake | ✅ | Full savepoint support |
| BigQuery | ❌ | No savepoint support |
| Generic | ⚠️ | Depends on the underlying database |

## Error Handling

When using savepoints, it's important to handle errors properly:

```python
adapter.begin_transaction()

try:
    # First batch
    adapter.create_savepoint("batch1")
    try:
        # Process first batch
        batcher.process_statements(batch1_statements, adapter.execute)
    except Exception as e:
        # Rollback to savepoint
        adapter.rollback_to_savepoint("batch1")
        # Handle error or re-raise
        raise
    
    # Second batch
    adapter.create_savepoint("batch2")
    try:
        # Process second batch
        batcher.process_statements(batch2_statements, adapter.execute)
    except Exception as e:
        # Rollback to savepoint
        adapter.rollback_to_savepoint("batch2")
        # Handle error or re-raise
        raise
    
    # Commit if everything succeeded
    adapter.commit_transaction()
except Exception as e:
    # Rollback the entire transaction if any unhandled errors
    adapter.rollback_transaction()
    raise
```
