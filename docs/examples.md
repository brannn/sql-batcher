# Usage Examples

This document provides a collection of examples demonstrating how to use SQL Batcher in various scenarios.

## Basic Usage

### Simple Batching

```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters import TrinoAdapter

# Create adapter and batcher
adapter = TrinoAdapter(
    host="trino.example.com",
    port=8080,
    user="trino",
    catalog="hive",
    schema="default"
)
batcher = SQLBatcher(adapter=adapter)

# Process statements
statements = [
    "INSERT INTO users (id, name) VALUES (1, 'John')",
    "INSERT INTO users (id, name) VALUES (2, 'Jane')",
    # ... many more statements
]

batcher.process_statements(statements, adapter.execute)
```

### With Context Manager

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
    batcher.add_statement("INSERT INTO users (id, name) VALUES (1, 'John')")
    batcher.add_statement("INSERT INTO users (id, name) VALUES (2, 'Jane')")
    
    # Batches are automatically flushed on exit
```

### With Transaction Management

```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters import PostgreSQLAdapter

# Create adapter
adapter = PostgreSQLAdapter(
    host="localhost",
    port=5432,
    user="postgres",
    password="password",
    database="mydb"
)

# Begin a transaction
adapter.begin_transaction()

try:
    # Process statements within the transaction
    with SQLBatcher(adapter=adapter) as batcher:
        batcher.add_statement("INSERT INTO users (id, name) VALUES (1, 'John')")
        batcher.add_statement("INSERT INTO users (id, name) VALUES (2, 'Jane')")
    
    # Commit the transaction
    adapter.commit_transaction()
except Exception as e:
    # Rollback on error
    adapter.rollback_transaction()
    raise
```

### With Savepoints

```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters import PostgreSQLAdapter

# Create adapter
adapter = PostgreSQLAdapter(
    host="localhost",
    port=5432,
    user="postgres",
    password="password",
    database="mydb"
)

# Begin a transaction
adapter.begin_transaction()

try:
    # Create a savepoint
    adapter.create_savepoint("batch1")
    
    # Process first batch
    with SQLBatcher(adapter=adapter) as batcher:
        batcher.add_statement("INSERT INTO users (id, name) VALUES (1, 'John')")
        batcher.add_statement("INSERT INTO users (id, name) VALUES (2, 'Jane')")
    
    # Create another savepoint
    adapter.create_savepoint("batch2")
    
    # Process second batch
    with SQLBatcher(adapter=adapter) as batcher:
        batcher.add_statement("INSERT INTO orders (user_id, product) VALUES (1, 'Product A')")
    
    # Commit the transaction
    adapter.commit_transaction()
except Exception as e:
    # Rollback to savepoint or entire transaction
    adapter.rollback_transaction()
    raise
```

### With Insert Merging

```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters import TrinoAdapter

# Create adapter and batcher with insert merging enabled
adapter = TrinoAdapter(
    host="trino.example.com",
    port=8080,
    user="trino"
)
batcher = SQLBatcher(
    adapter=adapter,
    merge_inserts=True  # Enable insert merging
)

# Process statements with insert merging
statements = [
    "INSERT INTO users (id, name) VALUES (1, 'John')",
    "INSERT INTO users (id, name) VALUES (2, 'Jane')",
    "INSERT INTO users (id, name) VALUES (3, 'Bob')",
]

# These will be merged into a single statement
batcher.process_statements(statements, adapter.execute)
```

### With Query Collection

```python
from sql_batcher import SQLBatcher
from sql_batcher.query_collector import ListQueryCollector
from sql_batcher.adapters import TrinoAdapter

# Create adapter and query collector
adapter = TrinoAdapter(
    host="trino.example.com",
    port=8080,
    user="trino"
)
collector = ListQueryCollector()

# Process statements with query collection
with SQLBatcher(adapter=adapter) as batcher:
    statements = [
        "INSERT INTO users (id, name) VALUES (1, 'John')",
        "INSERT INTO users (id, name) VALUES (2, 'Jane')",
    ]
    batcher.process_statements(statements, adapter.execute, collector)

# Get collected queries
queries = collector.get_all()
for query in queries:
    print(f"Query: {query['query']}")
    print(f"Size: {query['size']} bytes")
```

## Async Examples

### Basic Async Usage

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
    batcher = AsyncSQLBatcher(adapter=adapter)
    
    # Process statements asynchronously
    statements = [
        "INSERT INTO users (id, name) VALUES (1, 'John')",
        "INSERT INTO users (id, name) VALUES (2, 'Jane')",
    ]
    
    await batcher.process_statements(statements, adapter.execute)
    
    # Close the connection
    await adapter.close()

# Run the async function
asyncio.run(main())
```

### Async with Context Manager

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
        await batcher.add_statement("INSERT INTO users (id, name) VALUES (1, 'John')")
        await batcher.add_statement("INSERT INTO users (id, name) VALUES (2, 'Jane')")
        
        # Batches are automatically flushed on exit
    
    # Close the connection
    await adapter.close()

# Run the async function
asyncio.run(main())
```

### Async with Transaction Management

```python
import asyncio
from sql_batcher import AsyncSQLBatcher
from sql_batcher.adapters.async_postgresql import AsyncPostgreSQLAdapter

async def main():
    # Create async adapter
    adapter = AsyncPostgreSQLAdapter(
        dsn="postgresql://user:pass@localhost:5432/dbname"
    )
    
    # Begin a transaction
    await adapter.begin_transaction()
    
    try:
        # Use async context manager
        async with AsyncSQLBatcher(adapter=adapter) as batcher:
            # Add statements asynchronously
            await batcher.add_statement("INSERT INTO users (id, name) VALUES (1, 'John')")
            await batcher.add_statement("INSERT INTO users (id, name) VALUES (2, 'Jane')")
            
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

## Advanced Examples

### Dynamic Batch Size Adjustment

```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters import TrinoAdapter

# Create adapter and batcher with column-based adjustment
adapter = TrinoAdapter(
    host="trino.example.com",
    port=8080,
    user="trino"
)
batcher = SQLBatcher(
    adapter=adapter,
    auto_adjust_for_columns=True,  # Enable column-based adjustment
    reference_column_count=10,     # Reference column count
    min_adjustment_factor=0.5,     # Minimum adjustment factor
    max_adjustment_factor=2.0      # Maximum adjustment factor
)

# Process statements with dynamic batch size adjustment
statements = [
    "INSERT INTO wide_table (col1, col2, col3, ..., col20) VALUES (...)",
    # More statements with many columns
]

batcher.process_statements(statements, adapter.execute)
```

### Dry Run Mode

```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters import TrinoAdapter
from sql_batcher.query_collector import ListQueryCollector

# Create adapter, collector, and batcher in dry run mode
adapter = TrinoAdapter(
    host="trino.example.com",
    port=8080,
    user="trino"
)
collector = ListQueryCollector()
batcher = SQLBatcher(
    adapter=adapter,
    dry_run=True  # Enable dry run mode
)

# Process statements without executing them
statements = [
    "INSERT INTO users (id, name) VALUES (1, 'John')",
    "INSERT INTO users (id, name) VALUES (2, 'Jane')",
]

batcher.process_statements(statements, adapter.execute, collector)

# Get collected queries that would have been executed
queries = collector.get_all()
for query in queries:
    print(f"Would execute: {query['query']}")
```

### Custom Execute Function

```python
from sql_batcher import SQLBatcher
from sql_batcher.adapters import GenericAdapter

# Define a custom execute function
def custom_execute(sql: str) -> list:
    print(f"Executing: {sql}")
    # Custom execution logic here
    return []

# Create a generic adapter with the custom execute function
adapter = GenericAdapter(
    connection=None,  # No connection needed for this example
    execute_func=custom_execute
)

# Process statements with the custom execute function
with SQLBatcher(adapter=adapter) as batcher:
    batcher.add_statement("INSERT INTO users (id, name) VALUES (1, 'John')")
    batcher.add_statement("INSERT INTO users (id, name) VALUES (2, 'Jane')")
```
