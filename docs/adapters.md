# Database Adapters

SQL Batcher provides optimized adapters for popular databases. These adapters handle database-specific features and optimizations.

## Why Use Database Adapters?

- **Database-Specific Optimizations**: Each adapter is optimized for its specific database
- **Consistent Interface**: All adapters provide a consistent interface for SQL Batcher
- **Feature Support**: Adapters handle database-specific features like transactions and savepoints
- **Connection Management**: Adapters manage database connections and resources

## Supported Adapters

### Trino

```python
from sql_batcher.adapters import TrinoAdapter

adapter = TrinoAdapter(
    host="trino.example.com",
    port=8080,
    user="trino",
    catalog="hive",
    schema="default",
    role="admin",  # Trino role (sets 'x-trino-role' HTTP header as 'system=ROLE{role}')
    max_query_size=600_000  # 600KB limit to provide buffer for Trino's 1MB limit
)
```

### PostgreSQL

```python
from sql_batcher.adapters import PostgreSQLAdapter

# Connection parameters can be provided as individual arguments
connection_params = {
    "host": "localhost",
    "port": 5432,
    "user": "postgres",
    "password": "password",
    "database": "mydb"
}

adapter = PostgreSQLAdapter(connection_params=connection_params)
```

### Snowflake

```python
from sql_batcher.adapters import SnowflakeAdapter

adapter = SnowflakeAdapter(
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
from sql_batcher.adapters import BigQueryAdapter

adapter = BigQueryAdapter(
    project_id="my-project",
    dataset_id="my_dataset",
    location="US"
)
```

### Generic Adapter

For databases without a specialized adapter:

```python
from sql_batcher.adapters import GenericAdapter

adapter = GenericAdapter(
    connection=my_db_connection,
    execute_func=my_execute_function,
    close_func=my_close_function
)
```

## Adapter Interface

All adapters implement the `SQLAdapter` interface, which includes:

### Core Methods

- `execute(sql: str) -> List[Any]`: Execute a SQL statement and return results
- `get_max_query_size() -> int`: Get the maximum query size in bytes
- `close() -> None`: Close the database connection

### Transaction Methods

- `begin_transaction() -> None`: Begin a transaction
- `commit_transaction() -> None`: Commit the current transaction
- `rollback_transaction() -> None`: Rollback the current transaction

### Savepoint Methods

- `create_savepoint(name: str) -> None`: Create a savepoint with the given name
- `rollback_to_savepoint(name: str) -> None`: Rollback to the savepoint with the given name
- `release_savepoint(name: str) -> None`: Release the savepoint with the given name

## Async Adapters

SQL Batcher also provides async versions of all adapters for use with AsyncSQLBatcher. See the [Async Support](async.md) documentation for details.
