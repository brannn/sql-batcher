"""Integration tests for async batcher."""

import os

import pytest

from sql_batcher import AsyncSQLBatcher
from sql_batcher.adapters.async_bigquery import AsyncBigQueryAdapter
from sql_batcher.adapters.async_postgresql import AsyncPostgreSQLAdapter
from sql_batcher.adapters.async_snowflake import AsyncSnowflakeAdapter
from sql_batcher.adapters.async_trino import AsyncTrinoAdapter


# Skip tests if database is not available
def skip_if_no_db(db_type: str) -> pytest.MarkDecorator:
    """Skip test if database is not available."""
    env_var = f"SQL_BATCHER_TEST_{db_type.upper()}_DSN"
    return pytest.mark.skipif(
        not os.getenv(env_var),
        reason=f"{db_type} database not available (set {env_var} to enable)",
    )


@pytest.fixture
async def postgres_adapter():
    """Create a PostgreSQL adapter for testing."""
    dsn = os.getenv("SQL_BATCHER_TEST_POSTGRESQL_DSN")
    if not dsn:
        pytest.skip("PostgreSQL database not available")

    adapter = AsyncPostgreSQLAdapter(
        dsn=dsn,
        min_size=1,
        max_size=1,
    )
    try:
        await adapter.connect()
        yield adapter
    finally:
        await adapter.disconnect()


@pytest.fixture
async def trino_adapter():
    """Create a Trino adapter for testing."""
    host = os.getenv("SQL_BATCHER_TEST_TRINO_HOST")
    if not host:
        pytest.skip("Trino database not available")

    adapter = AsyncTrinoAdapter(
        host=host,
        port=int(os.getenv("SQL_BATCHER_TEST_TRINO_PORT", "8080")),
        user=os.getenv("SQL_BATCHER_TEST_TRINO_USER", "trino"),
        catalog=os.getenv("SQL_BATCHER_TEST_TRINO_CATALOG"),
        schema=os.getenv("SQL_BATCHER_TEST_TRINO_SCHEMA"),
    )
    try:
        await adapter.connect()
        yield adapter
    finally:
        await adapter.disconnect()


@pytest.fixture
async def snowflake_adapter():
    """Create a Snowflake adapter for testing."""
    account = os.getenv("SQL_BATCHER_TEST_SNOWFLAKE_ACCOUNT")
    if not account:
        pytest.skip("Snowflake database not available")

    adapter = AsyncSnowflakeAdapter(
        account=account,
        user=os.getenv("SQL_BATCHER_TEST_SNOWFLAKE_USER", ""),
        password=os.getenv("SQL_BATCHER_TEST_SNOWFLAKE_PASSWORD", ""),
        warehouse=os.getenv("SQL_BATCHER_TEST_SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SQL_BATCHER_TEST_SNOWFLAKE_DATABASE"),
        schema=os.getenv("SQL_BATCHER_TEST_SNOWFLAKE_SCHEMA"),
    )
    try:
        await adapter.connect()
        yield adapter
    finally:
        await adapter.disconnect()


@pytest.fixture
async def bigquery_adapter():
    """Create a BigQuery adapter for testing."""
    project_id = os.getenv("SQL_BATCHER_TEST_BIGQUERY_PROJECT")
    if not project_id:
        pytest.skip("BigQuery database not available")

    adapter = AsyncBigQueryAdapter(
        project_id=project_id,
        credentials_path=os.getenv("SQL_BATCHER_TEST_BIGQUERY_CREDENTIALS"),
        location=os.getenv("SQL_BATCHER_TEST_BIGQUERY_LOCATION"),
    )
    try:
        await adapter.connect()
        yield adapter
    finally:
        await adapter.disconnect()


@skip_if_no_db("postgresql")
@pytest.mark.asyncio
async def test_postgresql_integration(postgres_adapter):
    """Test PostgreSQL integration."""
    batcher = AsyncSQLBatcher(adapter=postgres_adapter, max_bytes=1000)

    # Create test table
    await postgres_adapter.execute(
        """
        CREATE TABLE IF NOT EXISTS test_async (
            id SERIAL PRIMARY KEY,
            name TEXT
        )
    """
    )

    try:
        # Test batch insert
        statements = [
            "INSERT INTO test_async (name) VALUES ('test1')",
            "INSERT INTO test_async (name) VALUES ('test2')",
            "INSERT INTO test_async (name) VALUES ('test3')",
        ]

        processed = await batcher.process_statements(
            statements, postgres_adapter.execute
        )
        assert processed == len(statements)

        # Verify data
        result = await postgres_adapter.execute("SELECT COUNT(*) FROM test_async")
        assert result[0][0] == len(statements)

    finally:
        # Cleanup
        await postgres_adapter.execute("DROP TABLE IF EXISTS test_async")


@skip_if_no_db("trino")
@pytest.mark.asyncio
async def test_trino_integration(trino_adapter):
    """Test Trino integration."""
    batcher = AsyncSQLBatcher(adapter=trino_adapter, max_bytes=1000)

    # Create test table
    await trino_adapter.execute(
        """
        CREATE TABLE IF NOT EXISTS test_async (
            id BIGINT,
            name VARCHAR
        )
    """
    )

    try:
        # Test batch insert
        statements = [
            "INSERT INTO test_async VALUES (1, 'test1')",
            "INSERT INTO test_async VALUES (2, 'test2')",
            "INSERT INTO test_async VALUES (3, 'test3')",
        ]

        processed = await batcher.process_statements(statements, trino_adapter.execute)
        assert processed == len(statements)

        # Verify data
        result = await trino_adapter.execute("SELECT COUNT(*) FROM test_async")
        assert result[0][0] == len(statements)

    finally:
        # Cleanup
        await trino_adapter.execute("DROP TABLE IF EXISTS test_async")


@skip_if_no_db("snowflake")
@pytest.mark.asyncio
async def test_snowflake_integration(snowflake_adapter):
    """Test Snowflake integration."""
    batcher = AsyncSQLBatcher(adapter=snowflake_adapter, max_bytes=1000)

    # Create test table
    await snowflake_adapter.execute(
        """
        CREATE TABLE IF NOT EXISTS test_async (
            id NUMBER,
            name STRING
        )
    """
    )

    try:
        # Test batch insert
        statements = [
            "INSERT INTO test_async VALUES (1, 'test1')",
            "INSERT INTO test_async VALUES (2, 'test2')",
            "INSERT INTO test_async VALUES (3, 'test3')",
        ]

        processed = await batcher.process_statements(
            statements, snowflake_adapter.execute
        )
        assert processed == len(statements)

        # Verify data
        result = await snowflake_adapter.execute("SELECT COUNT(*) FROM test_async")
        assert result[0][0] == len(statements)

    finally:
        # Cleanup
        await snowflake_adapter.execute("DROP TABLE IF EXISTS test_async")


@skip_if_no_db("bigquery")
@pytest.mark.asyncio
async def test_bigquery_integration(bigquery_adapter):
    """Test BigQuery integration."""
    batcher = AsyncSQLBatcher(adapter=bigquery_adapter, max_bytes=1000)

    # Create test table
    await bigquery_adapter.execute(
        """
        CREATE TABLE IF NOT EXISTS test_async (
            id INT64,
            name STRING
        )
    """
    )

    try:
        # Test batch insert
        statements = [
            "INSERT INTO test_async VALUES (1, 'test1')",
            "INSERT INTO test_async VALUES (2, 'test2')",
            "INSERT INTO test_async VALUES (3, 'test3')",
        ]

        processed = await batcher.process_statements(
            statements, bigquery_adapter.execute
        )
        assert processed == len(statements)

        # Verify data
        result = await bigquery_adapter.execute("SELECT COUNT(*) FROM test_async")
        assert result[0][0] == len(statements)

    finally:
        # Cleanup
        await bigquery_adapter.execute("DROP TABLE IF EXISTS test_async")


@skip_if_no_db("postgresql")
@pytest.mark.asyncio
async def test_postgresql_transaction_integration(postgres_adapter):
    """Test PostgreSQL transaction integration."""
    batcher = AsyncSQLBatcher(adapter=postgres_adapter, max_bytes=1000)

    # Create test table
    await postgres_adapter.execute(
        """
        CREATE TABLE IF NOT EXISTS test_async (
            id SERIAL PRIMARY KEY,
            name TEXT
        )
    """
    )

    try:
        # Test transaction
        await postgres_adapter.begin_transaction()

        statements = [
            "INSERT INTO test_async (name) VALUES ('test1')",
            "INSERT INTO test_async (name) VALUES ('test2')",
        ]

        processed = await batcher.process_statements(
            statements, postgres_adapter.execute
        )
        assert processed == len(statements)

        # Rollback and verify no data
        await postgres_adapter.rollback_transaction()
        result = await postgres_adapter.execute("SELECT COUNT(*) FROM test_async")
        assert result[0][0] == 0

        # Test commit
        await postgres_adapter.begin_transaction()
        processed = await batcher.process_statements(
            statements, postgres_adapter.execute
        )
        await postgres_adapter.commit_transaction()
        result = await postgres_adapter.execute("SELECT COUNT(*) FROM test_async")
        assert result[0][0] == len(statements)

    finally:
        # Cleanup
        await postgres_adapter.execute("DROP TABLE IF EXISTS test_async")
