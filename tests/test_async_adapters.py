"""Tests for async adapters."""

from typing import Any, Optional
from unittest.mock import AsyncMock, MagicMock

import pytest

from sql_batcher.adapters.async_bigquery import AsyncBigQueryAdapter
from sql_batcher.adapters.async_postgresql import AsyncPostgreSQLAdapter
from sql_batcher.adapters.async_snowflake import AsyncSnowflakeAdapter
from sql_batcher.adapters.async_trino import AsyncTrinoAdapter
from sql_batcher.exceptions import AdapterConnectionError, AdapterExecutionError


@pytest.fixture
def mock_asyncpg_pool():
    """Create a mock asyncpg pool."""
    pool = AsyncMock()
    pool.acquire.return_value.__aenter__.return_value = AsyncMock()
    return pool


@pytest.fixture
def mock_trino_client():
    """Create a mock Trino client."""
    client = AsyncMock()
    client.execute.return_value = []
    return client


@pytest.fixture
def mock_snowflake_conn():
    """Create a mock Snowflake connection."""
    conn = AsyncMock()
    cursor = AsyncMock()
    conn.cursor.return_value = cursor
    return conn


@pytest.fixture
def mock_bigquery_client():
    """Create a mock BigQuery client."""
    client = AsyncMock()
    job = AsyncMock()
    client.query.return_value = job
    return client


@pytest.mark.asyncio
async def test_postgresql_adapter(mock_asyncpg_pool, monkeypatch):
    """Test PostgreSQL adapter."""
    # Mock asyncpg.create_pool
    monkeypatch.setattr(
        "asyncpg.create_pool", AsyncMock(return_value=mock_asyncpg_pool)
    )

    adapter = AsyncPostgreSQLAdapter(
        dsn="postgresql://user:pass@localhost:5432/dbname",
        min_size=1,
        max_size=1,
    )

    # Test connect
    await adapter.connect()
    assert adapter.pool == mock_asyncpg_pool

    # Test execute
    await adapter.execute("SELECT 1")
    mock_asyncpg_pool.acquire.return_value.__aenter__.return_value.execute.assert_called_once_with(
        "SELECT 1"
    )

    # Test disconnect
    await adapter.disconnect()
    mock_asyncpg_pool.close.assert_called_once()

    # Test error handling
    mock_asyncpg_pool.acquire.return_value.__aenter__.return_value.execute.side_effect = Exception(
        "Test error"
    )
    with pytest.raises(AdapterExecutionError):
        await adapter.execute("SELECT 1")


@pytest.mark.asyncio
async def test_trino_adapter(mock_trino_client, monkeypatch):
    """Test Trino adapter."""
    # Mock TrinoClient
    monkeypatch.setattr(
        "trino.async_client.TrinoClient", MagicMock(return_value=mock_trino_client)
    )

    adapter = AsyncTrinoAdapter(
        host="trino.example.com",
        port=8080,
        user="trino",
    )

    # Test connect
    await adapter.connect()
    assert adapter.client == mock_trino_client

    # Test execute
    await adapter.execute("SELECT 1")
    mock_trino_client.execute.assert_called_once_with("SELECT 1")

    # Test disconnect
    await adapter.disconnect()
    mock_trino_client.close.assert_called_once()

    # Test error handling
    mock_trino_client.execute.side_effect = Exception("Test error")
    with pytest.raises(AdapterExecutionError):
        await adapter.execute("SELECT 1")


@pytest.mark.asyncio
async def test_snowflake_adapter(mock_snowflake_conn, monkeypatch):
    """Test Snowflake adapter."""
    # Mock snowflake.connect
    monkeypatch.setattr(
        "snowflake.connector.async_connector.connect",
        AsyncMock(return_value=mock_snowflake_conn),
    )

    adapter = AsyncSnowflakeAdapter(
        account="test_account",
        user="test_user",
        password="test_password",
    )

    # Test connect
    await adapter.connect()
    assert adapter.conn == mock_snowflake_conn

    # Test execute
    await adapter.execute("SELECT 1")
    mock_snowflake_conn.cursor.return_value.execute.assert_called_once_with("SELECT 1")

    # Test disconnect
    await adapter.disconnect()
    mock_snowflake_conn.close.assert_called_once()

    # Test error handling
    mock_snowflake_conn.cursor.return_value.execute.side_effect = Exception(
        "Test error"
    )
    with pytest.raises(AdapterExecutionError):
        await adapter.execute("SELECT 1")


@pytest.mark.asyncio
async def test_bigquery_adapter(mock_bigquery_client, monkeypatch):
    """Test BigQuery adapter."""
    # Mock bigquery.Client
    monkeypatch.setattr(
        "google.cloud.bigquery.Client", MagicMock(return_value=mock_bigquery_client)
    )

    adapter = AsyncBigQueryAdapter(
        project_id="test_project",
    )

    # Test connect
    await adapter.connect()
    assert adapter.client == mock_bigquery_client

    # Test execute
    await adapter.execute("SELECT 1")
    mock_bigquery_client.query.assert_called_once_with("SELECT 1")

    # Test disconnect
    await adapter.disconnect()
    mock_bigquery_client.close.assert_called_once()

    # Test error handling
    mock_bigquery_client.query.side_effect = Exception("Test error")
    with pytest.raises(AdapterExecutionError):
        await adapter.execute("SELECT 1")


@pytest.mark.asyncio
async def test_adapter_connection_errors():
    """Test connection error handling for all adapters."""
    # PostgreSQL
    adapter = AsyncPostgreSQLAdapter(dsn="invalid")
    with pytest.raises(AdapterConnectionError):
        await adapter.connect()

    # Trino
    adapter = AsyncTrinoAdapter(host="invalid")
    with pytest.raises(AdapterConnectionError):
        await adapter.connect()

    # Snowflake
    adapter = AsyncSnowflakeAdapter(
        account="invalid", user="invalid", password="invalid"
    )
    with pytest.raises(AdapterConnectionError):
        await adapter.connect()

    # BigQuery
    adapter = AsyncBigQueryAdapter(project_id="invalid")
    with pytest.raises(AdapterConnectionError):
        await adapter.connect()


@pytest.mark.asyncio
async def test_adapter_transaction_handling(mock_asyncpg_pool, monkeypatch):
    """Test transaction handling for all adapters."""
    # Mock asyncpg.create_pool
    monkeypatch.setattr(
        "asyncpg.create_pool", AsyncMock(return_value=mock_asyncpg_pool)
    )

    # Test PostgreSQL adapter
    adapter = AsyncPostgreSQLAdapter(
        dsn="postgresql://user:pass@localhost:5432/dbname",
        min_size=1,
        max_size=1,
    )
    await adapter.connect()

    # Test transaction methods
    await adapter.begin_transaction()
    mock_asyncpg_pool.acquire.return_value.__aenter__.return_value.execute.assert_called_with(
        "BEGIN"
    )

    await adapter.commit_transaction()
    mock_asyncpg_pool.acquire.return_value.__aenter__.return_value.execute.assert_called_with(
        "COMMIT"
    )

    await adapter.rollback_transaction()
    mock_asyncpg_pool.acquire.return_value.__aenter__.return_value.execute.assert_called_with(
        "ROLLBACK"
    )

    # Test error handling
    mock_asyncpg_pool.acquire.return_value.__aenter__.return_value.execute.side_effect = Exception(
        "Test error"
    )
    with pytest.raises(AdapterExecutionError):
        await adapter.begin_transaction()
