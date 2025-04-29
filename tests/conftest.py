"""
Test configuration and fixtures.
"""

import os
from typing import Any, Dict, List
from unittest.mock import MagicMock

import pytest


# Define test markers
def pytest_configure(config: Any) -> None:
    """Configure pytest with custom markers and options."""
    # Add markers
    config.addinivalue_line("markers", "core: tests that don't require database connections")
    config.addinivalue_line("markers", "db: tests that require actual database connections")
    config.addinivalue_line("markers", "postgresql: tests that require PostgreSQL database connections")
    config.addinivalue_line("markers", "snowflake: tests that require Snowflake database connections")
    config.addinivalue_line("markers", "trino: tests that require Trino database connections")
    config.addinivalue_line("markers", "bigquery: tests that require BigQuery database connections")

    # Add command-line options
    config.addinivalue_line("addopts", "--postgres --snowflake --trino --bigquery")


def pytest_addoption(parser: Any) -> None:
    """Add command-line options for database tests."""
    parser.addoption(
        "--postgres",
        action="store_true",
        default=False,
        help="Run PostgreSQL tests",
    )
    parser.addoption(
        "--snowflake",
        action="store_true",
        default=False,
        help="Run Snowflake tests",
    )
    parser.addoption(
        "--trino",
        action="store_true",
        default=False,
        help="Run Trino tests",
    )
    parser.addoption(
        "--bigquery",
        action="store_true",
        default=False,
        help="Run BigQuery tests",
    )


def pytest_collection_modifyitems(config: Any, items: List[Any]) -> None:
    """Add markers to tests based on their requirements and skip if needed."""
    # Add markers based on test file names
    for item in items:
        if "test_postgresql_adapter" in item.nodeid and "_mock" not in item.nodeid:
            item.add_marker(pytest.mark.postgresql)
        elif "test_snowflake_adapter" in item.nodeid and "_mock" not in item.nodeid:
            item.add_marker(pytest.mark.snowflake)
        elif "test_trino_adapter" in item.nodeid and "_mock" not in item.nodeid:
            item.add_marker(pytest.mark.trino)
        elif "test_bigquery_adapter" in item.nodeid and "_mock" not in item.nodeid:
            item.add_marker(pytest.mark.bigquery)

    # Skip tests based on available connections
    skip_postgres = pytest.mark.skip(reason="PostgreSQL connection not available")
    skip_snowflake = pytest.mark.skip(reason="Snowflake connection not available")
    skip_trino = pytest.mark.skip(reason="Trino connection not available")
    skip_bigquery = pytest.mark.skip(reason="BigQuery connection not available")

    for item in items:
        if "postgresql" in item.keywords and not config.getoption("--postgres"):
            item.add_marker(skip_postgres)
        elif "snowflake" in item.keywords and not config.getoption("--snowflake"):
            item.add_marker(skip_snowflake)
        elif "trino" in item.keywords and not config.getoption("--trino"):
            item.add_marker(skip_trino)
        elif "bigquery" in item.keywords and not config.getoption("--bigquery"):
            item.add_marker(skip_bigquery)


@pytest.fixture(scope="session")
def has_postgres_connection() -> bool:
    """Check if PostgreSQL connection is available."""
    try:
        import psycopg2

        conn = psycopg2.connect(
            host=os.getenv("POSTGRES_HOST", "localhost"),
            port=int(os.getenv("POSTGRES_PORT", "5432")),
            user=os.getenv("POSTGRES_USER", "postgres"),
            password=os.getenv("POSTGRES_PASSWORD", "postgres"),
            database=os.getenv("POSTGRES_DB", "postgres"),
        )
        conn.close()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def has_snowflake_connection() -> bool:
    """Check if Snowflake connection is available."""
    try:
        import snowflake.connector

        conn = snowflake.connector.connect(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
        )
        conn.close()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def has_trino_connection() -> bool:
    """Check if Trino connection is available."""
    try:
        import trino

        conn = trino.dbapi.connect(
            host=os.getenv("TRINO_HOST", "localhost"),
            port=int(os.getenv("TRINO_PORT", "8080")),
            user=os.getenv("TRINO_USER", "trino"),
            catalog=os.getenv("TRINO_CATALOG"),
            schema=os.getenv("TRINO_SCHEMA"),
        )
        conn.close()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def has_bigquery_connection() -> bool:
    """Check if BigQuery connection is available."""
    try:
        from google.cloud import bigquery

        client = bigquery.Client()
        client.close()
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def postgres_connection_params() -> Dict[str, Any]:
    """Get PostgreSQL connection parameters."""
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
        "database": os.getenv("POSTGRES_DB", "postgres"),
    }


@pytest.fixture(scope="session")
def snowflake_connection_params() -> Dict[str, str]:
    """Get Snowflake connection parameters."""
    return {
        "account": os.getenv("SNOWFLAKE_ACCOUNT", ""),
        "user": os.getenv("SNOWFLAKE_USER", ""),
        "password": os.getenv("SNOWFLAKE_PASSWORD", ""),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", ""),
        "database": os.getenv("SNOWFLAKE_DATABASE", ""),
        "schema": os.getenv("SNOWFLAKE_SCHEMA", ""),
    }


@pytest.fixture(scope="session")
def trino_connection_params() -> Dict[str, Any]:
    """Get Trino connection parameters."""
    return {
        "host": os.getenv("TRINO_HOST", "localhost"),
        "port": int(os.getenv("TRINO_PORT", "8080")),
        "user": os.getenv("TRINO_USER", "trino"),
        "catalog": os.getenv("TRINO_CATALOG", ""),
        "schema": os.getenv("TRINO_SCHEMA", ""),
    }


@pytest.fixture(scope="session")
def bigquery_connection_params() -> Dict[str, str]:
    """Get BigQuery connection parameters."""
    return {
        "project_id": os.getenv("BIGQUERY_PROJECT_ID", ""),
        "dataset_id": os.getenv("BIGQUERY_DATASET_ID", ""),
        "location": os.getenv("BIGQUERY_LOCATION", "US"),
    }


@pytest.fixture
def mock_db_connection() -> MagicMock:
    """Create a mock database connection for testing."""
    mock_connection = MagicMock()
    mock_cursor = MagicMock()

    # Configure cursor with initial state
    mock_cursor.description = [
        ["id", "INT", None, None, None, None, None],
        ["name", "VARCHAR", None, None, None, None, None],
    ]
    mock_cursor.fetchone.return_value = (1, "Test")
    mock_cursor.fetchmany.return_value = [(1, "Test")]
    mock_cursor.fetchall.return_value = [(1, "Test")]

    # Configure cursor to handle execute calls
    def execute_side_effect(sql: str) -> None:
        if sql.strip().upper().startswith("SELECT"):
            mock_cursor.description = [
                ["id", "INT", None, None, None, None, None],
                ["name", "VARCHAR", None, None, None, None, None],
            ]
            mock_cursor.fetchall.return_value = [(1, "Test")]
        else:
            mock_cursor.description = None
            mock_cursor.fetchall.return_value = []
        return None

    # Set up the execute method with side effect
    setattr(mock_cursor, "execute", MagicMock(side_effect=execute_side_effect))

    # Set up cursor reset
    setattr(mock_cursor, "reset_mock", MagicMock(return_value=None))

    # Configure connection to return the mock cursor
    setattr(mock_connection, "cursor", MagicMock(return_value=mock_cursor))

    return mock_connection
