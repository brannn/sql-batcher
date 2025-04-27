"""
SQL Batcher adapters.

This module provides adapters for various SQL databases.
"""

from sql_batcher.adapters.async_bigquery import AsyncBigQueryAdapter
from sql_batcher.adapters.async_postgresql import AsyncPostgreSQLAdapter
from sql_batcher.adapters.async_snowflake import AsyncSnowflakeAdapter
from sql_batcher.adapters.async_trino import AsyncTrinoAdapter
from sql_batcher.adapters.bigquery import BigQueryAdapter
from sql_batcher.adapters.postgresql import PostgreSQLAdapter
from sql_batcher.adapters.snowflake import SnowflakeAdapter
from sql_batcher.adapters.spark import SparkAdapter
from sql_batcher.adapters.trino import TrinoAdapter

__all__ = [
    "AsyncBigQueryAdapter",
    "AsyncPostgreSQLAdapter",
    "AsyncSnowflakeAdapter",
    "AsyncTrinoAdapter",
    "BigQueryAdapter",
    "PostgreSQLAdapter",
    "SnowflakeAdapter",
    "SparkAdapter",
    "TrinoAdapter",
]

# Optional adapters are imported lazily to avoid hard dependencies
try:
    from sql_batcher.adapters.async_bigquery import AsyncBigQueryAdapter

    __all__.append("AsyncBigQueryAdapter")
except ImportError:
    pass

try:
    from sql_batcher.adapters.async_postgresql import AsyncPostgreSQLAdapter

    __all__.append("AsyncPostgreSQLAdapter")
except ImportError:
    pass

try:
    from sql_batcher.adapters.async_snowflake import AsyncSnowflakeAdapter

    __all__.append("AsyncSnowflakeAdapter")
except ImportError:
    pass

try:
    from sql_batcher.adapters.async_trino import AsyncTrinoAdapter

    __all__.append("AsyncTrinoAdapter")
except ImportError:
    pass

try:
    from sql_batcher.adapters.bigquery import BigQueryAdapter

    __all__.append("BigQueryAdapter")
except ImportError:
    pass

try:
    from sql_batcher.adapters.postgresql import PostgreSQLAdapter

    __all__.append("PostgreSQLAdapter")
except ImportError:
    pass

try:
    from sql_batcher.adapters.snowflake import SnowflakeAdapter

    __all__.append("SnowflakeAdapter")
except ImportError:
    pass

try:
    from sql_batcher.adapters.spark import SparkAdapter

    __all__.append("SparkAdapter")
except ImportError:
    pass

try:
    from sql_batcher.adapters.trino import TrinoAdapter

    __all__.append("TrinoAdapter")
except ImportError:
    pass
