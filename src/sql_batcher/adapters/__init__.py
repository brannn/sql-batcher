"""
SQL Batcher adapters.

This package contains adapters for different database systems.
Adapters provide a consistent interface for SQL Batcher to
communicate with various database engines.
"""

from sql_batcher.adapters.async_base import AsyncGenericAdapter, AsyncSQLAdapter
from sql_batcher.adapters.base import SQLAdapter
from sql_batcher.adapters.generic import GenericAdapter

__all__ = ["SQLAdapter", "GenericAdapter", "AsyncSQLAdapter", "AsyncGenericAdapter"]

# Trino adapter is lazily imported to avoid hard dependency
try:
    pass

    __all__.append("TrinoAdapter")
except ImportError:
    pass

# Async Trino adapter is lazily imported to avoid hard dependency
try:
    pass

    __all__.append("AsyncTrinoAdapter")
except ImportError:
    pass

# Snowflake adapter is lazily imported to avoid hard dependency
try:
    pass

    __all__.append("SnowflakeAdapter")
except ImportError:
    pass

# Async Snowflake adapter is lazily imported to avoid hard dependency
try:
    pass

    __all__.append("AsyncSnowflakeAdapter")
except ImportError:
    pass

# BigQuery adapter is lazily imported to avoid hard dependency
try:
    pass

    __all__.append("BigQueryAdapter")
except ImportError:
    pass

# Async BigQuery adapter is lazily imported to avoid hard dependency
try:
    pass

    __all__.append("AsyncBigQueryAdapter")
except ImportError:
    pass

# PostgreSQL adapter is lazily imported to avoid hard dependency
try:
    pass

    __all__.append("PostgreSQLAdapter")
except ImportError:
    pass

# Async PostgreSQL adapter is lazily imported to avoid hard dependency
try:
    pass

    __all__.append("AsyncPostgreSQLAdapter")
except ImportError:
    pass
