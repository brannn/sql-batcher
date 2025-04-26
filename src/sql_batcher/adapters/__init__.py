"""
SQL Batcher adapters.

This package contains adapters for different database systems.
Adapters provide a consistent interface for SQL Batcher to
communicate with various database engines.
"""

from sql_batcher.adapters.base import SQLAdapter
from sql_batcher.adapters.generic import GenericAdapter

__all__ = ["SQLAdapter", "GenericAdapter"]

# Trino adapter is lazily imported to avoid hard dependency
try:
    pass

    __all__.append("TrinoAdapter")
except ImportError:
    pass

# Snowflake adapter is lazily imported to avoid hard dependency
try:
    pass

    __all__.append("SnowflakeAdapter")
except ImportError:
    pass

# Spark adapter is lazily imported to avoid hard dependency
try:
    pass

    __all__.append("SparkAdapter")
except ImportError:
    pass

# BigQuery adapter is lazily imported to avoid hard dependency
try:
    pass

    __all__.append("BigQueryAdapter")
except ImportError:
    pass

# PostgreSQL adapter is lazily imported to avoid hard dependency
try:
    pass

    __all__.append("PostgreSQLAdapter")
except ImportError:
    pass
