"""
SQL Batcher package.

This package provides utilities for batching SQL statements
and executing them efficiently.
"""

__version__ = "0.1.0"

# Import core components
from sql_batcher.adapters.base import AsyncSQLAdapter, SQLAdapter
from sql_batcher.async_batcher import AsyncSQLBatcher
from sql_batcher.batcher import SQLBatcher
from sql_batcher.exceptions import (
    AdapterConnectionError,
    AdapterExecutionError,
    BatchSizeExceededError,
    MaxRetriesExceededError,
    TimeoutError,
)

__all__ = [
    "AsyncSQLAdapter",
    "AsyncSQLBatcher",
    "SQLAdapter",
    "SQLBatcher",
    "AdapterConnectionError",
    "AdapterExecutionError",
    "BatchSizeExceededError",
    "MaxRetriesExceededError",
    "TimeoutError",
]
