"""SQL Batcher - A Python library for efficient SQL batching.

This package provides tools for batching SQL statements, managing database
connections, and optimizing database operations.
"""

from sql_batcher.async_batcher import AsyncSQLBatcher
from sql_batcher.batch_manager import BatchManager
from sql_batcher.batcher import SQLBatcher
from sql_batcher.exceptions import (
    InsertMergerError,
    PluginError,
    QueryCollectorError,
    RetryError,
    SQLBatcherError,
)
from sql_batcher.hook_manager import HookManager
from sql_batcher.retry_manager import RetryManager

__version__ = "0.1.0"

__all__ = [
    "SQLBatcher",
    "AsyncSQLBatcher",
    "BatchManager",
    "RetryManager",
    "HookManager",
    "SQLBatcherError",
    "RetryError",
    "PluginError",
    "QueryCollectorError",
    "InsertMergerError",
]
