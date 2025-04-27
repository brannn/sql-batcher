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
from sql_batcher.hooks.plugins import (
    HookContext,
    HookType,
    Plugin,
    PluginManager,
    SQLPreprocessor,
    MetricsCollector,
    QueryLogger,
)
from sql_batcher.insert_merger import InsertMerger
from sql_batcher.query_collector import ListQueryCollector
from sql_batcher.retry_manager import RetryManager
from sql_batcher.batch_manager import BatchManager

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
    "HookContext",
    "HookType",
    "Plugin",
    "PluginManager",
    "SQLPreprocessor",
    "MetricsCollector",
    "QueryLogger",
    "InsertMerger",
    "ListQueryCollector",
    "RetryManager",
    "BatchManager",
]
