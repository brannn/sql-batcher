"""Hooks package for SQL Batcher.

This package contains the plugin system and hook management.
"""

from sql_batcher.hooks.plugins import (
    HookContext,
    HookType,
    MetricsCollector,
    Plugin,
    PluginManager,
    QueryLogger,
    SQLPreprocessor,
)

__all__ = [
    "Plugin",
    "PluginManager",
    "HookType",
    "HookContext",
    "SQLPreprocessor",
    "MetricsCollector",
    "QueryLogger",
]
