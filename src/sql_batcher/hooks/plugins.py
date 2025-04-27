"""Plugin system for SQL Batcher.

This module provides a plugin system for extending SQL Batcher functionality
through hooks and custom processors.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, TypeVar


class HookType(Enum):
    """Types of hooks available in the plugin system."""

    PRE_BATCH = auto()
    POST_BATCH = auto()
    PRE_EXECUTE = auto()
    POST_EXECUTE = auto()
    ON_ERROR = auto()


@dataclass
class HookContext:
    """Context information passed to hooks."""

    hook_type: HookType
    statements: List[str]
    metadata: Dict[str, Any]
    error: Optional[Exception] = None


T = TypeVar("T")


class Plugin(ABC):
    """Base class for sql-batcher plugins."""

    @abstractmethod
    def get_name(self) -> str:
        """Get the plugin name."""

    @abstractmethod
    def get_hooks(self) -> Dict[HookType, List[Callable[[HookContext], Any]]]:
        """Get the hooks registered by this plugin."""


class PluginManager:
    """Manages plugins and their hooks."""

    def __init__(self) -> None:
        """Initialize the plugin manager."""
        self._plugins: List[Plugin] = []
        self._hooks: Dict[HookType, List[Callable[[HookContext], Any]]] = {
            hook_type: [] for hook_type in HookType
        }

    def register_plugin(self, plugin: Plugin) -> None:
        """Register a plugin.

        Args:
            plugin: The plugin to register
        """
        self._plugins.append(plugin)
        for hook_type, hooks in plugin.get_hooks().items():
            self._hooks[hook_type].extend(hooks)

    def unregister_plugin(self, plugin_name: str) -> None:
        """Unregister a plugin by name.

        Args:
            plugin_name: Name of the plugin to unregister
        """
        for plugin in self._plugins[:]:
            if plugin.get_name() == plugin_name:
                self._plugins.remove(plugin)
                # Remove plugin's hooks
                for hook_type in HookType:
                    self._hooks[hook_type] = [
                        hook
                        for hook in self._hooks[hook_type]
                        if hook not in plugin.get_hooks()[hook_type]
                    ]

    def get_plugins(self) -> List[Plugin]:
        """Get all registered plugins.

        Returns:
            List of registered plugins
        """
        return self._plugins.copy()

    def get_hooks(self, hook_type: HookType) -> List[Callable[[HookContext], Any]]:
        """Get hooks of a specific type.

        Args:
            hook_type: Type of hooks to get

        Returns:
            List of hooks of the specified type
        """
        return self._hooks[hook_type].copy()

    async def execute_hooks(
        self,
        hook_type: HookType,
        statements: List[str],
        metadata: Optional[Dict[str, Any]] = None,
        error: Optional[Exception] = None,
    ) -> List[Any]:
        """Execute hooks of a specific type.

        Args:
            hook_type: Type of hooks to execute
            statements: SQL statements being processed
            metadata: Optional metadata to pass to hooks
            error: Optional error that occurred

        Returns:
            List of results from hook execution
        """
        context = HookContext(
            hook_type=hook_type,
            statements=statements,
            metadata=metadata or {},
            error=error,
        )

        results = []
        for hook in self._hooks[hook_type]:
            try:
                result = await hook(context)
                results.append(result)
            except Exception as e:
                # Log hook execution error but continue with other hooks
                print(f"Error executing hook {hook.__name__}: {e}")

        return results


class SQLPreprocessor(Plugin):
    """Plugin for preprocessing SQL statements."""

    def __init__(self, preprocess_func: Callable[[str], str]) -> None:
        """Initialize the SQL preprocessor.

        Args:
            preprocess_func: Function to preprocess SQL statements
        """
        self._preprocess_func = preprocess_func

    def get_name(self) -> str:
        """Get the plugin name."""
        return "sql_preprocessor"

    def get_hooks(self) -> Dict[HookType, List[Callable[[HookContext], Any]]]:
        """Get the hooks registered by this plugin."""

        async def pre_batch_hook(context: HookContext) -> None:
            context.statements[:] = [
                self._preprocess_func(stmt) for stmt in context.statements
            ]

        async def pre_execute_hook(context: HookContext) -> None:
            context.statements[:] = [
                self._preprocess_func(stmt) for stmt in context.statements
            ]

        return {
            HookType.PRE_BATCH: [pre_batch_hook],
            HookType.PRE_EXECUTE: [pre_execute_hook],
        }


class MetricsCollector(Plugin):
    """Plugin for collecting execution metrics."""

    def __init__(self) -> None:
        """Initialize the metrics collector."""
        self._metrics: Dict[str, Any] = {}

    def get_name(self) -> str:
        """Get the plugin name."""
        return "metrics_collector"

    def get_hooks(self) -> Dict[HookType, List[Callable[[HookContext], Any]]]:
        """Get the hooks registered by this plugin."""

        async def pre_batch_hook(context: HookContext) -> None:
            self._metrics["batch_size"] = len(context.statements)
            self._metrics["batch_bytes"] = sum(
                len(s.encode()) for s in context.statements
            )

        async def post_batch_hook(context: HookContext) -> None:
            self._metrics["successful_batches"] = (
                self._metrics.get("successful_batches", 0) + 1
            )

        async def on_error_hook(context: HookContext) -> None:
            self._metrics["failed_batches"] = self._metrics.get("failed_batches", 0) + 1
            if context.error:
                self._metrics.setdefault("errors", []).append(str(context.error))

        return {
            HookType.PRE_BATCH: [pre_batch_hook],
            HookType.POST_BATCH: [post_batch_hook],
            HookType.ON_ERROR: [on_error_hook],
        }

    def get_metrics(self) -> Dict[str, Any]:
        """Get collected metrics.

        Returns:
            Dictionary of collected metrics
        """
        return self._metrics.copy()


class QueryLogger(Plugin):
    """Plugin for logging SQL queries."""

    def __init__(self, log_func: Callable[[str], None]) -> None:
        """Initialize the query logger.

        Args:
            log_func: Function to log queries
        """
        self._log_func = log_func

    def get_name(self) -> str:
        """Get the plugin name."""
        return "query_logger"

    def get_hooks(self) -> Dict[HookType, List[Callable[[HookContext], Any]]]:
        """Get the hooks registered by this plugin."""

        async def pre_execute_hook(context: HookContext) -> None:
            for stmt in context.statements:
                self._log_func(f"Executing: {stmt}")

        async def post_execute_hook(context: HookContext) -> None:
            self._log_func(f"Executed {len(context.statements)} statements")

        async def on_error_hook(context: HookContext) -> None:
            if context.error:
                self._log_func(f"Error: {context.error}")

        return {
            HookType.PRE_EXECUTE: [pre_execute_hook],
            HookType.POST_EXECUTE: [post_execute_hook],
            HookType.ON_ERROR: [on_error_hook],
        }
