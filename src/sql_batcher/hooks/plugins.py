"""Plugin system for SQL Batcher.

This module provides functionality for plugins and hooks in the SQL Batcher system.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional, TypeVar

from sql_batcher.exceptions import InsertMergerError


class HookType(Enum):
    """Hook types for plugins.
    
    These hooks allow plugins to intercept and modify behavior at key points
    in the SQL batching process.
    """

    PRE_BATCH = "pre_batch"  # Called before processing a batch of statements
    POST_BATCH = "post_batch"  # Called after processing a batch of statements
    PRE_EXECUTE = "pre_execute"  # Called before executing SQL statements
    POST_EXECUTE = "post_execute"  # Called after executing SQL statements
    ON_ERROR = "on_error"  # Called when an error occurs during processing


class HookContext:
    """Context object passed to hooks."""
    def __init__(self, **kwargs: Any) -> None:
        """Initialize the context with the given data."""
        self._data = kwargs
        # Set default attributes
        self.statements: List[str] = kwargs.get('statements', [])
        self.metadata: Dict[str, Any] = kwargs.get('metadata', {})
        self.error: Optional[Exception] = kwargs.get('error')
        self.hook_type: Optional[HookType] = kwargs.get('hook_type')

    def get(self, key: str, default: Any = None) -> Any:
        """Get a value from the context."""
        return self._data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """Set a value in the context."""
        self._data[key] = value
        if hasattr(self, key):
            setattr(self, key, value)


T = TypeVar("T")


class Plugin(ABC):
    """Base class for plugins."""
    def __init__(self, name: str) -> None:
        """Initialize the plugin.
        
        Args:
            name: Name of the plugin
        """
        self.name = name
        self._hooks: Dict[HookType, List[Callable[[HookContext], Any]]] = {}

    def get_name(self) -> str:
        """Get the plugin name."""
        return self.name

    def get_hooks(self, hook_type: Optional[HookType] = None) -> Dict[HookType, List[Callable[[HookContext], Any]]]:
        """Get the plugin's hooks.
        
        Args:
            hook_type: Optional hook type to filter by
            
        Returns:
            Dictionary of hook types to hook functions
        """
        if hook_type is None:
            return self._hooks
        return {hook_type: self._hooks.get(hook_type, [])}

    def register_hook(self, hook_type: HookType, hook: Callable[[HookContext], Any]) -> None:
        """Register a hook.
        
        Args:
            hook_type: Type of hook to register
            hook: Hook function to register
        """
        if hook_type not in self._hooks:
            self._hooks[hook_type] = []
        self._hooks[hook_type].append(hook)

    def clear_hooks(self) -> None:
        """Clear all registered hooks."""
        self._hooks.clear()


class PluginManager:
    """Manager for plugins."""
    def __init__(self) -> None:
        """Initialize the plugin manager."""
        self._plugins: Dict[str, Plugin] = {}
        self._hooks: Dict[HookType, List[Callable[[HookContext], Any]]] = {}

    def register_plugin(self, plugin: Plugin) -> None:
        """Register a plugin.
        
        Args:
            plugin: Plugin to register
        """
        if plugin.get_name() in self._plugins:
            raise InsertMergerError(f"Plugin {plugin.get_name()} already registered")
        self._plugins[plugin.get_name()] = plugin
        self._update_hooks()

    def unregister_plugin(self, name: str) -> None:
        """Unregister a plugin.
        
        Args:
            name: Name of plugin to unregister
        """
        if name not in self._plugins:
            raise InsertMergerError(f"Plugin {name} not registered")
        del self._plugins[name]
        self._update_hooks()

    def get_plugins(self) -> List[Plugin]:
        """Get all registered plugins."""
        return list(self._plugins.values())

    def get_hooks(self, hook_type: HookType) -> List[Callable[[HookContext], Any]]:
        """Get hooks of a specific type.
        
        Args:
            hook_type: Type of hooks to get
            
        Returns:
            List of hook functions
        """
        return self._hooks.get(hook_type, [])

    def _update_hooks(self) -> None:
        """Update the hook registry."""
        self._hooks.clear()
        for plugin in self._plugins.values():
            for hook_type, hooks in plugin.get_hooks().items():
                if hook_type not in self._hooks:
                    self._hooks[hook_type] = []
                self._hooks[hook_type].extend(hooks)

    async def execute_hooks(
        self,
        hook_type: HookType,
        statements: List[str],
        metadata: Optional[Dict[str, Any]] = None,
        error: Optional[Exception] = None,
    ) -> None:
        """Execute hooks of a specific type.
        
        Args:
            hook_type: Type of hooks to execute
            statements: SQL statements being processed
            metadata: Optional metadata to pass to hooks
            error: Optional error that occurred
        """
        context = HookContext(
            hook_type=hook_type,
            statements=statements,
            metadata=metadata or {},
            error=error,
        )

        for hook in self.get_hooks(hook_type):
            try:
                await hook(context)
            except Exception as e:
                # If it's an error hook, don't re-raise
                if hook_type == HookType.ON_ERROR:
                    continue
                # For other hooks, raise the error
                raise e


class SQLPreprocessor(Plugin):
    """Plugin for preprocessing SQL statements."""

    def __init__(self, preprocess_func: Callable[[str], str]) -> None:
        """Initialize the SQL preprocessor.

        Args:
            preprocess_func: Function to preprocess SQL statements
        """
        super().__init__("sql_preprocessor")
        self._preprocess_func = preprocess_func

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
        super().__init__("metrics_collector")
        self._metrics: Dict[str, Any] = {}

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
        super().__init__("query_logger")
        self._log_func = log_func

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
