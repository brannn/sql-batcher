"""
Plugin system for SQL Batcher.

This module provides the plugin system for SQL Batcher, allowing for extensibility
and customization of the batching process.
"""

import asyncio
from enum import Enum
from typing import Any, Awaitable, Callable, Dict, List, Optional, TypeVar

T = TypeVar("T")


class HookType(Enum):
    """Types of hooks that can be registered."""

    PRE_BATCH = "pre_batch"
    POST_BATCH = "post_batch"
    PRE_EXECUTE = "pre_execute"
    POST_EXECUTE = "post_execute"
    ON_ERROR = "on_error"


class Plugin:
    """
    Base class for SQL Batcher plugins.

    This class defines the interface that all plugins must implement.
    Each plugin can register hooks for different stages of the batching process.
    """

    def __init__(self, name: str) -> None:
        """Initialize the plugin.

        Args:
            name: Name of the plugin
        """
        self.name = name
        self._hooks: Dict[HookType, List[Callable[..., Any]]] = {}

    def register_hook(self, hook_type: HookType, hook: Callable[..., Any]) -> None:
        """Register a hook for a specific event.

        Args:
            hook_type: Type of hook to register
            hook: Hook function to register
        """
        if hook_type not in self._hooks:
            self._hooks[hook_type] = []
        self._hooks[hook_type].append(hook)

    def get_hooks(self, hook_type: HookType) -> List[Callable[..., Any]]:
        """Get all hooks registered for a specific event.

        Args:
            hook_type: Type of hook to get

        Returns:
            List of registered hooks
        """
        return self._hooks.get(hook_type, [])

    def clear_hooks(self, hook_type: Optional[HookType] = None) -> None:
        """Clear all hooks or hooks of a specific type.

        Args:
            hook_type: Optional type of hooks to clear
        """
        if hook_type:
            self._hooks.pop(hook_type, None)
        else:
            self._hooks.clear()


class PluginManager:
    """
    Manager for SQL Batcher plugins.

    This class manages the registration and execution of plugins and their hooks.
    """

    def __init__(self) -> None:
        """Initialize the plugin manager."""
        self._plugins: Dict[str, Plugin] = {}

    def register_plugin(self, plugin: Plugin) -> None:
        """Register a plugin.

        Args:
            plugin: Plugin to register
        """
        self._plugins[plugin.name] = plugin

    def unregister_plugin(self, plugin_name: str) -> None:
        """Unregister a plugin by name.

        Args:
            plugin_name: Name of the plugin to unregister
        """
        self._plugins.pop(plugin_name, None)

    def get_plugins(self) -> List[Plugin]:
        """Get all registered plugins.

        Returns:
            List of registered plugins
        """
        return list(self._plugins.values())

    async def execute_hooks(
        self,
        hook_type: HookType,
        statements: List[str],
        metadata: Optional[Dict[str, Any]] = None,
        error: Optional[Exception] = None,
    ) -> None:
        """Execute all hooks registered for a specific event.

        Args:
            hook_type: Type of hook to execute
            statements: List of SQL statements being processed
            metadata: Optional metadata for the hook
            error: Optional error that occurred
        """
        for plugin in self._plugins.values():
            for hook in plugin.get_hooks(hook_type):
                if asyncio.iscoroutinefunction(hook):
                    await hook(statements, metadata, error)
                else:
                    hook(statements, metadata, error)
