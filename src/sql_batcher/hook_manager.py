"""
Hook manager for SQL Batcher.

This module provides functionality to manage and execute hooks for SQL Batcher.
"""

from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Set, Type

from sql_batcher.exceptions import PluginError
from sql_batcher.hooks.plugins import HookContext, HookType, Plugin


class HookManager:
    """Manages plugins and their hooks."""

    def __init__(self) -> None:
        """Initialize the hook manager."""
        self._plugins: Dict[str, Plugin] = {}  # Store plugins by name
        self._hooks: Dict[HookType, List[Callable[[HookContext], Any]]] = defaultdict(list)

    def register_plugin(self, plugin: Plugin) -> None:
        """Register a plugin.

        Args:
            plugin: The plugin to register
        """
        name = plugin.get_name()
        if name in self._plugins:
            # If plugin already exists, remove its hooks first
            self.unregister_plugin(name)
        
        self._plugins[name] = plugin
        for hook_type, hooks in plugin.get_hooks().items():
            self._hooks[hook_type].extend(hooks)

    def unregister_plugin(self, plugin_name: str) -> None:
        """Unregister a plugin by name.

        Args:
            plugin_name: Name of the plugin to unregister
        """
        if plugin_name not in self._plugins:
            return

        plugin = self._plugins[plugin_name]
        plugin_hooks = plugin.get_hooks()
        
        # Remove all hooks for this plugin
        for hook_type in HookType:
            if hook_type in plugin_hooks:
                self._hooks[hook_type] = [
                    hook for hook in self._hooks[hook_type]
                    if hook not in plugin_hooks[hook_type]
                ]
        
        # Remove the plugin
        del self._plugins[plugin_name]

    def get_plugins(self) -> List[Plugin]:
        """Get all registered plugins.

        Returns:
            List of registered plugins
        """
        return list(self._plugins.values())

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
