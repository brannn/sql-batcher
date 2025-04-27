"""Hook manager for SQL Batcher.

This module manages the plugin system and hook execution.
"""

from enum import Enum, auto
from typing import Any, Callable, Dict, List, Optional

from sql_batcher.hooks.plugins import HookContext, HookType, Plugin


class HookManager:
    """Manages plugins and their hooks."""

    def __init__(self) -> None:
        """Initialize the hook manager."""
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
