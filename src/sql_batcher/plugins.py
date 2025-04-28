"""
Plugin system for SQL Batcher.

This module provides the base Plugin class and related functionality
for extending SQL Batcher with plugins.
"""

from enum import Enum
from typing import Any, Dict, List, Optional

from sql_batcher.hooks.plugins import HookType


class Plugin:
    """Base class for SQL Batcher plugins."""

    def __init__(self, name: str) -> None:
        """Initialize a plugin.

        Args:
            name: Unique name for the plugin
        """
        self.name = name
        self._hooks: Dict[HookType, List[Any]] = {
            hook_type: [] for hook_type in HookType
        }

    def get_name(self) -> str:
        """Get the plugin name.

        Returns:
            The plugin name
        """
        return self.name

    def get_hooks(self, hook_type: Optional[HookType] = None) -> Dict[HookType, List[Any]]:
        """Get registered hooks.

        Args:
            hook_type: Optional specific hook type to get

        Returns:
            Dictionary mapping hook types to lists of hook functions
        """
        if hook_type is not None:
            return {hook_type: self._hooks[hook_type]}
        return self._hooks

    def register_hook(self, hook_type: HookType, hook_fn: Any) -> None:
        """Register a hook function.

        Args:
            hook_type: Type of hook to register
            hook_fn: Hook function to register
        """
        self._hooks[hook_type].append(hook_fn)

    def clear_hooks(self, hook_type: Optional[HookType] = None) -> None:
        """Clear registered hooks.

        Args:
            hook_type: Optional specific hook type to clear
        """
        if hook_type is not None:
            self._hooks[hook_type] = []
        else:
            for hooks in self._hooks.values():
                hooks.clear()
