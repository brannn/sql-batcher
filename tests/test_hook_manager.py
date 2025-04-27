"""Tests for the hook manager."""

from typing import Any, Dict, List

import pytest

from sql_batcher.hook_manager import HookManager
from sql_batcher.hooks.plugins import HookContext, HookType, Plugin


class TestPlugin(Plugin):
    """Test plugin for testing the hook manager."""

    def __init__(self) -> None:
        """Initialize the test plugin."""
        self.pre_batch_called = False
        self.post_batch_called = False
        self.pre_execute_called = False
        self.post_execute_called = False
        self.on_error_called = False

    def get_name(self) -> str:
        """Get the plugin name."""
        return "test_plugin"

    def get_hooks(self) -> Dict[HookType, List[Any]]:
        """Get the hooks registered by this plugin."""

        async def pre_batch_hook(context: HookContext) -> None:
            self.pre_batch_called = True

        async def post_batch_hook(context: HookContext) -> None:
            self.post_batch_called = True

        async def pre_execute_hook(context: HookContext) -> None:
            self.pre_execute_called = True

        async def post_execute_hook(context: HookContext) -> None:
            self.post_execute_called = True

        async def on_error_hook(context: HookContext) -> None:
            self.on_error_called = True

        return {
            HookType.PRE_BATCH: [pre_batch_hook],
            HookType.POST_BATCH: [post_batch_hook],
            HookType.PRE_EXECUTE: [pre_execute_hook],
            HookType.POST_EXECUTE: [post_execute_hook],
            HookType.ON_ERROR: [on_error_hook],
        }


class TestHookManager:
    """Test suite for HookManager."""

    @pytest.fixture
    def hook_manager(self) -> HookManager:
        """Create a HookManager instance for testing."""
        return HookManager()

    @pytest.fixture
    def test_plugin(self) -> TestPlugin:
        """Create a test plugin."""
        return TestPlugin()

    @pytest.mark.asyncio
    async def test_plugin_registration(
        self, hook_manager: HookManager, test_plugin: TestPlugin
    ) -> None:
        """Test plugin registration."""
        # Register plugin
        hook_manager.register_plugin(test_plugin)
        assert test_plugin in hook_manager.get_plugins()

        # Check hooks
        for hook_type in HookType:
            hooks = hook_manager.get_hooks(hook_type)
            assert len(hooks) == 1

    @pytest.mark.asyncio
    async def test_plugin_unregistration(
        self, hook_manager: HookManager, test_plugin: TestPlugin
    ) -> None:
        """Test plugin unregistration."""
        # Register plugin
        hook_manager.register_plugin(test_plugin)
        assert test_plugin in hook_manager.get_plugins()

        # Unregister plugin
        hook_manager.unregister_plugin("test_plugin")
        assert test_plugin not in hook_manager.get_plugins()

        # Check hooks
        for hook_type in HookType:
            hooks = hook_manager.get_hooks(hook_type)
            assert len(hooks) == 0

    @pytest.mark.asyncio
    async def test_hook_execution(
        self, hook_manager: HookManager, test_plugin: TestPlugin
    ) -> None:
        """Test hook execution."""
        # Register plugin
        hook_manager.register_plugin(test_plugin)

        # Execute hooks
        statements = ["SELECT 1"]
        metadata = {"test": "data"}

        # Test pre-batch hook
        await hook_manager.execute_hooks(HookType.PRE_BATCH, statements, metadata)
        assert test_plugin.pre_batch_called
        assert not test_plugin.post_batch_called

        # Test post-batch hook
        await hook_manager.execute_hooks(HookType.POST_BATCH, statements, metadata)
        assert test_plugin.post_batch_called

        # Test pre-execute hook
        await hook_manager.execute_hooks(HookType.PRE_EXECUTE, statements, metadata)
        assert test_plugin.pre_execute_called

        # Test post-execute hook
        await hook_manager.execute_hooks(HookType.POST_EXECUTE, statements, metadata)
        assert test_plugin.post_execute_called

        # Test on-error hook
        error = Exception("Test error")
        await hook_manager.execute_hooks(HookType.ON_ERROR, statements, metadata, error)
        assert test_plugin.on_error_called

    @pytest.mark.asyncio
    async def test_multiple_plugins(self, hook_manager: HookManager) -> None:
        """Test multiple plugins working together."""
        # Create plugins
        plugin1 = TestPlugin()
        plugin2 = TestPlugin()

        # Register plugins
        hook_manager.register_plugin(plugin1)
        hook_manager.register_plugin(plugin2)

        # Execute hooks
        statements = ["SELECT 1"]
        metadata = {"test": "data"}

        # Test pre-batch hooks
        await hook_manager.execute_hooks(HookType.PRE_BATCH, statements, metadata)
        assert plugin1.pre_batch_called
        assert plugin2.pre_batch_called

        # Test post-batch hooks
        await hook_manager.execute_hooks(HookType.POST_BATCH, statements, metadata)
        assert plugin1.post_batch_called
        assert plugin2.post_batch_called

    @pytest.mark.asyncio
    async def test_hook_error_handling(self, hook_manager: HookManager) -> None:
        """Test error handling in hooks."""

        # Create plugin with failing hook
        class FailingPlugin(Plugin):
            def get_name(self) -> str:
                return "failing_plugin"

            def get_hooks(self) -> Dict[HookType, List[Any]]:
                async def failing_hook(context: HookContext) -> None:
                    raise Exception("Hook failed")

                return {
                    HookType.PRE_BATCH: [failing_hook],
                }

        # Register plugin
        hook_manager.register_plugin(FailingPlugin())

        # Execute hooks - should not raise exception
        statements = ["SELECT 1"]
        await hook_manager.execute_hooks(HookType.PRE_BATCH, statements, {})

    @pytest.mark.asyncio
    async def test_hook_context(self, hook_manager: HookManager) -> None:
        """Test hook context creation and usage."""

        # Create plugin that modifies context
        class ContextModifyingPlugin(Plugin):
            def get_name(self) -> str:
                return "context_modifying_plugin"

            def get_hooks(self) -> Dict[HookType, List[Any]]:
                async def modify_context(context: HookContext) -> None:
                    context.metadata["modified"] = True
                    context.statements.append("SELECT 2")

                return {
                    HookType.PRE_BATCH: [modify_context],
                }

        # Register plugin
        hook_manager.register_plugin(ContextModifyingPlugin())

        # Execute hooks
        statements = ["SELECT 1"]
        metadata = {"test": "data"}
        await hook_manager.execute_hooks(HookType.PRE_BATCH, statements, metadata)

        # Check context modifications
        assert metadata["modified"]
        assert len(statements) == 2
        assert "SELECT 2" in statements
