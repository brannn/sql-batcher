"""Tests for async batcher context."""

from typing import Any, Callable, Dict, List
from unittest.mock import AsyncMock

import pytest

from sql_batcher.async_batcher import AsyncSQLBatcher
from sql_batcher.adapters.base import AsyncSQLAdapter
from sql_batcher.exceptions import AdapterConnectionError
from sql_batcher.hooks.plugins import HookContext, HookType, Plugin, PluginManager


class TestPlugin(Plugin):
    """Test plugin for async batcher context."""

    def __init__(self):
        """Initialize the test plugin."""
        self.initialized = False
        self.cleaned_up = False

    def get_name(self) -> str:
        """Get the plugin name."""
        return "test_plugin"

    def get_hooks(self) -> Dict[HookType, List[Callable[[HookContext], Any]]]:
        """Get the hooks registered by this plugin."""
        async def pre_batch_hook(context: HookContext) -> None:
            self.initialized = True

        async def post_batch_hook(context: HookContext) -> None:
            self.cleaned_up = True

        return {
            HookType.PRE_BATCH: [pre_batch_hook],
            HookType.POST_BATCH: [post_batch_hook],
        }


@pytest.fixture
def mock_adapter():
    """Create a mock adapter for testing."""
    adapter = AsyncMock(spec=AsyncSQLAdapter)
    adapter.execute = AsyncMock(return_value=None)
    return adapter


@pytest.fixture
def test_plugin():
    """Create a test plugin."""
    return TestPlugin()


@pytest.fixture
def async_batcher(mock_adapter, test_plugin):
    """Create an async batcher with a test plugin."""
    batcher = AsyncSQLBatcher(adapter=mock_adapter)
    batcher.register_plugin(test_plugin)
    return batcher


@pytest.mark.asyncio
async def test_context_manager_normal_flow(async_batcher, test_plugin):
    """Test normal flow of context manager."""
    async with async_batcher as batcher:
        # Add a statement to trigger hooks
        batcher.add_statement("SELECT 1")
        # Flush to trigger hooks
        await batcher.flush(lambda sql: None)
        assert test_plugin.initialized
        assert not test_plugin.cleaned_up

    assert test_plugin.initialized
    assert test_plugin.cleaned_up


@pytest.mark.asyncio
async def test_context_manager_error_flow(async_batcher, test_plugin):
    """Test error flow of context manager."""
    with pytest.raises(Exception):
        async with async_batcher as batcher:
            # Add a statement to trigger hooks
            batcher.add_statement("SELECT 1")
            # Flush to trigger hooks
            await batcher.flush(lambda sql: None)
            assert test_plugin.initialized
            assert not test_plugin.cleaned_up
            raise Exception("Test error")

    assert test_plugin.initialized
    assert test_plugin.cleaned_up


@pytest.mark.asyncio
async def test_context_manager_empty_batch(mock_adapter, test_plugin):
    """Test context manager with empty batch."""
    # Create batcher with test plugin
    batcher = AsyncSQLBatcher(mock_adapter)
    batcher.register_plugin(test_plugin)

    # Use context manager without adding statements
    async with batcher as b:
        pass

    # Verify plugin state
    assert test_plugin.initialized
    assert test_plugin.cleaned_up
    assert not test_plugin.error_handled

    # Verify adapter was not called
    assert mock_adapter.execute.call_count == 0


@pytest.mark.asyncio
async def test_context_manager_multiple_plugins(mock_adapter):
    """Test context manager with multiple plugins."""
    # Create plugins
    plugin1 = TestPlugin()
    plugin2 = TestPlugin()

    # Create batcher with multiple plugins
    batcher = AsyncSQLBatcher(mock_adapter)
    batcher.register_plugin(plugin1)
    batcher.register_plugin(plugin2)

    # Use context manager
    async with batcher as b:
        b.add_statement("SELECT 1")

    # Verify both plugins were initialized and cleaned up
    assert plugin1.initialized and plugin2.initialized
    assert plugin1.cleaned_up and plugin2.cleaned_up
    assert not plugin1.error_handled and not plugin2.error_handled


@pytest.mark.asyncio
async def test_context_manager_with_partial_batch(mock_adapter, test_plugin):
    """Test context manager with partially filled batch."""
    # Create batcher with test plugin
    batcher = AsyncSQLBatcher(mock_adapter)
    batcher.register_plugin(test_plugin)

    # Use context manager with partial batch
    async with batcher as b:
        # Add statement but don't reach flush threshold
        b.add_statement("SELECT 1")

    # Verify plugin state
    assert test_plugin.initialized
    assert test_plugin.cleaned_up
    assert not test_plugin.error_handled

    # Verify partial batch was flushed on exit
    assert mock_adapter.execute.call_count == 1


@pytest.mark.asyncio
async def test_context_manager_with_plugin_error(mock_adapter):
    """Test context manager when plugin hook raises error."""

    class ErrorPlugin(Plugin):
        """Plugin that raises an error in its hook."""

        def get_name(self) -> str:
            return "error_plugin"

        def get_hooks(self) -> dict[HookType, list[Any]]:
            async def error_hook(context: HookContext) -> None:
                raise Exception("Plugin error")

            return {HookType.PRE_BATCH: [error_hook]}

    # Create batcher with error plugin
    batcher = AsyncSQLBatcher(mock_adapter)
    batcher.register_plugin(ErrorPlugin())

    # Use context manager
    with pytest.raises(Exception, match="Plugin error"):
        async with batcher as b:
            b.add_statement("SELECT 1")

    # Verify adapter was not called
    assert mock_adapter.execute.call_count == 0


@pytest.mark.asyncio
async def test_async_batcher_context_manager_error_handling():
    """Test error handling in the async batcher context manager."""
    mock_adapter = AsyncMock()
    mock_adapter.execute = AsyncMock(side_effect=AdapterConnectionError("Test error"))

    async with AsyncSQLBatcher(adapter=mock_adapter) as _:
        with pytest.raises(AdapterConnectionError):
            await mock_adapter.execute("SELECT 1")
