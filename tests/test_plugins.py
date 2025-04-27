"""Tests for the plugin system."""

from typing import Any, Dict, List
from unittest.mock import AsyncMock, MagicMock

import pytest

from sql_batcher.plugins import (
    HookContext,
    HookType,
    MetricsCollector,
    Plugin,
    PluginManager,
    QueryLogger,
    SQLPreprocessor,
)


class TestPlugin(Plugin):
    """Test plugin for testing the plugin system."""

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


@pytest.fixture
def plugin_manager():
    """Create a plugin manager for testing."""
    return PluginManager()


@pytest.fixture
def test_plugin():
    """Create a test plugin."""
    return TestPlugin()


@pytest.mark.asyncio
async def test_plugin_registration(plugin_manager, test_plugin):
    """Test plugin registration."""
    # Register plugin
    plugin_manager.register_plugin(test_plugin)
    assert test_plugin in plugin_manager.get_plugins()

    # Check hooks
    for hook_type in HookType:
        hooks = plugin_manager.get_hooks(hook_type)
        assert len(hooks) == 1


@pytest.mark.asyncio
async def test_plugin_unregistration(plugin_manager, test_plugin):
    """Test plugin unregistration."""
    # Register plugin
    plugin_manager.register_plugin(test_plugin)
    assert test_plugin in plugin_manager.get_plugins()

    # Unregister plugin
    plugin_manager.unregister_plugin("test_plugin")
    assert test_plugin not in plugin_manager.get_plugins()

    # Check hooks
    for hook_type in HookType:
        hooks = plugin_manager.get_hooks(hook_type)
        assert len(hooks) == 0


@pytest.mark.asyncio
async def test_hook_execution(plugin_manager, test_plugin):
    """Test hook execution."""
    # Register plugin
    plugin_manager.register_plugin(test_plugin)

    # Execute hooks
    statements = ["SELECT 1"]
    metadata = {"test": "data"}

    # Test pre-batch hook
    await plugin_manager.execute_hooks(HookType.PRE_BATCH, statements, metadata)
    assert test_plugin.pre_batch_called
    assert not test_plugin.post_batch_called

    # Test post-batch hook
    await plugin_manager.execute_hooks(HookType.POST_BATCH, statements, metadata)
    assert test_plugin.post_batch_called

    # Test pre-execute hook
    await plugin_manager.execute_hooks(HookType.PRE_EXECUTE, statements, metadata)
    assert test_plugin.pre_execute_called

    # Test post-execute hook
    await plugin_manager.execute_hooks(HookType.POST_EXECUTE, statements, metadata)
    assert test_plugin.post_execute_called

    # Test on-error hook
    error = Exception("Test error")
    await plugin_manager.execute_hooks(HookType.ON_ERROR, statements, metadata, error)
    assert test_plugin.on_error_called


@pytest.mark.asyncio
async def test_sql_preprocessor():
    """Test SQL preprocessor plugin."""

    # Create preprocessor
    def preprocess_sql(sql: str) -> str:
        return sql.replace("test", "TEST")

    preprocessor = SQLPreprocessor(preprocess_sql)

    # Test hooks
    statements = ["SELECT * FROM test_table"]
    context = HookContext(
        hook_type=HookType.PRE_BATCH,
        statements=statements,
        metadata={},
    )

    # Test pre-batch hook
    await preprocessor.get_hooks()[HookType.PRE_BATCH][0](context)
    assert context.statements[0] == "SELECT * FROM TEST_table"

    # Test pre-execute hook
    context = HookContext(
        hook_type=HookType.PRE_EXECUTE,
        statements=statements,
        metadata={},
    )
    await preprocessor.get_hooks()[HookType.PRE_EXECUTE][0](context)
    assert context.statements[0] == "SELECT * FROM TEST_table"


@pytest.mark.asyncio
async def test_metrics_collector():
    """Test metrics collector plugin."""
    # Create collector
    collector = MetricsCollector()

    # Test hooks
    statements = ["SELECT 1", "SELECT 2"]
    context = HookContext(
        hook_type=HookType.PRE_BATCH,
        statements=statements,
        metadata={},
    )

    # Test pre-batch hook
    await collector.get_hooks()[HookType.PRE_BATCH][0](context)
    metrics = collector.get_metrics()
    assert metrics["batch_size"] == 2
    assert metrics["batch_bytes"] > 0

    # Test post-batch hook
    await collector.get_hooks()[HookType.POST_BATCH][0](context)
    metrics = collector.get_metrics()
    assert metrics["successful_batches"] == 1

    # Test on-error hook
    error = Exception("Test error")
    context = HookContext(
        hook_type=HookType.ON_ERROR,
        statements=statements,
        metadata={},
        error=error,
    )
    await collector.get_hooks()[HookType.ON_ERROR][0](context)
    metrics = collector.get_metrics()
    assert metrics["failed_batches"] == 1
    assert str(error) in metrics["errors"]


@pytest.mark.asyncio
async def test_query_logger():
    """Test query logger plugin."""
    # Create logger
    log_calls = []

    def log_func(message: str) -> None:
        log_calls.append(message)

    logger = QueryLogger(log_func)

    # Test hooks
    statements = ["SELECT 1", "SELECT 2"]
    context = HookContext(
        hook_type=HookType.PRE_EXECUTE,
        statements=statements,
        metadata={},
    )

    # Test pre-execute hook
    await logger.get_hooks()[HookType.PRE_EXECUTE][0](context)
    assert len(log_calls) == 2
    assert "Executing: SELECT 1" in log_calls
    assert "Executing: SELECT 2" in log_calls

    # Test post-execute hook
    await logger.get_hooks()[HookType.POST_EXECUTE][0](context)
    assert "Executed 2 statements" in log_calls

    # Test on-error hook
    error = Exception("Test error")
    context = HookContext(
        hook_type=HookType.ON_ERROR,
        statements=statements,
        metadata={},
        error=error,
    )
    await logger.get_hooks()[HookType.ON_ERROR][0](context)
    assert f"Error: {error}" in log_calls


@pytest.mark.asyncio
async def test_multiple_plugins(plugin_manager):
    """Test multiple plugins working together."""
    # Create plugins
    preprocessor = SQLPreprocessor(lambda sql: sql.upper())
    collector = MetricsCollector()
    log_calls = []
    logger = QueryLogger(lambda msg: log_calls.append(msg))

    # Register plugins
    plugin_manager.register_plugin(preprocessor)
    plugin_manager.register_plugin(collector)
    plugin_manager.register_plugin(logger)

    # Execute hooks
    statements = ["select 1", "select 2"]
    metadata = {"test": "data"}

    # Test pre-batch hooks
    await plugin_manager.execute_hooks(HookType.PRE_BATCH, statements, metadata)
    assert statements[0] == "SELECT 1"
    assert statements[1] == "SELECT 2"
    assert collector.get_metrics()["batch_size"] == 2

    # Test post-batch hooks
    await plugin_manager.execute_hooks(HookType.POST_BATCH, statements, metadata)
    assert collector.get_metrics()["successful_batches"] == 1

    # Test error handling
    error = Exception("Test error")
    await plugin_manager.execute_hooks(HookType.ON_ERROR, statements, metadata, error)
    assert collector.get_metrics()["failed_batches"] == 1
    assert f"Error: {error}" in log_calls


@pytest.mark.asyncio
async def test_hook_error_handling(plugin_manager):
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
    plugin_manager.register_plugin(FailingPlugin())

    # Execute hooks - should not raise exception
    statements = ["SELECT 1"]
    await plugin_manager.execute_hooks(HookType.PRE_BATCH, statements, {})


@pytest.mark.asyncio
async def test_hook_context():
    """Test hook context."""
    # Create context
    statements = ["SELECT 1"]
    metadata = {"test": "data"}
    error = Exception("Test error")
    context = HookContext(
        hook_type=HookType.PRE_BATCH,
        statements=statements,
        metadata=metadata,
        error=error,
    )

    # Test context attributes
    assert context.hook_type == HookType.PRE_BATCH
    assert context.statements == statements
    assert context.metadata == metadata
    assert context.error == error

    # Test context modification
    context.statements.append("SELECT 2")
    assert len(context.statements) == 2
    assert "SELECT 2" in context.statements
