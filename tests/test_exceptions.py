"""
Property-based tests for the exceptions module using Hypothesis.
"""

import pytest
from hypothesis import given, strategies as st, settings, Verbosity
from sql_batcher.exceptions import (
    SQLBatcherError,
    BatchManagerError,
    RetryManagerError,
    HookManagerError,
    PluginError,
    InsertMergerError,
    AsyncBatcherError,
    QueryCollectorError
)

# Configure Hypothesis settings
settings.register_profile("dev", verbosity=Verbosity.verbose)
settings.load_profile("dev")

# Custom strategies for exception tests
def error_message():
    """Generate valid error messages."""
    return st.text(alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd', 'P', 'Zs')), min_size=1, max_size=100)

def error_code():
    """Generate valid error codes."""
    return st.integers(min_value=1000, max_value=9999)

class TestExceptions:
    @given(
        message=error_message(),
        code=error_code()
    )
    def test_sql_batcher_error(self, message, code):
        """Test SQLBatcherError."""
        error = SQLBatcherError(message, code)
        assert str(error) == message
        assert error.code == code
        assert isinstance(error, Exception)

    @given(
        message=error_message(),
        code=error_code()
    )
    def test_batch_manager_error(self, message, code):
        """Test BatchManagerError."""
        error = BatchManagerError(message, code)
        assert str(error) == message
        assert error.code == code
        assert isinstance(error, SQLBatcherError)

    @given(
        message=error_message(),
        code=error_code()
    )
    def test_retry_manager_error(self, message, code):
        """Test RetryManagerError."""
        error = RetryManagerError(message, code)
        assert str(error) == message
        assert error.code == code
        assert isinstance(error, SQLBatcherError)

    @given(
        message=error_message(),
        code=error_code()
    )
    def test_hook_manager_error(self, message, code):
        """Test HookManagerError."""
        error = HookManagerError(message, code)
        assert str(error) == message
        assert error.code == code
        assert isinstance(error, SQLBatcherError)

    @given(
        message=error_message(),
        code=error_code()
    )
    def test_plugin_error(self, message, code):
        """Test PluginError."""
        error = PluginError(message, code)
        assert str(error) == message
        assert error.code == code
        assert isinstance(error, SQLBatcherError)

    @given(
        message=error_message(),
        code=error_code()
    )
    def test_insert_merger_error(self, message, code):
        """Test InsertMergerError."""
        error = InsertMergerError(message, code)
        assert str(error) == message
        assert error.code == code
        assert isinstance(error, SQLBatcherError)

    @given(
        message=error_message(),
        code=error_code()
    )
    def test_async_batcher_error(self, message, code):
        """Test AsyncBatcherError."""
        error = AsyncBatcherError(message, code)
        assert str(error) == message
        assert error.code == code
        assert isinstance(error, SQLBatcherError)

    @given(
        message=error_message(),
        code=error_code()
    )
    def test_query_collector_error(self, message, code):
        """Test QueryCollectorError."""
        error = QueryCollectorError(message, code)
        assert str(error) == message
        assert error.code == code
        assert isinstance(error, SQLBatcherError)

    @given(
        message=error_message()
    )
    def test_error_without_code(self, message):
        """Test error without code."""
        error = SQLBatcherError(message)
        assert str(error) == message
        assert error.code == 1000  # Default code
        assert isinstance(error, Exception)

    @given(
        message=error_message(),
        code=error_code()
    )
    def test_error_inheritance(self, message, code):
        """Test error inheritance."""
        error = SQLBatcherError(message, code)
        assert isinstance(error, Exception)
        assert isinstance(error, SQLBatcherError)
        
        batch_error = BatchManagerError(message, code)
        assert isinstance(batch_error, Exception)
        assert isinstance(batch_error, SQLBatcherError)
        assert isinstance(batch_error, BatchManagerError)
        
        retry_error = RetryManagerError(message, code)
        assert isinstance(retry_error, Exception)
        assert isinstance(retry_error, SQLBatcherError)
        assert isinstance(retry_error, RetryManagerError)
        
        hook_error = HookManagerError(message, code)
        assert isinstance(hook_error, Exception)
        assert isinstance(hook_error, SQLBatcherError)
        assert isinstance(hook_error, HookManagerError)
        
        plugin_error = PluginError(message, code)
        assert isinstance(plugin_error, Exception)
        assert isinstance(plugin_error, SQLBatcherError)
        assert isinstance(plugin_error, PluginError)
        
        insert_error = InsertMergerError(message, code)
        assert isinstance(insert_error, Exception)
        assert isinstance(insert_error, SQLBatcherError)
        assert isinstance(insert_error, InsertMergerError)
        
        async_error = AsyncBatcherError(message, code)
        assert isinstance(async_error, Exception)
        assert isinstance(async_error, SQLBatcherError)
        assert isinstance(async_error, AsyncBatcherError)
        
        query_error = QueryCollectorError(message, code)
        assert isinstance(query_error, Exception)
        assert isinstance(query_error, SQLBatcherError)
        assert isinstance(query_error, QueryCollectorError)

    @given(
        message=error_message(),
        code=error_code()
    )
    def test_error_equality(self, message, code):
        """Test error equality."""
        error1 = SQLBatcherError(message, code)
        error2 = SQLBatcherError(message, code)
        error3 = SQLBatcherError("different message", code)
        error4 = SQLBatcherError(message, code + 1)
        
        assert error1 == error2
        assert error1 != error3
        assert error1 != error4
        assert error1 != "not an error"

    @given(
        message=error_message(),
        code=error_code()
    )
    def test_error_repr(self, message, code):
        """Test error representation."""
        error = SQLBatcherError(message, code)
        expected_repr = f"SQLBatcherError(message='{message}', code={code})"
        assert repr(error) == expected_repr 