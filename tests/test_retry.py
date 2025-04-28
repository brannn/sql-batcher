"""
Property-based tests for the retry module using Hypothesis.
"""

import pytest
import asyncio
from hypothesis import given, strategies as st, settings, Verbosity
from sql_batcher.retry import Retry, RetryError

# Configure Hypothesis settings
settings.register_profile("dev", verbosity=Verbosity.verbose)
settings.load_profile("dev")

# Custom strategies for retry tests
def max_retries():
    """Generate valid max retries values."""
    return st.integers(min_value=0, max_value=10)

def retry_delay():
    """Generate valid retry delay values."""
    return st.floats(min_value=0.1, max_value=60.0)

def retry_backoff():
    """Generate valid retry backoff values."""
    return st.floats(min_value=1.0, max_value=5.0)

def retry_data():
    """Generate valid retry data."""
    return st.dictionaries(
        keys=st.text(alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd', 'P')), min_size=1, max_size=20),
        values=st.one_of(
            st.integers(),
            st.floats(allow_infinity=False, allow_nan=False),
            st.text(alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd', 'P', 'Zs')), min_size=0, max_size=50),
            st.none()
        ),
        min_size=0,
        max_size=10
    )

class TestRetry:
    @given(
        max_retries=max_retries(),
        retry_delay=retry_delay(),
        retry_backoff=retry_backoff()
    )
    def test_retry_initialization(self, max_retries, retry_delay, retry_backoff):
        """Test Retry initialization with different parameters."""
        retry = Retry(max_retries=max_retries, retry_delay=retry_delay, retry_backoff=retry_backoff)
        assert retry.max_retries == max_retries
        assert retry.retry_delay == retry_delay
        assert retry.retry_backoff == retry_backoff

    @given(
        max_retries=max_retries(),
        retry_delay=retry_delay(),
        retry_backoff=retry_backoff(),
        data=retry_data()
    )
    def test_retry_success(self, max_retries, retry_delay, retry_backoff, data):
        """Test successful retry."""
        retry = Retry(max_retries=max_retries, retry_delay=retry_delay, retry_backoff=retry_backoff)
        
        def success_func():
            return data
        
        result = retry.execute(success_func)
        assert result == data
        assert retry.attempts == 0

    @given(
        max_retries=max_retries(),
        retry_delay=retry_delay(),
        retry_backoff=retry_backoff(),
        data=retry_data()
    )
    def test_retry_failure(self, max_retries, retry_delay, retry_backoff, data):
        """Test retry with failure."""
        retry = Retry(max_retries=max_retries, retry_delay=retry_delay, retry_backoff=retry_backoff)
        
        def fail_func():
            raise Exception("Test error")
        
        with pytest.raises(Exception):
            retry.execute(fail_func)
        
        assert retry.attempts == max_retries + 1

    @given(
        max_retries=max_retries(),
        retry_delay=retry_delay(),
        retry_backoff=retry_backoff(),
        data=retry_data()
    )
    def test_retry_partial_success(self, max_retries, retry_delay, retry_backoff, data):
        """Test retry with partial success."""
        retry = Retry(max_retries=max_retries, retry_delay=retry_delay, retry_backoff=retry_backoff)
        attempts = 0
        
        def partial_success_func():
            nonlocal attempts
            attempts += 1
            if attempts < 2:
                raise Exception("Test error")
            return data
        
        result = retry.execute(partial_success_func)
        assert result == data
        assert retry.attempts == 1

    @given(
        max_retries=max_retries(),
        retry_delay=retry_delay(),
        retry_backoff=retry_backoff(),
        data=retry_data()
    )
    def test_retry_delay_increases(self, max_retries, retry_delay, retry_backoff, data):
        """Test that retry delay increases with each attempt."""
        retry = Retry(max_retries=max_retries, retry_delay=retry_delay, retry_backoff=retry_backoff)
        
        def fail_func():
            raise Exception("Test error")
        
        try:
            retry.execute(fail_func)
        except Exception:
            pass
        
        expected_delay = retry_delay * (retry_backoff ** (retry.attempts - 1))
        assert retry.get_delay() == expected_delay

    @given(
        max_retries=max_retries(),
        retry_delay=retry_delay(),
        retry_backoff=retry_backoff(),
        data=retry_data()
    )
    def test_retry_reset(self, max_retries, retry_delay, retry_backoff, data):
        """Test resetting retry state."""
        retry = Retry(max_retries=max_retries, retry_delay=retry_delay, retry_backoff=retry_backoff)
        
        def fail_func():
            raise Exception("Test error")
        
        try:
            retry.execute(fail_func)
        except Exception:
            pass
        
        retry.reset()
        assert retry.attempts == 0
        assert retry.get_delay() == retry_delay

    @given(
        max_retries=max_retries(),
        retry_delay=retry_delay(),
        retry_backoff=retry_backoff(),
        data=retry_data()
    )
    def test_retry_with_custom_exception(self, max_retries, retry_delay, retry_backoff, data):
        """Test retry with custom exception."""
        retry = Retry(max_retries=max_retries, retry_delay=retry_delay, retry_backoff=retry_backoff)
        
        class CustomError(Exception):
            pass
        
        def fail_func():
            raise CustomError("Test error")
        
        with pytest.raises(CustomError):
            retry.execute(fail_func)
        
        assert retry.attempts == max_retries + 1

    @given(
        max_retries=max_retries(),
        retry_delay=retry_delay(),
        retry_backoff=retry_backoff(),
        data=retry_data()
    )
    def test_retry_with_exception_filter(self, max_retries, retry_delay, retry_backoff, data):
        """Test retry with exception filter."""
        retry = Retry(max_retries=max_retries, retry_delay=retry_delay, retry_backoff=retry_backoff)
        
        class RetryableError(Exception):
            pass
        
        class NonRetryableError(Exception):
            pass
        
        def fail_func():
            raise NonRetryableError("Test error")
        
        with pytest.raises(NonRetryableError):
            retry.execute(fail_func, exception_filter=lambda e: isinstance(e, RetryableError))
        
        assert retry.attempts == 0

    @given(
        max_retries=max_retries(),
        retry_delay=retry_delay(),
        retry_backoff=retry_backoff(),
        data=retry_data()
    )
    def test_retry_with_callback(self, max_retries, retry_delay, retry_backoff, data):
        """Test retry with callback."""
        retry = Retry(max_retries=max_retries, retry_delay=retry_delay, retry_backoff=retry_backoff)
        callback_called = False
        
        def callback(attempt, exception):
            nonlocal callback_called
            callback_called = True
            assert attempt == retry.attempts
            assert isinstance(exception, Exception)
        
        def fail_func():
            raise Exception("Test error")
        
        with pytest.raises(Exception):
            retry.execute(fail_func, callback=callback)
        
        assert callback_called
        assert retry.attempts == max_retries + 1

    @pytest.mark.asyncio
    @given(
        max_retries=max_retries(),
        retry_delay=retry_delay(),
        retry_backoff=retry_backoff(),
        data=retry_data()
    )
    async def test_async_retry(self, max_retries, retry_delay, retry_backoff, data):
        """Test async retry."""
        retry = Retry(max_retries=max_retries, retry_delay=retry_delay, retry_backoff=retry_backoff)
        
        async def async_func():
            return data
        
        result = await retry.execute_async(async_func)
        assert result == data
        assert retry.attempts == 0

    @pytest.mark.asyncio
    @given(
        max_retries=max_retries(),
        retry_delay=retry_delay(),
        retry_backoff=retry_backoff(),
        data=retry_data()
    )
    async def test_async_retry_failure(self, max_retries, retry_delay, retry_backoff, data):
        """Test async retry with failure."""
        retry = Retry(max_retries=max_retries, retry_delay=retry_delay, retry_backoff=retry_backoff)
        
        async def async_fail_func():
            raise Exception("Test error")
        
        with pytest.raises(Exception):
            await retry.execute_async(async_fail_func)
        
        assert retry.attempts == max_retries + 1 