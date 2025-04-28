"""
Property-based tests for the RetryManager class using Hypothesis.
"""

import pytest
from hypothesis import given, strategies as st, settings, Verbosity
from sql_batcher.retry_manager import RetryManager
from sql_batcher.exceptions import RetryManagerError

# Configure Hypothesis settings
settings.register_profile("dev", verbosity=Verbosity.verbose)
settings.load_profile("dev")

# Custom strategies for retry manager tests
def max_retries():
    """Generate valid max retries values."""
    return st.integers(min_value=0, max_value=10)

def retry_delay():
    """Generate valid retry delay values."""
    return st.floats(min_value=0.1, max_value=60.0)

def retry_backoff():
    """Generate valid retry backoff values."""
    return st.floats(min_value=1.0, max_value=5.0)

def retry_key():
    """Generate valid retry keys."""
    return st.text(alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd', 'P')), min_size=1, max_size=50)

class TestRetryManager:
    @given(
        max_retries=max_retries(),
        retry_delay=retry_delay(),
        retry_backoff=retry_backoff()
    )
    def test_retry_manager_initialization(self, max_retries, retry_delay, retry_backoff):
        """Test RetryManager initialization with different parameters."""
        manager = RetryManager(max_retries=max_retries, retry_delay=retry_delay, retry_backoff=retry_backoff)
        assert manager.max_retries == max_retries
        assert manager.retry_delay == retry_delay
        assert manager.retry_backoff == retry_backoff

    @given(
        max_retries=max_retries(),
        retry_delay=retry_delay(),
        retry_backoff=retry_backoff(),
        key=retry_key()
    )
    def test_add_retry(self, max_retries, retry_delay, retry_backoff, key):
        """Test adding a retry."""
        manager = RetryManager(max_retries=max_retries, retry_delay=retry_delay, retry_backoff=retry_backoff)
        manager.add_retry(key)
        assert key in manager.retries
        assert manager.retries[key].attempts == 1

    @given(
        max_retries=max_retries(),
        retry_delay=retry_delay(),
        retry_backoff=retry_backoff(),
        key=retry_key()
    )
    def test_max_retries_limit(self, max_retries, retry_delay, retry_backoff, key):
        """Test that retries respect max retries limit."""
        manager = RetryManager(max_retries=max_retries, retry_delay=retry_delay, retry_backoff=retry_backoff)
        
        # Add retries up to max retries
        for _ in range(max_retries):
            manager.add_retry(key)
        
        # Verify retry attempts
        assert manager.retries[key].attempts == max_retries
        
        # Add one more retry
        with pytest.raises(RetryManagerError):
            manager.add_retry(key)

    @given(
        max_retries=max_retries(),
        retry_delay=retry_delay(),
        retry_backoff=retry_backoff(),
        key=retry_key()
    )
    def test_get_retry_delay(self, max_retries, retry_delay, retry_backoff, key):
        """Test getting retry delay."""
        manager = RetryManager(max_retries=max_retries, retry_delay=retry_delay, retry_backoff=retry_backoff)
        manager.add_retry(key)
        
        # Calculate expected delay
        expected_delay = retry_delay * (retry_backoff ** (manager.retries[key].attempts - 1))
        
        # Verify delay
        assert manager.get_retry_delay(key) == expected_delay

    @given(
        max_retries=max_retries(),
        retry_delay=retry_delay(),
        retry_backoff=retry_backoff(),
        key=retry_key()
    )
    def test_get_nonexistent_retry_delay(self, max_retries, retry_delay, retry_backoff, key):
        """Test getting delay for nonexistent retry."""
        manager = RetryManager(max_retries=max_retries, retry_delay=retry_delay, retry_backoff=retry_backoff)
        assert manager.get_retry_delay(key) == retry_delay

    @given(
        max_retries=max_retries(),
        retry_delay=retry_delay(),
        retry_backoff=retry_backoff(),
        key=retry_key()
    )
    def test_clear_retry(self, max_retries, retry_delay, retry_backoff, key):
        """Test clearing a retry."""
        manager = RetryManager(max_retries=max_retries, retry_delay=retry_delay, retry_backoff=retry_backoff)
        manager.add_retry(key)
        manager.clear_retry(key)
        assert key not in manager.retries

    @given(
        max_retries=max_retries(),
        retry_delay=retry_delay(),
        retry_backoff=retry_backoff(),
        key=retry_key()
    )
    def test_clear_nonexistent_retry(self, max_retries, retry_delay, retry_backoff, key):
        """Test clearing a nonexistent retry."""
        manager = RetryManager(max_retries=max_retries, retry_delay=retry_delay, retry_backoff=retry_backoff)
        manager.clear_retry(key)  # Should not raise an error

    @given(
        max_retries=max_retries(),
        retry_delay=retry_delay(),
        retry_backoff=retry_backoff(),
        key=retry_key()
    )
    def test_get_retry_attempts(self, max_retries, retry_delay, retry_backoff, key):
        """Test getting retry attempts."""
        manager = RetryManager(max_retries=max_retries, retry_delay=retry_delay, retry_backoff=retry_backoff)
        manager.add_retry(key)
        assert manager.get_retry_attempts(key) == 1

    @given(
        max_retries=max_retries(),
        retry_delay=retry_delay(),
        retry_backoff=retry_backoff(),
        key=retry_key()
    )
    def test_get_nonexistent_retry_attempts(self, max_retries, retry_delay, retry_backoff, key):
        """Test getting attempts for nonexistent retry."""
        manager = RetryManager(max_retries=max_retries, retry_delay=retry_delay, retry_backoff=retry_backoff)
        assert manager.get_retry_attempts(key) == 0

    @given(
        max_retries=max_retries(),
        retry_delay=retry_delay(),
        retry_backoff=retry_backoff(),
        key=retry_key()
    )
    def test_has_max_retries(self, max_retries, retry_delay, retry_backoff, key):
        """Test checking if retry has reached max retries."""
        manager = RetryManager(max_retries=max_retries, retry_delay=retry_delay, retry_backoff=retry_backoff)
        
        # Add retries up to max retries
        for _ in range(max_retries):
            manager.add_retry(key)
        
        # Verify max retries reached
        assert manager.has_max_retries(key)

    @given(
        max_retries=max_retries(),
        retry_delay=retry_delay(),
        retry_backoff=retry_backoff(),
        key=retry_key()
    )
    def test_has_not_max_retries(self, max_retries, retry_delay, retry_backoff, key):
        """Test checking if retry has not reached max retries."""
        manager = RetryManager(max_retries=max_retries, retry_delay=retry_delay, retry_backoff=retry_backoff)
        manager.add_retry(key)
        assert not manager.has_max_retries(key)

    @given(
        max_retries=max_retries(),
        retry_delay=retry_delay(),
        retry_backoff=retry_backoff(),
        key=retry_key()
    )
    def test_retry_delay_increases(self, max_retries, retry_delay, retry_backoff, key):
        """Test that retry delay increases with each attempt."""
        manager = RetryManager(max_retries=max_retries, retry_delay=retry_delay, retry_backoff=retry_backoff)
        
        # Add multiple retries
        for i in range(min(3, max_retries)):
            manager.add_retry(key)
            expected_delay = retry_delay * (retry_backoff ** i)
            assert manager.get_retry_delay(key) == expected_delay 