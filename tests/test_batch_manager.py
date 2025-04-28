"""
Property-based tests for the BatchManager class using Hypothesis.
"""

import pytest
from hypothesis import given, strategies as st, settings, Verbosity
from sql_batcher.batch_manager import BatchManager
from sql_batcher.exceptions import BatchManagerError

# Configure Hypothesis settings
settings.register_profile("dev", verbosity=Verbosity.verbose)
settings.load_profile("dev")

# Custom strategies for batch manager tests
def batch_size():
    """Generate valid batch sizes."""
    return st.integers(min_value=1, max_value=1000)

def batch_timeout():
    """Generate valid batch timeouts."""
    return st.floats(min_value=0.1, max_value=60.0)

def batch_key():
    """Generate valid batch keys."""
    return st.text(alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd', 'P')), min_size=1, max_size=50)

class TestBatchManager:
    @given(
        batch_size=batch_size(),
        batch_timeout=batch_timeout()
    )
    def test_batch_manager_initialization(self, batch_size, batch_timeout):
        """Test BatchManager initialization with different parameters."""
        manager = BatchManager(batch_size=batch_size, batch_timeout=batch_timeout)
        assert manager.batch_size == batch_size
        assert manager.batch_timeout == batch_timeout

    @given(
        batch_size=batch_size(),
        batch_timeout=batch_timeout(),
        key=batch_key()
    )
    def test_add_to_batch(self, batch_size, batch_timeout, key):
        """Test adding items to a batch."""
        manager = BatchManager(batch_size=batch_size, batch_timeout=batch_timeout)
        manager.add_to_batch(key, "test_value")
        assert key in manager.batches
        assert len(manager.batches[key]) == 1

    @given(
        batch_size=batch_size(),
        batch_timeout=batch_timeout(),
        key=batch_key()
    )
    def test_batch_size_limit(self, batch_size, batch_timeout, key):
        """Test that batches respect size limits."""
        manager = BatchManager(batch_size=batch_size, batch_timeout=batch_timeout)
        
        # Add items up to batch size
        for i in range(batch_size):
            manager.add_to_batch(key, f"value_{i}")
        
        # Verify batch size
        assert len(manager.batches[key]) == batch_size
        
        # Add one more item
        manager.add_to_batch(key, "overflow")
        
        # Verify batch was flushed
        assert key not in manager.batches or len(manager.batches[key]) == 1

    @given(
        batch_size=batch_size(),
        batch_timeout=batch_timeout(),
        key=batch_key()
    )
    def test_batch_timeout(self, batch_size, batch_timeout, key):
        """Test that batches respect timeout limits."""
        manager = BatchManager(batch_size=batch_size, batch_timeout=batch_timeout)
        manager.add_to_batch(key, "test_value")
        
        # Simulate timeout
        manager.batches[key].last_update = 0
        
        # Add another item
        manager.add_to_batch(key, "new_value")
        
        # Verify batch was flushed
        assert key not in manager.batches or len(manager.batches[key]) == 1

    @given(
        batch_size=batch_size(),
        batch_timeout=batch_timeout(),
        key=batch_key()
    )
    def test_flush_batch(self, batch_size, batch_timeout, key):
        """Test flushing a batch."""
        manager = BatchManager(batch_size=batch_size, batch_timeout=batch_timeout)
        
        # Add items
        for i in range(batch_size):
            manager.add_to_batch(key, f"value_{i}")
        
        # Flush batch
        items = manager.flush_batch(key)
        
        # Verify flush
        assert len(items) == batch_size
        assert key not in manager.batches

    @given(
        batch_size=batch_size(),
        batch_timeout=batch_timeout(),
        key=batch_key()
    )
    def test_flush_nonexistent_batch(self, batch_size, batch_timeout, key):
        """Test flushing a nonexistent batch."""
        manager = BatchManager(batch_size=batch_size, batch_timeout=batch_timeout)
        items = manager.flush_batch(key)
        assert len(items) == 0

    @given(
        batch_size=batch_size(),
        batch_timeout=batch_timeout(),
        key=batch_key()
    )
    def test_get_batch_size(self, batch_size, batch_timeout, key):
        """Test getting batch size."""
        manager = BatchManager(batch_size=batch_size, batch_timeout=batch_timeout)
        
        # Add items
        for i in range(batch_size):
            manager.add_to_batch(key, f"value_{i}")
        
        # Verify batch size
        assert manager.get_batch_size(key) == batch_size

    @given(
        batch_size=batch_size(),
        batch_timeout=batch_timeout(),
        key=batch_key()
    )
    def test_get_nonexistent_batch_size(self, batch_size, batch_timeout, key):
        """Test getting size of nonexistent batch."""
        manager = BatchManager(batch_size=batch_size, batch_timeout=batch_timeout)
        assert manager.get_batch_size(key) == 0

    @given(
        batch_size=batch_size(),
        batch_timeout=batch_timeout(),
        key=batch_key()
    )
    def test_clear_batch(self, batch_size, batch_timeout, key):
        """Test clearing a batch."""
        manager = BatchManager(batch_size=batch_size, batch_timeout=batch_timeout)
        
        # Add items
        for i in range(batch_size):
            manager.add_to_batch(key, f"value_{i}")
        
        # Clear batch
        manager.clear_batch(key)
        
        # Verify batch is cleared
        assert key not in manager.batches

    @given(
        batch_size=batch_size(),
        batch_timeout=batch_timeout(),
        key=batch_key()
    )
    def test_clear_nonexistent_batch(self, batch_size, batch_timeout, key):
        """Test clearing a nonexistent batch."""
        manager = BatchManager(batch_size=batch_size, batch_timeout=batch_timeout)
        manager.clear_batch(key)  # Should not raise an error

    @given(
        batch_size=batch_size(),
        batch_timeout=batch_timeout(),
        key=batch_key()
    )
    def test_batch_is_full(self, batch_size, batch_timeout, key):
        """Test checking if a batch is full."""
        manager = BatchManager(batch_size=batch_size, batch_timeout=batch_timeout)
        
        # Add items up to batch size
        for i in range(batch_size):
            manager.add_to_batch(key, f"value_{i}")
        
        # Verify batch is full
        assert manager.is_batch_full(key)

    @given(
        batch_size=batch_size(),
        batch_timeout=batch_timeout(),
        key=batch_key()
    )
    def test_batch_is_not_full(self, batch_size, batch_timeout, key):
        """Test checking if a batch is not full."""
        manager = BatchManager(batch_size=batch_size, batch_timeout=batch_timeout)
        
        # Add fewer items than batch size
        for i in range(batch_size - 1):
            manager.add_to_batch(key, f"value_{i}")
        
        # Verify batch is not full
        assert not manager.is_batch_full(key)

    @given(
        batch_size=batch_size(),
        batch_timeout=batch_timeout(),
        key=batch_key()
    )
    def test_batch_has_timed_out(self, batch_size, batch_timeout, key):
        """Test checking if a batch has timed out."""
        manager = BatchManager(batch_size=batch_size, batch_timeout=batch_timeout)
        manager.add_to_batch(key, "test_value")
        
        # Simulate timeout
        manager.batches[key].last_update = 0
        
        # Verify batch has timed out
        assert manager.has_batch_timed_out(key)

    @given(
        batch_size=batch_size(),
        batch_timeout=batch_timeout(),
        key=batch_key()
    )
    def test_batch_has_not_timed_out(self, batch_size, batch_timeout, key):
        """Test checking if a batch has not timed out."""
        manager = BatchManager(batch_size=batch_size, batch_timeout=batch_timeout)
        manager.add_to_batch(key, "test_value")
        
        # Verify batch has not timed out
        assert not manager.has_batch_timed_out(key) 