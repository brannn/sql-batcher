"""
Property-based tests for the AsyncBatcher class using Hypothesis and pytest-asyncio.
"""

import pytest
import asyncio
from hypothesis import given, strategies as st, settings, Verbosity
from sql_batcher.async_batcher import AsyncBatcher
from sql_batcher.exceptions import AsyncBatcherError

# Configure Hypothesis settings
settings.register_profile("dev", verbosity=Verbosity.verbose)
settings.load_profile("dev")

# Custom strategies for async batcher tests
def batch_size():
    """Generate valid batch sizes."""
    return st.integers(min_value=1, max_value=1000)

def batch_timeout():
    """Generate valid batch timeouts."""
    return st.floats(min_value=0.1, max_value=60.0)

def batch_key():
    """Generate valid batch keys."""
    return st.text(alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd', 'P')), min_size=1, max_size=50)

def batch_data():
    """Generate valid batch data."""
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

class TestAsyncBatcher:
    @pytest.mark.asyncio
    @given(
        batch_size=batch_size(),
        batch_timeout=batch_timeout()
    )
    async def test_async_batcher_initialization(self, batch_size, batch_timeout):
        """Test AsyncBatcher initialization with different parameters."""
        batcher = AsyncBatcher(batch_size=batch_size, batch_timeout=batch_timeout)
        assert batcher.batch_size == batch_size
        assert batcher.batch_timeout == batch_timeout

    @pytest.mark.asyncio
    @given(
        batch_size=batch_size(),
        batch_timeout=batch_timeout(),
        key=batch_key(),
        data=batch_data()
    )
    async def test_add_to_batch(self, batch_size, batch_timeout, key, data):
        """Test adding items to a batch."""
        batcher = AsyncBatcher(batch_size=batch_size, batch_timeout=batch_timeout)
        await batcher.add_to_batch(key, data)
        assert key in batcher.batches
        assert len(batcher.batches[key]) == 1

    @pytest.mark.asyncio
    @given(
        batch_size=batch_size(),
        batch_timeout=batch_timeout(),
        key=batch_key()
    )
    async def test_batch_size_limit(self, batch_size, batch_timeout, key):
        """Test that batches respect size limits."""
        batcher = AsyncBatcher(batch_size=batch_size, batch_timeout=batch_timeout)
        
        # Add items up to batch size
        for i in range(batch_size):
            await batcher.add_to_batch(key, {"value": i})
        
        # Verify batch size
        assert len(batcher.batches[key]) == batch_size
        
        # Add one more item
        await batcher.add_to_batch(key, {"value": "overflow"})
        
        # Verify batch was flushed
        assert key not in batcher.batches or len(batcher.batches[key]) == 1

    @pytest.mark.asyncio
    @given(
        batch_size=batch_size(),
        batch_timeout=batch_timeout(),
        key=batch_key()
    )
    async def test_batch_timeout(self, batch_size, batch_timeout, key):
        """Test that batches respect timeout limits."""
        batcher = AsyncBatcher(batch_size=batch_size, batch_timeout=batch_timeout)
        await batcher.add_to_batch(key, {"value": "test"})
        
        # Simulate timeout
        batcher.batches[key].last_update = 0
        
        # Add another item
        await batcher.add_to_batch(key, {"value": "new"})
        
        # Verify batch was flushed
        assert key not in batcher.batches or len(batcher.batches[key]) == 1

    @pytest.mark.asyncio
    @given(
        batch_size=batch_size(),
        batch_timeout=batch_timeout(),
        key=batch_key()
    )
    async def test_flush_batch(self, batch_size, batch_timeout, key):
        """Test flushing a batch."""
        batcher = AsyncBatcher(batch_size=batch_size, batch_timeout=batch_timeout)
        
        # Add items
        for i in range(batch_size):
            await batcher.add_to_batch(key, {"value": i})
        
        # Flush batch
        items = await batcher.flush_batch(key)
        
        # Verify flush
        assert len(items) == batch_size
        assert key not in batcher.batches

    @pytest.mark.asyncio
    @given(
        batch_size=batch_size(),
        batch_timeout=batch_timeout(),
        key=batch_key()
    )
    async def test_flush_nonexistent_batch(self, batch_size, batch_timeout, key):
        """Test flushing a nonexistent batch."""
        batcher = AsyncBatcher(batch_size=batch_size, batch_timeout=batch_timeout)
        items = await batcher.flush_batch(key)
        assert len(items) == 0

    @pytest.mark.asyncio
    @given(
        batch_size=batch_size(),
        batch_timeout=batch_timeout(),
        key=batch_key()
    )
    async def test_get_batch_size(self, batch_size, batch_timeout, key):
        """Test getting batch size."""
        batcher = AsyncBatcher(batch_size=batch_size, batch_timeout=batch_timeout)
        
        # Add items
        for i in range(batch_size):
            await batcher.add_to_batch(key, {"value": i})
        
        # Verify batch size
        assert batcher.get_batch_size(key) == batch_size

    @pytest.mark.asyncio
    @given(
        batch_size=batch_size(),
        batch_timeout=batch_timeout(),
        key=batch_key()
    )
    async def test_get_nonexistent_batch_size(self, batch_size, batch_timeout, key):
        """Test getting size of nonexistent batch."""
        batcher = AsyncBatcher(batch_size=batch_size, batch_timeout=batch_timeout)
        assert batcher.get_batch_size(key) == 0

    @pytest.mark.asyncio
    @given(
        batch_size=batch_size(),
        batch_timeout=batch_timeout(),
        key=batch_key()
    )
    async def test_clear_batch(self, batch_size, batch_timeout, key):
        """Test clearing a batch."""
        batcher = AsyncBatcher(batch_size=batch_size, batch_timeout=batch_timeout)
        
        # Add items
        for i in range(batch_size):
            await batcher.add_to_batch(key, {"value": i})
        
        # Clear batch
        await batcher.clear_batch(key)
        
        # Verify batch is cleared
        assert key not in batcher.batches

    @pytest.mark.asyncio
    @given(
        batch_size=batch_size(),
        batch_timeout=batch_timeout(),
        key=batch_key()
    )
    async def test_clear_nonexistent_batch(self, batch_size, batch_timeout, key):
        """Test clearing a nonexistent batch."""
        batcher = AsyncBatcher(batch_size=batch_size, batch_timeout=batch_timeout)
        await batcher.clear_batch(key)  # Should not raise an error

    @pytest.mark.asyncio
    @given(
        batch_size=batch_size(),
        batch_timeout=batch_timeout(),
        key=batch_key()
    )
    async def test_batch_is_full(self, batch_size, batch_timeout, key):
        """Test checking if a batch is full."""
        batcher = AsyncBatcher(batch_size=batch_size, batch_timeout=batch_timeout)
        
        # Add items up to batch size
        for i in range(batch_size):
            await batcher.add_to_batch(key, {"value": i})
        
        # Verify batch is full
        assert batcher.is_batch_full(key)

    @pytest.mark.asyncio
    @given(
        batch_size=batch_size(),
        batch_timeout=batch_timeout(),
        key=batch_key()
    )
    async def test_batch_is_not_full(self, batch_size, batch_timeout, key):
        """Test checking if a batch is not full."""
        batcher = AsyncBatcher(batch_size=batch_size, batch_timeout=batch_timeout)
        
        # Add fewer items than batch size
        for i in range(batch_size - 1):
            await batcher.add_to_batch(key, {"value": i})
        
        # Verify batch is not full
        assert not batcher.is_batch_full(key)

    @pytest.mark.asyncio
    @given(
        batch_size=batch_size(),
        batch_timeout=batch_timeout(),
        key=batch_key()
    )
    async def test_batch_has_timed_out(self, batch_size, batch_timeout, key):
        """Test checking if a batch has timed out."""
        batcher = AsyncBatcher(batch_size=batch_size, batch_timeout=batch_timeout)
        await batcher.add_to_batch(key, {"value": "test"})
        
        # Simulate timeout
        batcher.batches[key].last_update = 0
        
        # Verify batch has timed out
        assert batcher.has_batch_timed_out(key)

    @pytest.mark.asyncio
    @given(
        batch_size=batch_size(),
        batch_timeout=batch_timeout(),
        key=batch_key()
    )
    async def test_batch_has_not_timed_out(self, batch_size, batch_timeout, key):
        """Test checking if a batch has not timed out."""
        batcher = AsyncBatcher(batch_size=batch_size, batch_timeout=batch_timeout)
        await batcher.add_to_batch(key, {"value": "test"})
        
        # Verify batch has not timed out
        assert not batcher.has_batch_timed_out(key)

    @pytest.mark.asyncio
    @given(
        batch_size=batch_size(),
        batch_timeout=batch_timeout(),
        key=batch_key()
    )
    async def test_concurrent_batch_operations(self, batch_size, batch_timeout, key):
        """Test concurrent batch operations."""
        batcher = AsyncBatcher(batch_size=batch_size, batch_timeout=batch_timeout)
        
        # Create multiple tasks to add items concurrently
        tasks = []
        for i in range(batch_size):
            tasks.append(batcher.add_to_batch(key, {"value": i}))
        
        # Wait for all tasks to complete
        await asyncio.gather(*tasks)
        
        # Verify batch size
        assert batcher.get_batch_size(key) == batch_size

    @pytest.mark.asyncio
    @given(
        batch_size=batch_size(),
        batch_timeout=batch_timeout(),
        key=batch_key()
    )
    async def test_concurrent_flush_operations(self, batch_size, batch_timeout, key):
        """Test concurrent flush operations."""
        batcher = AsyncBatcher(batch_size=batch_size, batch_timeout=batch_timeout)
        
        # Add items
        for i in range(batch_size):
            await batcher.add_to_batch(key, {"value": i})
        
        # Create multiple tasks to flush concurrently
        tasks = []
        for _ in range(3):
            tasks.append(batcher.flush_batch(key))
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks)
        
        # Verify only one flush was successful
        successful_flushes = sum(1 for result in results if len(result) == batch_size)
        assert successful_flushes == 1 