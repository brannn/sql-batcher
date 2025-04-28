"""
Property-based tests for the HookManager class using Hypothesis.
"""

import pytest
from hypothesis import given, strategies as st, settings, Verbosity
from sql_batcher.hook_manager import HookManager
from sql_batcher.exceptions import HookManagerError

# Configure Hypothesis settings
settings.register_profile("dev", verbosity=Verbosity.verbose)
settings.load_profile("dev")

# Custom strategies for hook manager tests
def hook_name():
    """Generate valid hook names."""
    return st.text(alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd', 'P')), min_size=1, max_size=50)

def hook_priority():
    """Generate valid hook priorities."""
    return st.integers(min_value=0, max_value=100)

def hook_data():
    """Generate valid hook data."""
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

class TestHookManager:
    @given(
        hook_name=hook_name(),
        priority=hook_priority()
    )
    def test_register_hook(self, hook_name, priority):
        """Test registering a hook."""
        manager = HookManager()
        
        def test_hook(data):
            return data
        
        manager.register_hook(hook_name, test_hook, priority)
        assert hook_name in manager.hooks
        assert len(manager.hooks[hook_name]) == 1
        assert manager.hooks[hook_name][0].priority == priority

    @given(
        hook_name=hook_name(),
        priority=hook_priority()
    )
    def test_register_duplicate_hook(self, hook_name, priority):
        """Test registering a duplicate hook."""
        manager = HookManager()
        
        def test_hook(data):
            return data
        
        # Register first hook
        manager.register_hook(hook_name, test_hook, priority)
        
        # Register duplicate hook
        with pytest.raises(HookManagerError):
            manager.register_hook(hook_name, test_hook, priority)

    @given(
        hook_name=hook_name(),
        priority=hook_priority()
    )
    def test_unregister_hook(self, hook_name, priority):
        """Test unregistering a hook."""
        manager = HookManager()
        
        def test_hook(data):
            return data
        
        # Register hook
        manager.register_hook(hook_name, test_hook, priority)
        
        # Unregister hook
        manager.unregister_hook(hook_name, test_hook)
        assert hook_name not in manager.hooks or len(manager.hooks[hook_name]) == 0

    @given(
        hook_name=hook_name(),
        priority=hook_priority()
    )
    def test_unregister_nonexistent_hook(self, hook_name, priority):
        """Test unregistering a nonexistent hook."""
        manager = HookManager()
        
        def test_hook(data):
            return data
        
        # Unregister nonexistent hook
        manager.unregister_hook(hook_name, test_hook)  # Should not raise an error

    @given(
        hook_name=hook_name(),
        priority=hook_priority(),
        data=hook_data()
    )
    def test_execute_hook(self, hook_name, priority, data):
        """Test executing a hook."""
        manager = HookManager()
        
        def test_hook(hook_data):
            return {**hook_data, "processed": True}
        
        # Register hook
        manager.register_hook(hook_name, test_hook, priority)
        
        # Execute hook
        result = manager.execute_hook(hook_name, data)
        assert result["processed"] is True

    @given(
        hook_name=hook_name(),
        data=hook_data()
    )
    def test_execute_nonexistent_hook(self, hook_name, data):
        """Test executing a nonexistent hook."""
        manager = HookManager()
        result = manager.execute_hook(hook_name, data)
        assert result == data

    @given(
        hook_name=hook_name(),
        priorities=st.lists(hook_priority(), min_size=2, max_size=5, unique=True)
    )
    def test_hook_priority_order(self, hook_name, priorities):
        """Test that hooks are executed in priority order."""
        manager = HookManager()
        execution_order = []
        
        # Register hooks with different priorities
        for i, priority in enumerate(priorities):
            def create_hook(index):
                def hook(data):
                    execution_order.append(index)
                    return data
                return hook
            
            manager.register_hook(hook_name, create_hook(i), priority)
        
        # Execute hooks
        manager.execute_hook(hook_name, {})
        
        # Verify execution order matches priority order
        assert execution_order == sorted(range(len(priorities)), key=lambda i: priorities[i])

    @given(
        hook_name=hook_name(),
        priority=hook_priority(),
        data=hook_data()
    )
    def test_hook_error_handling(self, hook_name, priority, data):
        """Test error handling in hooks."""
        manager = HookManager()
        
        def error_hook(hook_data):
            raise Exception("Test error")
        
        # Register error hook
        manager.register_hook(hook_name, error_hook, priority)
        
        # Execute hook
        with pytest.raises(Exception):
            manager.execute_hook(hook_name, data)

    @given(
        hook_name=hook_name(),
        priority=hook_priority(),
        data=hook_data()
    )
    def test_hook_data_modification(self, hook_name, priority, data):
        """Test that hooks can modify data."""
        manager = HookManager()
        
        def modify_hook(hook_data):
            return {**hook_data, "modified": True}
        
        # Register modification hook
        manager.register_hook(hook_name, modify_hook, priority)
        
        # Execute hook
        result = manager.execute_hook(hook_name, data)
        assert result["modified"] is True
        assert all(k in result for k in data.keys())

    @given(
        hook_name=hook_name(),
        priority=hook_priority(),
        data=hook_data()
    )
    def test_multiple_hooks(self, hook_name, priority, data):
        """Test executing multiple hooks."""
        manager = HookManager()
        
        def hook1(hook_data):
            return {**hook_data, "hook1": True}
        
        def hook2(hook_data):
            return {**hook_data, "hook2": True}
        
        # Register multiple hooks
        manager.register_hook(hook_name, hook1, priority)
        manager.register_hook(hook_name, hook2, priority + 1)
        
        # Execute hooks
        result = manager.execute_hook(hook_name, data)
        assert result["hook1"] is True
        assert result["hook2"] is True
        assert all(k in result for k in data.keys()) 