"""
Property-based tests for the plugins module using Hypothesis.
"""

import pytest
from hypothesis import given, strategies as st, settings, Verbosity
from sql_batcher.plugins import PluginManager, Plugin
from sql_batcher.exceptions import PluginError

# Configure Hypothesis settings
settings.register_profile("dev", verbosity=Verbosity.verbose)
settings.load_profile("dev")

# Custom strategies for plugin tests
def plugin_name():
    """Generate valid plugin names."""
    return st.text(alphabet=st.characters(whitelist_categories=('Lu', 'Ll', 'Nd', 'P')), min_size=1, max_size=50)

def plugin_version():
    """Generate valid plugin versions."""
    return st.tuples(
        st.integers(min_value=0, max_value=9),
        st.integers(min_value=0, max_value=9),
        st.integers(min_value=0, max_value=9)
    ).map(lambda x: f"{x[0]}.{x[1]}.{x[2]}")

def plugin_data():
    """Generate valid plugin data."""
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

class TestPluginManager:
    @given(
        plugin_name=plugin_name(),
        plugin_version=plugin_version()
    )
    def test_register_plugin(self, plugin_name, plugin_version):
        """Test registering a plugin."""
        manager = PluginManager()
        
        class TestPlugin(Plugin):
            def __init__(self):
                super().__init__(plugin_name, plugin_version)
            
            def process(self, data):
                return data
        
        plugin = TestPlugin()
        manager.register_plugin(plugin)
        assert plugin_name in manager.plugins
        assert manager.plugins[plugin_name] == plugin

    @given(
        plugin_name=plugin_name(),
        plugin_version=plugin_version()
    )
    def test_register_duplicate_plugin(self, plugin_name, plugin_version):
        """Test registering a duplicate plugin."""
        manager = PluginManager()
        
        class TestPlugin(Plugin):
            def __init__(self):
                super().__init__(plugin_name, plugin_version)
            
            def process(self, data):
                return data
        
        plugin1 = TestPlugin()
        plugin2 = TestPlugin()
        
        # Register first plugin
        manager.register_plugin(plugin1)
        
        # Register duplicate plugin
        with pytest.raises(PluginError):
            manager.register_plugin(plugin2)

    @given(
        plugin_name=plugin_name(),
        plugin_version=plugin_version()
    )
    def test_unregister_plugin(self, plugin_name, plugin_version):
        """Test unregistering a plugin."""
        manager = PluginManager()
        
        class TestPlugin(Plugin):
            def __init__(self):
                super().__init__(plugin_name, plugin_version)
            
            def process(self, data):
                return data
        
        plugin = TestPlugin()
        manager.register_plugin(plugin)
        manager.unregister_plugin(plugin_name)
        assert plugin_name not in manager.plugins

    @given(
        plugin_name=plugin_name(),
        plugin_version=plugin_version()
    )
    def test_unregister_nonexistent_plugin(self, plugin_name, plugin_version):
        """Test unregistering a nonexistent plugin."""
        manager = PluginManager()
        manager.unregister_plugin(plugin_name)  # Should not raise an error

    @given(
        plugin_name=plugin_name(),
        plugin_version=plugin_version(),
        data=plugin_data()
    )
    def test_process_data(self, plugin_name, plugin_version, data):
        """Test processing data through a plugin."""
        manager = PluginManager()
        
        class TestPlugin(Plugin):
            def __init__(self):
                super().__init__(plugin_name, plugin_version)
            
            def process(self, data):
                return {**data, "processed": True}
        
        plugin = TestPlugin()
        manager.register_plugin(plugin)
        
        result = manager.process_data(plugin_name, data)
        assert result["processed"] is True
        assert all(k in result for k in data.keys())

    @given(
        plugin_name=plugin_name(),
        data=plugin_data()
    )
    def test_process_data_nonexistent_plugin(self, plugin_name, data):
        """Test processing data through a nonexistent plugin."""
        manager = PluginManager()
        result = manager.process_data(plugin_name, data)
        assert result == data

    @given(
        plugin_name=plugin_name(),
        plugin_version=plugin_version(),
        data=plugin_data()
    )
    def test_plugin_error_handling(self, plugin_name, plugin_version, data):
        """Test error handling in plugins."""
        manager = PluginManager()
        
        class ErrorPlugin(Plugin):
            def __init__(self):
                super().__init__(plugin_name, plugin_version)
            
            def process(self, data):
                raise Exception("Test error")
        
        plugin = ErrorPlugin()
        manager.register_plugin(plugin)
        
        with pytest.raises(Exception):
            manager.process_data(plugin_name, data)

    @given(
        plugin_name=plugin_name(),
        plugin_version=plugin_version(),
        data=plugin_data()
    )
    def test_plugin_data_modification(self, plugin_name, plugin_version, data):
        """Test that plugins can modify data."""
        manager = PluginManager()
        
        class ModifyPlugin(Plugin):
            def __init__(self):
                super().__init__(plugin_name, plugin_version)
            
            def process(self, data):
                return {**data, "modified": True}
        
        plugin = ModifyPlugin()
        manager.register_plugin(plugin)
        
        result = manager.process_data(plugin_name, data)
        assert result["modified"] is True
        assert all(k in result for k in data.keys())

    @given(
        plugin_name=plugin_name(),
        plugin_version=plugin_version(),
        data=plugin_data()
    )
    def test_multiple_plugins(self, plugin_name, plugin_version, data):
        """Test processing data through multiple plugins."""
        manager = PluginManager()
        
        class Plugin1(Plugin):
            def __init__(self):
                super().__init__(f"{plugin_name}_1", plugin_version)
            
            def process(self, data):
                return {**data, "plugin1": True}
        
        class Plugin2(Plugin):
            def __init__(self):
                super().__init__(f"{plugin_name}_2", plugin_version)
            
            def process(self, data):
                return {**data, "plugin2": True}
        
        plugin1 = Plugin1()
        plugin2 = Plugin2()
        
        manager.register_plugin(plugin1)
        manager.register_plugin(plugin2)
        
        result1 = manager.process_data(f"{plugin_name}_1", data)
        result2 = manager.process_data(f"{plugin_name}_2", data)
        
        assert result1["plugin1"] is True
        assert result2["plugin2"] is True
        assert all(k in result1 for k in data.keys())
        assert all(k in result2 for k in data.keys()) 