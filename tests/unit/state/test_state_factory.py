import pytest

from stream_cdc.state.factory import StateManagerFactory
from stream_cdc.state.base import StateManager
from stream_cdc.utils.exceptions import UnsupportedTypeError


class TestStateManagerFactory:
    """Test cases for StateManagerFactory class"""

    def setup_method(self):
        """Setup before each test - clear the registry"""
        self.original_registry = StateManagerFactory.REGISTRY.copy()
        StateManagerFactory.REGISTRY = {}

    def teardown_method(self):
        """Teardown after each test - restore the registry"""
        StateManagerFactory.REGISTRY = self.original_registry

    def test_register_state_manager(self):
        """Test registering a state manager class"""
        class MockStateManager(StateManager):
            def store(self): pass
            def read(self): pass

        StateManagerFactory.register_state_manager("mock", MockStateManager)

        assert "mock" in StateManagerFactory.REGISTRY
        assert StateManagerFactory.REGISTRY["mock"] == MockStateManager

    def test_register_state_manager_case_insensitive(self):
        """Test that registration is case-insensitive"""
        class MockStateManager(StateManager):
            def store(self): pass
            def read(self): pass

        StateManagerFactory.register_state_manager("MoCK", MockStateManager)

        assert "mock" in StateManagerFactory.REGISTRY
        assert StateManagerFactory.REGISTRY["mock"] == MockStateManager

    def test_create_registered_state_manager(self):
        """Test creating a registered state manager"""
        class MockStateManager(StateManager):
            def __init__(self, **kwargs):
                self.init_kwargs = kwargs
            def store(self): pass
            def read(self): pass

        StateManagerFactory.register_state_manager("mock", MockStateManager)

        test_kwargs = {"param1": "value1", "param2": "value2"}
        manager = StateManagerFactory.create("mock", **test_kwargs)

        assert isinstance(manager, MockStateManager)
        assert manager.init_kwargs == test_kwargs

    def test_create_unregistered_state_manager(self):
        """Test creating an unregistered state manager raises the correct error"""
        with pytest.raises(UnsupportedTypeError):
            StateManagerFactory.create("nonexistent")

    def test_create_case_insensitive(self):
        """Test that creation is case-insensitive"""
        class MockStateManager(StateManager):
            def store(self): pass
            def read(self): pass

        StateManagerFactory.register_state_manager("mock", MockStateManager)

        manager = StateManagerFactory.create("MoCK")

        assert isinstance(manager, MockStateManager)

