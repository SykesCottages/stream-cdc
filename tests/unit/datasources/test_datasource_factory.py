import pytest
from stream_cdc.datasources.factory import DataSourceFactory
from stream_cdc.datasources.base import DataSource
from stream_cdc.utils.exceptions import UnsupportedTypeError


@pytest.fixture(autouse=True)
def reset_datasource_factory():
    """Reset the DataSourceFactory registry before and after each test."""
    original_registry = DataSourceFactory.REGISTRY.copy()
    DataSourceFactory.REGISTRY = {}
    yield
    DataSourceFactory.REGISTRY = original_registry


class MockDataSource(DataSource):
    """Mock implementation of DataSource for testing."""

    def __init__(self, **kwargs):
        self.init_args = kwargs

    def connect(self):
        pass

    def listen(self):
        yield {"event": "test"}

    def disconnect(self):
        pass


def test_register_datasource():
    """Test registering a data source implementation."""
    # Register a mock data source
    DataSourceFactory.register_datasource("mock", MockDataSource)

    # Check that it was registered
    assert "mock" in DataSourceFactory.REGISTRY
    assert DataSourceFactory.REGISTRY["mock"] == MockDataSource


def test_register_datasource_case_insensitive():
    """Test that data source registration is case insensitive."""
    # Register with mixed case
    DataSourceFactory.register_datasource("MockSource", MockDataSource)

    # Should be stored lowercase
    assert "mocksource" in DataSourceFactory.REGISTRY
    assert DataSourceFactory.REGISTRY["mocksource"] == MockDataSource


def test_create_datasource():
    """Test creating a data source instance."""
    # Register a mock data source
    DataSourceFactory.register_datasource("mock", MockDataSource)

    # Create an instance with some arguments
    datasource = DataSourceFactory.create("mock", host="localhost", port=3306)

    # Check type and passed arguments
    assert isinstance(datasource, MockDataSource)
    assert datasource.init_args == {"host": "localhost", "port": 3306}


def test_create_datasource_case_insensitive():
    """Test that data source creation is case insensitive."""
    # Register a mock data source
    DataSourceFactory.register_datasource("mock", MockDataSource)

    # Create with different case
    datasource = DataSourceFactory.create("MOCK")

    # Should still work
    assert isinstance(datasource, MockDataSource)


def test_create_unsupported_datasource():
    """Test error when creating an unsupported data source type."""
    # Try to create an unregistered data source type
    with pytest.raises(UnsupportedTypeError) as exc_info:
        DataSourceFactory.create("unsupported")

    # Error message should contain the unsupported type and available types
    assert "Unsupported data source type: unsupported" in str(exc_info.value)
    assert "Supported types: []" in str(exc_info.value)


def test_create_unsupported_datasource_with_registered_types():
    """Test error includes registered data source types."""
    # Register some data sources
    DataSourceFactory.register_datasource("mysql", MockDataSource)
    DataSourceFactory.register_datasource("postgres", MockDataSource)

    # Try to create an unregistered data source type
    with pytest.raises(UnsupportedTypeError) as exc_info:
        DataSourceFactory.create("mongodb")

    # Error should list available types
    assert "Supported types: ['mysql', 'postgres']" in str(exc_info.value)

