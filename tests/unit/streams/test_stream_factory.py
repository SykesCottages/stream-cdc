import pytest
from stream_cdc.streams.factory import StreamFactory
from stream_cdc.streams.base import Stream
from stream_cdc.utils.exceptions import UnsupportedTypeError


@pytest.fixture(autouse=True)
def reset_stream_factory():
    """Reset the StreamFactory registry before and after each test."""
    original_registry = StreamFactory.REGISTRY.copy()
    StreamFactory.REGISTRY = {}
    yield
    StreamFactory.REGISTRY = original_registry


class MockStream(Stream):
    """Mock implementation of Stream for testing."""

    def send(self):
        pass

    def close(self):
        pass


def test_register_stream():
    """Test registering a stream implementation."""
    # Register a mock stream
    StreamFactory.register_stream("mock", MockStream)

    # Check that it was registered
    assert "mock" in StreamFactory.REGISTRY
    assert StreamFactory.REGISTRY["mock"] == MockStream


def test_register_stream_case_insensitive():
    """Test that stream registration is case insensitive."""
    # Register with mixed case
    StreamFactory.register_stream("MockStream", MockStream)

    # Should be stored lowercase
    assert "mockstream" in StreamFactory.REGISTRY
    assert StreamFactory.REGISTRY["mockstream"] == MockStream


def test_create_stream():
    """Test creating a stream instance."""
    # Register a mock stream
    StreamFactory.register_stream("mock", MockStream)

    # Create an instance with some arguments
    stream = StreamFactory.create("mock")

    # Check type and passed arguments
    assert isinstance(stream, MockStream)


def test_create_stream_case_insensitive():
    """Test that stream creation is case insensitive."""
    # Register a mock stream
    StreamFactory.register_stream("mock", MockStream)

    # Create with different case
    stream = StreamFactory.create("MOCK")

    # Should still work
    assert isinstance(stream, MockStream)


def test_create_unsupported_stream():
    """Test error when creating an unsupported stream type."""
    # Try to create an unregistered stream type
    with pytest.raises(UnsupportedTypeError) as exc_info:
        StreamFactory.create("unsupported")

    # Error message should contain the unsupported type and available types
    assert "Unsupported stream type: unsupported" in str(exc_info.value)
    assert "Supported types: []" in str(exc_info.value)


def test_create_unsupported_stream_with_registered_types():
    """Test error includes registered stream types."""
    # Register some streams
    StreamFactory.register_stream("mock1", MockStream)
    StreamFactory.register_stream("mock2", MockStream)

    # Try to create an unregistered stream type
    with pytest.raises(UnsupportedTypeError) as exc_info:
        StreamFactory.create("unsupported")

    # Error should list available types
    assert "Supported types: ['mock1', 'mock2']" in str(exc_info.value)
