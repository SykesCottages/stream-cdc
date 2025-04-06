import pytest
from stream_cdc.streams.factory import StreamFactory
from stream_cdc.streams.base import Stream
from stream_cdc.utils.exceptions import UnsupportedTypeError


class TestStreamFactory:
    """Test cases for StreamFactory"""

    @pytest.fixture(autouse=True)
    def reset_stream_factory(self):
        """Reset the StreamFactory registry before and after each test."""
        original_registry = StreamFactory.REGISTRY.copy()
        StreamFactory.REGISTRY = {}
        yield
        StreamFactory.REGISTRY = original_registry

    class MockStream(Stream):
        """Mock implementation of Stream for testing."""

        def send(self, messages):
            pass

        def close(self):
            pass

    def test_register_stream(self):
        """Test registering a stream implementation."""
        # Register a mock stream
        StreamFactory.register_stream("mock", self.MockStream)

        # Check that it was registered
        assert "mock" in StreamFactory.REGISTRY
        assert StreamFactory.REGISTRY["mock"] == self.MockStream

    def test_register_stream_case_insensitive(self):
        """Test that stream registration is case insensitive."""
        # Register with mixed case
        StreamFactory.register_stream("MockStream", self.MockStream)

        # Should be stored lowercase
        assert "mockstream" in StreamFactory.REGISTRY
        assert StreamFactory.REGISTRY["mockstream"] == self.MockStream

    def test_create_stream(self):
        """Test creating a stream instance."""
        # Register a mock stream
        StreamFactory.register_stream("mock", self.MockStream)

        # Create an instance
        stream = StreamFactory.create("mock")

        # Check type
        assert isinstance(stream, self.MockStream)

    def test_create_stream_case_insensitive(self):
        """Test that stream creation is case insensitive."""
        # Register a mock stream
        StreamFactory.register_stream("mock", self.MockStream)

        # Create with different case
        stream = StreamFactory.create("MOCK")

        # Should still work
        assert isinstance(stream, self.MockStream)

    def test_create_unsupported_stream(self):
        """Test error when creating an unsupported stream type."""
        # Try to create an unregistered stream type
        with pytest.raises(UnsupportedTypeError) as exc_info:
            StreamFactory.create("unsupported")

        # Error message should contain the unsupported type and available types
        assert "Unsupported stream type: unsupported" in str(exc_info.value)
        assert "Supported types: []" in str(exc_info.value)

    def test_create_unsupported_stream_with_registered_types(self):
        """Test error includes registered stream types."""
        # Register some streams
        StreamFactory.register_stream("mock1", self.MockStream)
        StreamFactory.register_stream("mock2", self.MockStream)

        # Try to create an unregistered stream type
        with pytest.raises(UnsupportedTypeError) as exc_info:
            StreamFactory.create("unsupported")

        # Error should list available types
        error_message = str(exc_info.value)
        assert "Supported types:" in error_message
        assert "mock1" in error_message
        assert "mock2" in error_message

