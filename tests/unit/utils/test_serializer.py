import pytest
from datetime import datetime, timezone
from stream_cdc.utils.serializer import Serializer


@pytest.fixture
def serializer():
    """Fixture to create a Serializer instance for tests."""
    return Serializer()


def test_serialize_simple_dict(serializer):
    """Test serializing a simple dictionary."""
    data = {"key": "value", "number": 123}
    result = serializer.serialize(data)
    assert result == data


def test_serialize_nested_dict(serializer):
    """Test serializing a nested dictionary."""
    data = {"outer": {"inner": "value"}, "array": [1, 2, 3]}
    result = serializer.serialize(data)
    assert result == data


def test_serialize_with_datetime(serializer):
    """Test serializing data with datetime objects."""
    now = datetime.now(timezone.utc)
    data = {"timestamp": now, "value": "test"}

    result = serializer.serialize(data)

    # Datetime should be converted to string
    assert isinstance(result["timestamp"], str)
    assert result["value"] == "test"


def test_serialize_custom_object(serializer):
    """Test serializing a custom object that can't be JSON serialized."""

    class CustomObject:
        # This class doesn't have a proper JSON representation
        pass

    data = {"object": CustomObject()}

    # Two possible outcomes:
    # 1. The serializer converts it to string and logs the exception
    # 2. The serializer handles it some other way

    result = serializer.serialize(data)

    # Regardless of implementation, we should get a result
    assert result is not None

    # If it was serialized as a string, we should see the class name
    # somewhere in the result
    if isinstance(result, str):
        assert "CustomObject" in result
    # If it was serialized as a dict, the 'object' key should be a string
    elif isinstance(result, dict) and "object" in result:
        assert isinstance(result["object"], str)


def test_serialize_array(serializer):
    """Test serializing an array."""
    data = [1, 2, 3, "test"]
    result = serializer.serialize(data)
    assert result == data


def test_serialize_complex_nested_structure(serializer):
    """Test serializing a complex nested structure."""
    data = {
        "level1": {"level2": {"level3": [{"key": "value1"}, {"key": "value2"}]}},
        "numbers": [1, 2, 3],
    }
    result = serializer.serialize(data)
    assert result == data
