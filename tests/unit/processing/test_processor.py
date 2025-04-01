import pytest
import time
from stream_cdc.processing.processor import StreamProcessor
from stream_cdc.streams.base import Stream


class MockStream(Stream):
    """Mock implementation of Stream for testing."""

    def __init__(self):
        self.sent_messages = []
        self.closed = False

    def send(self, messages):
        self.sent_messages.extend(messages)

    def close(self):
        self.closed = True


class MockSerializer:
    """Mock implementation of Serializer for testing."""

    def serialize(self, data):
        data["serialized"] = True
        return data


@pytest.fixture
def mock_stream():
    """Fixture to provide a mock stream."""
    return MockStream()


@pytest.fixture
def mock_serializer():
    """Fixture to provide a mock serializer."""
    return MockSerializer()


@pytest.fixture
def processor(mock_stream, mock_serializer):
    """Fixture to provide a StreamProcessor instance."""
    return StreamProcessor(
        stream=mock_stream,
        serializer=mock_serializer,
        batch_size=3,
        flush_interval=2.0
    )


def test_processor_init(processor, mock_stream, mock_serializer):
    """Test processor initialization."""
    assert processor.stream == mock_stream
    assert processor.serializer == mock_serializer
    assert processor.batch_size == 3
    assert processor.flush_interval == 2.0
    assert processor.buffer == []
    assert processor.last_flush_time <= time.time()


def test_process_single_event(processor, mock_stream):
    """Test processing a single event."""
    # Process an event
    event = {"type": "test", "value": 123}
    processor.process(event)

    # Buffer should contain the serialized event
    assert len(processor.buffer) == 1
    assert processor.buffer[0] == {"type": "test", "value": 123, "serialized": True}

    # Stream should not have received any messages yet (batch not full)
    assert len(mock_stream.sent_messages) == 0


def test_process_batch_size_flush(processor, mock_stream):
    """Test flushing when batch size is reached."""
    # Process events up to batch size
    for i in range(3):
        processor.process({"id": i})

    # Buffer should be empty after flush
    assert len(processor.buffer) == 0

    # Stream should have received all messages
    assert len(mock_stream.sent_messages) == 3
    assert mock_stream.sent_messages[0] == {"id": 0, "serialized": True}
    assert mock_stream.sent_messages[1] == {"id": 1, "serialized": True}
    assert mock_stream.sent_messages[2] == {"id": 2, "serialized": True}


def test_process_flush_interval(processor, mock_stream):
    """Test flushing when flush interval is reached."""
    # Set last flush time to past
    processor.last_flush_time = time.time() - 3.0  # 3 seconds ago

    # Process a single event
    processor.process({"id": 123})

    # Buffer should be empty after time-based flush
    assert len(processor.buffer) == 0

    # Stream should have received the message
    assert len(mock_stream.sent_messages) == 1
    assert mock_stream.sent_messages[0] == {"id": 123, "serialized": True}


def test_flush_empty_buffer(processor, mock_stream):
    """Test flushing with empty buffer."""
    # Buffer starts empty
    assert len(processor.buffer) == 0

    # Flush should not send anything
    processor.flush()

    # Stream should not have received any messages
    assert len(mock_stream.sent_messages) == 0


def test_flush_with_messages(processor, mock_stream):
    """Test flushing with messages in buffer."""
    # Add some messages to buffer
    processor.buffer = [
        {"id": 1, "serialized": True},
        {"id": 2, "serialized": True}
    ]

    # Record the last flush time
    old_last_flush_time = processor.last_flush_time

    # Flush the buffer
    processor.flush()

    # Buffer should be empty
    assert len(processor.buffer) == 0

    # Stream should have received the messages
    assert len(mock_stream.sent_messages) == 2

    # Last flush time should be updated
    assert processor.last_flush_time > old_last_flush_time


def test_close(processor, mock_stream):
    """Test closing the processor."""
    # Add some messages to buffer
    processor.buffer = [
        {"id": 1, "serialized": True},
        {"id": 2, "serialized": True}
    ]

    # Close the processor
    processor.close()

    # Buffer should be empty (flushed)
    assert len(processor.buffer) == 0

    # Stream should have received the messages
    assert len(mock_stream.sent_messages) == 2

    # Stream should have been closed
    assert mock_stream.closed


