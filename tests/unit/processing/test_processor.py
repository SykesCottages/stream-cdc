import pytest
import time
from unittest.mock import MagicMock
from stream_cdc.processing.processor import StreamProcessor


class TestStreamProcessor:
    """Test cases for StreamProcessor implementation"""

    @pytest.fixture
    def mock_stream(self):
        """Fixture to provide a mock stream."""
        stream = MagicMock()
        return stream

    @pytest.fixture
    def mock_datasource(self):
        """Fixture to provide a mock data source."""
        datasource = MagicMock()
        return datasource

    @pytest.fixture
    def mock_state_manager(self):
        """Fixture to provide a mock state manager."""
        state_manager = MagicMock()
        return state_manager

    @pytest.fixture
    def processor(self, mock_stream, mock_datasource, mock_state_manager):
        """Fixture to provide a StreamProcessor instance."""
        return StreamProcessor(
            stream=mock_stream,
            datasource=mock_datasource,
            state_manager=mock_state_manager,
            batch_size=3,
            flush_interval=2.0,
        )

    def test_processor_init(
        self, processor, mock_stream, mock_datasource, mock_state_manager
    ):
        """Test processor initialization."""
        assert processor.stream == mock_stream
        assert processor.datasource == mock_datasource
        assert processor.state_manager == mock_state_manager
        assert processor.batch_size == 3
        assert processor.flush_interval == 2.0
        assert processor.buffer == []
        assert processor.last_flush_time <= time.time()
        assert processor._current_iterator is None

    def test_process_single_event(self, processor):
        """Test processing a single event."""
        # Process an event with metadata
        event = {
            "metadata": {
                "position": "gtid:123",
                "datasource_type": "mysql",
                "source": "localhost",
            }
        }

        # Process event
        processor.process(event)

        # Buffer should contain the serialized event
        assert len(processor.buffer) == 1
        assert processor.buffer[0]["metadata"] == event["metadata"]

        # Stream should not have received any messages yet (batch not full)
        processor.stream.send.assert_not_called()

    def test_process_batch_size_flush(self, processor, mock_stream):
        """Test flushing when batch size is reached."""
        # Create a side effect to capture the messages sent to the stream
        sent_messages = []

        def capture_messages(messages):
            sent_messages.extend(messages)
            return None

        # Set up the mock to capture the messages
        mock_stream.send.side_effect = capture_messages

        # Process events up to batch size
        for i in range(3):
            processor.process(
                {
                    "metadata": {
                        "position": f"gtid:{i}",
                        "datasource_type": "mysql",
                        "source": "localhost",
                    }
                }
            )

        # Buffer should be empty after flush
        assert len(processor.buffer) == 0

        # Stream should have received all messages
        mock_stream.send.assert_called_once()

        # Verify we captured 3 messages
        assert len(sent_messages) == 3

        # Verify the content of the messages
        for i, msg in enumerate(sent_messages):
            assert msg["metadata"]["position"] == f"gtid:{i}"
            assert msg["metadata"]["datasource_type"] == "mysql"
            assert msg["metadata"]["source"] == "localhost"

    def test_process_flush_interval(self, processor):
        """Test flushing when flush interval is reached."""
        # Set last flush time to past
        processor.last_flush_time = time.time() - 3.0  # 3 seconds ago

        # Process a single event
        processor.process(
            {
                "metadata": {
                    "position": "gtid:123",
                    "datasource_type": "mysql",
                    "source": "localhost",
                }
            }
        )

        # Buffer should be empty after time-based flush
        assert len(processor.buffer) == 0

        # Stream should have received the message
        processor.stream.send.assert_called_once()

    def test_flush_empty_buffer(self, processor, mock_stream):
        """Test flushing with empty buffer."""
        # Buffer starts empty
        assert len(processor.buffer) == 0

        # Flush should not send anything
        processor.flush()

        # Stream should not have received any messages
        processor.stream.send.assert_not_called()

    def test_flush_with_messages(self, processor, mock_state_manager):
        """Test flushing with messages in buffer."""
        # Add some messages to buffer
        processor.buffer = [
            {
                "metadata": {
                    "position": "gtid:1",
                    "datasource_type": "mysql",
                    "source": "localhost",
                }
            },
            {
                "metadata": {
                    "position": "gtid:2",
                    "datasource_type": "mysql",
                    "source": "localhost",
                }
            },
        ]

        # Record the last flush time
        old_last_flush_time = processor.last_flush_time

        # Flush the buffer
        processor.flush()

        # Buffer should be empty
        assert len(processor.buffer) == 0

        # Stream should have received the messages
        processor.stream.send.assert_called_once()

        # Last flush time should be updated
        assert processor.last_flush_time > old_last_flush_time

        # State should be saved with the last position
        mock_state_manager.store.assert_called_once()
        store_args = mock_state_manager.store.call_args[1]
        assert store_args["datasource_type"] == "mysql"
        assert store_args["datasource_source"] == "localhost"
        assert store_args["state_position"] == {"gtid": "gtid:2"}

    def test_close(self, processor):
        """Test closing the processor."""
        # Add some messages to buffer
        processor.buffer = [
            {
                "metadata": {
                    "position": "gtid:1",
                    "datasource_type": "mysql",
                    "source": "localhost",
                }
            },
        ]

        # Close the processor
        processor.stop()

        # Buffer should be empty (flushed)
        assert len(processor.buffer) == 0

        # Stream should have received the messages and been closed
        processor.stream.send.assert_called_once()
        processor.stream.close.assert_called_once()

        # Data source should be disconnected
        processor.datasource.disconnect.assert_called_once()
