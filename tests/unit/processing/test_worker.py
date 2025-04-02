import pytest
from unittest.mock import MagicMock, patch
from stream_cdc.processing.worker import Worker
from stream_cdc.utils.exceptions import ProcessingError


@pytest.fixture
def mock_data_source():
    """Fixture to provide a mock data source."""
    data_source = MagicMock()

    # Configure the listen method to yield some events
    data_source.listen.return_value = [
        {"id": 1, "type": "insert"},
        {"id": 2, "type": "update"},
        {"id": 3, "type": "delete"},
    ]

    return data_source


@pytest.fixture
def mock_processor():
    """Fixture to provide a mock processor."""
    return MagicMock()


@pytest.fixture
def worker(mock_data_source, mock_processor):
    """Fixture to provide a Worker instance."""
    return Worker(data_source=mock_data_source, processor=mock_processor)


def test_worker_init(worker, mock_data_source, mock_processor):
    """Test worker initialization."""
    assert worker.data_source == mock_data_source
    assert worker.processor == mock_processor
    assert worker.running


def test_run_stop_flow(worker, mock_data_source, mock_processor):
    """Test run method with normal stop flow."""

    # Configure the worker to stop after processing
    def stop_worker(*args, **kwargs):
        worker.running = False

    # After handling the first event, stop the worker
    mock_processor.process.side_effect = stop_worker

    # Run the worker
    worker.run()

    # Verify data source was connected
    mock_data_source.connect.assert_called_once()

    # Verify processor processed the first event
    mock_processor.process.assert_called_once()

    # Verify cleanup occurred
    mock_processor.close.assert_called_once()
    mock_data_source.disconnect.assert_called_once()


def test_run_with_exception(worker, mock_data_source, mock_processor):
    """Test run method with exception during processing."""
    # Simulate an error during processing
    mock_processor.process.side_effect = Exception("Test error")

    # Run the worker, expecting an exception
    with pytest.raises(ProcessingError) as exc_info:
        worker.run()

    # Verify error message
    assert "Processing failed: Test error" in str(exc_info.value)

    # Verify cleanup occurred even with error
    mock_processor.close.assert_called_once()
    mock_data_source.disconnect.assert_called_once()


def test_run_process_multiple_events(worker, mock_data_source, mock_processor):
    """Test processing multiple events."""
    # Get number of events from the data source
    events = list(mock_data_source.listen.return_value)

    # Configure worker to stop after all events
    event_count = 0

    def process_and_count(*args, **kwargs):
        nonlocal event_count
        event_count += 1
        if event_count >= len(events):
            worker.running = False

    mock_processor.process.side_effect = process_and_count

    # Run the worker
    worker.run()

    # Verify correct number of events processed
    assert mock_processor.process.call_count == len(events)


def test_stop(worker):
    """Test stop method."""
    # Mock the exit function to avoid actually exiting during test
    with patch("stream_cdc.processing.worker.exit") as mock_exit:
        with patch("stream_cdc.processing.worker.time.sleep") as mock_sleep:
            worker.stop()

            # Verify running flag was set to False
            assert not worker.running

            # Verify worker sleeps to allow clean shutdown
            mock_sleep.assert_called_once_with(2)

            # Verify worker exits with code 0
            mock_exit.assert_called_once_with(0)
