import pytest
from unittest.mock import MagicMock, patch
from stream_cdc.processing.worker import Worker
from stream_cdc.processing.processor import StreamProcessor
from stream_cdc.utils.exceptions import ProcessingError


class TestWorker:
    """Test cases for Worker implementation"""

    @pytest.fixture
    def mock_processor(self):
        """Fixture to provide a mock processor."""
        processor = MagicMock(spec=StreamProcessor)
        return processor

    @pytest.fixture
    def worker(self, mock_processor):
        """Fixture to provide a Worker instance."""
        return Worker(processor=mock_processor)

    def test_worker_init(self, worker, mock_processor):
        """Test worker initialization."""
        assert worker.processor == mock_processor
        assert worker.running is True

    def test_run_stop_flow(self, worker, mock_processor):
        """Test run method with normal stop flow."""
        # Configure the worker to stop after processing
        def set_stop():
            worker.running = False
            return True

        # After handling the first event, stop the worker
        mock_processor.process_next.side_effect = set_stop

        # Run the worker
        worker.run()

        # Verify processor was started
        mock_processor.start.assert_called_once()

        # Verify processor processed events
        mock_processor.process_next.assert_called_once()

        # Verify cleanup occurred
        mock_processor.stop.assert_called_once()

    def test_run_with_exception(self, worker, mock_processor):
        """Test run method with exception during processing."""
        # Simulate an error during processing
        mock_processor.process_next.side_effect = Exception("Test error")

        # Run the worker, expecting an exception
        with pytest.raises(ProcessingError) as exc_info:
            worker.run()

        # Verify error message
        assert "Processing failed: Test error" in str(exc_info.value)

        # Verify cleanup occurred even with error
        mock_processor.stop.assert_called_once()

    def test_run_process_multiple_events(self, worker, mock_processor):
        """Test processing multiple events."""
        # Configure worker to process three events, then stop
        event_count = 0

        def process_and_count():
            nonlocal event_count
            event_count += 1
            if event_count >= 3:
                worker.running = False
            return True  # Indicate that processing was successful

        mock_processor.process_next.side_effect = process_and_count

        # Run the worker
        worker.run()

        # Verify processor handled correct number of events
        assert mock_processor.process_next.call_count == 3

        # Verify cleanup occurred
        mock_processor.stop.assert_called_once()

    def test_stop(self, worker):
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

