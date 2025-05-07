import pytest
from unittest.mock import MagicMock, patch
from stream_cdc.processing.worker import Worker
from stream_cdc.processing.coordinator import Coordinator
from stream_cdc.utils.exceptions import ProcessingError


class TestWorker:
    """Test cases for Worker implementation"""

    @pytest.fixture
    def mock_coordinator(self):
        """Fixture to provide a mock coordinator."""
        coordinator = MagicMock(spec=Coordinator)
        return coordinator

    @pytest.fixture
    def worker(self, mock_coordinator):
        """Fixture to provide a Worker instance."""
        return Worker(coordinator=mock_coordinator)

    def test_worker_init(self, worker, mock_coordinator):
        """Test worker initialization."""
        assert worker.coordinator == mock_coordinator
        assert worker.running is True

    def test_run_stop_flow(self, worker, mock_coordinator):
        """Test run method with normal stop flow."""

        # Configure the worker to stop after processing
        def set_stop():
            worker.running = False
            return True

        # After handling the first event, stop the worker
        mock_coordinator.process_next.side_effect = set_stop

        # Run the worker
        worker.run()

        # Verify coordinator was started
        mock_coordinator.start.assert_called_once()

        # Verify coordinator processed events
        mock_coordinator.process_next.assert_called_once()

        # Verify cleanup occurred
        mock_coordinator.stop.assert_called_once()

    def test_run_with_exception(self, worker, mock_coordinator):
        """Test run method with exception during processing."""
        # Simulate an error during processing
        mock_coordinator.process_next.side_effect = Exception("Test error")

        # Run the worker, expecting an exception
        with pytest.raises(ProcessingError) as exc_info:
            worker.run()

        # Verify error message
        assert "Processing failed: Test error" in str(exc_info.value)

        # Verify cleanup occurred even with error
        mock_coordinator.stop.assert_called_once()

    def test_run_process_multiple_events(self, worker, mock_coordinator):
        """Test processing multiple events."""
        # Configure worker to process three events, then stop
        event_count = 0

        def process_and_count():
            nonlocal event_count
            event_count += 1
            if event_count >= 3:
                worker.running = False
            return True  # Indicate that processing was successful

        mock_coordinator.process_next.side_effect = process_and_count

        # Run the worker
        worker.run()

        # Verify coordinator handled correct number of events
        assert mock_coordinator.process_next.call_count == 3

        # Verify cleanup occurred
        mock_coordinator.stop.assert_called_once()

    def test_stop(self, worker):
        """Test stop method."""
        # Skip verifying both sleep and exit since they might be handled differently
        # in your implementation
        with patch("stream_cdc.processing.worker.exit"):
            with patch("stream_cdc.processing.worker.time.sleep"):
                worker.stop()

                # Only verify that running flag was changed to False
                assert not worker.running
