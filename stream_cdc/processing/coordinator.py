from typing import Iterator, List, Dict, Any, Optional, Protocol
import time
from stream_cdc.utils.logger import logger
from stream_cdc.streams.base import Stream
from stream_cdc.datasources.base import DataSource
from stream_cdc.state.base import StateManager
from stream_cdc.utils.exceptions import ProcessingError
from stream_cdc.processing.processors import EventProcessor


class FlushPolicy(Protocol):
    """Protocol defining a component that determines when to flush the buffer."""

    def should_flush(
        self, buffer: List[Dict[str, Any]], last_flush_time: float
    ) -> bool:
        """Determine if the buffer should be flushed."""
        ...

    def reset(self) -> None:
        """Reset the flush policy state after a flush."""
        ...


class BatchSizeAndTimePolicy:
    """
    Flush policy based on batch size and elapsed time.
    """

    def __init__(self, batch_size: int, flush_interval: float):
        self.batch_size = batch_size
        self.flush_interval = flush_interval

    def should_flush(
        self, buffer: List[Dict[str, Any]], last_flush_time: float
    ) -> bool:
        """
        Determine if buffer should be flushed based on size or elapsed time.

        Returns:
            bool: True if buffer should be flushed, False otherwise
        """
        if not buffer:
            return False

        batch_size_reached = len(buffer) >= self.batch_size
        time_interval_elapsed = time.time() - last_flush_time >= self.flush_interval

        return batch_size_reached or time_interval_elapsed

    def reset(self) -> None:
        """Reset the policy state (no state to reset in this implementation)."""
        pass


class StateCheckpointManager:
    """
    Handles the checkpointing of state from a datasource to a state manager.
    """

    def __init__(self, datasource: DataSource, state_manager: StateManager):
        self.datasource = datasource
        self.state_manager = state_manager
        self._last_saved_position: str = ""

    def load_state(self) -> None:
        """Load the last saved state and configure the datasource."""
        if not self.state_manager:
            logger.warning("No state manager configured, skipping state loading")
            return

        try:
            datasource_type = self.datasource.get_source_type()
            datasource_id = self.datasource.get_source_id()

            if not datasource_type or not datasource_id:
                logger.warning(
                    "Unable to determine data source type or source identifier"
                )
                return

            position = self.state_manager.read(
                datasource_type=datasource_type,
                datasource_source=datasource_id,
            )

            if not position:
                logger.info("No saved state found, starting from default position")
                return

            logger.info(f"Retrieved state from storage: {position}")

            if not isinstance(position, str) or not position:
                logger.warning(f"Invalid position format retrieved: {position}")
                return

            logger.info(f"Resuming from saved position: {position}")

            self.datasource.set_start_position(position)
        except Exception as e:
            logger.error(f"Error loading state: {e}")

    def save_state(self) -> None:
        """Save the current position state from the datasource."""
        if not self.state_manager:
            return

        try:
            position = self.datasource.get_current_position()
            logger.debug(f"Position: {position}")

            if not position or not isinstance(position, str) or not position:
                logger.debug("No valid position available from datasource")
                return

            datasource_type = self.datasource.get_source_type()
            datasource_id = self.datasource.get_source_id()

            if not datasource_type or not datasource_id:
                logger.warning(
                    "Unable to determine data source type or source identifier"
                )
                return

            if self._last_saved_position == position:
                logger.debug(
                    f"Position {position} already saved, skipping duplicate save"
                )
                return

            self.state_manager.store(
                datasource_type=datasource_type,
                datasource_source=datasource_id,
                state_position=position,
            )

            self._last_saved_position = position

            logger.debug(
                f"Updated state for {datasource_type}:{datasource_id} to {position}"
            )
        except Exception as e:
            logger.error(f"Failed to save state: {e}")


class Coordinator:
    """
    Coordinator orchestrates the flow between DataSource, StateManager, and Stream.

    This class is responsible for coordinating the data flow from the data source
    to the stream, while managing state persistence.
    """

    def __init__(
        self,
        datasource: DataSource,
        state_manager: StateManager,
        stream: Stream,
        event_processor: EventProcessor,
        flush_policy: FlushPolicy,
    ) -> None:
        """
        Initialize the Coordinator.

        Args:
            datasource: The data source to retrieve events from
            state_manager: The state manager to load/save position state
            stream: The stream to send processed events to
            event_processor: Component that processes events before buffering
            flush_policy: Component that determines when to flush the buffer
        """
        self.datasource = datasource
        self.stream = stream
        self.event_processor = event_processor
        self.flush_policy = flush_policy
        self.state_checkpoint_manager = StateCheckpointManager(
            datasource, state_manager
        )

        self.buffer: List[Dict[str, Any]] = []
        self.last_flush_time = time.time()
        self._current_iterator: Optional[Iterator[Dict[str, Any]]] = None

    def start(self) -> None:
        """Start the coordinator by loading state and connecting to datasource."""
        try:
            self.state_checkpoint_manager.load_state()
            self.datasource.connect()
            logger.info("Connected to data source")
        except Exception as e:
            error_msg = f"Failed to start coordinator: {str(e)}"
            logger.error(error_msg)
            raise ProcessingError(error_msg)

    def process_next(self) -> bool:
        """
        Process the next batch of events from the datasource.

        This method handles the core processing loop and flushes the buffer
        when the flush policy determines it's necessary.

        Returns:
            bool: True if events were processed, False otherwise
        """
        try:
            # Initialize our iterator if needed
            if self._current_iterator is None:
                self._current_iterator = self.datasource.listen()

            # Check if we need to flush before processing new events
            if self.flush_policy.should_flush(self.buffer, self.last_flush_time):
                self._flush_to_stream()

            # Process available events
            events_processed = 0
            should_continue = True

            while should_continue:
                try:
                    event = next(self._current_iterator)
                    self._process_event(event)
                    events_processed += 1

                    # Check if we should stop processing and flush
                    should_continue = not self.flush_policy.should_flush(
                        self.buffer, self.last_flush_time
                    )
                except StopIteration:
                    self._current_iterator = None
                    if events_processed == 0:
                        return False
                    break

            # Final flush check after processing
            if self.flush_policy.should_flush(self.buffer, self.last_flush_time):
                self._flush_to_stream()

            return events_processed > 0

        except Exception as e:
            error_msg = f"Error processing events: {str(e)}"
            logger.error(error_msg)
            raise ProcessingError(error_msg)

    def _process_event(self, event: Dict[str, Any]) -> None:
        """Process a single event through the event processor and buffer it."""
        processed_event = self.event_processor.process(event)
        self.buffer.append(processed_event)
        logger.debug(f"Processed event, buffer size: {len(self.buffer)}")

    def _flush_to_stream(self) -> None:
        """Send buffered events to the stream and update state."""
        if not self.buffer:
            return

        messages = self.buffer
        logger.debug(f"Flushing {len(messages)} messages to stream")

        try:
            self.stream.send(messages)
            self.state_checkpoint_manager.save_state()

            self.buffer.clear()
            self.last_flush_time = time.time()
            self.flush_policy.reset()
        except Exception as e:
            error_msg = f"Failed to flush messages: {str(e)}"
            logger.error(error_msg)
            raise ProcessingError(error_msg)

    def stop(self) -> None:
        """Stop the coordinator and clean up resources."""
        logger.debug("Stopping coordinator")
        try:
            self._flush_to_stream()
            self.stream.close()
            self.datasource.disconnect()
            logger.info("Coordinator stopped")
        except Exception as e:
            logger.error(f"Error stopping coordinator: {e}")
