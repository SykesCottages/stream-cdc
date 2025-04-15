from typing import List, Dict, Any, Optional, Iterator
import time
from stream_cdc.utils.logger import logger
from stream_cdc.streams.base import Stream
from stream_cdc.utils.serializer import Serializer
from stream_cdc.datasources.base import DataSource
from stream_cdc.state.base import StateManager
from stream_cdc.utils.exceptions import ProcessingError


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
        batch_size: int = 10,
        flush_interval: float = 5.0,
    ) -> None:
        """
        Initialize the Coordinator.

        Args:
            datasource: The data source to retrieve events from
            state_manager: The state manager to load/save position state
            stream: The stream to send processed events to
            batch_size: Maximum number of events to batch before sending
            flush_interval: Maximum time (seconds) to wait before forcing a flush
        """
        self.datasource = datasource
        self.state_manager = state_manager
        self.stream = stream
        self.serializer = Serializer()
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.buffer: List[Dict[str, Any]] = []
        self.last_flush_time = time.time()
        self._current_iterator: Optional[Iterator[Dict[str, Any]]] = None

    def start(self) -> None:
        """Start the coordinator by loading state and connecting to datasource."""
        try:
            self._load_state()
            self.datasource.connect()
            logger.info("Connected to data source")
        except Exception as e:
            error_msg = f"Failed to start coordinator: {str(e)}"
            logger.error(error_msg)
            raise ProcessingError(error_msg)

    def _load_state(self) -> None:
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

            if not isinstance(position, dict) or len(position) == 0:
                logger.warning(f"Invalid position format retrieved: {position}")
                return

            logger.info(f"Resuming from saved position: {position}")
            self.datasource.set_position(position)
        except Exception as e:
            logger.error(f"Error loading state: {e}")

    def _get_datasource_type(self) -> Optional[str]:
        """Determine the type of datasource for state management."""
        if hasattr(self.datasource, "SCHEMA_VERSION"):
            schema_version = getattr(self.datasource, "SCHEMA_VERSION", "")
            if schema_version.startswith("mysql-"):
                return "mysql"

        if hasattr(self.datasource, "__class__") and hasattr(
            self.datasource.__class__, "__name__"
        ):
            class_name = self.datasource.__class__.__name__.lower()
            if "mysql" in class_name:
                return "mysql"

        logger.warning("Could not determine data source type for state management")
        return None

    def _get_datasource_source(self) -> Optional[str]:
        """Determine the source identifier for state management."""
        if hasattr(self.datasource, "host"):
            host = getattr(self.datasource, "host")
            if host:
                return host

        if hasattr(self.datasource, "connection_settings"):
            conn_settings = getattr(self.datasource, "connection_settings", {})
            if isinstance(conn_settings, dict) and "host" in conn_settings:
                return conn_settings["host"]

        logger.warning(
            "Could not determine data source identifier for state management"
        )
        return None

    def process_next(self) -> bool:
        """
        Process the next batch of events from the datasource.

        Returns:
            bool: True if events were processed, False otherwise
        """
        try:
            if self._current_iterator is None:
                self._current_iterator = self.datasource.listen()

            start_time = time.time()
            events_processed = 0

            # Process events until batch size or time interval is reached
            while (
                events_processed < self.batch_size
                and time.time() - start_time < self.flush_interval
            ):
                try:
                    event = next(self._current_iterator)
                    self._process_event(event)
                    events_processed += 1
                except StopIteration:
                    self._current_iterator = None
                    if events_processed == 0:
                        return False
                    break

            # Check if we should flush based on buffer size or time
            if self.buffer and (
                len(self.buffer) >= self.batch_size
                or time.time() - self.last_flush_time >= self.flush_interval
            ):
                self._flush_to_stream()

            return events_processed > 0

        except Exception as e:
            error_msg = f"Error processing events: {str(e)}"
            logger.error(error_msg)
            raise ProcessingError(error_msg)

    def _process_event(self, event: Dict[str, Any]) -> None:
        """Process a single event from the datasource."""
        if "metadata" not in event:
            error_msg = "Message missing metadata"
            logger.error(error_msg)
            raise ProcessingError(error_msg)

        serialized_event: Dict[str, Any] = self.serializer.serialize(event)
        self.buffer.append(serialized_event)
        logger.debug(f"Processed event, buffer size: {len(self.buffer)}")

        if (
            len(self.buffer) >= self.batch_size
            or time.time() - self.last_flush_time >= self.flush_interval
        ):
            self._flush_to_stream()

    def _flush_to_stream(self) -> None:
        """Send buffered events to the stream and update state."""
        if not self.buffer:
            return

        messages = self.buffer
        logger.debug(f"Prepared {len(messages)} messages for sending")

        try:
            self.stream.send(messages)
            self._save_state()

            self.buffer.clear()
            self.last_flush_time = time.time()
        except Exception as e:
            error_msg = f"Failed to flush messages: {str(e)}"
            logger.error(error_msg)
            raise ProcessingError(error_msg)

    def _save_state(self) -> None:
        """Save the current position state from the datasource."""
        if not self.state_manager:
            return

        try:
            position = self.datasource.get_position()

            if not position or not isinstance(position, dict) or len(position) == 0:
                logger.debug("No valid position available from datasource")
                return

            datasource_type = self.datasource.get_source_type()
            datasource_id = self.datasource.get_source_id()

            if not datasource_type or not datasource_id:
                logger.warning(
                    "Unable to determine data source type or source identifier"
                )
                return

            if (
                hasattr(self, "_last_saved_position")
                and self._last_saved_position == position
            ):
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
