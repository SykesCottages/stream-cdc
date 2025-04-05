from typing import List, Dict, Any, Optional, Iterator
import time
from stream_cdc.utils.logger import logger
from stream_cdc.streams.base import Stream
from stream_cdc.utils.serializer import Serializer
from stream_cdc.datasources.base import DataSource
from stream_cdc.state.base import StateManager
from stream_cdc.utils.exceptions import ProcessingError


class StreamProcessor:
    def __init__(
        self,
        stream: Stream,
        data_source: DataSource,
        state_manager: StateManager,
        batch_size: int,
        flush_interval: float,
    ) -> None:
        self.stream = stream
        self.serializer = Serializer()
        self.data_source = data_source
        self.state_manager = state_manager
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.buffer: List[Dict[str, Any]] = []
        self.last_flush_time = time.time()
        self._current_iterator: Optional[Iterator[Dict[str, Any]]] = None

    def start(self) -> None:
        try:
            self._load_state()
            self.data_source.connect()
            logger.info("Connected to data source")
        except Exception as e:
            error_msg = f"Failed to start processor: {str(e)}"
            logger.error(error_msg)
            raise ProcessingError(error_msg)

    def _load_state(self) -> None:
        if not self.state_manager:
            logger.warning("No state manager configured, skipping state loading")
            return

        try:
            datasource_type = self._get_datasource_type()
            datasource_source = self._get_datasource_source()

            if not datasource_type or not datasource_source:
                logger.warning(
                    "Unable to determine data source type or source identifier"
                )
                return

            # Directly query DynamoDB for debugging
            logger.debug(f"Retrieving state for {datasource_type}:{datasource_source}")
            position = self.state_manager.read(
                datasource_type=datasource_type,
                datasource_source=datasource_source,
            )

            logger.debug(f"Retrieved raw position data: {position}")

            if not position:
                logger.info("No saved state found, starting from default position")
                return

            logger.info(f"Retrieved state from storage: {position}")

            if not isinstance(position, dict) or len(position) == 0:
                logger.warning(f"Invalid position format retrieved: {position}")
                return

            logger.info(f"Resuming from saved position: {position}")
            self.data_source.set_position(position)

            # Log current position after setting
            current_pos = self.data_source.get_position()
            logger.debug(f"Data source position after setting: {current_pos}")
        except Exception as e:
            logger.error(f"Error loading state: {e}")

    def _get_datasource_type(self) -> Optional[str]:
        if hasattr(self.data_source, "SCHEMA_VERSION"):
            schema_version = getattr(self.data_source, "SCHEMA_VERSION", "")
            if schema_version.startswith("mysql-"):
                return "mysql"

        if hasattr(self.data_source, "__class__") and hasattr(
            self.data_source.__class__, "__name__"
        ):
            class_name = self.data_source.__class__.__name__.lower()
            if "mysql" in class_name:
                return "mysql"

        logger.warning("Could not determine data source type for state management")
        return None

    def _get_datasource_source(self) -> Optional[str]:
        if hasattr(self.data_source, "host"):
            host = getattr(self.data_source, "host")
            if host:
                logger.debug(f"Using host '{host}' as data source identifier")
                return host

        if hasattr(self.data_source, "connection_settings"):
            conn_settings = getattr(self.data_source, "connection_settings", {})
            if isinstance(conn_settings, dict) and "host" in conn_settings:
                logger.debug(
                    f"Using connection host '{conn_settings['host']}' as data source identifier"
                )
                return conn_settings["host"]

        logger.warning("Could not determine data source identifier for state management")
        return None

    def process_next(self) -> bool:
        try:
            if self._current_iterator is None:
                self._current_iterator = self.data_source.listen()

            start_time = time.time()
            events_processed = 0

            while (events_processed < self.batch_size and
                   time.time() - start_time < self.flush_interval):
                try:
                    event = next(self._current_iterator)
                    self.process(event)
                    events_processed += 1
                except StopIteration:
                    self._current_iterator = None
                    if events_processed == 0:
                        return False
                    break

            if self.buffer and (
                events_processed >= self.batch_size or
                time.time() - self.last_flush_time >= self.flush_interval
            ):
                self.flush()

            return events_processed > 0

        except Exception as e:
            error_msg = f"Error processing events: {str(e)}"
            logger.error(error_msg)
            raise ProcessingError(error_msg)

    def process(self, event: Dict[str, Any]) -> None:
        if "metadata" not in event:
            error_msg = "Message missing metadata"
            logger.error(error_msg)
            raise ProcessingError(error_msg)

        serialized_event: Dict[str, Any] = self.serializer.serialize(event)
        self.buffer.append(serialized_event)
        logger.debug(f"Processed event, buffer size: {len(self.buffer)}")

        if (len(self.buffer) >= self.batch_size or
            time.time() - self.last_flush_time >= self.flush_interval):
            self.flush()

    def flush(self) -> None:
        if not self.buffer:
            return

        messages = self.buffer
        logger.debug(f"Prepared {len(messages)} messages for sending")

        try:
            self.stream.send(messages)
            self._save_state(messages)

            self.buffer.clear()
            self.last_flush_time = time.time()
        except Exception as e:
            error_msg = f"Failed to flush messages: {str(e)}"
            logger.error(error_msg)
            raise ProcessingError(error_msg)

    def _save_state(self, messages: List[Dict[str, Any]]) -> None:
        if not self.state_manager or not messages:
            return

        try:
            last_message = messages[-1]
            if "metadata" not in last_message:
                return

            metadata = last_message["metadata"]
            datasource_type = metadata.get("datasource_type", "unknown")
            datasource_source = metadata.get("source", "unknown")

            if "position" not in metadata or not metadata["position"]:
                return

            # Remove transaction status check to save state immediately
            position = (
                {"gtid": metadata["position"]}
                if isinstance(metadata["position"], str)
                else metadata["position"]
            )

            # Avoid saving the same position repeatedly
            if hasattr(self, "_last_saved_position") and self._last_saved_position == position:
                logger.debug(f"Position {position} already saved, skipping duplicate save")
                return

            logger.debug(f"Saving position: {position}")

            self.state_manager.store(
                datasource_type=datasource_type,
                datasource_source=datasource_source,
                state_position=position,
            )

            self._last_saved_position = position

            logger.debug(
                f"Updated state for {datasource_type}:{datasource_source} to {position}"
            )
        except Exception as e:
            logger.error(f"Failed to save state: {e}")

    def stop(self) -> None:
        logger.debug("Stopping processor")
        try:
            self.flush()
            self.stream.close()
            self.data_source.disconnect()
            logger.info("Processor stopped")
        except Exception as e:
            logger.error(f"Error stopping processor: {e}")

