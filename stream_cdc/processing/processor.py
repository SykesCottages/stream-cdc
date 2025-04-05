from typing import List, Dict, Any
import time
from stream_cdc.utils.logger import logger
from stream_cdc.streams.factory import Stream
from stream_cdc.utils.serializer import Serializer
from stream_cdc.datasources.base import DataSource
from stream_cdc.state.base import StateManager


class StreamProcessor:
    """
    Processes data change events and sends them to a stream.

    This class manages the buffering and batching of events before sending them
    to a stream. It can be configured to flush the buffer based on size or time
    interval.
    """

    def __init__(
        self,
        stream: Stream,
        serializer: Serializer,
        batch_size: int,
        flush_interval: float,
        data_source: DataSource,
        state_manager: StateManager,
    ) -> None:
        """
        Initialize the processor with a stream, serializer, and batching parameters.

        Args:
            stream (Stream): The stream to send processed events to.
            serializer (Serializer): The serializer to use for event serialization.
            batch_size (int): The maximum number of events to buffer before flushing.
            flush_interval (float): The maximum time (in seconds) to wait before flushing.
            data_source (Optional[DataSource]): The data source to get position from.
            state_manager (Optional[StateManager]): The state manager to store position.
        """
        self.stream = stream
        self.serializer = serializer
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.buffer: List[Dict[str, Any]] = []
        self.last_flush_time = time.time()
        self.data_source = data_source
        self.state_manager = state_manager

    def process(self, event: Dict[str, Any]) -> None:
        """
        Process a data change event.

        Serializes the event, adds it to the buffer, and flushes the buffer if
        necessary based on the configured batch size or flush interval.

        Args:
            event (Dict[str, Any]): The data change event to process.
        """
        if "metadata" not in event:
            raise Exception("Message missing metadata")

        serialized_event: Dict[str, Any] = self.serializer.serialize(event)
        self.buffer.append(serialized_event)
        logger.debug(f"Processed event, buffer size: {len(self.buffer)}")

        if (
            len(self.buffer) >= self.batch_size
            or time.time() - self.last_flush_time >= self.flush_interval
        ):
            self.flush()

    def flush(self) -> None:
        """
        Flush the buffer, sending all buffered events to the stream.

        This method sends all buffered events to the stream and clears the buffer.
        If the buffer is empty, this method does nothing.
        """
        if not self.buffer:
            return

        messages = self.buffer
        logger.debug(f"Prepared {len(messages)} messages for sending")

        self.stream.send(messages)


        if self.state_manager and messages:
            last_message = messages[-1]

            if "metadata" in last_message:
                metadata = last_message["metadata"]
                datasource_type = metadata.get("datasource_type", "unknown")
                datasource_source = metadata.get("source", "unknown")

                if "position" in metadata:
                    position = metadata["position"]

                    self.state_manager.store(
                        datasource_type=datasource_type,
                        datasource_source=datasource_source,
                        state_position=position,
                    )
                    logger.debug(
                        f"Updated state for {datasource_type}:{datasource_source} to {position}"
                    )

        self.buffer.clear()
        self.last_flush_time = time.time()

    def close(self) -> None:
        """
        Close the processor, flushing any remaining events.

        This method flushes any events remaining in the buffer and closes the
        underlying stream.
        """
        logger.debug("Closing processor, flushing remaining messages")
        self.flush()
        self.stream.close()
