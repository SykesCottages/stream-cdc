from typing import List, Dict, Any
import time
import logging
from stream_factory import Stream
from serializer import Serializer

class StreamProcessor:
    def __init__(self, stream: Stream, serializer: Serializer,
                 batch_size: int, flush_interval: float) -> None:
        self.stream = stream
        self.serializer = serializer
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.buffer: List[Dict[str, Any]] = []
        self.last_flush_time = time.time()
        self.logger = logging.getLogger("cdc_worker")

    def process(self, event: Dict[str, Any]) -> None:
        serialized_event: Dict[str, Any] = self.serializer.serialize(event)
        self.buffer.append(serialized_event)
        if (len(self.buffer) >= self.batch_size or
                time.time() - self.last_flush_time >= self.flush_interval):
            self.flush()

    def flush(self) -> None:
        if not self.buffer:
            return

        messages = self.buffer
        self.logger.debug(f"Prepared {len(messages)} messages for sending")
        self.stream.send(messages)
        self.buffer.clear()
        self.last_flush_time = time.time()

    def close(self) -> None:
        self.flush()
        self.stream.close()

