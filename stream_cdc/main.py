import os
import signal
from typing import Any
from dotenv import load_dotenv
import time

from stream_cdc.utils.logger import Logger
from stream_cdc.streams.factory import StreamFactory
from stream_cdc.datasources.factory import DataSourceFactory
from stream_cdc.state.factory import StateManagerFactory
from stream_cdc.processing.coordinator import Coordinator
from stream_cdc.processing.worker import Worker
from stream_cdc.config.loader import AppConfig
from stream_cdc.utils.serializer import Serializer


class DefaultEventProcessor:
    """Default implementation of event processing logic."""

    def __init__(self):
        self.serializer = Serializer()

    def process(self, event: dict) -> dict:
        """Process a single event by serializing it."""
        return self.serializer.serialize(event)


class BatchSizeAndTimePolicy:
    """
    Flush policy based on batch size and elapsed time.
    """

    def __init__(self, batch_size: int, flush_interval: float):
        self.batch_size = batch_size
        self.flush_interval = flush_interval

    def should_flush(self, buffer: list, last_flush_time: float) -> bool:
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


def main() -> None:
    """
    Main entry point for the stream-cdc application.

    This function initializes the application, including loading configuration,
    setting up the logger, creating all necessary components, and starting the worker.
    It also sets up signal handlers for graceful shutdown.
    """
    load_dotenv()

    logger_instance = Logger(log_level="INFO")
    logger = logger_instance.get_logger()

    app_config = AppConfig.load()

    if app_config.log_level != "INFO":
        logger_instance = Logger(log_level=app_config.log_level)
        logger = logger_instance.get_logger()

    stream_type = os.getenv("STREAM_TYPE", "sqs").lower()
    datasource_type = os.getenv("DS_TYPE", "mysql").lower()
    state_manager_type = os.getenv("STATE_MANAGER_TYPE", "dynamodb").lower()

    stream = StreamFactory.create(stream_type)
    datasource = DataSourceFactory.create(datasource_type)
    state_manager = StateManagerFactory.create(state_manager_type)

    # Create the components needed by the coordinator
    event_processor = DefaultEventProcessor()
    flush_policy = BatchSizeAndTimePolicy(
        batch_size=app_config.batch_size, flush_interval=app_config.flush_interval
    )

    # Create the coordinator with improved component structure
    coordinator = Coordinator(
        datasource=datasource,
        state_manager=state_manager,
        stream=stream,
        event_processor=event_processor,
        flush_policy=flush_policy,
    )

    worker = Worker(coordinator)

    def signal_handler(sig: Any, _frame: Any) -> None:
        logger.info("Shutdown signal received")
        worker.stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    worker.run()


if __name__ == "__main__":
    main()
