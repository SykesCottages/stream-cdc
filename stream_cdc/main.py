import os
import signal
from typing import Any
from dotenv import load_dotenv

from stream_cdc.utils.logger import Logger
from stream_cdc.streams.factory import StreamFactory
from stream_cdc.datasources.factory import DataSourceFactory
from stream_cdc.state.factory import StateManagerFactory
from stream_cdc.processing.coordinator import Coordinator, BatchSizeAndTimePolicy
from stream_cdc.processing.processors import DefaultEventProcessor
from stream_cdc.processing.worker import Worker
from stream_cdc.config.loader import AppConfig


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

    # Create the components needed by the coordinator
    stream_settings = {"source": os.getenv("DB_HOST")}
    stream = StreamFactory.create(stream_type, **stream_settings)
    datasource = DataSourceFactory.create(datasource_type)
    state_manager = StateManagerFactory.create(state_manager_type)
    event_processor = DefaultEventProcessor()
    flush_policy = BatchSizeAndTimePolicy(
        batch_size=app_config.batch_size, flush_interval=app_config.flush_interval
    )

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
