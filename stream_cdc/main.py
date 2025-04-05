import os
import signal
from dotenv import load_dotenv
from stream_cdc.utils.logger import Logger
from stream_cdc.streams.factory import StreamFactory
from stream_cdc.processing.processor import StreamProcessor
from stream_cdc.datasources.factory import DataSourceFactory
from stream_cdc.state.factory import StateManagerFactory
from typing import Any
from stream_cdc.processing.worker import Worker
from stream_cdc.config.loader import AppConfig


def main() -> None:
    """
    Main entry point for the stream-cdc application.

    This function initializes the application, including loading the configuration,
    setting up the logger, creating the stream, serializer, processor, data source,
    and worker. It also sets up signal handlers for graceful shutdown and starts
    the worker.

    The function does not return under normal operation; it continues running until
    a shutdown signal is received.
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

    # Create components
    stream = StreamFactory.create(stream_type)
    datasource = DataSourceFactory.create(datasource_type)

    state_manager = StateManagerFactory.create(state_manager_type)

    # Create processor with state manager
    processor = StreamProcessor(
        stream=stream,
        batch_size=app_config.batch_size,
        flush_interval=2.0,
        data_source=datasource,
        state_manager=state_manager,
    )

    # Create worker with state manager
    worker = Worker(processor)

    # Signal handling and run
    def signal_handler(sig: Any, _frame: Any) -> None:
        logger.info("Shutdown signal received")
        worker.stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    worker.run()


if __name__ == "__main__":
    main()
