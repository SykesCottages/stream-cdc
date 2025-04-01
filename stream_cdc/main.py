import os
import signal
from dotenv import load_dotenv
from stream_cdc.utils.logger import Logger
from stream_cdc.streams.factory import StreamFactory
from stream_cdc.processing.processor import StreamProcessor
from stream_cdc.datasources.factory import DataSourceFactory
from stream_cdc.utils.serializer import Serializer
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

    stream = StreamFactory.create(stream_type)
    serializer = Serializer()
    processor = StreamProcessor(stream, serializer, app_config.batch_size, app_config.flush_interval)
    datasource = DataSourceFactory.create(datasource_type)
    worker = Worker(datasource, processor)

    def signal_handler(sig: Any, _frame: Any) -> None:
        """
        Handle operating system signals for graceful shutdown.

        This function is called when the application receives a shutdown signal
        (e.g., SIGINT, SIGTERM). It logs the event and stops the worker.

        Args:
            sig (Any): The signal received.
            _frame (Any): The current stack frame (unused).
        """
        logger.info("Shutdown signal received")
        worker.stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    worker.run()


if __name__ == "__main__":
    main()

