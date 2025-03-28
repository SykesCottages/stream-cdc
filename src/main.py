import os
import signal
from dotenv import load_dotenv
from logger import Logger
from stream_factory import StreamFactory
from processor import StreamProcessor
from config_loader import EnvConfigLoader
from datasource_factory import DataSourceFactory
from serializer import Serializer
from typing import Any
from worker import Worker


def main() -> None:
    load_dotenv()

    logger_instance = Logger(log_level="INFO")
    logger = logger_instance.get_logger()

    config_loader = EnvConfigLoader()
    app_config = config_loader.load_app_config()

    if app_config.log_level != "INFO":
        logger_instance = Logger(log_level=app_config.log_level)
        logger = logger_instance.get_logger()

    stream_factory = StreamFactory(config_loader)
    stream = stream_factory.create(os.getenv("STREAM_TYPE", "sqs").lower())
    serializer = Serializer()
    processor = StreamProcessor(stream, serializer, app_config.batch_size, app_config.flush_interval)

    data_source_factory = DataSourceFactory(config_loader)
    data_source = data_source_factory.create(os.getenv("DB_TYPE", "mysql").lower())

    worker = Worker(data_source, processor)

    def signal_handler(sig: Any, _frame: Any) -> None:
        logger.info("Shutdown signal received")
        worker.stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("Starting worker")
    worker.run()


if __name__ == "__main__":
    main()
