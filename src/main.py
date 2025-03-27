import os
import time
import logging
import signal
from typing import Any
from dotenv import load_dotenv
from stream_factory import StreamFactory
from processor import StreamProcessor
from config_loader import EnvConfigLoader
from datasource_factory import DataSourceFactory, DataSource
from serializer import Serializer
from exceptions import ProcessingError

class Worker:
    def __init__(self, data_source: DataSource, processor: StreamProcessor,
                 logger: logging.Logger) -> None:
        self.data_source = data_source
        self.processor = processor
        self.logger = logger
        self.running = True

    def run(self) -> None:
        try:
            self.data_source.connect()
            self.logger.info("Connected to data source")
            while self.running:
                for change in self.data_source.listen():
                    if not self.running:
                        break
                    self.logger.debug(">>> start event")
                    self.logger.debug(f">>>event {change}")
                    self.processor.process(change)
        except Exception as e:
            self.logger.error(f"Worker error: {e}")
            raise ProcessingError(f"Processing failed: {str(e)}")
        finally:
            self.processor.close()
            self.data_source.disconnect()
            self.logger.info("Worker stopped gracefully")

    def stop(self) -> None:
        self.running = False
        time.sleep(2)
        exit(0)

def setup_logging(log_level: str) -> logging.Logger:
    logger = logging.getLogger("cdc_worker")
    logger.setLevel(getattr(logging, log_level, logging.INFO))
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    logger.addHandler(handler)
    logger.debug(f"Logging level set to {log_level}")
    return logger

def main() -> None:
    load_dotenv()
    config_loader = EnvConfigLoader()
    app_config = config_loader.load_app_config()
    logger = setup_logging(app_config.log_level)
    stream_factory = StreamFactory(config_loader)
    stream = stream_factory.create(os.getenv("STREAM_TYPE", "sqs").lower())
    serializer = Serializer()
    processor = StreamProcessor(stream, serializer, app_config.batch_size,
                                app_config.flush_interval)
    data_source_factory = DataSourceFactory(config_loader)
    data_source = data_source_factory.create(os.getenv("DB_TYPE",
                                                       "mysql").lower())
    worker = Worker(data_source, processor, logger)

    def signal_handler(sig: Any, _frame: Any) -> None:
        logger.info("Shutdown signal received")
        worker.stop()

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    logger.info("Starting worker")
    worker.run()

if __name__ == "__main__":
    main()

