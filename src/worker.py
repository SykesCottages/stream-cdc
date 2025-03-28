from logger import logger
import time
from datasource_factory import DataSource
from processor import StreamProcessor
from exceptions import ProcessingError


class Worker:
    def __init__(self, data_source: DataSource, processor: StreamProcessor) -> None:
        self.data_source = data_source
        self.processor = processor
        self.running = True

    def run(self) -> None:
        try:
            self.data_source.connect()
            logger.info("Connected to data source")
            while self.running:
                for change in self.data_source.listen():
                    if not self.running:
                        break
                    logger.info(f"Processing event from {change['table']}")
                    self.processor.process(change)
        except Exception as e:
            logger.error(f"Worker error: {e}")
            raise ProcessingError(f"Processing failed: {str(e)}")
        finally:
            self.processor.close()
            self.data_source.disconnect()
            logger.info("Worker stopped gracefully")

    def stop(self) -> None:
        logger.info("Stop signal received")
        self.running = False
        time.sleep(2)
        exit(0)
