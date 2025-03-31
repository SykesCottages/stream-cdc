from stream_cdc.utils.logger import logger
import time
from stream_cdc.datasources.factory import DataSource
from stream_cdc.processing.processor import StreamProcessor
from stream_cdc.utils.exceptions import ProcessingError


class Worker:
    def __init__(self, data_source: DataSource, processor: StreamProcessor) -> None:
        self.data_source = data_source
        self.processor = processor
        self.running = True

    def run(self) -> None:
        try:
            logger.info("Worker Started")
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
