from stream_cdc.utils.logger import logger
import time
from stream_cdc.datasources.factory import DataSource
from stream_cdc.processing.processor import StreamProcessor
from stream_cdc.utils.exceptions import ProcessingError


class Worker:
    """
    Main worker class that orchestrates the data flow pipeline.

    This class connects a data source to a processor, continuously pulling changes
    from the data source and passing them to the processor for processing and
    eventual delivery to a stream.
    """

    def __init__(
        self, data_source: DataSource, processor: StreamProcessor
    ) -> None:
        """
        Initialize the worker with a data source and processor.

        Args:
            data_source (DataSource): The data source to listen for changes from.
            processor (StreamProcessor): The processor to handle the changes.
        """
        self.data_source = data_source
        self.processor = processor
        self.running = True

    def run(self) -> None:
        """
        Run the worker, processing changes from the data source.

        This method starts the worker loop, connecting to the data source,
        listening for changes, and passing them to the processor. The loop
        continues until the worker is stopped.

        Raises:
            ProcessingError: If processing fails.
        """
        try:
            logger.info("Worker Started")
            self.data_source.connect()
            logger.info("Connected to data source")
            while self.running:
                for change in self.data_source.listen():
                    if not self.running:
                        break
                    self.processor.process(change)
        except Exception as e:
            logger.error(f"Worker error: {e}")
            raise ProcessingError(f"Processing failed: {str(e)}")
        finally:
            self.processor.close()
            self.data_source.disconnect()
            logger.info("Worker stopped gracefully")

    def stop(self) -> None:
        """
        Stop the worker gracefully.

        This method signals the worker to stop processing, waits for a short
        period to allow ongoing processing to complete, and then exits.
        """
        logger.info("Stop signal received")
        self.running = False
        time.sleep(2)
        exit(0)
