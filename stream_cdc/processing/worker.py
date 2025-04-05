from stream_cdc.utils.logger import logger
import time
from stream_cdc.processing.processor import StreamProcessor
from stream_cdc.utils.exceptions import ProcessingError


class Worker:
    """
    Main worker class that orchestrates the data flow pipeline.

    This class connects a data source to a processor, continuously pulling changes
    from the data source and passing them to the processor for processing and
    eventual delivery to a stream.
    """

    def __init__(self, processor: StreamProcessor) -> None:
        """
        Initialize the worker with a data source and processor.

        Args:
            data_source (DataSource): The data source to listen for changes from.
            processor (StreamProcessor): The processor to handle the changes.
            state_manager (Optional[StateManager]): The state manager to load/save state.
        """
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
            self.processor.start()

            while self.running:
                self.processor.process_next()

        except Exception as e:
            logger.error(f"Worker error: {e}")
            raise ProcessingError(f"Processing failed: {str(e)}")
        finally:
            self.processor.stop()
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
