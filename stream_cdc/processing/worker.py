import time
from stream_cdc.utils.logger import logger
from stream_cdc.processing.coordinator import Coordinator
from stream_cdc.utils.exceptions import ProcessingError


class Worker:
    """
    Main worker class that orchestrates the data flow pipeline.

    This class manages the coordinator, handling the lifecycle of the data
    processing pipeline and graceful shutdown procedures.
    """

    def __init__(self, coordinator: Coordinator) -> None:
        """
        Initialize the worker with a coordinator.

        Args:
            coordinator: The coordinator responsible for data flow orchestration
        """
        self.coordinator = coordinator
        self.running = True

    def run(self) -> None:
        """
        Run the worker, initiating the data processing pipeline.

        This method starts the coordinator and continuously processes data until
        the worker is stopped.

        Raises:
            ProcessingError: If processing fails.
        """
        try:
            logger.info("Worker started")
            self.coordinator.start()

            while self.running:
                self.coordinator.process_next()

        except Exception as e:
            logger.error(f"Worker error: {e}")
            raise ProcessingError(f"Processing failed: {str(e)}")
        finally:
            self.coordinator.stop()
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

