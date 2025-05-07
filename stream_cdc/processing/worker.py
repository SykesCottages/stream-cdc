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
        self._stopping = False

    def run(self) -> None:
        """
        Run the worker, initiating the data processing pipeline.

        This method starts the coordinator and continuously processes data until
        the worker is stopped.

        Raises:
            ProcessingError: If processing fails.
        """
        if not self.coordinator:
            raise ProcessingError("No coordinator provided")

        try:
            logger.info("Worker started")
            self.coordinator.start()

            idle_count = 0
            max_idle_count = 10
            idle_sleep_time = 0.1

            while self.running:
                if not self.running:
                    break

                # Process next batch of events
                events_processed = self.coordinator.process_next()

                # If no events were processed, implement backoff sleep

                if not events_processed:
                    idle_count += 1
                    if idle_count >= max_idle_count:
                        # Exponential backoff with a cap
                        sleep_time = min(
                            idle_sleep_time
                            * (1.5 ** min(idle_count - max_idle_count, 10)),
                            5,
                        )
                        time.sleep(sleep_time)
                else:
                    idle_count = 0

        except Exception as e:
            logger.error(f"Worker error: {e}")
            raise ProcessingError(f"Processing failed: {str(e)}")
        finally:
            # Only stop if not already stopping to prevent recursive calls
            if not self._stopping:
                self._stopping = True
                self._stop_coordinator()
                logger.info("Worker stopped gracefully")

    def _stop_coordinator(self) -> None:
        """Stop the coordinator safely."""
        if self.coordinator:
            try:
                self.coordinator.stop()
            except Exception as e:
                logger.error(f"Error stopping coordinator: {e}")

    def stop(self) -> None:
        """
        Stop the worker gracefully.

        This method signals the worker to stop processing.
        """
        if self._stopping:
            logger.debug("Stop already in progress, ignoring duplicate call")
            return

        logger.info("Stop signal received")
        self._stopping = True
        self.running = False
