from dataclasses import dataclass
import os
from stream_cdc.utils.logger import logger


@dataclass
class AppConfig(object):
    """
    Application-wide configuration.

    This class represents the configuration for the entire application,
    including logging level, batch size, and flush interval.
    """

    log_level: str
    batch_size: int
    flush_interval: float

    @classmethod
    def load(cls) -> "AppConfig":
        """
        Create an AppConfig instance from environment variables.

        Returns:
            AppConfig: Configured instance with values from environment variables
                      or defaults if the environment variables are not set.
        """
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()
        batch_size = int(os.getenv("BATCH_SIZE", "10"))
        flush_interval = float(os.getenv("FLUSH_INTERVAL", "5.0"))

        logger.info(f"Config: log_level={log_level}, batch_size={batch_size}, interval={flush_interval}")

        return cls(
            log_level=log_level,
            batch_size=batch_size,
            flush_interval=flush_interval,
        )

