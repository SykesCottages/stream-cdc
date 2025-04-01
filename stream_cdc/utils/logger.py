import logging
import os
from typing import Optional


class Logger:
    """
    Singleton logger class for consistent logging across the application.

    This class provides a centralized logging mechanism with customizable log levels
    and formatting. It implements the singleton pattern to ensure that only one
    instance of the logger exists throughout the application.
    """

    _instance = None

    def __init__(self, log_level: str = "INFO", logger_name: Optional[str] = None):
        """
        Initialize the logger with a specific log level and optional name.

        Args:
            log_level (str): The logging level (e.g., "INFO", "DEBUG"). Defaults to "INFO".
            logger_name (str, optional): The name of the logger. Defaults to APP_NAME or "stream-cdc".
        """
        self.logger_name = logger_name or os.getenv("APP_NAME", "stream-cdc")
        self.logger = logging.getLogger(self.logger_name)

        self.logger.handlers.clear()

        console_handler = logging.StreamHandler()
        console_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
        self.logger.addHandler(console_handler)

        self.set_level(log_level)

        self.logger.propagate = False

    def set_level(self, log_level: str) -> None:
        """
        Set or update the logging level.

        Args:
            log_level (str): The logging level (e.g., "INFO", "DEBUG").
        """
        level = getattr(logging, log_level.upper(), logging.INFO)
        self.logger.setLevel(level)
        self.logger.info(f"Logging level set to {log_level}")

    @classmethod
    def get_logger(cls, log_level: str = "INFO") -> logging.Logger:
        """
        Get the singleton logger instance, creating it if it doesn't exist.

        Args:
            log_level (str): The logging level to use if creating a new instance.

        Returns:
            logging.Logger: The configured logger instance.
        """
        if cls._instance is None:
            cls._instance = Logger(log_level=log_level)
        return cls._instance.logger

    @classmethod
    def update_level(cls, log_level: str) -> None:
        """
        Update the logging level of the existing logger.

        Args:
            log_level (str): The new logging level.
        """
        if cls._instance is None:
            cls.get_logger(log_level=log_level)
        else:
            cls._instance.set_level(log_level)


logger = Logger.get_logger()

