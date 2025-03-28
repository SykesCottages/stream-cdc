from dataclasses import dataclass
import os
from logger import logger
from abc import ABC, abstractmethod
from typing import Any
from exceptions import UnsupportedTypeError, ConfigurationError


class StreamConfig(ABC):
    pass


class DataSourceConfig(ABC):
    pass


@dataclass
class SQSConfig(StreamConfig):
    queue_url: str
    region: str
    endpoint_url: str
    aws_access_key_id: str
    aws_secret_access_key: str


@dataclass
class MysqlConfig(DataSourceConfig):
    host: str
    user: str
    password: str
    port: int


@dataclass
class AppConfig:
    log_level: str
    batch_size: int
    flush_interval: float


class ConfigLoader(ABC):
    @abstractmethod
    def load_stream_config(self, stream_type: str) -> Any:
        pass

    @abstractmethod
    def load_datasource_config(self, db_type: str) -> Any:
        pass

    @abstractmethod
    def load_app_config(self) -> AppConfig:
        pass


class EnvConfigLoader(ConfigLoader):
    def load_stream_config(self, stream_type: str):
        logger.info(f"Loading stream config for: {stream_type}")
        match stream_type.lower():
            case "sqs":
                queue_url = os.getenv("SQS_QUEUE_URL")
                if not queue_url:
                    logger.error("SQS_QUEUE_URL is not set in environment")
                    raise ConfigurationError(
                        "SQS_QUEUE_URL is not set in environment"
                    )

                region = os.getenv("AWS_REGION")
                if not region:
                    logger.error("AWS_REGION is not set in environment")
                    raise ConfigurationError(
                        "AWS_REGION is not set in environment"
                    )

                return SQSConfig(
                    queue_url=queue_url,
                    region=region,
                    endpoint_url=os.getenv("AWS_ENDPOINT_URL", ""),
                    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", ""),
                    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", ""),
                )
            case _:
                logger.error(f"Unsupported stream type: {stream_type}")
                raise UnsupportedTypeError(
                    f"Stream type '{stream_type}' not supported. "
                    f"Supported types: ['sqs']"
                )

    def load_datasource_config(self, db_type: str):
        logger.info(f"Loading datasource config for: {db_type}")
        match db_type.lower():
            case "mysql":
                host = os.getenv("DB_HOST")
                if not host:
                    logger.error("DB_HOST is not set in environment")
                    raise ConfigurationError("DB_HOST is not set in environment")

                user = os.getenv("DB_USER")
                if not user:
                    logger.error("DB_USER is not set in environment")
                    raise ConfigurationError("DB_USER is not set in environment")

                password = os.getenv("DB_PASSWORD")
                if not password:
                    logger.error("DB_PASSWORD is not set in environment")
                    raise ConfigurationError(
                        "DB_PASSWORD is not set in environment"
                    )

                port = int(os.getenv("DB_PORT", "3306"))
                return MysqlConfig(host, user, password, port)
            case _:
                logger.error(f"Unsupported database type: {db_type}")
                raise UnsupportedTypeError(
                    f"Database type '{db_type}' not supported. "
                    f"Supported types: ['mysql']"
                )

    def load_app_config(self) -> AppConfig:
        logger.info("Loading application config")
        log_level = os.getenv("LOG_LEVEL", "INFO").upper()
        batch_size = int(os.getenv("BATCH_SIZE", "10"))
        flush_interval = float(os.getenv("FLUSH_INTERVAL", "5.0"))

        logger.info(
            f"Config: log_level={log_level}, "
            f"batch_size={batch_size}, interval={flush_interval}"
        )

        return AppConfig(
            log_level=log_level,
            batch_size=batch_size,
            flush_interval=flush_interval,
        )

