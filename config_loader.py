from dataclasses import dataclass
import os
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
    def load_stream_config(self, stream_type: str) -> StreamConfig:
        match stream_type.lower():
            case "sqs":
                queue_url = os.getenv("SQS_QUEUE_URL")
                if not queue_url:
                    raise ConfigurationError("SQS_QUEUE_URL is not set in environment")
                return SQSConfig(
                    queue_url=queue_url,
                    region=os.getenv("AWS_REGION", "us-east-1"),
                    endpoint_url=os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566"),
                    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "dummy"),
                    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "dummy"),
                )
            case _:
                raise UnsupportedTypeError(
                    f"Stream type '{stream_type}' not supported. "
                    f"Supported types: ['sqs']"
                )

    def load_datasource_config(self, db_type: str) -> DataSourceConfig:
        match db_type.lower():
            case "mysql":
                host = os.getenv("DB_HOST", "mysql")
                user = str(os.getenv("DB_USER"))
                password = str(os.getenv("DB_PASSWORD"))
                port = int(os.getenv("DB_PORT", "3306"))
                if not all([user, password]):
                    raise ValueError
                return MysqlConfig(host, user, password, port)
            case _:
                raise UnsupportedTypeError(
                    f"Database type '{db_type}' not supported. "
                    f"Supported types: ['mysql']"
                )

    def load_app_config(self) -> AppConfig:
        return AppConfig(
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
            batch_size=int(os.getenv("BATCH_SIZE", "10")),
            flush_interval=float(os.getenv("FLUSH_INTERVAL", "5.0"))
        )

