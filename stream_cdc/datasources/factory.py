from abc import ABC, abstractmethod
from typing import Generator, Any, Dict, ClassVar, Type, Optional
import os
from stream_cdc.utils.logger import logger
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import GtidEvent
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent
from stream_cdc.utils.exceptions import UnsupportedTypeError, DataSourceError, ConfigurationError


class DataSource(ABC):
    """Base abstract class for all data source implementations."""

    @abstractmethod
    def connect(self) -> None:
        """Connect to the data source."""
        pass

    @abstractmethod
    def listen(self) -> Generator[Dict[str, Any], None, None]:
        """Listen for changes from the data source."""
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """Disconnect from the data source."""
        pass


class MySQLDataSource(DataSource):
    """MySQL binlog implementation of the DataSource interface."""

    def __init__(
        self,
        host: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        port: Optional[int] = None,
        server_id: int = 1234
    ):
        """
        Initialize the MySQL data source.

        Args:
            host: MySQL host
            user: MySQL user
            password: MySQL password
            port: MySQL port
            server_id: Server ID for binlog connection
        """
        self.host = host or os.getenv("DB_HOST")
        if not self.host:
            raise ConfigurationError("DB_HOST is required")

        self.user = user or os.getenv("DB_USER")
        if not self.user:
            raise ConfigurationError("DB_USER is required")

        self.password = password or os.getenv("DB_PASSWORD")
        if not self.password:
            raise ConfigurationError("DB_PASSWORD is required")

        self.port = port or int(os.getenv("DB_PORT", "3306"))
        self.server_id = server_id

        self.binlog_client = BinLogStreamReader
        self.client = None
        self.current_gtid = None

    def connect(self) -> None:
        """Connect to the MySQL binlog stream."""
        logger.info(f"Connecting to MySQL at {self.host}:{self.port}")
        self.client = self.binlog_client(
            connection_settings={
                "host": self.host,
                "user": self.user,
                "passwd": self.password,
                "port": self.port,
            },
            server_id=self.server_id,
            blocking=True,
            resume_stream=True,
            only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, GtidEvent],
        )
        logger.info("Connected to MySQL binlog stream")

    def listen(self) -> Generator[Dict[str, Any], None, None]:
        """Listen for changes from the MySQL binlog."""
        if not self.client:
            logger.error("Data source not connected")
            raise DataSourceError("Data source not connected")

        for event in self.client:
            # Just testing GTID -> this should be use to resume replication in the future
            if isinstance(event, GtidEvent):
                self.current_gtid = event.gtid
                logger.debug(f"Updated current GTID: {self.current_gtid}")
                continue

            for row in event.rows:
                logger.info(
                    f"Event: {type(event).__name__}, "
                    f"Payload: {row}, "
                    f"Schema: {event.schema}, "
                    f"Table: {event.table}, "
                    f"GTID: {self.current_gtid}"
                )
                yield {
                    "schema": event.schema,
                    "table": event.table,
                    "type": type(event).__name__,
                    "row": row,
                    "log_file": self.client.log_file,
                    "log_pos": self.client.log_pos,
                    "gtid": self.current_gtid,
                }

    def disconnect(self) -> None:
        """Disconnect from the MySQL binlog stream."""
        if self.client:
            logger.info("Disconnecting from MySQL")
            self.client.close()
            self.client = None


class DataSourceFactory:
    """Factory for creating DataSource implementations."""

    # Registry of available data source types
    REGISTRY: ClassVar[Dict[str, Type[DataSource]]] = {
        "mysql": MySQLDataSource
    }

    @classmethod
    def create(cls, db_type: str, **kwargs) -> DataSource:
        """
        Create a DataSource implementation based on requested type.

        Args:
            db_type: The type of data source to create
            **kwargs: Configuration parameters to pass to the data source implementation

        Returns:
            An initialized DataSource implementation

        Raises:
            UnsupportedTypeError: If the requested data source type is not supported
        """
        normalized_type = db_type.lower()
        logger.debug(f"Creating data source of type: {normalized_type}")

        if normalized_type not in cls.REGISTRY:
            supported = list(cls.REGISTRY.keys())
            logger.error(f"Unsupported database type: {db_type}. Supported types: {supported}")
            raise UnsupportedTypeError(f"Unsupported database type: {db_type}. Supported types: {supported}")

        datasource_class = cls.REGISTRY[normalized_type]
        return datasource_class(**kwargs)

