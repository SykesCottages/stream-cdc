from abc import ABC, abstractmethod
from typing import Generator, Any, Dict
from logger import logger
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import GtidEvent
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent
from config_loader import MysqlConfig
from exceptions import UnsupportedTypeError, DataSourceError


class DataSource(ABC):
    @abstractmethod
    def connect(self) -> None:
        pass

    @abstractmethod
    def listen(self) -> Generator[Dict[str, Any]]:
        pass

    @abstractmethod
    def disconnect(self) -> None:
        pass


class MySQLDataSource(DataSource):
    def __init__(self, config: MysqlConfig):
        self.host = config.host
        self.user = config.user
        self.password = config.password
        self.port = config.port
        self.binlog_client = BinLogStreamReader
        self.client = None
        self.current_gtid = None

    def connect(self) -> None:
        logger.info(f"Connecting to MySQL at {self.host}:{self.port}")
        self.client = self.binlog_client(
            connection_settings={
                "host": self.host,
                "user": self.user,
                "passwd": self.password,
                "port": self.port,
            },
            server_id=1234,
            blocking=True,
            resume_stream=True,
            only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent, GtidEvent],
        )
        logger.info("Connected to MySQL binlog stream")

    def listen(self) -> Generator[Dict[str, Any]]:
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
        if self.client:
            logger.info("Disconnecting from MySQL")
            self.client.close()
            self.client = None


class DataSourceFactory:
    def __init__(self, config_loader):
        self.config_loader = config_loader

    def create(self, db_type: str) -> DataSource:
        logger.debug(f"Creating data source of type: {db_type}")
        match db_type.lower():
            case "mysql":
                return MySQLDataSource(self.config_loader.load_datasource_config(db_type))
            case _:
                logger.error(f"Unsupported database type: {db_type}")
                raise UnsupportedTypeError(
                    f"Database type '{db_type}' is not supported. Supported types: ['mysql']"
                )
