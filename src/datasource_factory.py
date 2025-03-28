from abc import ABC, abstractmethod
from typing import Generator, Any, Dict
import logging
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent
)
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
    def __init__(self, config: MysqlConfig, binlog_client):
        self.host = config.host
        self.user = config.user
        self.password = config.password
        self.port = config.port
        self.binlog_client = binlog_client
        self.client = None

    def connect(self) -> None:
        logging.info(f"Connecting to MySQL at {self.host}:{self.port}")
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
            only_events=[WriteRowsEvent, UpdateRowsEvent, DeleteRowsEvent],
        )
        logging.info("Connected to MySQL binlog stream")

    def listen(self) -> Generator[Dict[str, Any]]:
        if not self.client:
            logging.error("Data source not connected")
            raise DataSourceError("Data source not connected")

        for event in self.client:
            for row in event.rows:
                logging.debug(
                    f"Event: {type(event).__name__}, "
                    f"Schema: {event.schema}, Table: {event.table}"
                )
                yield {
                    "schema": event.schema,
                    "table": event.table,
                    "type": type(event).__name__,
                    "row": row,
                    "log_file": self.client.log_file,
                    "log_pos": self.client.log_pos,
                }

    def disconnect(self) -> None:
        if self.client:
            logging.info("Disconnecting from MySQL")
            self.client.close()
            self.client = None


class DataSourceFactory:
    def __init__(self, config_loader):
        self.config_loader = config_loader
        self.binlog_client = BinLogStreamReader

    def create(self, db_type: str) -> DataSource:
        logging.debug(f"Creating data source of type: {db_type}")
        match db_type.lower():
            case "mysql":
                return MySQLDataSource(
                    self.config_loader.load_datasource_config(db_type),
                    self.binlog_client
                )
            case _:
                logging.error(f"Unsupported database type: {db_type}")
                raise UnsupportedTypeError(
                    f"Database type '{db_type}' is not supported. "
                    f"Supported types: ['mysql']"
                )

