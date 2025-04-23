from typing import Generator, Any, Dict, Optional, Union
import os
import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import GtidEvent, QueryEvent
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)

from stream_cdc.datasources.base import DataSource
from stream_cdc.utils.logger import logger
from stream_cdc.utils.exceptions import DataSourceError, ConfigurationError
from stream_cdc.position.position import Position


class GTIDPosition:
    """Position implementation for MySQL GTID-based positioning."""

    def __init__(self, gtid: Optional[str] = None):
        self.gtid = gtid

    def to_dict(self) -> Dict[str, str]:
        if not self.gtid:
            return {}
        return {"gtid": self.gtid}

    def from_dict(self, position_data: Dict[str, str]) -> None:
        if not position_data:
            self.gtid = None
            return

        if "gtid" not in position_data:
            raise ValueError("Invalid position data: missing gtid field")

        self.gtid = position_data["gtid"]

    def is_valid(self) -> bool:
        return self.gtid is not None and len(self.gtid) > 0

    def __str__(self) -> str:
        return f"GTID: {self.gtid}" if self.gtid else "No position"


class MySQLSettingsValidator:
    """Validates MySQL server settings required for CDC operation."""

    def __init__(
        self,
        host: Union[str, None],
        user: Union[str, None],
        password: Union[str, None],
        port: Union[int, None],
    ):
        if not host:
            raise ConfigurationError("Database host is required for validation")
        if not user:
            raise ConfigurationError("Database user is required for validation")
        if not password:
            raise ConfigurationError("Database password is required for validation")
        if not port:
            raise ConfigurationError("Database port is required for validation")

        self.host = host
        self.user = user
        self.password = password
        self.port = port

    def _get_required_settings(self) -> Dict[str, str]:
        return {
            "binlog_format": "ROW",
            "binlog_row_metadata": "FULL",
            "binlog_row_image": "FULL",
            "gtid_mode": "ON",
            "enforce_gtid_consistency": "ON",
        }

    def _fetch_actual_settings(self, cursor) -> Dict[str, str]:
        required_settings = self._get_required_settings()

        var_query = "SHOW GLOBAL VARIABLES WHERE Variable_name IN (%s)"
        placeholders = ", ".join(["%s"] * len(required_settings))
        cursor.execute(var_query % placeholders, list(required_settings.keys()))

        actual_settings = {}
        for var_name, var_value in cursor.fetchall():
            if var_name is not None and var_value is not None:
                actual_settings[var_name.lower()] = var_value

        return actual_settings

    def _verify_settings(self, actual_settings: Dict[str, str]) -> None:
        required_settings = self._get_required_settings()

        for setting, expected in required_settings.items():
            actual = actual_settings.get(setting)

            if actual is None:
                logger.error(f"MySQL setting {setting} not found in server variables")
                raise ConfigurationError(f"MySQL setting {setting} not found")

            if actual.upper() != expected.upper():
                logger.error(
                    f"MySQL setting {setting} is set to {actual}, expected {expected}"
                )
                raise ConfigurationError(
                    f"MySQL setting {setting} is incorrect: "
                    f"expected={expected}, actual={actual}"
                )

            logger.info(f"MySQL setting {setting} is correctly set to {actual}")

    def validate(self) -> None:
        try:
            conn = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                port=self.port,
            )

            with conn.cursor() as cursor:
                actual_settings = self._fetch_actual_settings(cursor)
                self._verify_settings(actual_settings)

            conn.close()

        except pymysql.Error as e:
            error_msg = f"Failed to connect to MySQL: {e}"
            logger.error(error_msg)
            raise ConfigurationError(error_msg)
        except Exception as e:
            if not isinstance(e, ConfigurationError):
                error_msg = f"Failed to validate MySQL settings: {e}"
                logger.error(error_msg)
                raise ConfigurationError(error_msg)
            raise


class MySQLDataSource(DataSource):
    """MySQL CDC data source that streams changes from MySQL binlog."""

    def __init__(
        self,
        host: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        port: Optional[int] = None,
        server_id: int = 1234,
    ):
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

        # Use GTIDPosition object instead of raw string
        self.position = GTIDPosition()
        self.current_transaction_gtid = None

        self.transaction_complete = False
        self.last_event_time = 0
        self.event_timeout = 30

    def _validate_settings(self) -> None:
        try:
            validator = MySQLSettingsValidator(
                host=self.host,
                user=self.user,
                password=self.password,
                port=self.port,
            )
            validator.validate()
        except Exception as e:
            logger.error(f"MySQL settings validation failed: {e}")
            raise

    def _create_binlog_client(self) -> BinLogStreamReader:
        connection_settings = {
            "host": self.host,
            "user": self.user,
            "passwd": self.password,
            "port": self.port,
        }

        client_args = {
            "connection_settings": connection_settings,
            "server_id": self.server_id,
            "blocking": True,
            "resume_stream": True,
            "only_events": [
                WriteRowsEvent,
                UpdateRowsEvent,
                DeleteRowsEvent,
                GtidEvent,
                QueryEvent,
            ],
        }

        # Only set auto_position if we have a valid GTID
        if self.position and self.position.is_valid():
            logger.debug(f"Setting auto_position to: {self.position.gtid}")
            client_args["auto_position"] = self.position.gtid

        return self.binlog_client(**client_args)

    def connect(self) -> None:
        logger.info(f"Connecting to MySQL at {self.host}:{self.port}")

        self._validate_settings()

        try:
            self.client = self._create_binlog_client()
            logger.info("Connected to MySQL binlog stream")
        except Exception as e:
            error_msg = f"Failed to connect to MySQL: {str(e)}"
            logger.error(error_msg)
            raise DataSourceError(error_msg)

    def _get_event_type(self, event) -> str:
        if isinstance(event, WriteRowsEvent):
            return "Insert"
        if isinstance(event, UpdateRowsEvent):
            return "Update"
        if isinstance(event, DeleteRowsEvent):
            return "Delete"
        return type(event).__name__

    def _process_query_event(self, event) -> None:
        try:
            query = event.query
            if isinstance(query, bytes):
                query = query.decode("utf-8")
            elif not isinstance(query, str):
                logger.warning(f"Query is not a string or bytes: {type(query)}")
                return

            if query == "COMMIT":
                self.transaction_complete = True
                logger.debug(f"Transaction {self.current_transaction_gtid} COMMITTED")

                # Update the position with the committed transaction GTID
                if self.current_transaction_gtid:
                    self.position.gtid = self.current_transaction_gtid
                    logger.debug(f"Updated position to {self.position}")
        except Exception as e:
            logger.warning(f"Failed to process query: {e}")

    def listen(self) -> Generator[Dict[str, Any], None, None]:
        if not self.client:
            raise DataSourceError("Data source not connected")

        try:
            for event in self.client:
                # Track GTID events
                if isinstance(event, GtidEvent):
                    self.current_transaction_gtid = event.gtid
                    self.transaction_complete = False
                    logger.debug(
                        f"New transaction GTID: {self.current_transaction_gtid} - {event.dump()}"
                    )
                    continue

                # Process COMMIT events
                if isinstance(event, QueryEvent):
                    self._process_query_event(event)
                    continue

                # Skip events without rows
                if not hasattr(event, "rows") or not event.rows:
                    continue

                # Process row events
                formatted_events = self._format_row_events(event)
                for formatted_event in formatted_events:
                    yield formatted_event

        except Exception as e:
            logger.error(f"Error processing binlog: {str(e)}")
            raise DataSourceError(f"Error processing binlog: {str(e)}")

    def _format_row_events(self, event) -> Generator[Dict[str, Any], None, None]:
        if not self.current_transaction_gtid:
            logger.warning("Processing row event without GTID context")
            return

        event_type = self._get_event_type(event)
        logger.debug(
            f"Processing {event_type} event GTID: {self.current_transaction_gtid}"
        )

        for row in event.rows:
            yield self._create_event_dict(event, event_type, row)

    def _create_event_dict(self, event, event_type: str, row) -> Dict[str, Any]:
        result = {
            "event_type": event_type,
            "gtid": self.current_transaction_gtid,
            "database": event.schema,
            "table": event.table,
            "content": row
        }

        return result

    def disconnect(self) -> None:
        if not self.client:
            return

        logger.info("Disconnecting from MySQL")
        try:
            self.client.close()
        except Exception as e:
            logger.error(f"Error while disconnecting from MySQL: {e}")
        finally:
            self.client = None

    def get_position(self) -> Optional[Position]:
        return self.position

    def set_position(self, position_data: Position) -> None:
        if not position_data:
            logger.warning("No position data provided")
            return

        if isinstance(position_data, dict):
            self.position.from_dict(position_data)
        elif isinstance(position_data, GTIDPosition):
            self.position = position_data
        else:
            logger.warning(f"Unexpected position type: {type(position_data)}")

        logger.info(f"Position set to: {self.position}")

    def get_source_type(self) -> str:
        return "mysql"

    def get_source_id(self) -> str:
        if not self.host:
            raise DataSourceError("No host configured")
        return self.host

