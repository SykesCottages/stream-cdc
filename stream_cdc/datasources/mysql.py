from typing import Generator, Any, Dict, Optional, Union
import os
import random
import time
from functools import lru_cache
import pymysql
from pymysql.cursors import Cursor
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import GtidEvent, QueryEvent
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)
from collections import namedtuple
from stream_cdc.datasources.base import DataSource
from stream_cdc.utils.logger import logger
from stream_cdc.utils.exceptions import DataSourceError, ConfigurationError


ServerInfo = namedtuple("ServerInfo", ["server_uuid", "gtid_executed"])


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
        self.conn = None

    def _get_connection(self):
        """Get a database connection, creating it if necessary."""
        if self.conn is None:
            try:
                self.conn = pymysql.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password,
                    port=self.port,
                    connect_timeout=5,
                )
            except Exception as e:
                error_msg = f"Failed to connect to MySQL: {e}"
                logger.error(error_msg)
                raise ConfigurationError(error_msg)
        return self.conn

    def _get_required_settings(self) -> Dict[str, str]:
        return {
            "binlog_format": "ROW",
            "binlog_row_metadata": "FULL",
            "binlog_row_image": "FULL",
            "gtid_mode": "ON",
            "enforce_gtid_consistency": "ON",
        }

    def _fetch_actual_settings(self, cursor: Cursor) -> Dict[str, str]:
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
            conn = self._get_connection()
            with conn.cursor() as cursor:
                actual_settings = self._fetch_actual_settings(cursor)
                self._verify_settings(actual_settings)
        except Exception as e:
            if not isinstance(e, ConfigurationError):
                error_msg = f"Failed to validate MySQL settings: {e}"
                logger.error(error_msg)
                raise ConfigurationError(error_msg)
            raise
        finally:
            if self.conn:
                try:
                    self.conn.close()
                except Exception:
                    pass
                self.conn = None


class MySQLDataSource(DataSource):
    """MySQL CDC data source that streams changes from MySQL binlog."""

    def __init__(
        self,
        host: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        port: Optional[int] = None,
        server_id: Optional[int] = None,
    ):
        # Initialize with a truly random server_id if not provided
        self.server_id = server_id or random.randint(10000, 1000000)
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
        self.binlog_client = BinLogStreamReader
        self.client = None
        self.conn = None

        # Use GTIDPosition object instead of raw string
        self.start_position = None
        self.current_position = None

        self.transaction_complete = False
        self.last_event_time = 0
        self.event_timeout = 30
        self._is_connected = False

    @lru_cache(maxsize=1)
    def _get_connection(self):
        """Get a database connection with caching to avoid repeated connects."""
        if self.conn is None:
            try:
                self.conn = pymysql.connect(
                    host=self.host,
                    user=self.user,
                    password=self.password,
                    port=self.port,
                    connect_timeout=5,
                )
            except Exception as e:
                error_msg = f"Failed to connect to MySQL: {e}"
                logger.error(error_msg)
                raise ConfigurationError(error_msg)
        return self.conn

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
        """Create a binlog stream reader client."""
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
        if self.start_position not in (None, ""):
            start_position = self._format_gtid(self.start_position)
            client_args["auto_position"] = start_position

        return self.binlog_client(**client_args)

    def _get_gtid_server_info(self, cursor: Cursor) -> ServerInfo:
        """Get GTID and server UUID information."""
        cursor.execute("SELECT @@GLOBAL.SERVER_UUID")
        server_uuid = cursor.fetchone()
        if server_uuid is None:
            raise ConfigurationError("Could not retrieve SERVER_UUID")
        server_uuid = server_uuid[0]

        cursor.execute("SELECT @@GLOBAL.GTID_EXECUTED")
        result = cursor.fetchone()
        if result is None:
            raise ConfigurationError("No GTID_EXECUTED found in MySQL server")
        gtid_executed = result[0]

        return ServerInfo(server_uuid=server_uuid, gtid_executed=gtid_executed)

    def _format_gtid(self, gtid: str) -> str:
        """Format the GTID string to match the expected format."""
        if not gtid:
            return ""

        try:
            server_uuid = gtid.split(":")[0]
            gtid_position = gtid.split(":")[1]

            current_position = f"{server_uuid}:1-{gtid_position}"

            logger.debug(f"Formatting GTID: {gtid}")
            conn = self._get_connection()
            with conn.cursor() as cursor:
                info = self._get_gtid_server_info(cursor)
                if info.server_uuid != server_uuid:
                    raise ConfigurationError(
                        f"GTID server UUID {server_uuid} does not match "
                        f"current server UUID {info.server_uuid}"
                    )

                gtids = info.gtid_executed.split(",")
                for i, g in enumerate(gtids):
                    if g.startswith(server_uuid):
                        gtids[i] = current_position
                        break

            return gtids
        except Exception as e:
            logger.error(f"Error formatting GTID {gtid}: {e}")
            raise ConfigurationError(f"Invalid GTID format: {gtid}")

    def connect(self) -> None:
        """Connect to the MySQL binlog stream."""
        if self._is_connected:
            logger.debug("Already connected to MySQL")
            return

        logger.info(f"Connecting to MySQL at {self.host}:{self.port}")

        retry_count = 0
        max_retries = 5
        backoff_factor = 2  # Exponential backoff factor

        while retry_count < max_retries:
            try:
                self._validate_settings()
                self.client = self._create_binlog_client()
                self._is_connected = True
                logger.info("Connected to MySQL binlog stream")
                return
            except Exception as e:
                error_str = str(e)
                if (
                    "server_uuid/server_id" in error_str
                    and retry_count < max_retries - 1
                ):
                    old_server_id = self.server_id
                    # Use timestamp as part of the server_id to reduce collision chance
                    self.server_id = (
                        random.randint(100000, 9999999) + int(time.time()) % 1000000
                    )
                    logger.warning(
                        f"Server ID conflict detected. Retrying with new server_id: "
                        f"{old_server_id} -> {self.server_id}"
                    )

                    # Close any existing client
                    self._close_client()

                    retry_count += 1
                    # Use exponential backoff with jitter
                    sleep_time = (backoff_factor**retry_count) + random.uniform(
                        0.1, 1.0
                    )
                    logger.info(f"Retrying in {sleep_time:.2f} seconds...")
                    time.sleep(sleep_time)
                else:
                    error_msg = f"Failed to connect to MySQL: {error_str}"
                    logger.error(error_msg)
                    raise DataSourceError(error_msg)

        # If we've exhausted all retries
        raise DataSourceError(
            f"Failed to connect after {max_retries} attempts with different server IDs"
        )

    def _close_client(self) -> None:
        """Close the binlog client safely."""
        if self.client:
            try:
                self.client.close()
            except Exception as e:
                logger.debug(f"Error closing binlog client: {e}")
            finally:
                self.client = None

    def _get_event_type(self, event) -> str:
        """Get the type of binlog event."""
        if isinstance(event, WriteRowsEvent):
            return "Insert"
        if isinstance(event, UpdateRowsEvent):
            return "Update"
        if isinstance(event, DeleteRowsEvent):
            return "Delete"
        return type(event).__name__

    def _process_query_event(self, event) -> None:
        """Process a query event from the binlog."""
        try:
            query = event.query
            if isinstance(query, bytes):
                query = query.decode("utf-8")
            elif not isinstance(query, str):
                logger.warning(f"Query is not a string or bytes: {type(query)}")
                return

        except Exception as e:
            logger.warning(f"Failed to process query: {e}")

    def listen(self) -> Generator[Dict[str, Any], None, None]:
        """
        Listen for binlog events and yield them as dictionaries.

        Yields:
            Dict[str, Any]: Formatted event data
        """
        if not self._is_connected or not self.client:
            raise DataSourceError("Data source not connected")

        try:
            for event in self.client:
                # Track GTID events
                if isinstance(event, GtidEvent):
                    self.current_position = event.gtid
                    self.transaction_complete = False
                    logger.debug(f"New transaction GTID: {self.current_position}")
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
        """Format row events from the binlog into dictionaries."""
        if not self.current_position:
            logger.warning("Processing row event without GTID context")
            return

        event_type = self._get_event_type(event)
        logger.debug(f"Processing {event_type} event GTID: {self.current_position}")

        for row in event.rows:
            yield self._create_event_dict(event, event_type, row)

    def _create_event_dict(self, event, event_type: str, row) -> Dict[str, Any]:
        """Create a dictionary representation of an event."""
        result = {
            "event_type": event_type,
            "gtid": self.current_position,
            "database": event.schema,
            "table": event.table,
            "content": row,
        }

        return result

    def disconnect(self) -> None:
        """Disconnect from MySQL binlog stream."""
        if not self._is_connected:
            return

        logger.info("Disconnecting from MySQL")
        self._is_connected = False
        self._close_client()

        # Close the connection
        if self.conn:
            try:
                self.conn.close()
            except Exception as e:
                logger.debug(f"Error closing database connection: {e}")
            finally:
                self.conn = None

    def get_current_position(self) -> str:
        """Get the current binlog position."""
        if not self.current_position:
            logger.warning("No current position available")
            return ""
        return self.current_position

    def set_start_position(self, position: str) -> None:
        """Set the starting binlog position."""
        if not position:
            logger.warning("No position data provided")
            return

        self.start_position = position
        logger.info(f"Position set to: {self.start_position}")

    def get_source_type(self) -> str:
        """Get the data source type."""
        return "mysql"

    def get_source_id(self) -> str:
        """Get the data source identifier."""
        if not self.host:
            raise DataSourceError("No host configured")
        return self.host
