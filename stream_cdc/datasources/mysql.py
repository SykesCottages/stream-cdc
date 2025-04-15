from typing import Generator, Any, Dict, Optional, Union
import os
import time
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


class MySQLSettingsValidator:
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
            raise ConfigurationError(
                "Database password is required for validation"
            )
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
                logger.error(
                    f"MySQL setting {setting} not found in server variables"
                )
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
    SCHEMA_VERSION = "mysql-31-03-2025"

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
        self.current_gtid = None
        self.last_seen_event_gtid = None

        # For tracking transaction state
        self.transaction_complete = False

        # For monitoring stream health
        self.last_event_time = 0
        self.event_timeout = 30  # Seconds before reconnecting due to inactivity

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

    def _create_event_schema(self, metadata: dict, spec: dict):
        return {
            "version": self.SCHEMA_VERSION,
            "metadata": metadata,
            "spec": spec,
        }

    def _create_binlog_client(
        self, auto_position: Optional[str] = None
    ) -> BinLogStreamReader:
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

        if auto_position:
            logger.debug(f"Setting auto_position to: {auto_position}")
            client_args["auto_position"] = auto_position

        return self.binlog_client(**client_args)

    def connect(self) -> None:
        logger.info(f"Connecting to MySQL at {self.host}:{self.port}")

        self._validate_settings()

        try:
            if not self.current_gtid:
                logger.info(
                    "No stored position, connecting from current position"
                )
                self.client = self._create_binlog_client()
                logger.info("Connected to MySQL binlog stream")
                return

            logger.info(f"Attempting to resume from GTID: {self.current_gtid}")
            try:
                # Create a proper GTID set for MySQL
                gtid_set = (
                    self.current_gtid.split(":")[0]
                    + ":1-"
                    + self.current_gtid.split(":")[1]
                )
                logger.info(f"Using GTID set for auto_position: {gtid_set}")

                self.client = self._create_binlog_client(auto_position=gtid_set)
                logger.info(
                    f"Successfully connected using GTID set: {gtid_set}"
                )
                return
            except Exception as e:
                logger.error(
                    f"Failed to resume from GTID {self.current_gtid}: {e}"
                )
                import traceback

                logger.error(f"Exception details: {traceback.format_exc()}")

            # Fall back to connecting without position
            logger.info("Connecting without position specification")
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

    def _handle_query_event(self, event) -> None:
        try:
            # Check if query is bytes and decode if needed
            query = event.query
            if isinstance(query, bytes):
                query = query.decode("utf-8")
            elif not isinstance(query, str):
                logger.warning(f"Query is not a string or bytes: {type(query)}")
                return

            if query == "COMMIT":
                self.transaction_complete = True
                logger.debug(
                    f"Transaction {self.last_seen_event_gtid} COMMITTED"
                )
        except Exception as e:
            logger.warning(f"Failed to process query: {e}")

    def _handle_gtid_event(self, event) -> None:
        old_gtid = self.current_gtid
        self.current_gtid = event.gtid
        self.last_seen_event_gtid = event.gtid
        self.transaction_complete = False

        # First GTID event
        if not old_gtid:
            logger.debug(f"Initial GTID set: {self.current_gtid}")
            return

        # Parse and compare transaction numbers if available
        try:
            old_txn = int(old_gtid.split(":")[1])
            new_txn = int(event.gtid.split(":")[1])

            if new_txn < old_txn:
                logger.warning(
                    f"GTID went backward: {old_gtid} -> {event.gtid}"
                )
            elif new_txn > old_txn + 1:
                logger.warning(
                    f"GTID skipped: {old_gtid} -> {event.gtid}, "
                    f"missing {new_txn - old_txn - 1} transactions"
                )
            else:
                logger.debug(f"Updated current GTID: {self.current_gtid}")
        except (IndexError, ValueError):
            logger.debug(f"Updated current GTID to: {self.current_gtid}")

    def _process_row_event(
        self, event
    ) -> Generator[Dict[str, Any], None, None]:
        if not self.last_seen_event_gtid:
            logger.warning("Received row event before any GTID event")
            return

        event_type = self._get_event_type(event)
        transaction_status = (
            "COMPLETE" if self.transaction_complete else "IN_PROGRESS"
        )

        for row in event.rows:
            metadata = {
                "datasource_type": "mysql",
                "source": self.host,
                "timestamp": event.timestamp,
                "position": self.last_seen_event_gtid,
                "transaction_status": transaction_status,
            }

            spec = {
                "database": event.schema,
                "table": event.table,
                "event_type": event_type,
                "row": row,
                "gtid": self.last_seen_event_gtid,
            }

            output = self._create_event_schema(metadata, spec)
            yield output

    def _check_reconnect_needed(self) -> bool:
        if not self.last_event_time:
            return False

        current_time = time.time()
        if current_time - self.last_event_time > self.event_timeout:
            logger.warning(
                f"No events received for {self.event_timeout} seconds, reconnecting"
            )
            return True

        return False

    def listen(self) -> Generator[Dict[str, Any], None, None]:
        if not self.client:
            raise DataSourceError("Data source not connected")

        # Check if reconnection is needed due to timeout
        if self._check_reconnect_needed():
            self.disconnect()
            self.connect()

        try:
            if self.current_gtid:
                logger.debug(f"Listening from position: {self.current_gtid}")

            # Update last event time
            self.last_event_time = time.time()

            for event in self.client:
                self.last_event_time = time.time()

                # Handle GTID events - update our position tracking
                if isinstance(event, GtidEvent):
                    self._handle_gtid_event(event)
                    continue

                # Handle QUERY events - track transaction boundaries
                if isinstance(event, QueryEvent):
                    self._handle_query_event(event)
                    continue

                # Handle ROW events - yield data to consumers
                for row_event in self._process_row_event(event):
                    yield row_event

        except Exception as e:
            error_msg = f"Error while listening to MySQL binlog: {str(e)}"
            logger.error(error_msg)
            raise DataSourceError(error_msg)

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

    def get_position(self) -> Dict[str, str]:
        if self.current_gtid is None:
            return {}

        return {"gtid": self.current_gtid}

    def set_position(self, position: Dict[str, str]) -> None:
        if not position:
            logger.warning("No position data provided")
            return

        # Check for gtid key first, then try last_position
        if "gtid" in position:
            self.current_gtid = position["gtid"]
            logger.info(f"Set starting GTID position to {self.current_gtid}")
        elif "last_position" in position:
            self.current_gtid = position["last_position"]
            logger.info(
                f"Set starting GTID position to {self.current_gtid} "
                "(from last_position)"
            )
        else:
            logger.warning(
                "No valid GTID position found in the provided position data"
            )
            return
