from typing import Generator, Any, Dict, Optional, Union
from datetime import datetime, timezone
import os
import pymysql
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import GtidEvent
from pymysqlreplication.row_event import DeleteRowsEvent, UpdateRowsEvent, WriteRowsEvent

from stream_cdc.datasources.base import DataSource
from stream_cdc.utils.logger import logger
from stream_cdc.utils.exceptions import DataSourceError, ConfigurationError


class MySQLSettingsValidator:
    """Private validator for MySQL CDC settings."""

    def __init__(
        self,
        host: Union[str, None],
        user: Union[str, None],
        password: Union[str, None],
        port: Union[int, None]
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
        """Return the dictionary of required settings and their expected values."""
        return {
            "binlog_format": "ROW",
            "binlog_row_metadata": "FULL",
            "binlog_row_image": "FULL",
            "gtid_mode": "ON",
            "enforce_gtid_consistency": "ON"
        }

    def _fetch_actual_settings(self, cursor) -> Dict[str, str]:
        """Fetch the actual settings from the database."""
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
        """Verify that all required settings have the correct values."""
        required_settings = self._get_required_settings()

        for setting, expected in required_settings.items():
            actual = actual_settings.get(setting)

            if actual is None:
                logger.error(f"MySQL setting {setting} not found in server variables")
                raise ConfigurationError(f"MySQL setting {setting} not found")

            if actual.upper() != expected.upper():
                logger.error(f"MySQL setting {setting} is set to {actual}, expected {expected}")
                raise ConfigurationError(f"""
                    MySQL setting {setting} is incorrect: expected={expected},
                    actual={actual}
                    """)

            logger.info(f"MySQL setting {setting} is correctly set to {actual}")

    def validate(self) -> None:
        """Validate that MySQL has all the required settings for CDC."""
        try:
            conn = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                port=self.port
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
    """MySQL binlog implementation of the DataSource interface."""

    SCHEMA_VERSION = "mysql-31-03-2025"

    def __init__(
        self,
        host: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        port: Optional[int] = None,
        server_id: int = 1234
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

    def _validate_settings(self) -> None:
        """Validate MySQL settings required for CDC."""
        try:
            validator = MySQLSettingsValidator(
                host=self.host,
                user=self.user,
                password=self.password,
                port=self.port
            )
            validator.validate()
        except Exception as e:
            logger.error(f"MySQL settings validation failed: {e}")
            raise

    def _create_event_schema(self, metadata: dict, spec: dict):
        return {
            "version": self.SCHEMA_VERSION,
            "metadata": metadata,
            "spec": spec
        }

    def connect(self) -> None:
        logger.info(f"Connecting to MySQL at {self.host}:{self.port}")

        self._validate_settings()

        try:
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
        except Exception as e:
            error_msg = f"Failed to connect to MySQL: {str(e)}"
            logger.error(error_msg)
            raise DataSourceError(error_msg)

    def listen(self) -> Generator[Dict[str, Any], None, None]:
        if not self.client:
            raise DataSourceError("Data source not connected")

        def get_event_type(event) -> str:
            match event:
                case WriteRowsEvent():
                    return "Insert"

                case UpdateRowsEvent():
                    return "Update"

                case DeleteRowsEvent():
                    return "Delete"

                case _:
                    return type(event).__name__

        try:
            for event in self.client:
                if isinstance(event, GtidEvent):
                    self.current_gtid = event.gtid
                    logger.debug(f"Updated current GTID: {self.current_gtid}")
                    continue

                for row in event.rows:
                    metadata = {
                        "datasource_type": "mysql",
                        "source": self.host,
                        "timestamp": datetime.now(timezone.utc)
                    }

                    spec = {
                        "database": event.schema,
                        "table": event.table,
                        "event_type": get_event_type(event),
                        "row": row,
                        "gtid": self.current_gtid
                    }

                    output = self._create_event_schema(metadata, spec)
                    logger.debug(
                        f"Event: {output}"
                    )

                    yield output

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

