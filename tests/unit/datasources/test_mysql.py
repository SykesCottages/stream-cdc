import pytest
import time
import os
from unittest.mock import patch, MagicMock
from stream_cdc.datasources.mysql import MySQLDataSource, MySQLSettingsValidator
from stream_cdc.utils.exceptions import ConfigurationError, DataSourceError
from pymysqlreplication.event import GtidEvent
from pymysqlreplication.row_event import (
    WriteRowsEvent,
    UpdateRowsEvent,
    DeleteRowsEvent,
)


class TestMySQLSettingsValidator:
    """Test cases for MySQLSettingsValidator implementation"""

    @pytest.fixture
    def mock_cursor(self):
        """Fixture to provide a mock database cursor."""
        cursor = MagicMock()

        # Mock fetch of MySQL settings
        cursor.fetchall.return_value = [
            ("binlog_format", "ROW"),
            ("binlog_row_metadata", "FULL"),
            ("binlog_row_image", "FULL"),
            ("gtid_mode", "ON"),
            ("enforce_gtid_consistency", "ON"),
        ]

        return cursor

    @pytest.fixture
    def mock_connection(self, mock_cursor):
        """Fixture to provide a mock database connection."""
        conn = MagicMock()
        conn.__enter__.return_value = mock_cursor
        conn.cursor.return_value = conn
        return conn

    def test_validator_init(self):
        """Test MySQLSettingsValidator initialization."""
        with patch("pymysql.connect") as mock_connect:
            # Prevent actual database connection
            mock_connect.return_value = MagicMock()

            validator = MySQLSettingsValidator(
                host="localhost", user="testuser", password="testpass", port=3306
            )

            assert validator.host == "localhost"
            assert validator.user == "testuser"
            assert validator.password == "testpass"
            assert validator.port == 3306

            # Verify connection attempt
            mock_connect.assert_called_once_with(
                host="localhost", user="testuser", password="testpass", port=3306
            )

    def test_validator_init_missing_values(self):
        """Test initialization with missing values."""
        # Missing host
        with pytest.raises(ConfigurationError) as exc_info:
            MySQLSettingsValidator(host=None, user="user", password="pass", port=3306)
        assert "Database host is required" in str(exc_info.value)

        # Missing user
        with pytest.raises(ConfigurationError) as exc_info:
            MySQLSettingsValidator(host="host", user=None, password="pass", port=3306)
        assert "Database user is required" in str(exc_info.value)

        # Missing password
        with pytest.raises(ConfigurationError) as exc_info:
            MySQLSettingsValidator(host="host", user="user", password=None, port=3306)
        assert "Database password is required" in str(exc_info.value)

        # Missing port
        with pytest.raises(ConfigurationError) as exc_info:
            MySQLSettingsValidator(host="host", user="user", password="pass", port=None)
        assert "Database port is required" in str(exc_info.value)

    def test_get_required_settings(self):
        """Test getting required MySQL settings."""
        with patch("pymysql.connect") as mock_connect:
            # Prevent actual database connection
            mock_connect.return_value = MagicMock()

            validator = MySQLSettingsValidator(
                host="localhost", user="testuser", password="testpass", port=3306
            )

            required_settings = validator._get_required_settings()

            assert required_settings["binlog_format"] == "ROW"
            assert required_settings["binlog_row_metadata"] == "FULL"
            assert required_settings["binlog_row_image"] == "FULL"
            assert required_settings["gtid_mode"] == "ON"
            assert required_settings["enforce_gtid_consistency"] == "ON"

    def test_fetch_actual_settings(self, mock_cursor):
        """Test fetching actual settings from MySQL."""
        with patch("pymysql.connect") as mock_connect:
            # Prevent actual database connection
            mock_connect.return_value = MagicMock()

            validator = MySQLSettingsValidator(
                host="localhost", user="testuser", password="testpass", port=3306
            )

            actual_settings = validator._fetch_actual_settings(mock_cursor)

            assert actual_settings["binlog_format"] == "ROW"
            assert actual_settings["binlog_row_metadata"] == "FULL"
            assert actual_settings["binlog_row_image"] == "FULL"
            assert actual_settings["gtid_mode"] == "ON"
            assert actual_settings["enforce_gtid_consistency"] == "ON"

            # Verify the query was executed correctly
            mock_cursor.execute.assert_called_once()
            # Verify the query includes all required settings
            query_params = mock_cursor.execute.call_args[0][1]
            assert len(query_params) == 5
            assert all(
                param in query_params
                for param in [
                    "binlog_format",
                    "binlog_row_metadata",
                    "binlog_row_image",
                    "gtid_mode",
                    "enforce_gtid_consistency",
                ]
            )

    def test_verify_settings_success(self):
        """Test successful settings verification."""
        with patch("pymysql.connect") as mock_connect:
            # Prevent actual database connection
            mock_connect.return_value = MagicMock()

            validator = MySQLSettingsValidator(
                host="localhost", user="testuser", password="testpass", port=3306
            )

            # All settings match requirements
            actual_settings = {
                "binlog_format": "ROW",
                "binlog_row_metadata": "FULL",
                "binlog_row_image": "FULL",
                "gtid_mode": "ON",
                "enforce_gtid_consistency": "ON",
            }

            # Should not raise exception
            validator._verify_settings(actual_settings)

    def test_verify_settings_missing_setting(self):
        """Test verification with missing setting."""
        with patch("pymysql.connect") as mock_connect:
            # Prevent actual database connection
            mock_connect.return_value = MagicMock()

            validator = MySQLSettingsValidator(
                host="localhost", user="testuser", password="testpass", port=3306
            )

            # Missing binlog_format
            actual_settings = {
                "binlog_row_metadata": "FULL",
                "binlog_row_image": "FULL",
                "gtid_mode": "ON",
                "enforce_gtid_consistency": "ON",
            }

            with pytest.raises(ConfigurationError) as exc_info:
                validator._verify_settings(actual_settings)

            assert "MySQL setting binlog_format not found" in str(exc_info.value)

    def test_verify_settings_incorrect_value(self):
        """Test verification with incorrect setting value."""
        with patch("pymysql.connect") as mock_connect:
            # Prevent actual database connection
            mock_connect.return_value = MagicMock()

            validator = MySQLSettingsValidator(
                host="localhost", user="testuser", password="testpass", port=3306
            )

            # binlog_format is STATEMENT instead of ROW
            actual_settings = {
                "binlog_format": "STATEMENT",
                "binlog_row_metadata": "FULL",
                "binlog_row_image": "FULL",
                "gtid_mode": "ON",
                "enforce_gtid_consistency": "ON",
            }

            with pytest.raises(ConfigurationError) as exc_info:
                validator._verify_settings(actual_settings)

            assert "MySQL setting binlog_format is incorrect" in str(exc_info.value)
            assert "expected=ROW" in str(exc_info.value)
            assert "actual=STATEMENT" in str(exc_info.value)

    def test_validate_success(self, mock_connection):
        """Test successful validation."""
        with patch("pymysql.connect") as mock_connect:
            mock_connect.return_value = mock_connection

            validator = MySQLSettingsValidator(
                host="localhost", user="testuser", password="testpass", port=3306
            )

            # Should not raise exception
            validator.validate()

            # Verify connection was created with correct parameters
            mock_connect.assert_called_once_with(
                host="localhost", user="testuser", password="testpass", port=3306
            )

            # Verify connection was closed
            mock_connection.close.assert_called_once()

    def test_validate_connection_error(self):
        """Test validation with connection error."""
        with patch("pymysql.connect") as mock_connect:
            # First return a valid connection for the __init__ to succeed
            mock_conn = MagicMock()
            mock_connect.return_value = mock_conn

            # Create the validator object
            validator = MySQLSettingsValidator(
                host="localhost", user="testuser", password="testpass", port=3306
            )

            # Now make the cursor throw an exception when used
            mock_cursor = MagicMock()
            mock_conn.cursor.return_value = mock_cursor
            mock_cursor.__enter__.side_effect = Exception("Connection refused")

            # Test the validate method
            with pytest.raises(ConfigurationError) as exc_info:
                validator.validate()

            # Verify the error message
            assert "Failed to connect to MySQL: Connection refused" in str(
                exc_info.value
            )


class TestMySQLDataSource:
    """Test cases for MySQLDataSource implementation"""

    @pytest.fixture
    def mysql_env_vars(self):
        """Environment variables for MySQL test."""
        env_vars = {
            "DB_HOST": "localhost",
            "DB_USER": "testuser",
            "DB_PASSWORD": "testpass",
            "DB_PORT": "3306",
        }
        with patch.dict(os.environ, env_vars):
            yield env_vars

    @pytest.fixture
    def mysql_data_source(self):
        """Fixture to provide a MySQLDataSource instance."""
        # Mock the pymysql.connect to prevent actual database connection
        with patch("pymysql.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            # Create the data source with patched connection
            data_source = MySQLDataSource(
                host="localhost",
                user="testuser",
                password="testpass",
                port=3306,
                server_id=1000,
            )
            # Set attributes for testing
            data_source.client = None
            data_source.current_position = None
            data_source.transaction_complete = False
            data_source.last_event_time = time.time()

            yield data_source

    def test_mysql_datasource_init(self):
        """Test MySQLDataSource initialization."""
        # Mock the pymysql.connect to prevent actual database connection
        with patch("pymysql.connect") as mock_connect:
            mock_connect.return_value = MagicMock()

            data_source = MySQLDataSource(
                host="localhost",
                user="testuser",
                password="testpass",
                port=3306,
                server_id=1234,
            )

            assert data_source.host == "localhost"
            assert data_source.user == "testuser"
            assert data_source.password == "testpass"
            assert data_source.port == 3306
            assert data_source.server_id == 1234
            assert data_source.client is None
            assert data_source.current_position is None

    def test_create_event_dict(self, mysql_data_source):
        """Test creating event dictionary."""
        event = MagicMock()
        event.schema = "testdb"
        event.table = "users"

        event_type = "Insert"
        row = {"id": 1, "name": "Test User"}

        # Set the current transaction GTID
        mysql_data_source.current_position = "12345678-1234-1234-1234-123456789abc:1"

        result = mysql_data_source._create_event_dict(event, event_type, row)

        assert result["event_type"] == "Insert"
        assert result["gtid"] == "12345678-1234-1234-1234-123456789abc:1"
        assert result["database"] == "testdb"
        assert result["table"] == "users"
        assert result["content"] == row

    def test_listen_with_gtid_events(self, mysql_data_source):
        """Test listen method with GTID events."""
        # Create a mock binlog client
        mock_binlog_client = MagicMock()

        # Set up client with some events
        gtid_event = MagicMock(spec=GtidEvent)
        gtid_event.gtid = "12345678-1234-1234-1234-123456789abc:1"

        # Add dump method to the gtid_event
        gtid_event.dump.return_value = "GTID event dump"

        write_event = MagicMock(spec=WriteRowsEvent)
        write_event.schema = "testdb"
        write_event.table = "users"
        write_event.rows = [{"data": {"id": 1, "name": "Test User"}}]

        # First yield a GTID event, then a row event
        mock_binlog_client.__iter__.return_value = [gtid_event, write_event]

        # Set the client directly
        mysql_data_source.client = mock_binlog_client
        mysql_data_source.last_event_time = time.time()  # Initialize to avoid reconnect

        # Get events from listen generator
        events = list(mysql_data_source.listen())

        # Should only get one event (the row event)
        assert len(events) == 1
        assert events[0]["event_type"] == "Insert"
        assert events[0]["database"] == "testdb"
        assert events[0]["table"] == "users"
        assert events[0]["gtid"] == "12345678-1234-1234-1234-123456789abc:1"

    def test_listen_with_different_event_types(self, mysql_data_source):
        """Test listen method with different event types."""
        # Set up GTID first (required for row events)
        gtid_event = MagicMock(spec=GtidEvent)
        gtid_event.gtid = "12345678-1234-1234-1234-123456789abc:1"
        gtid_event.dump.return_value = "GTID event dump"

        # Create a mock binlog client
        mock_binlog_client = MagicMock()

        # Set up client with different event types
        write_event = MagicMock(spec=WriteRowsEvent)
        write_event.schema = "testdb"
        write_event.table = "users"
        write_event.rows = [{"data": {"id": 1, "name": "New User"}}]

        update_event = MagicMock(spec=UpdateRowsEvent)
        update_event.schema = "testdb"
        update_event.table = "users"
        update_event.rows = [
            {
                "before": {"id": 1, "name": "Old User"},
                "after": {"id": 1, "name": "Updated User"},
            }
        ]

        delete_event = MagicMock(spec=DeleteRowsEvent)
        delete_event.schema = "testdb"
        delete_event.table = "users"
        delete_event.rows = [{"data": {"id": 2, "name": "Deleted User"}}]

        # Initialize GTID first, then yield all event types
        mock_binlog_client.__iter__.return_value = [
            gtid_event,
            write_event,
            update_event,
            delete_event,
        ]

        # Set the client directly
        mysql_data_source.client = mock_binlog_client
        mysql_data_source.last_event_time = time.time()

        # Get events from listen generator
        events = list(mysql_data_source.listen())

        # Should get three events (one for each row event)
        assert len(events) == 3

        # Check first event (write)
        assert events[0]["event_type"] == "Insert"
        assert events[0]["database"] == "testdb"
        assert events[0]["table"] == "users"
        assert events[0]["gtid"] == "12345678-1234-1234-1234-123456789abc:1"

        # Check second event (update)
        assert events[1]["event_type"] == "Update"
        assert events[1]["database"] == "testdb"
        assert events[1]["table"] == "users"
        assert events[1]["gtid"] == "12345678-1234-1234-1234-123456789abc:1"

        # Check third event (delete)
        assert events[2]["event_type"] == "Delete"
        assert events[2]["database"] == "testdb"
        assert events[2]["table"] == "users"
        assert events[2]["gtid"] == "12345678-1234-1234-1234-123456789abc:1"

    def test_listen_error(self, mysql_data_source):
        """Test listen method with error during iteration."""
        # Create a mock binlog client
        mock_binlog_client = MagicMock()

        # Set up client to raise error during iteration
        mock_binlog_client.__iter__.side_effect = Exception("Iterator error")

        # Set the client directly
        mysql_data_source.client = mock_binlog_client
        mysql_data_source.last_event_time = time.time()  # Initialize to avoid reconnect

        with pytest.raises(DataSourceError) as exc_info:
            next(mysql_data_source.listen())

        assert "Error processing binlog: Iterator error" in str(exc_info.value)
