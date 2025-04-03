import pytest
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


@pytest.fixture
def mock_cursor():
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
def mock_connection(mock_cursor):
    """Fixture to provide a mock database connection."""
    conn = MagicMock()
    conn.__enter__.return_value = mock_cursor
    conn.cursor.return_value = conn
    return conn


@pytest.fixture
def mock_pymysql():
    """Fixture to provide a mock pymysql module."""
    with patch("stream_cdc.datasources.mysql.pymysql") as mock:
        mock.connect.return_value = MagicMock()
        yield mock


@pytest.fixture
def mysql_env_vars():
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
def mysql_data_source():
    """Fixture to provide a MySQLDataSource instance."""
    return MySQLDataSource(
        host="localhost",
        user="testuser",
        password="testpass",
        port=3306,
        server_id=1000,
    )


# Tests for MySQLSettingsValidator


def test_validator_init():
    """Test MySQLSettingsValidator initialization."""
    validator = MySQLSettingsValidator(
        host="localhost", user="testuser", password="testpass", port=3306
    )

    assert validator.host == "localhost"
    assert validator.user == "testuser"
    assert validator.password == "testpass"
    assert validator.port == 3306


def test_validator_init_missing_values():
    """Test initialization with missing values."""
    # Missing host
    with pytest.raises(ConfigurationError) as exc_info:
        MySQLSettingsValidator(
            host=None, user="user", password="pass", port=3306
        )
    assert "Database host is required" in str(exc_info.value)

    # Missing user
    with pytest.raises(ConfigurationError) as exc_info:
        MySQLSettingsValidator(
            host="host", user=None, password="pass", port=3306
        )
    assert "Database user is required" in str(exc_info.value)

    # Missing password
    with pytest.raises(ConfigurationError) as exc_info:
        MySQLSettingsValidator(
            host="host", user="user", password=None, port=3306
        )
    assert "Database password is required" in str(exc_info.value)

    # Missing port
    with pytest.raises(ConfigurationError) as exc_info:
        MySQLSettingsValidator(
            host="host", user="user", password="pass", port=None
        )
    assert "Database port is required" in str(exc_info.value)


def test_get_required_settings():
    """Test getting required MySQL settings."""
    validator = MySQLSettingsValidator(
        host="localhost", user="testuser", password="testpass", port=3306
    )

    required_settings = validator._get_required_settings()

    assert required_settings["binlog_format"] == "ROW"
    assert required_settings["binlog_row_metadata"] == "FULL"
    assert required_settings["binlog_row_image"] == "FULL"
    assert required_settings["gtid_mode"] == "ON"
    assert required_settings["enforce_gtid_consistency"] == "ON"


def test_fetch_actual_settings(mock_cursor):
    """Test fetching actual settings from MySQL."""
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


def test_verify_settings_success():
    """Test successful settings verification."""
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


def test_verify_settings_missing_setting():
    """Test verification with missing setting."""
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


def test_verify_settings_incorrect_value():
    """Test verification with incorrect setting value."""
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


def test_validate_success(mock_pymysql, mock_connection):
    """Test successful validation."""
    validator = MySQLSettingsValidator(
        host="localhost", user="testuser", password="testpass", port=3306
    )

    mock_pymysql.connect.return_value = mock_connection

    # Should not raise exception
    validator.validate()

    # Verify connection was created with correct parameters
    mock_pymysql.connect.assert_called_once_with(
        host="localhost", user="testuser", password="testpass", port=3306
    )

    # Verify connection was closed
    mock_connection.close.assert_called_once()


def test_validate_connection_error(mock_pymysql):
    """Test validation with connection error."""
    validator = MySQLSettingsValidator(
        host="localhost", user="testuser", password="testpass", port=3306
    )

    # Simulate connection error
    mock_pymysql.Error = Exception
    mock_pymysql.connect.side_effect = mock_pymysql.Error("Connection refused")

    with pytest.raises(ConfigurationError) as exc_info:
        validator.validate()

    assert "Failed to connect to MySQL: Connection refused" in str(
        exc_info.value
    )


# Tests for MySQLDataSource


def test_mysql_datasource_init():
    """Test MySQLDataSource initialization."""
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
    assert data_source.current_gtid is None


def test_mysql_datasource_init_from_env(mysql_env_vars):
    """Test initialization from environment variables."""
    data_source = MySQLDataSource()

    assert data_source.host == "localhost"
    assert data_source.user == "testuser"
    assert data_source.password == "testpass"
    assert data_source.port == 3306


def test_mysql_datasource_init_missing_values():
    """Test initialization with missing values."""
    with patch.dict(os.environ, {}, clear=True):
        # Missing host
        with pytest.raises(ConfigurationError) as exc_info:
            MySQLDataSource()
        assert "DB_HOST is required" in str(exc_info.value)

    with patch.dict(os.environ, {"DB_HOST": "localhost"}, clear=True):
        # Missing user
        with pytest.raises(ConfigurationError) as exc_info:
            MySQLDataSource()
        assert "DB_USER is required" in str(exc_info.value)

    with patch.dict(
        os.environ, {"DB_HOST": "localhost", "DB_USER": "user"}, clear=True
    ):
        # Missing password
        with pytest.raises(ConfigurationError) as exc_info:
            MySQLDataSource()
        assert "DB_PASSWORD is required" in str(exc_info.value)


def test_create_event_schema(mysql_data_source):
    """Test creating event schema."""
    # This is not really validating the schema... :S
    # @ToDo Need to add a schema validator or a schema registry
    metadata = {
        "datasource_type": "mysql",
        "source": "localhost",
        "timestamp": "1743598169",
    }

    spec = {
        "database": "testdb",
        "table": "users",
        "event_type": "Insert",
        "row": {"id": 1, "name": "Test User"},
        "gtid": "12345678-1234-1234-1234-123456789abc:1",
    }

    event = mysql_data_source._create_event_schema(metadata, spec)

    assert event["version"] == mysql_data_source.SCHEMA_VERSION
    assert event["metadata"] == metadata
    assert event["spec"] == spec


def test_connect():
    """Test connecting to MySQL."""
    # First, mock the BinLogStreamReader class at the module level
    with patch(
        "stream_cdc.datasources.mysql.BinLogStreamReader"
    ) as mock_binlog_reader:
        mock_client = MagicMock()
        mock_binlog_reader.return_value = mock_client

        # Then create the data source
        data_source = MySQLDataSource(
            host="localhost",
            user="testuser",
            password="testpass",
            port=3306,
            server_id=1000,
        )

        # Mock validation to avoid actual validation
        with patch.object(data_source, "_validate_settings"):
            data_source.connect()

            # Verify binlog client was created with correct parameters
            mock_binlog_reader.assert_called_once()
            # Check connection settings
            connection_settings = mock_binlog_reader.call_args[1][
                "connection_settings"
            ]
            assert connection_settings["host"] == "localhost"
            assert connection_settings["user"] == "testuser"
            assert connection_settings["passwd"] == "testpass"
            assert connection_settings["port"] == 3306

            # Check other parameters
            assert mock_binlog_reader.call_args[1]["server_id"] == 1000
            assert mock_binlog_reader.call_args[1]["blocking"] is True
            assert mock_binlog_reader.call_args[1]["resume_stream"] is True

            # Check event types
            assert (
                WriteRowsEvent in mock_binlog_reader.call_args[1]["only_events"]
            )
            assert (
                UpdateRowsEvent
                in mock_binlog_reader.call_args[1]["only_events"]
            )
            assert (
                DeleteRowsEvent
                in mock_binlog_reader.call_args[1]["only_events"]
            )
            assert GtidEvent in mock_binlog_reader.call_args[1]["only_events"]


def test_connect_validation_failure():
    """Test connect method with validation failure."""
    data_source = MySQLDataSource(
        host="localhost",
        user="testuser",
        password="testpass",
        port=3306,
        server_id=1000,
    )

    # Mock _validate_settings to simulate validation failure
    with patch.object(
        data_source,
        "_validate_settings",
        side_effect=ConfigurationError("Validation failed"),
    ):
        with pytest.raises(ConfigurationError) as exc_info:
            data_source.connect()

        assert "Validation failed" in str(exc_info.value)


def test_connect_client_error():
    """Test connect method with client creation failure."""
    # First, mock the BinLogStreamReader class at the module level
    with patch(
        "stream_cdc.datasources.mysql.BinLogStreamReader",
        side_effect=Exception("Client error"),
    ):
        # Then create the data source
        data_source = MySQLDataSource(
            host="localhost",
            user="testuser",
            password="testpass",
            port=3306,
            server_id=1000,
        )

        # Mock validation to avoid actual validation
        with patch.object(data_source, "_validate_settings"):
            with pytest.raises(DataSourceError) as exc_info:
                data_source.connect()

            assert "Failed to connect to MySQL: Client error" in str(
                exc_info.value
            )


def test_listen_without_connection(mysql_data_source):
    """Test listen method without prior connection."""
    # No client set
    mysql_data_source.client = None

    with pytest.raises(DataSourceError) as exc_info:
        next(mysql_data_source.listen())

    assert "Data source not connected" in str(exc_info.value)


def test_listen_with_gtid_events(mysql_data_source):
    """Test listen method with GTID events."""
    # Create a mock binlog client
    mock_binlog_client = MagicMock()

    # Set up client with some events
    gtid_event = MagicMock(spec=GtidEvent)
    gtid_event.gtid = "12345678-1234-1234-1234-123456789abc:1"

    write_event = MagicMock(spec=WriteRowsEvent)
    write_event.schema = "testdb"
    write_event.table = "users"
    write_event.timestamp = "1743598382"
    write_event.rows = [{"id": 1, "name": "Test User"}]

    # First yield a GTID event, then a row event
    mock_binlog_client.__iter__.return_value = [gtid_event, write_event]

    # Set the client directly
    mysql_data_source.client = mock_binlog_client

    # Get events from listen generator
    events = list(mysql_data_source.listen())

    # Should only get one event (the row event)
    assert len(events) == 1
    assert events[0]["spec"]["database"] == "testdb"
    assert events[0]["spec"]["table"] == "users"
    assert events[0]["spec"]["event_type"] == "Insert"
    assert events[0]["spec"]["row"] == {"id": 1, "name": "Test User"}
    assert events[0]["spec"]["gtid"] == "12345678-1234-1234-1234-123456789abc:1"


def test_listen_with_different_event_types(mysql_data_source):
    """Test listen method with different event types."""
    # Create a mock binlog client
    mock_binlog_client = MagicMock()

    # Set up client with some events
    write_event = MagicMock(spec=WriteRowsEvent)
    write_event.schema = "testdb"
    write_event.table = "users"
    write_event.timestamp = "1743598169"
    write_event.rows = [{"id": 1, "name": "New User"}]

    update_event = MagicMock(spec=UpdateRowsEvent)
    update_event.schema = "testdb"
    update_event.table = "users"
    update_event.timestamp = "1743598169"
    update_event.rows = [
        ({"id": 1, "name": "Old User"}, {"id": 1, "name": "Updated User"})
    ]

    delete_event = MagicMock(spec=DeleteRowsEvent)
    delete_event.schema = "testdb"
    delete_event.table = "users"
    delete_event.timestamp = "1743598169"
    delete_event.rows = [{"id": 2, "name": "Deleted User"}]

    # Yield all event types
    mock_binlog_client.__iter__.return_value = [
        write_event,
        update_event,
        delete_event,
    ]

    # Set the client directly
    mysql_data_source.client = mock_binlog_client

    # Get events from listen generator
    events = list(mysql_data_source.listen())

    # Should get three events
    assert len(events) == 3

    # Check event types
    assert events[0]["spec"]["event_type"] == "Insert"
    assert events[1]["spec"]["event_type"] == "Update"
    assert events[2]["spec"]["event_type"] == "Delete"


def test_listen_error(mysql_data_source):
    """Test listen method with error during iteration."""
    # Create a mock binlog client
    mock_binlog_client = MagicMock()

    # Set up client to raise error during iteration
    mock_binlog_client.__iter__.side_effect = Exception("Iterator error")

    # Set the client directly
    mysql_data_source.client = mock_binlog_client

    with pytest.raises(DataSourceError) as exc_info:
        next(mysql_data_source.listen())

    assert "Error while listening to MySQL binlog: Iterator error" in str(
        exc_info.value
    )


def test_disconnect(mysql_data_source):
    """Test disconnect method."""
    # Create a mock binlog client
    mock_binlog_client = MagicMock()

    # Set the client
    mysql_data_source.client = mock_binlog_client

    # Disconnect
    mysql_data_source.disconnect()

    # Client should be closed and set to None
    mock_binlog_client.close.assert_called_once()
    assert mysql_data_source.client is None


def test_disconnect_without_client(mysql_data_source):
    """Test disconnect method when no client exists."""
    # No client set
    mysql_data_source.client = None

    # Should not raise exception
    mysql_data_source.disconnect()


def test_disconnect_with_error(mysql_data_source):
    """Test disconnect method when client.close raises error."""
    # Create a mock binlog client
    mock_binlog_client = MagicMock()

    # Set up client to raise error on close
    mock_binlog_client.close.side_effect = Exception("Close error")

    # Set the client
    mysql_data_source.client = mock_binlog_client

    # Should not raise exception outside
    mysql_data_source.disconnect()

    # Client should still be set to None
    assert mysql_data_source.client is None
