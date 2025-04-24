import pytest
import os
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from stream_cdc.state.dynamodb import Dynamodb
from stream_cdc.utils.exceptions import ConfigurationError


class TestDynamodbStateManager:
    """Test cases for Dynamodb state manager implementation"""

    def setup_method(self):
        """Setup before each test"""
        self.env_vars = {
            "STATE_DYNAMODB_REGION": "us-west-2",
            "STATE_DYNAMODB_ENDPOINT_URL": "http://localhost:8000",
            "STATE_DYNAMODB_ACCESS_KEY": "test-access-key",
            "STATE_DYNAMODB_SECRET_KEY": "test-secret-key",
            "STATE_DYNAMODB_TABLE": "test-table",
            "STATE_DYNAMODB_CONNECT_TIMEOUT": "0.5",
            "STATE_DYNAMODB_READ_TIMEOUT": "0.5",
        }
        self.mock_client = MagicMock()

    def test_create_client_with_tcp_keepalive(self):
        """Test client creation with TCP keep-alive when botocore version supports it"""
        mock_config = MagicMock()
        mock_config_instance = MagicMock()
        mock_config.return_value = mock_config_instance

        # Mock botocore version
        mock_version = "1.28.0"  # Greater than required version

        with patch.dict(os.environ, self.env_vars):
            with patch(
                "boto3.client", return_value=self.mock_client
            ) as mock_boto_client:
                with patch("stream_cdc.state.dynamodb.Config", mock_config):
                    with patch("botocore.__version__", mock_version):
                        with patch("packaging.version.parse") as mock_parse:
                            # Setup the version comparison to return True
                            mock_parse.side_effect = lambda v: MagicMock(
                                **{"__ge__": lambda self, other: v == "1.28.0"}
                            )

                            # Mock _ensure_table_exists to avoid calling it
                            with patch.object(Dynamodb, "_ensure_table_exists"):
                                # Create instance (we don't need to store the instance)
                                Dynamodb()

                                # Check if config includes tcp_keepalive
                                mock_config.assert_called_once_with(
                                    connect_timeout=0.5,
                                    read_timeout=0.5,
                                    tcp_keepalive=True,
                                )

                                # Verify boto3 client was called with the config
                                mock_boto_client.assert_called_once()
                                call_kwargs = mock_boto_client.call_args.kwargs
                                assert call_kwargs["service_name"] == "dynamodb"
                                assert call_kwargs["region_name"] == "us-west-2"
                                assert (
                                    call_kwargs["endpoint_url"]
                                    == "http://localhost:8000"
                                )
                                assert (
                                    call_kwargs["aws_access_key_id"]
                                    == "test-access-key"
                                )
                                assert (
                                    call_kwargs["aws_secret_access_key"]
                                    == "test-secret-key"
                                )
                                assert call_kwargs["config"] == mock_config_instance

    def test_create_client_without_tcp_keepalive(self):
        """Test client creation without TCP keep-alive for older botocore"""
        mock_config = MagicMock()
        mock_config_instance = MagicMock()
        mock_config.return_value = mock_config_instance

        # Mock botocore version
        mock_version = "1.27.0"  # Less than required version

        with patch.dict(os.environ, self.env_vars):
            with patch(
                "boto3.client", return_value=self.mock_client
            ) as mock_boto_client:
                with patch("stream_cdc.state.dynamodb.Config", mock_config):
                    with patch("botocore.__version__", mock_version):
                        with patch("packaging.version.parse") as mock_parse:
                            # Setup the version comparison to return False
                            mock_parse.side_effect = lambda v: MagicMock(
                                **{"__ge__": lambda self, other: False}
                            )

                            # Mock _ensure_table_exists to avoid calling it
                            with patch.object(Dynamodb, "_ensure_table_exists"):
                                # Create instance (we don't need to store it)
                                Dynamodb()

                                # Config should be called without tcp_keepalive
                                mock_config.assert_called_once_with(
                                    connect_timeout=0.5, read_timeout=0.5
                                )

                                # Verify boto3 client was called with the config
                                mock_boto_client.assert_called_once()
                                call_kwargs = mock_boto_client.call_args.kwargs
                                assert call_kwargs["config"] == mock_config_instance

    def test_ensure_table_exists_when_table_exists(self):
        """Test _ensure_table_exists when table already exists"""
        # Mock the client's describe_table to succeed (table exists)
        mock_client = MagicMock()
        mock_client.describe_table.return_value = {"Table": {"TableName": "test-table"}}

        with patch.dict(os.environ, self.env_vars):
            with patch.object(Dynamodb, "_create_client", return_value=mock_client):
                # Create instance - no need to store the reference
                Dynamodb()

                # Verify describe_table was called
                mock_client.describe_table.assert_called_once_with(
                    TableName="test-table"
                )

    def test_ensure_table_exists_when_table_does_not_exist(self):
        """Test _ensure_table_exists when table does not exist"""
        # Mock the client's describe_table to raise ResourceNotFoundException
        mock_client = MagicMock()
        error_response = {
            "Error": {
                "Code": "ResourceNotFoundException",
                "Message": "Table not found",
            }
        }
        mock_client.describe_table.side_effect = ClientError(
            error_response, "DescribeTable"
        )

        with patch.dict(os.environ, self.env_vars):
            with patch.object(Dynamodb, "_create_client", return_value=mock_client):
                # Should raise ConfigurationError - we don't need the instance
                with pytest.raises(ConfigurationError) as exc_info:
                    Dynamodb()

                err_msg = "DynamoDB table test-table does not exist"
                assert err_msg in str(exc_info.value)

                # Verify describe_table was called
                mock_client.describe_table.assert_called_once_with(
                    TableName="test-table"
                )

    def test_store_success(self):
        """Test store method successfully stores data"""
        # Mock table exists
        mock_client = MagicMock()
        mock_client.describe_table.return_value = {"Table": {"TableName": "test-table"}}

        with patch.dict(os.environ, self.env_vars):
            with patch.object(Dynamodb, "_create_client", return_value=mock_client):
                # Create instance
                manager = Dynamodb()

                # Test store method
                result = manager.store(
                    datasource_type="postgres",
                    datasource_source="host1.example.com",
                    state_position="12345",
                )

                # Verify put_item was called with correct parameters
                mock_client.put_item.assert_called_once()
                call_args = mock_client.put_item.call_args[1]
                assert call_args["TableName"] == "test-table"
                assert call_args["Item"]["PK"]["S"] == "postgres"
                assert call_args["Item"]["SK"]["S"] == "host1.example.com"
                assert call_args["Item"]["position"]["S"] == "12345"

                # Verify result
                assert result is True

    def test_read_success(self):
        """Test read method successfully retrieves data"""
        # Mock table exists
        mock_client = MagicMock()
        mock_client.describe_table.return_value = {"Table": {"TableName": "test-table"}}

        # Mock get_item response
        mock_client.get_item.return_value = {
            "Item": {
                "PK": {"S": "postgres"},
                "SK": {"S": "host1.example.com"},
                "position": {"S": "12345"},
            }
        }

        with patch.dict(os.environ, self.env_vars):
            with patch.object(Dynamodb, "_create_client", return_value=mock_client):
                # Create instance
                manager = Dynamodb()

                # Test read method
                result = manager.read(
                    datasource_type="postgres",
                    datasource_source="host1.example.com",
                )

                # Verify get_item was called with correct parameters - using PK/SK
                mock_client.get_item.assert_called_once_with(
                    TableName="test-table",
                    Key={
                        "PK": {"S": "postgres"},
                        "SK": {"S": "host1.example.com"},
                    },
                )

                # Verify result
                assert result == "12345"

    def test_constructor_with_kwargs(self):
        """Test constructor with kwargs override"""
        # Mock _create_client and _ensure_table_exists to avoid actual calls
        with patch.object(Dynamodb, "_create_client", return_value=MagicMock()):
            with patch.object(Dynamodb, "_ensure_table_exists"):
                # Instead of clearing environment variables,
                # let's patch the os.getenv function to return our kwargs
                def mock_getenv(key, default=None):
                    env_to_kwargs = {
                        "STATE_DYNAMODB_REGION": "us-east-1",
                        "STATE_DYNAMODB_ENDPOINT_URL": "http://dynamodb.example.com",
                        "STATE_DYNAMODB_ACCESS_KEY": "custom-access-key",
                        "STATE_DYNAMODB_SECRET_KEY": "custom-secret-key",
                        "STATE_DYNAMODB_TABLE": "custom-table",
                        "STATE_DYNAMODB_CONNECT_TIMEOUT": "1.0",
                        "STATE_DYNAMODB_READ_TIMEOUT": "2.0",
                    }
                    return env_to_kwargs.get(key, default)

                with patch("os.getenv", side_effect=mock_getenv):
                    # Create instance
                    manager = Dynamodb()

                    # Verify attributes were set from our mocked environment values
                    assert manager.table_name == "custom-table"
                    assert manager.region == "us-east-1"
                    assert manager.endpoint_url == "http://dynamodb.example.com"
                    assert manager.aws_access_key == "custom-access-key"
                    assert manager.aws_secret_key == "custom-secret-key"
                    assert manager.connect_timeout == 1.0
                    assert manager.read_timeout == 2.0
