import os
import pytest
from unittest.mock import patch, MagicMock
from botocore.exceptions import ClientError
from stream_cdc.state.dynamodb import Dynamodb


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

        # Mock botocore version to be greater than required
        required_version = "1.27.84"
        mock_version = "1.28.0"

        with patch.dict(os.environ, self.env_vars):
            with patch(
                "boto3.client", return_value=self.mock_client
            ) as mock_boto_client:
                with patch("stream_cdc.state.dynamodb.Config", mock_config):
                    with patch("botocore.__version__", mock_version):
                        with patch("packaging.version.parse") as mock_parse:
                            # Setup the version comparison to return True
                            mock_parsed_version = MagicMock()
                            mock_parsed_version.__ge__.return_value = True
                            mock_parse.return_value = mock_parsed_version

                            manager = Dynamodb("test-table")

                            # Config should be called once with all parameters including tcp_keepalive
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
        """Test client creation without TCP keep-alive when botocore version doesn't support it"""
        mock_config = MagicMock()
        mock_config_instance = MagicMock()
        mock_config.return_value = mock_config_instance

        # Mock botocore version to be less than required
        required_version = "1.27.84"
        mock_version = "1.27.0"

        with patch.dict(os.environ, self.env_vars):
            with patch(
                "boto3.client", return_value=self.mock_client
            ) as mock_boto_client:
                with patch("stream_cdc.state.dynamodb.Config", mock_config):
                    with patch("botocore.__version__", mock_version):
                        with patch("packaging.version.parse") as mock_parse:
                            # Setup the version comparison to return False
                            mock_parsed_version = MagicMock()
                            mock_parsed_version.__ge__.return_value = False
                            mock_parse.return_value = mock_parsed_version

                            manager = Dynamodb("test-table")

                            # Config should be called once without tcp_keepalive
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
        self.mock_client.describe_table.return_value = {
            "Table": {"TableName": "test-table"}
        }

        with patch.object(
            Dynamodb, "_create_client", return_value=self.mock_client
        ):
            manager = Dynamodb(table_name="test-table")

            # Verify describe_table was called
            self.mock_client.describe_table.assert_called_once_with(
                TableName="test-table"
            )

            # create_table should not be called
            self.mock_client.create_table.assert_not_called()

    def test_ensure_table_exists_when_table_does_not_exist(self):
        """Test _ensure_table_exists when table does not exist"""
        # Mock the client's describe_table to raise ResourceNotFoundException
        error_response = {
            "Error": {
                "Code": "ResourceNotFoundException",
                "Message": "Table not found",
            }
        }
        self.mock_client.describe_table.side_effect = ClientError(
            error_response, "DescribeTable"
        )

        # Mock the waiter
        mock_waiter = MagicMock()
        self.mock_client.get_waiter.return_value = mock_waiter

        with patch.object(
            Dynamodb, "_create_client", return_value=self.mock_client
        ):
            manager = Dynamodb(table_name="test-table")

            # Verify describe_table was called
            self.mock_client.describe_table.assert_called_once_with(
                TableName="test-table"
            )

            # Verify create_table was called with correct parameters
            self.mock_client.create_table.assert_called_once()
            call_kwargs = self.mock_client.create_table.call_args.kwargs
            assert call_kwargs["TableName"] == "test-table"
            assert call_kwargs["KeySchema"] == [
                {"AttributeName": "datasource_type", "KeyType": "HASH"},
                {"AttributeName": "datasource_source", "KeyType": "RANGE"},
            ]

            # Verify waiter was used
            self.mock_client.get_waiter.assert_called_once_with("table_exists")
            mock_waiter.wait.assert_called_once_with(TableName="test-table")

    def test_ensure_table_exists_other_client_error(self):
        """Test _ensure_table_exists with other ClientError"""
        # Mock the client's describe_table to raise other ClientError
        error_response = {
            "Error": {
                "Code": "AccessDeniedException",
                "Message": "Access denied",
            }
        }
        self.mock_client.describe_table.side_effect = ClientError(
            error_response, "DescribeTable"
        )

        with patch.object(
            Dynamodb, "_create_client", return_value=self.mock_client
        ):
            with pytest.raises(ClientError):
                manager = Dynamodb(table_name="test-table")

    def test_store_success(self):
        """Test store method successfully stores data"""
        # Mock table exists
        self.mock_client.describe_table.return_value = {
            "Table": {"TableName": "test-table"}
        }

        with patch.object(
            Dynamodb, "_create_client", return_value=self.mock_client
        ):
            with patch.object(Dynamodb, "_ensure_table_exists"):
                manager = Dynamodb(table_name="test-table")

                # Test store method
                result = manager.store(
                    datasource_type="postgres",
                    datasource_source="host1.example.com",
                    state_position={"gtid": "12345"},
                )

                # Verify put_item was called with correct parameters
                self.mock_client.put_item.assert_called_once_with(
                    TableName="test-table",
                    Item={
                        "datasource_type": {"S": "postgres"},
                        "datasource_source": {"S": "host1.example.com"},
                        "gtid": {"S": "12345"},
                    },
                )

                # Verify result
                assert result is True

    def test_store_exception(self):
        """Test store method when an exception occurs"""
        # Mock table exists
        self.mock_client.describe_table.return_value = {
            "Table": {"TableName": "test-table"}
        }

        # Mock put_item to raise an exception
        self.mock_client.put_item.side_effect = Exception("Test exception")

        with patch.object(
            Dynamodb, "_create_client", return_value=self.mock_client
        ):
            with patch.object(Dynamodb, "_ensure_table_exists"):
                manager = Dynamodb(table_name="test-table")

                # Test store method
                result = manager.store(
                    datasource_type="postgres",
                    datasource_source="host1.example.com",
                    state_position={"gtid": "12345"},
                )

                # Verify result
                assert result is False

    def test_read_success(self):
        """Test read method successfully retrieves data"""
        # Mock table exists
        self.mock_client.describe_table.return_value = {
            "Table": {"TableName": "test-table"}
        }

        # Mock get_item response
        self.mock_client.get_item.return_value = {
            "Item": {
                "datasource_type": {"S": "postgres"},
                "datasource_source": {"S": "host1.example.com"},
                "gtid": {"S": "12345"},
                "other_data": {"S": "value"},
            }
        }

        with patch.object(
            Dynamodb, "_create_client", return_value=self.mock_client
        ):
            with patch.object(Dynamodb, "_ensure_table_exists"):
                manager = Dynamodb(table_name="test-table")

                # Test read method
                result = manager.read(
                    datasource_type="postgres",
                    datasource_source="host1.example.com",
                )

                # Verify get_item was called with correct parameters
                self.mock_client.get_item.assert_called_once_with(
                    TableName="test-table",
                    Key={
                        "datasource_type": {"S": "postgres"},
                        "datasource_source": {"S": "host1.example.com"},
                    },
                )

                # Verify result
                assert result == {"gtid": "12345", "other_data": "value"}

    def test_read_item_not_found(self):
        """Test read method when item is not found"""
        # Mock table exists
        self.mock_client.describe_table.return_value = {
            "Table": {"TableName": "test-table"}
        }

        # Mock get_item response with no Item
        self.mock_client.get_item.return_value = {}

        with patch.object(
            Dynamodb, "_create_client", return_value=self.mock_client
        ):
            with patch.object(Dynamodb, "_ensure_table_exists"):
                manager = Dynamodb(table_name="test-table")

                # Test read method
                result = manager.read(
                    datasource_type="postgres",
                    datasource_source="host1.example.com",
                )

                # Verify result is None when item not found
                assert result is None

    def test_read_exception(self):
        """Test read method when an exception occurs"""
        # Mock table exists
        self.mock_client.describe_table.return_value = {
            "Table": {"TableName": "test-table"}
        }

        # Mock get_item to raise an exception
        self.mock_client.get_item.side_effect = Exception("Test exception")

        with patch.object(
            Dynamodb, "_create_client", return_value=self.mock_client
        ):
            with patch.object(Dynamodb, "_ensure_table_exists"):
                manager = Dynamodb(table_name="test-table")

                # Test read method
                result = manager.read(
                    datasource_type="postgres",
                    datasource_source="host1.example.com",
                )

                # Verify result is None when exception occurs
                assert result is None

    def test_constructor_with_kwargs(self):
        """Test constructor with kwargs override"""
        with patch.object(
            Dynamodb, "_create_client", return_value=self.mock_client
        ):
            with patch.object(Dynamodb, "_ensure_table_exists"):
                # Override with kwargs
                manager = Dynamodb(
                    table_name="custom-table",
                    region="us-east-1",
                    endpoint_url="http://dynamodb.example.com",
                    aws_access_key="custom-access-key",
                    aws_secret_key="custom-secret-key",
                    connect_timeout=1.0,
                    read_timeout=2.0,
                )

                # Verify attributes were set from kwargs
                assert manager.table_name == "custom-table"
                assert manager.region == "us-east-1"
                assert manager.endpoint_url == "http://dynamodb.example.com"
                assert manager.aws_access_key == "custom-access-key"
                assert manager.aws_secret_key == "custom-secret-key"
                assert manager.connect_timeout == 1.0
                assert manager.read_timeout == 2.0
