import os
from unittest.mock import patch, MagicMock
from stream_cdc.state.dynamodb import Dynamodb

class TestDynamodbStateManager:
    """Test cases for Dynamodb state manager implementation"""

    def test_create_client(self):
        """Test that _create_client initializes boto3 client correctly"""

        env_vars = {
            "STATE_DYNAMODB_REGION": "us-west-2",
            "STATE_DYNAMODB_ENDPOINT_URL": "http://localhost:8000",
            "STATE_DYNAMODB_ACCESS_KEY": "test-access-key",
            "STATE_DYNAMODB_SECRET_KEY": "test-secret-key"
        }

        mock_client = MagicMock()
        with patch.dict(os.environ, env_vars):
            with patch('boto3.client', return_value=mock_client) as mock_boto_client:
                manager = Dynamodb()
                client = manager._create_client()

                mock_boto_client.assert_called_once_with(
                    service_name="dynamodb",
                    region_name="us-west-2",
                    endpoint_url="http://localhost:8000",
                    aws_access_key_id="test-access-key",
                    aws_secret_access_key="test-secret-key"
                )

                assert client == mock_client

    def test_store_method_exists(self):
        """Test that store method exists"""
        manager = Dynamodb()
        manager.store()

    def test_read_method_exists(self):
        """Test that read method exists"""
        manager = Dynamodb()
        manager.read()

