import pytest
import json
import os
from unittest.mock import patch, MagicMock
from stream_cdc.streams.sqs import SQS
from stream_cdc.utils.exceptions import ConfigurationError, StreamError


class TestSQS:
    """Test cases for SQS stream implementation"""

    @pytest.fixture
    def mock_boto3(self):
        """Mock boto3 client for tests."""
        with patch("boto3.client") as mock:
            yield mock

    @pytest.fixture
    def mock_sqs_client(self):
        """Fixture to provide a mock SQS client."""
        mock_client = MagicMock()
        mock_client.send_message_batch.return_value = {"Failed": []}
        return mock_client

    @pytest.fixture
    def sqs_instance(self, mock_boto3, mock_sqs_client):
        """Fixture to provide an SQS instance with mocked boto3 client."""
        mock_boto3.return_value = mock_sqs_client
        sqs = SQS(
            queue_url="https://test-queue-url",
            region="test-region",
            endpoint_url="https://test-endpoint",
            aws_access_key_id="test-key-id",
            aws_secret_access_key="test-secret-key",
        )
        return sqs

    @pytest.fixture
    def sqs_env_vars(self):
        """Environment variables for SQS test."""
        env_vars = {
            "SQS_QUEUE_URL": "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue",
            "AWS_REGION": "us-west-2",
            "AWS_ENDPOINT_URL": "https://sqs.us-west-2.amazonaws.com",
            "AWS_ACCESS_KEY_ID": "test-key-id",
            "AWS_SECRET_ACCESS_KEY": "test-secret-key",
        }
        with patch.dict(os.environ, env_vars):
            yield env_vars

    def test_init_from_env_vars(self, mock_boto3, sqs_env_vars):
        """Test initialization from environment variables."""
        sqs = SQS()

        assert (
            sqs.queue_url
            == "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue"
        )
        assert sqs.region == "us-west-2"
        assert sqs.endpoint_url == "https://sqs.us-west-2.amazonaws.com"
        assert sqs.aws_access_key_id == "test-key-id"
        assert sqs.aws_secret_access_key == "test-secret-key"

        mock_boto3.assert_called_once_with(
            "sqs",
            region_name="us-west-2",
            endpoint_url="https://sqs.us-west-2.amazonaws.com",
            aws_access_key_id="test-key-id",
            aws_secret_access_key="test-secret-key",
        )

    def test_init_with_missing_queue_url(self):
        """Test initialization with missing queue URL."""
        with patch.dict(os.environ, {}, clear=True):
            with pytest.raises(ConfigurationError) as exc_info:
                SQS()

            assert "SQS_QUEUE_URL is required" in str(exc_info.value)

    def test_init_with_missing_region(self):
        """Test initialization with missing region."""
        with patch.dict(
            os.environ, {"SQS_QUEUE_URL": "https://test-queue"}, clear=True
        ):
            with pytest.raises(ConfigurationError) as exc_info:
                SQS()

            assert "AWS_REGION is required" in str(exc_info.value)

    def test_init_with_missing_endpoint(self):
        """Test initialization with missing endpoint URL."""
        with patch.dict(
            os.environ,
            {"SQS_QUEUE_URL": "https://test-queue", "AWS_REGION": "us-west-2"},
            clear=True,
        ):
            with pytest.raises(ConfigurationError) as exc_info:
                SQS()

            assert "AWS_ENDPOINT_URL is required" in str(exc_info.value)

    def test_init_with_missing_access_key(self):
        """Test initialization with missing access key ID."""
        with patch.dict(
            os.environ,
            {
                "SQS_QUEUE_URL": "https://test-queue",
                "AWS_REGION": "us-west-2",
                "AWS_ENDPOINT_URL": "https://test-endpoint",
            },
            clear=True,
        ):
            with pytest.raises(ConfigurationError) as exc_info:
                SQS()

            assert "AWS_ACCESS_KEY_ID is required" in str(exc_info.value)

    def test_init_with_missing_secret_key(self):
        """Test initialization with missing secret key."""
        with patch.dict(
            os.environ,
            {
                "SQS_QUEUE_URL": "https://test-queue",
                "AWS_REGION": "us-west-2",
                "AWS_ENDPOINT_URL": "https://test-endpoint",
                "AWS_ACCESS_KEY_ID": "test-key-id",
            },
            clear=True,
        ):
            with pytest.raises(ConfigurationError) as exc_info:
                SQS()

            assert "AWS_SECRET_ACCESS_KEY is required" in str(exc_info.value)

    def test_init_with_explicit_params(self, mock_boto3):
        """Test initialization with explicit parameters."""
        sqs = SQS(
            queue_url="https://custom-queue-url",
            region="custom-region",
            endpoint_url="https://custom-endpoint",
            aws_access_key_id="custom-key-id",
            aws_secret_access_key="custom-secret-key",
        )

        assert sqs.queue_url == "https://custom-queue-url"
        assert sqs.region == "custom-region"
        assert sqs.endpoint_url == "https://custom-endpoint"
        assert sqs.aws_access_key_id == "custom-key-id"
        assert sqs.aws_secret_access_key == "custom-secret-key"

    def test_send_empty_messages(self, sqs_instance, mock_sqs_client):
        """Test sending empty message list."""
        sqs_instance.send([])

        # Ensure send_message_batch is not called with empty list
        mock_sqs_client.send_message_batch.assert_not_called()

    def test_send_single_batch(self, sqs_instance, mock_sqs_client):
        """Test sending a single batch of messages."""
        messages = [{"id": 1}, {"id": 2}, {"id": 3}]
        sqs_instance.send(messages)

        # Check if send_message_batch was called with expected parameters
        mock_sqs_client.send_message_batch.assert_called_once()
        call_args = mock_sqs_client.send_message_batch.call_args[1]

        assert call_args["QueueUrl"] == "https://test-queue-url"
        entries = call_args["Entries"]

        assert len(entries) == 3
        assert entries[0]["Id"] == "0"
        assert entries[1]["Id"] == "1"
        assert entries[2]["Id"] == "2"

        assert json.loads(entries[0]["MessageBody"]) == {"id": 1}
        assert json.loads(entries[1]["MessageBody"]) == {"id": 2}
        assert json.loads(entries[2]["MessageBody"]) == {"id": 3}

    def test_send_multiple_batches(self, sqs_instance, mock_sqs_client):
        """Test sending messages in multiple batches (due to SQS limits)."""
        # Create 15 messages (should be split into 2 batches)
        messages = [{"id": i} for i in range(15)]
        sqs_instance.send(messages)

        # Check if send_message_batch was called twice
        assert mock_sqs_client.send_message_batch.call_count == 2

        # First batch should have 10 messages
        first_batch = mock_sqs_client.send_message_batch.call_args_list[0][1]["Entries"]
        assert len(first_batch) == 10

        # Second batch should have 5 messages
        second_batch = mock_sqs_client.send_message_batch.call_args_list[1][1][
            "Entries"
        ]
        assert len(second_batch) == 5

    def test_send_with_message_too_large(self, sqs_instance, mock_sqs_client):
        """Test sending a message that exceeds SQS size limit."""
        # Create a message that will exceed 256KB when serialized
        large_message = {"data": "x" * 300 * 1024}  # 300KB of data

        # Mock logger to verify log message
        with patch("stream_cdc.streams.sqs.logger") as mock_logger:
            sqs_instance.send([large_message])
            # Verify that the appropriate log message was created
            mock_logger.info.assert_called_with("Dumping message to s3")

        # Verify the message was sent with the redacted content
        mock_sqs_client.send_message_batch.assert_called_once()
        # Extract the actual messages sent
        call_args = mock_sqs_client.send_message_batch.call_args[1]
        assert "Entries" in call_args
        # Verify content of redacted message
        sent_message_body = json.loads(call_args["Entries"][0]["MessageBody"])
        assert "redacted_to_s3" in sent_message_body

    def test_prepare_sqs_entries_json_error(self, sqs_instance, mock_sqs_client):
        """Test handling error when a message can't be converted to JSON."""

        # Create a message that can't be JSON serialized
        class UnserializableObject:
            def __repr__(self):
                return "UnserializableObject()"

        messages = [{"object": UnserializableObject()}]

        # Reset the mock to clear previous calls
        mock_sqs_client.reset_mock()

        # Mock logger to verify log message
        with patch("stream_cdc.streams.sqs.logger") as mock_logger:
            # The method continues after logging the error
            sqs_instance.send(messages)
            # Verify the error was logged properly
            mock_logger.error.assert_called_once()
            assert (
                "Failed to convert message to JSON" in mock_logger.error.call_args[0][0]
            )

        # Verify an empty batch was sent or no batch was sent
        if mock_sqs_client.send_message_batch.called:
            # Check if an empty batch was sent
            call_args = mock_sqs_client.send_message_batch.call_args[1]
            assert "Entries" in call_args
            # Verify the entries list is empty - no messages were sent
            assert len(call_args["Entries"]) == 0

    def test_send_with_failed_messages(self, sqs_instance, mock_sqs_client):
        """Test handling failed messages from SQS."""
        # Simulate SQS returning failed messages
        mock_sqs_client.send_message_batch.return_value = {
            "Failed": [
                {"Id": "0", "Message": "Error sending message"},
                {"Id": "2", "Message": "Another error"},
            ]
        }

        messages = [{"id": 1}, {"id": 2}, {"id": 3}]

        with pytest.raises(StreamError) as exc_info:
            sqs_instance.send(messages)

        assert "Failed to send 2 messages to SQS" in str(exc_info.value)
        assert "IDs: ['0', '2']" in str(exc_info.value)

    def test_close_method(self, sqs_instance):
        """Test close method (should do nothing for SQS)."""
        # Should not raise any exception
        sqs_instance.close()
