import pytest
from unittest.mock import patch, MagicMock
import json
import os
from stream_cdc.streams.sqs import SQS
from stream_cdc.utils.exceptions import ConfigurationError, StreamError


@pytest.fixture
def mock_boto3():
    """Mock boto3 client for tests."""
    with patch("boto3.client") as mock:
        yield mock


@pytest.fixture
def mock_sqs_client():
    """Fixture to provide a mock SQS client."""
    mock_client = MagicMock()
    mock_client.send_message_batch.return_value = {"Failed": []}
    return mock_client


@pytest.fixture
def sqs_instance(mock_boto3, mock_sqs_client):
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
def sqs_env_vars():
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


def test_init_from_env_vars(mock_boto3, sqs_env_vars):
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


def test_init_with_missing_queue_url():
    """Test initialization with missing queue URL."""
    with patch.dict(os.environ, {}, clear=True):
        with pytest.raises(ConfigurationError) as exc_info:
            SQS()

        assert "SQS_QUEUE_URL is required" in str(exc_info.value)


def test_init_with_missing_region():
    """Test initialization with missing region."""
    with patch.dict(
        os.environ, {"SQS_QUEUE_URL": "https://test-queue"}, clear=True
    ):
        with pytest.raises(ConfigurationError) as exc_info:
            SQS()

        assert "AWS_REGION is required" in str(exc_info.value)


def test_init_with_missing_endpoint():
    """Test initialization with missing endpoint URL."""
    with patch.dict(
        os.environ,
        {"SQS_QUEUE_URL": "https://test-queue", "AWS_REGION": "us-west-2"},
        clear=True,
    ):
        with pytest.raises(ConfigurationError) as exc_info:
            SQS()

        assert "AWS_ENDPOINT_URL is required" in str(exc_info.value)


def test_init_with_missing_access_key():
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


def test_init_with_missing_secret_key():
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


def test_init_with_explicit_params(mock_boto3):
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


def test_send_empty_messages(sqs_instance, mock_sqs_client):
    """Test sending empty message list."""
    sqs_instance.send([])

    # Ensure send_message_batch is not called with empty list
    mock_sqs_client.send_message_batch.assert_not_called()


def test_send_single_batch(sqs_instance, mock_sqs_client):
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


def test_send_multiple_batches(sqs_instance, mock_sqs_client):
    """Test sending messages in multiple batches (due to SQS limits)."""
    # Create 15 messages (should be split into 2 batches)
    messages = [{"id": i} for i in range(15)]
    sqs_instance.send(messages)

    # Check if send_message_batch was called twice
    assert mock_sqs_client.send_message_batch.call_count == 2

    # First batch should have 10 messages
    first_batch = mock_sqs_client.send_message_batch.call_args_list[0][1][
        "Entries"
    ]
    assert len(first_batch) == 10

    # Second batch should have 5 messages
    second_batch = mock_sqs_client.send_message_batch.call_args_list[1][1][
        "Entries"
    ]
    assert len(second_batch) == 5


def test_send_with_message_too_large(sqs_instance):
    """Test sending a message that exceeds SQS size limit."""
    # Create a message that will exceed 256KB when serialized
    large_message = {"data": "x" * 300 * 1024}  # 300KB of data

    with pytest.raises(StreamError) as exc_info:
        sqs_instance.send([large_message])

    assert "Message size exceeds SQS limit of 256KB" in str(exc_info.value)


def test_prepare_sqs_entries_json_error(sqs_instance):
    """Test handling error when a message can't be converted to JSON."""

    # Create a message that can't be JSON serialized
    class UnserializableObject:
        def __repr__(self):
            return "UnserializableObject()"

    messages = [{"object": UnserializableObject()}]

    with pytest.raises(StreamError) as exc_info:
        sqs_instance.send(messages)

    assert "Failed to convert message to JSON" in str(exc_info.value)


def test_send_with_failed_messages(sqs_instance, mock_sqs_client):
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


def test_close_method(sqs_instance):
    """Test close method (should do nothing for SQS)."""
    # Should not raise any exception
    sqs_instance.close()
