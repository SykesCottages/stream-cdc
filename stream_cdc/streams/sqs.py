from typing import List, Any, Dict, Optional
import json
import boto3
import os
from threading import Lock
from boto3.session import Session
from botocore.config import Config
from stream_cdc.streams.base import Stream
from stream_cdc.utils.logger import logger
from stream_cdc.utils.exceptions import ConfigurationError, StreamError


class SQS(Stream):
    """
    AWS SQS implementation of the Stream interface.

    This class provides functionality to send messages to an AWS Simple Queue Service
    (SQS) queue. It handles batch sending, respecting SQS's limits on batch size and
    message size.
    """

    # SQS has a hard limit of 10 messages per batch
    SQS_MAX_BATCH_SIZE = 10
    # SQS batch request size limit in bytes
    SQS_BATCH_REQUEST_SIZE_LIMIT = 262_000  # Slightly below the actual 262,144 limit
    # SQS individual message size limit in bytes
    SQS_MAX_MESSAGE_SIZE = 256 * 1024
    # Reduced effective size to account for metadata overhead
    SQS_EFFECTIVE_SIZE_LIMIT = 240 * 1024

    def __init__(
        self,
        queue_url: Optional[str] = None,
        region: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        source: Optional[str] = None,
    ):
        """
        Initialize the SQS stream with configuration.

        Args:
            queue_url: The URL of the SQS queue. Defaults to SQS_QUEUE_URL
                environment variable.
            region: The AWS region. Defaults to AWS_REGION environment variable.
            endpoint_url: The AWS endpoint URL. Defaults to AWS_ENDPOINT_URL
                environment variable.
            aws_access_key_id: The AWS access key ID. Defaults to AWS_ACCESS_KEY_ID
                environment variable.
            aws_secret_access_key: The AWS secret access key. Defaults to
                AWS_SECRET_ACCESS_KEY environment variable.
            source: The source identifier for the messages. Defaults to SOURCE
                 environment variable.

        Raises:
            ConfigurationError: If any required configuration parameter is missing.
        """
        self._init_configuration(
            queue_url,
            region,
            endpoint_url,
            aws_access_key_id,
            aws_secret_access_key,
            source,
        )
        self._client = None
        self._client_lock = Lock()
        self._session = None

    def _init_configuration(
        self,
        queue_url: Optional[str],
        region: Optional[str],
        endpoint_url: Optional[str],
        aws_access_key_id: Optional[str],
        aws_secret_access_key: Optional[str],
        source: Optional[str],
    ) -> None:
        self.source = source or os.getenv("SOURCE") or "stream_cdc"

        self.queue_url = queue_url or os.getenv("SQS_QUEUE_URL")
        if not self.queue_url:
            raise ConfigurationError("SQS_QUEUE_URL is required")

        self.region = region or os.getenv("AWS_REGION")
        if not self.region:
            raise ConfigurationError("AWS_REGION is required")

        self.endpoint_url = endpoint_url or os.getenv("AWS_ENDPOINT_URL")
        if not self.endpoint_url:
            raise ConfigurationError("AWS_ENDPOINT_URL is required")

        self.aws_access_key_id = aws_access_key_id or os.getenv("AWS_ACCESS_KEY_ID")
        if not self.aws_access_key_id:
            raise ConfigurationError("AWS_ACCESS_KEY_ID is required")

        self.aws_secret_access_key = aws_secret_access_key or os.getenv(
            "AWS_SECRET_ACCESS_KEY"
        )
        if not self.aws_secret_access_key:
            raise ConfigurationError("AWS_SECRET_ACCESS_KEY is required")

    def _create_session(self) -> Session:
        """
        Create a boto3 session with the configured credentials.

        Returns:
            Session: The configured boto3 session.
        """
        if self._session is None:
            self._session = boto3.session.Session(
                region_name=self.region,
                aws_access_key_id=self.aws_access_key_id,
                aws_secret_access_key=self.aws_secret_access_key,
            )
        return self._session

    def _get_client(self) -> Any:
        """
        Get or create the boto3 SQS client using connection pooling.

        Returns:
            Any: The configured boto3 SQS client.
        """
        if self._client is None:
            with self._client_lock:
                if self._client is None:
                    session = self._create_session()

                    config = Config(
                        connect_timeout=3,
                        read_timeout=5,
                        retries={"max_attempts": 3},
                        tcp_keepalive=True,
                    )

                    self._client = session.client(
                        "sqs", endpoint_url=self.endpoint_url, config=config
                    )

                    logger.debug(
                        f"Setup SQS client: {self.queue_url} - {self.endpoint_url} "
                        f"- {self.region}"
                    )

        return self._client

    def send(self, messages: List[Any]) -> None:
        """
        Send messages to SQS, respecting SQS batch size and total batch size limits.

        Batches messages according to SQS limitations:
        - Maximum of 10 messages per batch
        - Maximum of 262,144 bytes per batch request
        - Maximum of 256KB per individual message

        Args:
            messages: The messages to send. Each message should be serializable to JSON.

        Raises:
            StreamError: If message conversion or sending fails.
        """
        if not messages:
            return

        client = self._get_client()

        # Process messages, handling large ones and managing batch sizes
        batch_entries = []
        current_batch_size = 0

        for msg in messages:
            entry = self._prepare_message(msg)
            if not entry:
                continue

            entry_size = self._calculate_entry_size(entry)

            # If adding this entry would exceed batch size limit, send current batch
            if (
                current_batch_size + entry_size > self.SQS_BATCH_REQUEST_SIZE_LIMIT
                or len(batch_entries) >= self.SQS_MAX_BATCH_SIZE
            ):
                if batch_entries:
                    self._send_batch_to_sqs(client, batch_entries)
                    batch_entries = []
                    current_batch_size = 0

            if entry_size > self.SQS_BATCH_REQUEST_SIZE_LIMIT:
                simplified_entry = self._create_oversized_message_reference(
                    msg, entry["Id"]
                )
                if simplified_entry:
                    self._send_batch_to_sqs(client, [simplified_entry])
                continue

            batch_entries.append(entry)
            current_batch_size += entry_size

        # Send any remaining entries in the batch
        if batch_entries:
            self._send_batch_to_sqs(client, batch_entries)

    def _prepare_message(self, msg: Any) -> Optional[Dict]:
        """
        Prepare a single message for SQS, converting to JSON and handling size limits.

        Args:
            msg: The message to prepare

        Returns:
            Optional[Dict]: SQS entry dict or None if preparation failed
        """
        try:
            message_id = str(id(msg))
            message_body = json.dumps(msg)
            message_size = len(message_body.encode("utf-8"))

            # Check individual message size limit
            if message_size > self.SQS_EFFECTIVE_SIZE_LIMIT:
                logger.warning(f"Message size exceeds SQS limit: {message_size} bytes")
                return self._create_oversized_message_reference(msg, message_id)

            return {
                "Id": message_id,
                "MessageBody": message_body,
                "MessageAttributes": {
                    "source": {"StringValue": self.source, "DataType": "String"}
                },
            }
        except Exception as e:
            logger.error(f"Failed to prepare message: {e}")
            return None

    def _calculate_entry_size(self, entry: Dict) -> int:
        """
        Calculate the size of an SQS entry in bytes.

        Args:
            entry: The SQS entry dict

        Returns:
            int: Size in bytes
        """
        # Serialize the entry to get an accurate size estimate
        serialized = json.dumps(entry)
        return len(serialized.encode("utf-8"))

    def _create_oversized_message_reference(
        self, message: Any, message_id: str
    ) -> Optional[Dict]:
        """
        Create a reference message for oversized original messages.

        Args:
            message: The original large message
            message_id: Unique ID for the message

        Returns:
            Optional[Dict]: SQS entry dict or None if creation failed
        """
        try:
            # Extract key metadata while keeping the reference small
            simplified_message = {
                "original_size_exceeded": True,
                "message_type": type(message).__name__,
                "message_id": message_id,
            }

            # Add essential metadata if available
            if isinstance(message, dict):
                for key in ["event_type", "database", "table", "id"]:
                    if key in message:
                        simplified_message[key] = message[key]

            logger.info(f"Created reference for oversized message: {message_id}")

            entry = {
                "Id": message_id,
                "MessageBody": json.dumps(simplified_message),
                "MessageAttributes": {
                    "source": {"StringValue": self.source, "DataType": "String"},
                    "oversized": {"StringValue": "true", "DataType": "String"},
                },
            }

            # Verify the reference message isn't too large
            if self._calculate_entry_size(entry) > self.SQS_EFFECTIVE_SIZE_LIMIT:
                logger.error("Even the reference message exceeds size limits")
                # Create minimal reference with just ID and flag
                entry["MessageBody"] = json.dumps(
                    {
                        "original_size_exceeded": True,
                        "message_id": message_id,
                    }
                )

            return entry
        except Exception as e:
            logger.error(f"Failed to create reference message: {e}")
            return None

    def _send_batch_to_sqs(self, client: Any, entries: List[Dict]) -> None:
        """
        Send a batch of messages to SQS.

        Args:
            client: The boto3 SQS client
            entries: The formatted messages to send.

        Raises:
            StreamError: If any messages fail to send.
        """
        if not entries:
            return

        try:
            response = client.send_message_batch(
                QueueUrl=self.queue_url, Entries=entries
            )

            if "Failed" in response and response["Failed"]:
                failed_count = len(response["Failed"])
                failed_ids = [item["Id"] for item in response["Failed"]]

                for failed_msg in response["Failed"]:
                    logger.error(
                        f"Message {failed_msg['Id']} failed: "
                        f"{failed_msg.get('Message', 'Unknown error')}"
                    )

                retriable_errors = [
                    "InternalError",
                    "ServiceUnavailable",
                    "ThrottlingException",
                ]
                should_retry = any(
                    failed_msg.get("SenderFault", True) is False
                    or failed_msg.get("Code") in retriable_errors
                    for failed_msg in response["Failed"]
                )

                error_msg = (
                    f"Failed to send {failed_count} messages to SQS. IDs: {failed_ids}"
                )
                logger.error(error_msg)

                if should_retry:
                    logger.warning("Some messages may be retriable")

                if failed_count == len(entries):
                    raise StreamError(error_msg)

            else:
                successful_count = len(response.get("Successful", []))
                logger.debug(f"Successfully sent {successful_count} messages to SQS")

        except Exception as e:
            if "BatchRequestTooLong" in str(e):
                batch_size = sum(
                    len(entry.get("MessageBody", "").encode()) for entry in entries
                )
                logger.error(f"Batch size {batch_size} bytes exceeds SQS limit")
                if len(entries) > 1:
                    # If possible, split the batch and retry sending
                    mid = len(entries) // 2
                    logger.info(
                        f"Splitting batch of {len(entries)} messages and retrying"
                    )
                    self._send_batch_to_sqs(client, entries[:mid])
                    self._send_batch_to_sqs(client, entries[mid:])
                    return

            logger.error(f"SQS send_message_batch failed: {str(e)}")
            raise StreamError(f"Failed to send messages to SQS: {str(e)}")

    def close(self) -> None:
        """
        Clean up resources.

        No persistent resources to close for SQS connections.
        """
        pass
