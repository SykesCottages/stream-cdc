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
    # SQS message size limit in bytes
    SQS_MAX_MESSAGE_SIZE = 256 * 1024
    # Reduced effective size to account for metadata overhead
    SQS_EFFECTIVE_SIZE_LIMIT = 250 * 1024

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
            endpoint_url: The AWS endpoint URL. Defaults to AWS_ENDPOINT_URL environment
                variable.
            aws_access_key_id: The AWS access key ID. Defaults to AWS_ACCESS_KEY_ID
                environment variable.
            aws_secret_access_key: The AWS secret access key. Defaults to
                AWS_SECRET_ACCESS_KEY environment variable.
            source: The source identifier for the messages. Defaults to SOURCE
                 environment variable.

        Raises:
            ConfigurationError: If any required configuration parameter is missing.
        """
        self.source = source or os.getenv("SOURCE")
        if not self.source:
            self.source = "stream_cdc"

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

        self._client = None
        self._client_lock = Lock()
        self._session = None

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

                    # Configure the client with appropriate timeouts and TCP keepalive
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
        Send messages to SQS, respecting SQS batch size limits.

        Batches messages according to SQS's limitations (maximum of 10 messages
            per batch)
        and sends them to the configured queue.

        Args:
            messages: The messages to send. Each message should be serializable to JSON.

        Raises:
            StreamError: If message conversion or sending fails.
        """
        if not messages:
            return

        client = self._get_client()

        for i in range(0, len(messages), self.SQS_MAX_BATCH_SIZE):
            batch = messages[i : i + self.SQS_MAX_BATCH_SIZE]
            entries = self._prepare_sqs_entries(batch)

            if not entries:
                logger.warning("No valid entries to send in batch")
                continue

            logger.debug(f"Sending batch of {len(entries)} messages")
            self._send_batch_to_sqs(client, entries)

    def close(self) -> None:
        """
        No resources to close for SQS.

        This method is implemented to satisfy the Stream interface but does not
        perform any action as SQS does not maintain persistent connections.
        """
        # No need to close boto3 clients - they're managed by the SDK
        pass

    def _prepare_sqs_entries(self, batch: List[Any]) -> List[Dict]:
        """
        Convert messages to SQS batch entry format.

        Formats each message as required by the SQS send_message_batch API,
        including setting a unique ID for each message in the batch.

        Args:
            batch: The batch of messages to format.

        Returns:
            List[Dict]: The formatted messages ready for sending to SQS.
        """
        entries = []

        for idx, msg in enumerate(batch):
            try:
                message_body = json.dumps(msg)
            except Exception as e:
                logger.error(f"Failed to convert message to JSON: {e}")
                continue

            message_size = len(message_body.encode("utf-8"))

            # Check for SQS message size limit (256KB)
            if message_size > self.SQS_EFFECTIVE_SIZE_LIMIT:
                logger.warning(
                    f"Message size exceeds SQS limit of 256KB: {message_size} bytes"
                )
                message_body = self._handle_large_message(msg)
                if not message_body:
                    continue

            # Add the message to the batch
            entry = {
                "Id": str(idx),
                "MessageBody": message_body,
                "MessageAttributes": {
                    "source": {"StringValue": self.source, "DataType": "String"}
                },
            }
            entries.append(entry)

        return entries

    def _handle_large_message(self, message: Any) -> Optional[str]:
        """
        Handle messages that exceed the SQS size limit.

        Args:
            message: The original large message

        Returns:
            Optional[str]: JSON string of the modified message or None if processing
                failed
        """
        try:
            simplified_message = {
                "original_size_exceeded": True,
                "redacted_to_s3": "not implemented yet",
                "message_type": type(message).__name__,
            }

            # Add any important metadata from the original message if possible
            if isinstance(message, dict):
                if "event_type" in message:
                    simplified_message["event_type"] = message["event_type"]
                if "database" in message:
                    simplified_message["database"] = message["database"]
                if "table" in message:
                    simplified_message["table"] = message["table"]

            logger.info(
                f"Large message detected: {type(message).__name__}. "
                "Using simplified reference message."
            )

            return json.dumps(simplified_message)
        except Exception as e:
            logger.error(f"Failed to handle large message: {e}")
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

                # Log each failure with its specific error reason
                for failed_msg in response["Failed"]:
                    logger.error(
                        f"Message {failed_msg['Id']} failed: {
                            failed_msg.get('Message', 'Unknown error')
                        }"
                    )

                # Determine if we should retry these messages
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
            logger.error(f"SQS send_message_batch failed: {str(e)}")
            raise StreamError(f"Failed to send messages to SQS: {str(e)}")
