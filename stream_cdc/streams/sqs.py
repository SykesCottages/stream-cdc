from stream_cdc.streams.base import Stream
from stream_cdc.utils.logger import logger
from stream_cdc.utils.exceptions import ConfigurationError, StreamError
import json
import boto3
import os
from typing import List, Any, Dict, Optional


class SQS(Stream):
    """
    AWS SQS implementation of the Stream interface.

    This class provides functionality to send messages to an AWS Simple Queue Service
    (SQS) queue. It handles batch sending, respecting SQS's limits on batch size and
    message size.
    """

    # SQS has a hard limit of 10 messages per batch
    SQS_MAX_BATCH_SIZE = 10

    def __init__(
        self,
        queue_url: Optional[str] = None,
        region: Optional[str] = None,
        endpoint_url: Optional[str] = None,
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None
    ):
        """
        Initialize the SQS stream with configuration.

        Args:
            queue_url (Optional[str]): The URL of the SQS queue. Defaults to SQS_QUEUE_URL environment variable.
            region (Optional[str]): The AWS region. Defaults to AWS_REGION environment variable.
            endpoint_url (Optional[str]): The AWS endpoint URL. Defaults to AWS_ENDPOINT_URL environment variable.
            aws_access_key_id (Optional[str]): The AWS access key ID. Defaults to AWS_ACCESS_KEY_ID environment variable.
            aws_secret_access_key (Optional[str]): The AWS secret access key. Defaults to AWS_SECRET_ACCESS_KEY environment variable.

        Raises:
            ConfigurationError: If any required configuration parameter is missing.
        """
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

        self.aws_secret_access_key = aws_secret_access_key or os.getenv("AWS_SECRET_ACCESS_KEY", "")
        if not self.aws_secret_access_key:
            raise ConfigurationError("AWS_SECRET_ACCESS_KEY is required")

        self._client = self._create_client()

    def _create_client(self) -> Any:
        """
        Create and configure the boto3 SQS client.

        Returns:
            Any: The configured boto3 SQS client.
        """
        return boto3.client(
            "sqs",
            region_name=self.region,
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        )

    def send(self, messages: List[Any]) -> None:
        """
        Send messages to SQS, respecting SQS batch size limits.

        Batches messages according to SQS's limitations (maximum of 10 messages per batch)
        and sends them to the configured queue.

        Args:
            messages (List[Any]): The messages to send. Each message should be serializable to JSON.

        Raises:
            StreamError: If message conversion or sending fails.
        """
        if not messages:
            return

        for i in range(0, len(messages), self.SQS_MAX_BATCH_SIZE):
            batch = messages[i : i + self.SQS_MAX_BATCH_SIZE]
            entries = self._prepare_sqs_entries(batch)

            logger.debug(f"Sending batch of {len(batch)} messages")
            self._send_batch_to_sqs(entries)

    def close(self) -> None:
        """
        No resources to close for SQS.

        This method is implemented to satisfy the Stream interface but does not
        perform any action as SQS does not maintain persistent connections.
        """
        pass

    def _prepare_sqs_entries(self, batch: List[Any]) -> List[Dict]:
        """
        Convert messages to SQS batch entry format.

        Formats each message as required by the SQS send_message_batch API,
        including setting a unique ID for each message in the batch.

        Args:
            batch (List[Any]): The batch of messages to format.

        Returns:
            List[Dict]: The formatted messages ready for sending to SQS.

        Raises:
            StreamError: If a message cannot be converted to JSON or exceeds SQS size limits.
        """
        entries = []

        for idx, msg in enumerate(batch):
            try:
                message_body = json.dumps(msg)
                #  SQS size has playload size limit of 256KB
                if len(message_body.encode("utf-8")) > 256 * 1024:
                    logger.error(f"Message size exceeds SQS limit of 256KB: {msg}")
                    raise StreamError("Message size exceeds SQS limit of 256KB")

                entry = {"Id": str(idx), "MessageBody": message_body}
                entries.append(entry)
            except Exception as e:
                logger.error(f"Failed to convert message to JSON: {msg}")
                raise StreamError(f"Failed to convert message to JSON: {str(e)}")

        return entries

    def _send_batch_to_sqs(self, entries: List[Dict]) -> None:
        """
        Send a batch of messages to SQS.

        Args:
            entries (List[Dict]): The formatted messages to send.

        Raises:
            StreamError: If any messages fail to send.
        """
        response = self._client.send_message_batch(QueueUrl=self.queue_url, Entries=entries)

        if "Failed" in response and response["Failed"]:
            failed_count = len(response["Failed"])
            failed_ids = [item["Id"] for item in response["Failed"]]
            logger.error(f"Failed to send {failed_count} messages. IDs: {failed_ids}")
            raise StreamError(f"Failed to send {failed_count} messages to SQS. IDs: {failed_ids}")

