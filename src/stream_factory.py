from abc import ABC, abstractmethod
from typing import List, Any, Dict
import boto3
import json
from logger import logger
from exceptions import UnsupportedTypeError, StreamError
from config_loader import SQSConfig


class Stream(ABC):
    @abstractmethod
    def send(self, messages: List[Any]) -> None:
        """Send messages to the stream destination."""
        pass

    @abstractmethod
    def close(self) -> None:
        """Close any open connections or resources."""
        pass


class SQS(Stream):
    """AWS SQS implementation of the Stream interface."""

    # SQS has a hard limit of 10 messages per batch
    SQS_MAX_BATCH_SIZE = 10

    def __init__(self, config: SQSConfig):
        """Initialize the SQS stream with configuration."""
        self._client = self._create_client(config)
        self._queue_url = config.queue_url

    def send(self, messages: List[Any]) -> None:
        """Send messages to SQS, respecting SQS batch size limits."""
        if not messages:
            return

        for i in range(0, len(messages), self.SQS_MAX_BATCH_SIZE):
            batch = messages[i:i + self.SQS_MAX_BATCH_SIZE]
            entries = self._prepare_sqs_entries(batch)

            logger.debug(f"Sending batch of {len(batch)} messages")
            self._send_batch_to_sqs(entries)

    def close(self) -> None:
        """No resources to close for SQS."""
        pass

    def _create_client(self, config: SQSConfig) -> Any:
        """Create and configure the boto3 SQS client."""
        return boto3.client(
            "sqs",
            region_name=config.region,
            endpoint_url=config.endpoint_url,
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key,
        )

    def _prepare_sqs_entries(self, batch: List[Any]) -> List[Dict]:
        """Convert messages to SQS batch entry format."""
        entries = []

        for idx, msg in enumerate(batch):
            try:
                message_body = json.dumps(msg)
                # Check if message exceeds SQS size limit (256KB)
                if len(message_body.encode("utf-8")) > 256 * 1024:
                    logger.error(
                        f"Message size exceeds SQS limit of 256KB: {msg}"
                    )
                    raise StreamError("Message size exceeds SQS limit of 256KB")

                entry = {"Id": str(idx), "MessageBody": message_body}
                entries.append(entry)
            except Exception as e:
                logger.error(f"Failed to convert message to JSON: {msg}")
                raise StreamError(f"Failed to convert message to JSON: {str(e)}")

        return entries

    def _send_batch_to_sqs(self, entries: List[Dict]) -> None:
        """Send a batch of messages to SQS."""
        response = self._client.send_message_batch(
            QueueUrl=self._queue_url,
            Entries=entries
        )

        if "Failed" in response and response["Failed"]:
            failed_count = len(response["Failed"])
            failed_ids = [item["Id"] for item in response["Failed"]]
            logger.error(
                f"Failed to send {failed_count} messages. IDs: {failed_ids}"
            )
            raise StreamError(
                f"Failed to send {failed_count} messages to SQS. IDs: {failed_ids}"
            )


class StreamFactory:
    """Factory for creating Stream implementations."""

    def __init__(self, config_loader):
        """Initialize with a configuration loader."""
        self._config_loader = config_loader
        self._supported_types = ["sqs"]

    def create(self, stream_type: str) -> Stream:
        """Create a Stream implementation based on requested type."""
        normalized_type = stream_type.lower()
        logger.debug(f"Creating stream of type: {normalized_type}")

        if normalized_type not in self._supported_types:
            logger.error(
                f"Unsupported stream type: {stream_type}. "
                f"Supported types: {self._supported_types}"
            )
            raise UnsupportedTypeError(
                f"Stream type '{stream_type}' not supported. "
                f"Supported types: {self._supported_types}"
            )

        if normalized_type == "sqs":
            config = self._config_loader.load_stream_config(normalized_type)
            return SQS(config)

        # Should never reach here due to validation above
        raise UnsupportedTypeError(f"Unhandled stream type: {stream_type}")

