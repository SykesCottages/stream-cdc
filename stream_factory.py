from abc import ABC, abstractmethod
from typing import List, Any
import boto3
import json
import logging
from exceptions import UnsupportedTypeError, StreamError
from config_loader import SQSConfig

class Stream(ABC):
    @abstractmethod
    def send(self, messages: List[Any]) -> None:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

class SQS(Stream):
    def __init__(self, config: SQSConfig):
        self.client = boto3.client(
            "sqs",
            region_name=config.region,
            endpoint_url=config.endpoint_url,
            aws_access_key_id=config.aws_access_key_id,
            aws_secret_access_key=config.aws_secret_access_key
        )
        self.queue_url = config.queue_url
        self.max_batch_size = 10
        self.logger = logging.getLogger("cdc_worker")

    def send(self, messages: List[Any]) -> None:
        if not messages:
            return

        total_size = 0
        for i in range(0, len(messages), self.max_batch_size):
            batch = messages[i:i + self.max_batch_size]
            entries = [
                {"Id": str(idx), "MessageBody": json.dumps(msg)}
                for idx, msg in enumerate(batch)
            ]
            batch_size = sum(len(e["MessageBody"].encode('utf-8'))
                             for e in entries)
            total_size += batch_size
            self.logger.debug(f"Sending batch of {len(batch)} messages, "
                              f"total size: {batch_size} bytes")
            response = self.client.send_message_batch(
                QueueUrl=self.queue_url,
                Entries=entries
            )
            if "Failed" in response and response["Failed"]:
                raise StreamError(
                    f"Failed to send {len(response['Failed'])} messages to SQS"
                )
        self.logger.debug(f"Total batch size sent: {total_size} bytes")

    def close(self) -> None:
        pass

class StreamFactory:
    def __init__(self, config_loader):
        self.config_loader = config_loader

    def create(self, stream_type: str) -> Stream:
        match stream_type.lower():
            case "sqs":
                return SQS(self.config_loader.load_stream_config(stream_type))
            case _:
                raise UnsupportedTypeError(
                    f"Stream type '{stream_type}' not supported. "
                    f"Supported types: ['sqs']"
                )
