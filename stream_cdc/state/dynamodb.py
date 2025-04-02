from stream_cdc.state.base import StateManager
import os
import boto3
from typing import Any


class Dynamodb(StateManager):
    def __init__(self):
        self.region = os.getenv("STATE_DYNAMODB_REGION")
        self.endpoint_url = os.getenv("STATE_DYNAMODB_ENDPOINT_URL")
        self.aws_access_key = os.getenv("STATE_DYNAMODB_ACCESS_KEY")
        self.aws_secret_key = os.getenv("STATE_DYNAMODB_SECRET_KEY")

    def _create_client(self) -> Any:
        """
        Create and configure the boto3 Dynamodb client.

        Returns:
            Any: The Dynamodb clinet.
        """
        return boto3.client(
            service_name = "dynamodb",
            region_name = self.region,
            endpoint_url = self.endpoint_url,
            aws_access_key_id = self.aws_access_key,
            aws_secret_access_key = self.aws_secret_key
        )

    def store(self):
        pass

    def read(self):
        pass
