import os
import boto3
from typing import Any, Dict, Optional
from stream_cdc.utils.logger import logger
from botocore.exceptions import ClientError
from stream_cdc.state.base import StateManager
from botocore.config import Config
import botocore
import packaging.version
from stream_cdc.utils.exceptions import ConfigurationError


class Dynamodb(StateManager):
    def __init__(self, **kwargs):
        self.region = os.getenv("STATE_DYNAMODB_REGION")
        self.endpoint_url = os.getenv("STATE_DYNAMODB_ENDPOINT_URL")
        self.aws_access_key = os.getenv("STATE_DYNAMODB_ACCESS_KEY")
        self.aws_secret_key = os.getenv("STATE_DYNAMODB_SECRET_KEY")
        self.table_name = os.getenv("STATE_DYNAMODB_TABLE")
        self.connect_timeout = float(
            os.getenv("STATE_DYNAMODB_CONNECT_TIMEOUT", "5")
        )
        self.read_timeout = float(os.getenv("STATE_DYNAMODB_READ_TIMEOUT", "5"))

        logger.debug(
            f"DynamoDB configuration: region={self.region}, "
            f"endpoint={self.endpoint_url}, table={self.table_name}"
        )

        self.client = self._create_client()
        self._ensure_table_exists()

    def _create_client(self) -> Any:
        config_params = {
            "connect_timeout": self.connect_timeout,
            "read_timeout": self.read_timeout,
        }

        required_version = "1.27.84"
        current_version = botocore.__version__
        if packaging.version.parse(current_version) >= packaging.version.parse(
            required_version
        ):
            config_params["tcp_keepalive"] = True
            logger.debug("TCP keep-alive enabled for DynamoDB connections")

        my_config = Config(**config_params)

        return boto3.client(
            service_name="dynamodb",
            region_name=self.region,
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key,
            config=my_config,
        )

    def _ensure_table_exists(self) -> None:
        try:
            self.client.describe_table(TableName=self.table_name)
            logger.debug(f"DynamoDB table {self.table_name} exists.")
        except ClientError as e:
            if e.response["Error"]["Code"] != "ResourceNotFoundException":
                logger.error(f"Error checking table: {e}")
                raise

            error_msg = (
                f"DynamoDB table {self.table_name} does not exist. "
                "Please create it manually."
            )
            logger.error(error_msg)
            raise ConfigurationError(error_msg)

    def store(
        self,
        datasource_type: str,
        datasource_source: str,
        state_position: Dict[str, str],
    ) -> bool:
        try:
            position_attributes = {}
            for key, value in state_position.items():
                position_attributes[key] = {"S": value}

            item = {
                "datasource_type": {"S": datasource_type},
                "datasource_source": {"S": datasource_source},
                **position_attributes,
            }

            logger.debug(f"Storing state: {item}")
            self.client.put_item(TableName=self.table_name, Item=item)
            logger.info(
                f"State stored for {datasource_type}:{datasource_source} - "
                f"{state_position}"
            )
            return True
        except Exception as e:
            logger.error(f"Failed to store state: {e}")
            return False

    def read(
        self, datasource_type: str, datasource_source: str
    ) -> Optional[Dict[str, str]]:
        try:
            response = self.client.get_item(
                TableName=self.table_name,
                Key={
                    "datasource_type": {"S": datasource_type},
                    "datasource_source": {"S": datasource_source},
                },
            )

            if "Item" not in response:
                logger.info(
                    f"No state found for {datasource_type}:{datasource_source}"
                )
                return None

            result = {}
            for key, value in response["Item"].items():
                if key in ["datasource_type", "datasource_source"]:
                    continue

                if "S" in value:
                    result[key] = value["S"]

            logger.debug(f"Retrieved state: {result}")
            return result

        except Exception as e:
            logger.error(f"Failed to read state: {e}")
            return None
