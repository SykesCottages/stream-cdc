import os
import boto3
from typing import Any, Dict, Optional
from stream_cdc.utils.logger import logger
from botocore.exceptions import ClientError
from stream_cdc.state.base import StateManager
from botocore.config import Config
import botocore
import packaging.version


class Dynamodb(StateManager):
    def __init__(self, table_name: str, **kwargs):
        """
        Initialize the DynamoDB state manager.

        Args:
            table_name (str, optional): The DynamoDB table name. Defaults to env variable.
            **kwargs: Additional configuration options.
        """
        self.region = os.getenv("STATE_DYNAMODB_REGION")
        self.endpoint_url = os.getenv("STATE_DYNAMODB_ENDPOINT_URL")
        self.aws_access_key = os.getenv("STATE_DYNAMODB_ACCESS_KEY")
        self.aws_secret_key = os.getenv("STATE_DYNAMODB_SECRET_KEY")
        self.table_name = table_name or os.getenv("STATE_DYNAMODB_TABLE")
        self.connect_timeout = float(
            os.getenv("STATE_DYNAMODB_CONNECT_TIMEOUT", "0.5")
        )
        self.read_timeout = float(
            os.getenv("STATE_DYNAMODB_READ_TIMEOUT", "0.5")
        )

        # Override with kwargs if provided
        for key, value in kwargs.items():
            setattr(self, key, value)

        # Initialize client
        self.client = self._create_client()

        # Ensure table exists
        self._ensure_table_exists()

    def _create_client(self) -> Any:
        """
        Create and configure the boto3 DynamoDB client with TCP keep-alive
        if supported by the botocore version.

        Returns:
            Any: The DynamoDB client.
        """
        # Configure with basic timeouts
        config_params = {
            "connect_timeout": self.connect_timeout,
            "read_timeout": self.read_timeout,
        }

        # Add TCP keep-alive if botocore version supports it
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
        """
        Ensure the state table exists. If not, create it.
        """
        try:
            self.client.describe_table(TableName=self.table_name)
            logger.debug(f"DynamoDB table {self.table_name} exists.")
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                logger.info(f"Creating DynamoDB table {self.table_name}...")
                self.client.create_table(
                    TableName=self.table_name,
                    KeySchema=[
                        {"AttributeName": "datasource_type", "KeyType": "HASH"},
                        {
                            "AttributeName": "datasource_source",
                            "KeyType": "RANGE",
                        },
                    ],
                    AttributeDefinitions=[
                        {
                            "AttributeName": "datasource_type",
                            "AttributeType": "S",
                        },
                        {
                            "AttributeName": "datasource_source",
                            "AttributeType": "S",
                        },
                    ],
                    BillingMode="PAY_PER_REQUEST",
                )
                # Wait for table to be created
                waiter = self.client.get_waiter("table_exists")
                waiter.wait(TableName=self.table_name)
                logger.info(f"Table {self.table_name} created successfully.")
            else:
                logger.error(f"Error checking table: {e}")
                raise

    def store(
        self,
        datasource_type: str,
        datasource_source: str,
        state_position: Dict[str, str],
    ) -> bool:
        """
        Store state information in DynamoDB. Overwrites any existing state for the same key.

        Args:
            datasource_type (str): The type of datasource (e.g., "postgres", "mysql")
            datasource_source (str): The source identifier (e.g., "host1.example.com")
            state_position (Dict[str, str]): State information, like {"gtid": "12345"}

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Convert state_position to DynamoDB format
            state_position_db = {}
            for key, value in state_position.items():
                state_position_db[key] = {"S": value}

            # Create item with required fields
            item = {
                "datasource_type": {"S": datasource_type},
                "datasource_source": {"S": datasource_source},
                **state_position_db,
            }

            logger.debug(f"Storing state: {item}")
            self.client.put_item(TableName=self.table_name, Item=item)
            logger.info(
                f"State stored for {datasource_type}:{datasource_source}"
            )
            return True
        except Exception as e:
            logger.error(f"Failed to store state: {e}")
            return False

    def read(
        self, datasource_type: str, datasource_source: str
    ) -> Optional[Dict[str, str]]:
        """
        Read state information from DynamoDB.

        Args:
            datasource_type (str): The type of datasource (e.g., "postgres", "mysql")
            datasource_source (str): The source identifier (e.g., "host1.example.com")

        Returns:
            Optional[Dict[str, str]]: The state information if found, None otherwise
        """
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

            # Convert DynamoDB format back to regular dictionary
            # Exclude the key attributes
            result = {}
            for key, value in response["Item"].items():
                if key not in ["datasource_type", "datasource_source"]:
                    # Extract the value from the DynamoDB type descriptor
                    for type_descriptor, actual_value in value.items():
                        result[key] = actual_value
                        break

            logger.debug(f"Retrieved state: {result}")
            return result

        except Exception as e:
            logger.error(f"Failed to read state: {e}")
            return None
