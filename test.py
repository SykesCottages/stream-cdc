import boto3


def create_table():
    # Create a DynamoDB client
    client = boto3.client(
        "dynamodb",
        region_name="us-east-1",
        endpoint_url="http://localhost:4567",
        aws_access_key_id="dummy",
        aws_secret_access_key="dummy",
    )

    # Check if table exists
    try:
        client.describe_table(TableName="DataSourceStateTracker")
        print("Table already exists")
        return
    except client.exceptions.ResourceNotFoundException:
        # Table doesn't exist, create it
        print("Table doesn't exist. Creating now...")

    # Create table
    response = client.create_table(
        TableName="DataSourceStateTracker",
        KeySchema=[
            {
                "AttributeName": "id",
                "KeyType": "HASH",  # Partition key
            }
        ],
        AttributeDefinitions=[
            {
                "AttributeName": "id",
                "AttributeType": "S",  # String
            }
        ],
        ProvisionedThroughput={"ReadCapacityUnits": 5, "WriteCapacityUnits": 5},
    )

    print(
        f"Table created successfully: {response['TableDescription']['TableName']}"
    )

    # List all tables to verify
    tables = client.list_tables()
    print(f"All tables: {tables['TableNames']}")


if __name__ == "__main__":
    create_table()
