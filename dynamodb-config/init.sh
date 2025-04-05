#!/bin/bash

TABLE_NAME="DataSourceStateTracker"
LOCALSTACK_ENDPOINT="http://localhost:4566"

echo "Creating DynamoDB table in Localstack..."

aws dynamodb create-table \
    --endpoint-url $LOCALSTACK_ENDPOINT \
    --region us-east-1 \
    --table-name $TABLE_NAME \
    --attribute-definitions \
        AttributeName=datasource_type,AttributeType=S \
        AttributeName=datasource_source,AttributeType=S \
    --key-schema \
        AttributeName=datasource_type,KeyType=HASH \
        AttributeName=datasource_source,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST

echo "Waiting for table to become active..."
aws dynamodb wait table-exists \
    --endpoint-url $LOCALSTACK_ENDPOINT \
    --table-name $TABLE_NAME

if [ $? -eq 0 ]; then
    echo "Table created successfully!"
    exit 0
fi

echo "Failed to create table!"
