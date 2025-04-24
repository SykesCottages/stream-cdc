#!/bin/bash
set -e

DOCKER_COMPOSE_FILE="docker-compose.yml"
LOCALSTACK_ENDPOINT="http://localhost:4566"
QUEUE_NAME="my-queue"
TABLE_NAME="DataSourceStateTracker"
REGION="us-east-1"

echo "Starting docker-compose services..."
docker-compose -f "$DOCKER_COMPOSE_FILE" up -d

echo "Waiting for localstack to be ready..."
until curl -s "$LOCALSTACK_ENDPOINT/_localstack/health" | grep -q "\"dynamodb\": \"\(running\|available\)\""; do
  echo "Localstack not ready yet..."
  sleep 3
done

echo "Creating SQS queue: $QUEUE_NAME..."
aws --endpoint-url="$LOCALSTACK_ENDPOINT" --region="$REGION" sqs create-queue --queue-name "$QUEUE_NAME" > /dev/null
# aws --endpoint-url="http://localhost:4566" --region="us-east-1" sqs create-queue --queue-name "my-queue"

# Check if the DynamoDB table exists
echo "Checking if DynamoDB table $TABLE_NAME exists..."
if aws --endpoint-url="$LOCALSTACK_ENDPOINT" --region="$REGION" dynamodb describe-table --table-name "$TABLE_NAME" 2>/dev/null; then
  echo "Table $TABLE_NAME already exists."
else
  echo "Creating DynamoDB table: $TABLE_NAME..."
aws dynamodb create-table \
    --endpoint-url $LOCALSTACK_ENDPOINT \
    --region $REGION \
    --table-name $TABLE_NAME \
    --attribute-definitions \
        AttributeName=PK,AttributeType=S \
        AttributeName=SK,AttributeType=S \
    --key-schema \
        AttributeName=PK,KeyType=HASH \
        AttributeName=SK,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST
  # aws --endpoint-url="$LOCALSTACK_ENDPOINT" --region="$REGION" dynamodb create-table \
  #   --table-name "$TABLE_NAME" \
  #   --attribute-definitions AttributeName=id,AttributeType=S \
  #   --key-schema AttributeName=id,KeyType=HASH \
  #   --provisioned-throughput ReadCapacityUnits=5,WriteCapacityUnits=5 \
  #   --region "$REGION" > /dev/null
  echo "Table $TABLE_NAME created successfully."
fi

echo "All services are running and configured."

# source ./dynamodb-config/init.sh
