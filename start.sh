#!/bin/bash
set -e

DOCKER_COMPOSE_FILE="docker-compose.yml"
LOCALSTACK_ENDPOINT="http://localhost:4566"
QUEUE_NAME="my-queue"

echo "Starting docker-compose services..."
docker-compose -f "$DOCKER_COMPOSE_FILE" up -d

echo "Waiting for localstack to be ready..."
until curl -s "$LOCALSTACK_ENDPOINT" >/dev/null; do
  echo "Localstack not ready yet..."
  sleep 1
done

echo "Creating SQS queue: $QUEUE_NAME..."
aws --endpoint-url="$LOCALSTACK_ENDPOINT" sqs create-queue --queue-name "$QUEUE_NAME" > /dev/null

echo "Queue created. Services are running."
