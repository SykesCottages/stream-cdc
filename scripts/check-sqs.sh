#!/bin/bash
set -e

ENDPOINT_URL="http://localhost:4566"
SQS_QUEUE_URL="http://localhost:4566/000000000000/my-queue"
REGION="us-east-1"

COUNT=0
while true; do
    echo "Fetching up to 10 messages..."
    MESSAGES=$(aws --endpoint-url="$ENDPOINT_URL" --region $REGION sqs receive-message --queue-url "$SQS_QUEUE_URL" --max-number-of-messages 10 --attribute-names All --message-attribute-names All)
    QUEUE_SIZE=$(aws --endpoint-url="$ENDPOINT_URL" --region $REGION sqs get-queue-attributes --queue-url "$SQS_QUEUE_URL" --attribute-names ApproximateNumberOfMessages | jq -r '.Attributes.ApproximateNumberOfMessages')

    if [ -z "$MESSAGES" ] || [ "$MESSAGES" = "{}" ]; then
        echo "No messages in queue."
    else
        MESSAGE_COUNT=$(echo "$MESSAGES" | jq -r '.Messages | length')
        COUNT=$((COUNT + MESSAGE_COUNT))
        echo "$MESSAGES" | jq -r '.Messages[] | "Body: \(.Body), ReceiptHandle: \(.ReceiptHandle)"'
        echo "$MESSAGES" | jq -r '.Messages[].ReceiptHandle' | while read -r HANDLE; do
            aws --endpoint-url="$ENDPOINT_URL" --region $REGION sqs delete-message --queue-url "$SQS_QUEUE_URL" --receipt-handle "$HANDLE"
            echo "Deleted message with ReceiptHandle: $HANDLE"
        done
    fi

    echo "Messages processed: $COUNT"
    echo "Approximate messages in queue: $QUEUE_SIZE"
    sleep 2
done
