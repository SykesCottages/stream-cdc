#!/bin/bash

LOCALSTACK_ENDPOINT="http://localhost:4566"
TABLE_NAME="DataSourceStateTracker"
REGION="us-east-1"

BOLD='\033[1m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m'

command_exists() {
  command -v "$1" >/dev/null 2>&1
}

if ! command_exists aws; then
  echo -e "${RED}Error: AWS CLI is not installed. Please install it first.${NC}"
  exit 1
fi

echo -e "${BOLD}Checking if table ${TABLE_NAME} exists...${NC}"
if ! aws --endpoint-url="${LOCALSTACK_ENDPOINT}" --region="${REGION}" \
  dynamodb describe-table --table-name "${TABLE_NAME}" >/dev/null 2>&1; then
  echo -e "${RED}Table ${TABLE_NAME} does not exist in the DynamoDB instance.${NC}"
  exit 1
fi

echo -e "${BOLD}Retrieving item count...${NC}"
ITEM_COUNT=$(aws --endpoint-url="${LOCALSTACK_ENDPOINT}" --region="${REGION}" \
  dynamodb scan --table-name "${TABLE_NAME}" --select "COUNT" --query "Count" --output text)

echo -e "${GREEN}Found ${ITEM_COUNT} items in the ${TABLE_NAME} table.${NC}"

if [ "$ITEM_COUNT" -eq 0 ]; then
  echo -e "${YELLOW}The table is empty.${NC}"
  exit 0
fi

echo -e "${BOLD}Retrieving all items from the table...${NC}"
aws --endpoint-url="${LOCALSTACK_ENDPOINT}" --region="${REGION}" \
  dynamodb scan --table-name "${TABLE_NAME}" --output json | \
  jq '.Items' | jq -r '.' | sed 's/\\n/\n/g' | sed 's/\\t/\t/g'

echo -e "\n${GREEN}Successfully retrieved all items from ${TABLE_NAME}.${NC}"
