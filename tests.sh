#!/bin/bash

# Usage: ./test.sh script1.sh script2.sh ...
# Example: ./test.sh validate-cdc.sh test-db.sh

set -e

SCRIPT_DIR="$(dirname "$0")/scripts"

if [ $# -eq 0 ]; then
  echo "Usage: $0 <script-name> [<script-name> ...]"
  echo "Available scripts:"
  ls "$SCRIPT_DIR"
  exit 1
fi

for SCRIPT_NAME in "$@"; do
  SCRIPT_PATH="$SCRIPT_DIR/$SCRIPT_NAME"
  echo "🔹 Running $SCRIPT_NAME..."

  if [ ! -f "$SCRIPT_PATH" ]; then
    echo "❌ Error: '$SCRIPT_NAME' not found in $SCRIPT_DIR"
    exit 1
  fi

  chmod +x "$SCRIPT_PATH"
  "$SCRIPT_PATH"
  echo "✅ Finished $SCRIPT_NAME"
  echo
done
