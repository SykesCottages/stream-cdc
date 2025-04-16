#!/bin/bash
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# ======== CONFIGURATION SECTION - MODIFY THESE SETTINGS ========
# Container settings
CONTAINER_NAME="stream-cdc-mysql-1"
DB_USER="root"
DB_PASSWORD="password"
DB_NAME="test"

# Test control - Set to true/false to enable/disable specific test components
SETUP_DATABASE=false              # Create tables and initial data
RUN_BASIC_UPDATES=false           # Run basic update tests (status changes, etc.)
RUN_BATCH_OPERATIONS=false        # Run batch operations
RUN_TRANSACTIONS=false            # Run transaction-based operations
RUN_LARGE_DATA_TESTS=true         # Run tests with large data (>256KB)
INCLUDE_HUGE_DATA=false           # Include very large data (>1MB)

# Timing settings
INITIAL_WAIT_AFTER_SETUP=5        # Seconds to wait after initial setup
WAIT_BETWEEN_TESTS=3              # Seconds to wait between test scenarios

# Data size settings (in bytes) - FIXED VALUES
LARGE_DATA_SIZE=262144            # 256KB (256 * 1024)
HUGE_DATA_SIZE=1048576            # 1MB (1024 * 1024)
# ==============================================================

echo "==== Stream CDC Test Script ===="
echo "Enabled tests:"
echo "- Database setup: $(if $SETUP_DATABASE; then echo "ENABLED"; else echo "DISABLED"; fi)"
echo "- Basic updates: $(if $RUN_BASIC_UPDATES; then echo "ENABLED"; else echo "DISABLED"; fi)"
echo "- Batch operations: $(if $RUN_BATCH_OPERATIONS; then echo "ENABLED"; else echo "DISABLED"; fi)"
echo "- Transactions: $(if $RUN_TRANSACTIONS; then echo "ENABLED"; else echo "DISABLED"; fi)"
echo "- Large data (256KB): $(if $RUN_LARGE_DATA_TESTS; then echo "ENABLED"; else echo "DISABLED"; fi)"
echo "- Huge data (1MB): $(if $INCLUDE_HUGE_DATA; then echo "ENABLED"; else echo "DISABLED"; fi)"

# Fixed function to generate a large text blob of specified size
generate_large_blob() {
    local size=$1
    local lorem="Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. "
    local lorem_length=${#lorem}

    # Calculate required iterations more precisely
    local iterations=$(( (size + lorem_length - 1) / lorem_length ))
    local result=""

    # For very large data, build in chunks to avoid memory issues
    local chunk_size=1000
    local chunk_iterations=0

    if [ "$iterations" -gt "$chunk_size" ]; then
        chunk_iterations=$(( iterations / chunk_size ))
        local remainder_iterations=$(( iterations % chunk_size ))

        # Build a chunk first
        local chunk=""
        for ((i=0; i<chunk_size; i++)); do
            chunk+="$lorem"
        done

        # Apply the chunk multiple times
        for ((j=0; j<chunk_iterations; j++)); do
            result+="$chunk"
        done

        # Add any remainder
        for ((k=0; k<remainder_iterations; k++)); do
            result+="$lorem"
        done
    else
        # For smaller sizes, build directly
        for ((i=0; i<iterations; i++)); do
            result+="$lorem"
        done
    fi

    # Ensure exact size with substring - FIXED LINE
    local trimmed="${result:0:$size}"

    # Debug output to verify size
    echo "Generated blob of size: $size bytes" >&2
    echo "Actual generated size: ${#result} before trimming" >&2
    echo "Final size after trimming: ${#trimmed}" >&2

    # Return the trimmed result
    echo "$trimmed"
}

# Wait for MySQL to be ready
echo "Waiting for MySQL to be ready..."
until docker exec "$CONTAINER_NAME" mysqladmin ping -u"$DB_USER" -p"$DB_PASSWORD" --silent; do
    echo "MySQL not ready yet..."
    sleep 1
done
echo "MySQL is ready."

# Generate large text data
if $RUN_LARGE_DATA_TESTS; then
    echo "Generating test data of size ${LARGE_DATA_SIZE} bytes..."
    LARGE_BLOB=$(generate_large_blob $LARGE_DATA_SIZE)
    echo "Large blob generation complete. Size: ${#LARGE_BLOB} bytes"

    if $INCLUDE_HUGE_DATA; then
        echo "Generating huge test data of size ${HUGE_DATA_SIZE} bytes..."
        HUGE_BLOB=$(generate_large_blob $HUGE_DATA_SIZE)
        echo "Huge blob generation complete. Size: ${#HUGE_BLOB} bytes"
    fi
fi

# Test Scenario 4: Large Data Updates
if $RUN_LARGE_DATA_TESTS; then
    echo "Running test scenario 4: Large Data Updates"

    # To avoid issues with passing large strings via command line,
    # let's use MySQL's REPEAT function to generate large data directly in the database
    docker exec -i "$CONTAINER_NAME" mysql -u"$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" <<EOF
    -- Insert a new log entry with large data using MySQL's REPEAT function
    INSERT INTO user_logs (user_id, action, ip_address, details)
    VALUES (3, 'data_export', '10.0.0.10', REPEAT('A', $LARGE_DATA_SIZE));

    -- Verify the actual size of inserted data
    SELECT 'New log entry size', LENGTH(details) AS size
    FROM user_logs
    WHERE action = 'data_export' AND user_id = 3;
EOF

    if $INCLUDE_HUGE_DATA; then
        echo "Running huge data update operations..."

        docker exec -i "$CONTAINER_NAME" mysql -u"$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" <<EOF
        -- Insert very large data using MySQL's native functions
        INSERT INTO binary_data (name, description, content_type, binary_content)
        VALUES ('Huge Document', 'Test document of 1MB', 'text/plain', REPEAT('X', $HUGE_DATA_SIZE));

        -- Verify size of huge document
        SELECT 'Huge document size', LENGTH(binary_content) AS size
        FROM binary_data
        WHERE name = 'Huge Document';
EOF
    fi

    echo "Large data operations completed."
fi

echo "All selected test scenarios have been completed successfully."
echo "Test summary:"
echo "- Database setup: $(if $SETUP_DATABASE; then echo "COMPLETED"; else echo "SKIPPED"; fi)"
echo "- Basic updates: $(if $RUN_BASIC_UPDATES; then echo "COMPLETED"; else echo "SKIPPED"; fi)"
echo "- Batch operations: $(if $RUN_BATCH_OPERATIONS; then echo "COMPLETED"; else echo "SKIPPED"; fi)"
echo "- Transactions: $(if $RUN_TRANSACTIONS; then echo "COMPLETED"; else echo "SKIPPED"; fi)"
echo "- Large data tests: $(if $RUN_LARGE_DATA_TESTS; then echo "COMPLETED"; else echo "SKIPPED"; fi)"
echo "- Huge data tests: $(if $INCLUDE_HUGE_DATA; then echo "COMPLETED"; else echo "SKIPPED"; fi)"

