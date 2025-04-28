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
SETUP_DATABASE=true              # Create tables and initial data
RUN_BASIC_UPDATES=true           # Run basic update tests (status changes, etc.)
RUN_BATCH_OPERATIONS=true        # Run batch operations
RUN_TRANSACTIONS=true            # Run transaction-based operations
RUN_LARGE_DATA_TESTS=false        # Run tests with large data (>256KB)
INCLUDE_HUGE_DATA=true          # Include very large data (>1MB)

# Timing settings
INITIAL_WAIT_AFTER_SETUP=5       # Seconds to wait after initial setup
WAIT_BETWEEN_TESTS=3             # Seconds to wait between test scenarios

# Data size settings (in bytes) - FIXED VALUES
LARGE_DATA_SIZE=262144           # 256KB (256 * 1024)
HUGE_DATA_SIZE=1048576           # 1MB (1024 * 1024)
# ==============================================================

echo "==== Stream CDC Test Script ===="
echo "Enabled tests:"
echo "- Database setup: $(if $SETUP_DATABASE; then echo "ENABLED"; else echo "DISABLED"; fi)"
echo "- Basic updates: $(if $RUN_BASIC_UPDATES; then echo "ENABLED"; else echo "DISABLED"; fi)"
echo "- Batch operations: $(if $RUN_BATCH_OPERATIONS; then echo "ENABLED"; else echo "DISABLED"; fi)"
echo "- Transactions: $(if $RUN_TRANSACTIONS; then echo "ENABLED"; else echo "DISABLED"; fi)"
echo "- Large data (256KB): $(if $RUN_LARGE_DATA_TESTS; then echo "ENABLED"; else echo "DISABLED"; fi)"
echo "- Huge data (1MB): $(if $INCLUDE_HUGE_DATA; then echo "ENABLED"; else echo "DISABLED"; fi)"

generate_large_blob() {
    local size=$1
    local lorem="Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. "
    local lorem_length=${#lorem}

    local iterations=$(( (size + lorem_length - 1) / lorem_length ))
    local result=""

    local chunk_size=1000
    local chunk_iterations=0

    if [ "$iterations" -gt "$chunk_size" ]; then
        chunk_iterations=$(( iterations / chunk_size ))
        local remainder_iterations=$(( iterations % chunk_size ))

        local chunk=""
        for ((i=0; i<chunk_size; i++)); do
            chunk+="$lorem"
        done

        for ((j=0; j<chunk_iterations; j++)); do
            result+="$chunk"
        done

        for ((k=0; k<remainder_iterations; k++)); do
            result+="$lorem"
        done
    else
        for ((i=0; i<iterations; i++)); do
            result+="$lorem"
        done
    fi

    local trimmed="${result:0:$size}"

    echo "Generated blob of size: $size bytes" >&2
    echo "Actual generated size: ${#result} before trimming" >&2
    echo "Final size after trimming: ${#trimmed}" >&2

    echo "$trimmed"
}

echo "Waiting for MySQL to be ready..."
until docker exec "$CONTAINER_NAME" mysqladmin ping -u"$DB_USER" -p"$DB_PASSWORD" --silent; do
    echo "MySQL not ready yet..."
    sleep 1
done
echo "MySQL is ready."

if $SETUP_DATABASE; then
    echo "Setting up database tables..."
    docker exec -i "$CONTAINER_NAME" mysql -u"$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" <<EOF
    -- Create user_logs table
    CREATE TABLE IF NOT EXISTS user_logs (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL,
        action VARCHAR(255) NOT NULL,
        ip_address VARCHAR(45) NOT NULL,
        details LONGTEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Create binary_data table for huge data tests
    CREATE TABLE IF NOT EXISTS binary_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        description TEXT,
        content_type VARCHAR(100) NOT NULL,
        binary_content LONGBLOB NOT NULL,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Add initial test data
    INSERT INTO user_logs (user_id, action, ip_address, details)
    VALUES
        (1, 'login', '192.168.1.1', 'Regular login'),
        (2, 'update_profile', '192.168.1.2', 'Updated email address');
EOF

    if [ $? -eq 0 ]; then
        echo "Database setup completed successfully."
    else
        echo "Error: Database setup failed."
        exit 1
    fi

    echo "Waiting $INITIAL_WAIT_AFTER_SETUP seconds after initial setup..."
    sleep $INITIAL_WAIT_AFTER_SETUP
fi

if $RUN_BASIC_UPDATES; then
    echo "Running test scenario 1: Basic Updates"
    docker exec -i "$CONTAINER_NAME" mysql -u"$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" <<EOF
    -- Simple update to a user log
    UPDATE user_logs SET details = 'Updated details' WHERE user_id = 1;

    -- Insert a new log entry
    INSERT INTO user_logs (user_id, action, ip_address, details)
    VALUES (1, 'logout', '192.168.1.1', 'Regular logout');
EOF

    echo "Basic updates completed."
    echo "Waiting $WAIT_BETWEEN_TESTS seconds before next test..."
    sleep $WAIT_BETWEEN_TESTS
fi

if $RUN_BATCH_OPERATIONS; then
    echo "Running test scenario 2: Batch Operations"
    docker exec -i "$CONTAINER_NAME" mysql -u"$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" <<EOF
    -- Batch insert multiple records
    INSERT INTO user_logs (user_id, action, ip_address, details)
    VALUES
        (3, 'login', '192.168.1.3', 'First login'),
        (3, 'view_dashboard', '192.168.1.3', 'Viewed main dashboard'),
        (3, 'update_settings', '192.168.1.3', 'Changed notification settings'),
        (3, 'logout', '192.168.1.3', 'Session ended');

    -- Batch update
    UPDATE user_logs SET details = CONCAT(details, ' (audited)') WHERE user_id = 3;
EOF

    echo "Batch operations completed."
    echo "Waiting $WAIT_BETWEEN_TESTS seconds before next test..."
    sleep $WAIT_BETWEEN_TESTS
fi

if $RUN_TRANSACTIONS; then
    echo "Running test scenario 3: Transactions"
    docker exec -i "$CONTAINER_NAME" mysql -u"$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" <<EOF
    -- Start a transaction
    START TRANSACTION;

    -- Insert a record
    INSERT INTO user_logs (user_id, action, ip_address, details)
    VALUES (4, 'login', '192.168.1.4', 'Transaction test - login');

    -- Update the record
    UPDATE user_logs SET details = 'Transaction test - modified' WHERE user_id = 4;

    -- Insert another record
    INSERT INTO user_logs (user_id, action, ip_address, details)
    VALUES (4, 'logout', '192.168.1.4', 'Transaction test - logout');

    -- Commit the transaction
    COMMIT;
EOF

    echo "Transaction operations completed."
    echo "Waiting $WAIT_BETWEEN_TESTS seconds before next test..."
    sleep $WAIT_BETWEEN_TESTS
fi

if $RUN_LARGE_DATA_TESTS; then
    echo "Generating test data of size ${LARGE_DATA_SIZE} bytes..."
    LARGE_BLOB=$(generate_large_blob $LARGE_DATA_SIZE)
    echo "Large blob generation complete. Size: ${#LARGE_BLOB} bytes"

    if $INCLUDE_HUGE_DATA; then
        echo "Generating huge test data of size ${HUGE_DATA_SIZE} bytes..."
        HUGE_BLOB=$(generate_large_blob $HUGE_DATA_SIZE)
        echo "Huge blob generation complete. Size: ${#HUGE_BLOB} bytes"
    fi

    echo "Running test scenario 4: Large Data Updates"

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
