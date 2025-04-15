#!/bin/bash
set -e

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# ======== CONFIGURATION SECTION - MODIFY THESE SETTINGS ========
RUN_VALIDATION_AFTER=true

# Container settings
CONTAINER_NAME="stream-cdc-mysql-1"
DB_USER="root"
DB_PASSWORD="password"
DB_NAME="test"

# Test control - Set to true/false to enable/disable specific test components
SETUP_DATABASE=true              # Create tables and initial data
RUN_BASIC_UPDATES=true           # Run basic update tests (status changes, etc.)
RUN_BATCH_OPERATIONS=true        # Run batch insert/update operations
RUN_TRANSACTIONS=true            # Run transaction-based operations
RUN_LARGE_DATA_TESTS=false       # Run tests with large data (>256KB)
INCLUDE_HUGE_DATA=false          # Include very large data (>1MB) - can be memory intensive

# Timing settings
INITIAL_WAIT_AFTER_SETUP=5       # Seconds to wait after initial setup
WAIT_BETWEEN_TESTS=3             # Seconds to wait between test scenarios

# Data size settings (in bytes)
LARGE_DATA_SIZE=262144           # 256KB
HUGE_DATA_SIZE=1048576           # 1MB
# ==============================================================

echo "==== Stream CDC Test Script ===="
echo "Enabled tests:"
echo "- Database setup: $(if $SETUP_DATABASE; then echo "ENABLED"; else echo "DISABLED"; fi)"
echo "- Basic updates: $(if $RUN_BASIC_UPDATES; then echo "ENABLED"; else echo "DISABLED"; fi)"
echo "- Batch operations: $(if $RUN_BATCH_OPERATIONS; then echo "ENABLED"; else echo "DISABLED"; fi)"
echo "- Transactions: $(if $RUN_TRANSACTIONS; then echo "ENABLED"; else echo "DISABLED"; fi)"
echo "- Large data (256KB): $(if $RUN_LARGE_DATA_TESTS; then echo "ENABLED"; else echo "DISABLED"; fi)"
echo "- Huge data (1MB): $(if $INCLUDE_HUGE_DATA; then echo "ENABLED"; else echo "DISABLED"; fi)"

# Function to generate a large text blob of specified size
generate_large_blob() {
    local size=$1
    local lorem="Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua. Ut enim ad minim veniam, quis nostrud exercitation ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis aute irure dolor in reprehenderit in voluptate velit esse cillum dolore eu fugiat nulla pariatur. Excepteur sint occaecat cupidatat non proident, sunt in culpa qui officia deserunt mollit anim id est laborum. "
    local result=""
    local lorem_length=${#lorem}
    local iterations=$(( size / lorem_length + 1 ))

    for ((i=0; i<iterations; i++)); do
        result+="$lorem"
    done

    echo "${result:0:$size}"
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
    echo "Generating test data..."
    LARGE_BLOB=$(generate_large_blob $LARGE_DATA_SIZE)

    if $INCLUDE_HUGE_DATA; then
        echo "Generating huge test data (this may take a moment)..."
        HUGE_BLOB=$(generate_large_blob $HUGE_DATA_SIZE)
    fi
fi

# Setup database tables and initial data
if $SETUP_DATABASE; then
    echo "Setting up database schema and initial data..."

    docker exec -i "$CONTAINER_NAME" mysql -u"$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" <<EOF
    -- Drop existing tables if they exist
    DROP TABLE IF EXISTS user_logs;
    DROP TABLE IF EXISTS orders;
    DROP TABLE IF EXISTS products;
    DROP TABLE IF EXISTS binary_data;
    DROP TABLE IF EXISTS users;

    -- Create users table with various data types
    CREATE TABLE users (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        email VARCHAR(255) UNIQUE,
        status ENUM('active', 'inactive', 'pending', 'suspended'),
        profile_data JSON,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    );

    -- Create orders table with foreign key relationship
    CREATE TABLE orders (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT NOT NULL,
        total_amount DECIMAL(10, 2) NOT NULL,
        status ENUM('pending', 'processing', 'shipped', 'delivered', 'cancelled'),
        shipping_address TEXT,
        notes TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
    );

    -- Create products table
    CREATE TABLE products (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        description TEXT,
        price DECIMAL(10, 2) NOT NULL,
        stock_quantity INT NOT NULL DEFAULT 0,
        category VARCHAR(100),
        is_available BOOLEAN DEFAULT TRUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    );

    -- Create user_logs table with large text fields
    CREATE TABLE user_logs (
        id INT AUTO_INCREMENT PRIMARY KEY,
        user_id INT,
        action VARCHAR(50) NOT NULL,
        ip_address VARCHAR(45),
        user_agent TEXT,
        details MEDIUMTEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL
    );

    -- Create binary_data table for testing large binary data
    CREATE TABLE binary_data (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255) NOT NULL,
        description TEXT,
        binary_content LONGTEXT,
        content_type VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );

    -- Insert sample data into users table
    INSERT INTO users (name, email, status, profile_data) VALUES
        ('Alice Smith', 'alice.smith@example.com', 'active', '{"age": 28, "interests": ["reading", "hiking"], "address": {"city": "Boston", "state": "MA"}}'),
        ('Bob Johnson', 'bob.johnson@example.com', 'inactive', '{"age": 35, "interests": ["gaming", "cooking"], "address": {"city": "New York", "state": "NY"}}'),
        ('Charlie Brown', 'charlie.brown@example.com', 'pending', '{"age": 42, "interests": ["football", "music"], "address": {"city": "Chicago", "state": "IL"}}'),
        ('Diana Prince', 'diana.prince@example.com', 'active', '{"age": 31, "interests": ["photography", "travel"], "address": {"city": "Seattle", "state": "WA"}}'),
        ('Ethan Hunt', 'ethan.hunt@example.com', 'inactive', '{"age": 39, "interests": ["running", "climbing"], "address": {"city": "Denver", "state": "CO"}}');

    -- Insert sample data into products table (just a few entries)
    INSERT INTO products (name, description, price, stock_quantity, category, is_available) VALUES
        ('Smartphone X', 'Latest model with advanced features', 799.99, 25, 'Electronics', TRUE),
        ('Laptop Pro', '15-inch laptop with fast processor', 1299.99, 10, 'Electronics', TRUE),
        ('Wireless Headphones', 'Noise-cancelling wireless headphones', 199.99, 50, 'Audio', TRUE);

    -- Insert basic orders
    INSERT INTO orders (user_id, total_amount, status, shipping_address, notes) VALUES
        (1, 125.99, 'delivered', '123 Main St, Boston, MA 02108', 'Leave at the door'),
        (2, 249.99, 'processing', '456 Park Ave, New York, NY 10022', 'Call before delivery'),
        (3, 58.25, 'pending', '789 Lake St, Chicago, IL 60601', NULL);
EOF

    # Add large data entries if enabled
    if $RUN_LARGE_DATA_TESTS; then
        echo "Adding large data test entries..."

        docker exec -i "$CONTAINER_NAME" mysql -u"$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" <<EOF
        -- Insert user log with large text content
        INSERT INTO user_logs (user_id, action, ip_address, details) VALUES
            (1, 'system_export', '192.168.1.10', '$LARGE_BLOB');

        -- Insert binary data entry with large content
        INSERT INTO binary_data (name, description, content_type, binary_content) VALUES
            ('Large Document', 'Test document exceeding 256KB', 'text/plain', '$LARGE_BLOB');
EOF

        # Add huge data if enabled
        if $INCLUDE_HUGE_DATA; then
            echo "Adding huge data test entries..."

            docker exec -i "$CONTAINER_NAME" mysql -u"$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" <<EOF
            -- Insert binary data with very large content
            INSERT INTO binary_data (name, description, content_type, binary_content) VALUES
                ('Huge Document', 'Test document exceeding 1MB', 'text/plain', '$HUGE_BLOB');
EOF
        fi
    fi

    echo "Database setup completed successfully."
    echo "Waiting $INITIAL_WAIT_AFTER_SETUP seconds for initial data to be processed..."
    sleep $INITIAL_WAIT_AFTER_SETUP
fi

# Test Scenario 1: Basic Updates
if $RUN_BASIC_UPDATES; then
    echo "Running test scenario 1: Basic Updates"

    docker exec -i "$CONTAINER_NAME" mysql -u"$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" <<EOF
    -- Update user status
    UPDATE users SET status = 'active' WHERE name = 'Bob Johnson';

    -- Update product price
    UPDATE products SET price = 849.99, stock_quantity = 20 WHERE name = 'Smartphone X';

    -- Update order status
    UPDATE orders SET status = 'shipped' WHERE user_id = 2 AND status = 'processing';
EOF

    echo "Basic updates completed."
    echo "Waiting $WAIT_BETWEEN_TESTS seconds for changes to be processed..."
    sleep $WAIT_BETWEEN_TESTS
fi

# Test Scenario 2: Batch Operations
if $RUN_BATCH_OPERATIONS; then
    echo "Running test scenario 2: Batch Operations"

    docker exec -i "$CONTAINER_NAME" mysql -u"$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" <<EOF
    -- Insert batch of new users
    INSERT INTO users (name, email, status, profile_data) VALUES
        ('Sam Taylor', 'sam.taylor@example.com', 'active', '{"age": 32, "interests": ["tennis", "movies"]}'),
        ('Lisa Wang', 'lisa.wang@example.com', 'active', '{"age": 29, "interests": ["cycling", "photography"]}'),
        ('Derek Johnson', 'derek.johnson@example.com', 'pending', '{"age": 45, "interests": ["golf", "cooking"]}');

    -- Insert new orders for these users
    INSERT INTO orders (user_id, total_amount, status, shipping_address)
    SELECT id, 125.50, 'pending', CONCAT(id, ' New Street, New City, State 12345')
    FROM users
    WHERE email IN ('sam.taylor@example.com', 'lisa.wang@example.com', 'derek.johnson@example.com');

    -- Batch update products
    UPDATE products
    SET stock_quantity = stock_quantity + 10,
        updated_at = CURRENT_TIMESTAMP
    WHERE category = 'Electronics';
EOF

    echo "Batch operations completed."
    echo "Waiting $WAIT_BETWEEN_TESTS seconds for changes to be processed..."
    sleep $WAIT_BETWEEN_TESTS
fi

# Test Scenario 3: Transactions
if $RUN_TRANSACTIONS; then
    echo "Running test scenario 3: Transactions"

    docker exec -i "$CONTAINER_NAME" mysql -u"$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" <<EOF
    START TRANSACTION;

    -- Update product availability
    UPDATE products SET is_available = FALSE, stock_quantity = 0 WHERE stock_quantity < 15;

    -- Log the inventory change
    INSERT INTO user_logs (user_id, action, ip_address, details)
    VALUES (1, 'inventory_update', '10.0.0.5', 'Automated inventory adjustment for low stock items');

    -- Add a new product
    INSERT INTO products (name, description, price, stock_quantity, category, is_available)
    VALUES ('Smart Speaker', 'Voice-controlled smart home speaker', 129.99, 40, 'Electronics', TRUE);

    COMMIT;
EOF

    echo "Transaction operations completed."
    echo "Waiting $WAIT_BETWEEN_TESTS seconds for changes to be processed..."
    sleep $WAIT_BETWEEN_TESTS
fi

# Test Scenario 4: Large Data Updates
if $RUN_LARGE_DATA_TESTS; then
    echo "Running test scenario 4: Large Data Updates"

    docker exec -i "$CONTAINER_NAME" mysql -u"$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" <<EOF
    -- Update large data record
    UPDATE binary_data
    SET description = 'Updated with new information'
    WHERE name = 'Large Document';

    -- Insert a new log entry with large data
    INSERT INTO user_logs (user_id, action, ip_address, details)
    VALUES (3, 'data_export', '10.0.0.10', CONCAT('Export log prefix: ', LEFT('$LARGE_BLOB', 10000)));
EOF

    if $INCLUDE_HUGE_DATA; then
        echo "Running huge data update operations..."

        docker exec -i "$CONTAINER_NAME" mysql -u"$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" <<EOF
        -- Update huge data record if it exists
        UPDATE binary_data
        SET description = 'Updated huge document'
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

if $RUN_VALIDATION_AFTER; then
  echo "Running CDC validation..."
  "$SCRIPT_DIR/validate-cdc.sh"
fi
