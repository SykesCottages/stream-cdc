#!/bin/bash
set -e

CONTAINER_NAME="cdc-worker-mysql-1"
DB_USER="root"
DB_PASSWORD="password"
DB_NAME="test"

echo "Waiting for MySQL to be ready..."
until docker exec "$CONTAINER_NAME" mysqladmin ping -u"$DB_USER" -p"$DB_PASSWORD" --silent; do
    echo "MySQL not ready yet..."
    sleep 1
done

echo "Dropping and recreating users table..."
docker exec -i "$CONTAINER_NAME" mysql -u"$DB_USER" -p"$DB_PASSWORD" "$DB_NAME" <<EOF
DROP TABLE IF EXISTS users;

CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE,
    status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

INSERT INTO users (name, email, status) VALUES
    ('Alice Smith', 'alice.smith@example.com', 'active'),
    ('Bob Johnson', 'bob.johnson@example.com', 'inactive'),
    ('Charlie Brown', 'charlie.brown@example.com', 'pending'),
    ('Diana Prince', 'diana.prince@example.com', 'active'),
    ('Ethan Hunt', 'ethan.hunt@example.com', 'inactive'),
    ('Hannah Lee', 'hannah.lee@example.com', 'inactive'),
    ('Julia Adams', 'julia.adams@example.com', 'active');
