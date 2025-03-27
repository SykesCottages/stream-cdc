-- Drop the user if it exists (ignore errors if it doesn't)
DROP USER IF EXISTS 'cdc-connector'@'%';

-- Create the user
CREATE USER 'cdc-connector'@'%' IDENTIFIED BY 'password';

-- Grant privileges
GRANT REPLICATION CLIENT ON *.* TO 'cdc-connector'@'%';
GRANT REPLICATION SLAVE ON *.* TO 'cdc-connector'@'%';

-- Flush privileges
FLUSH PRIVILEGES;

-- Create database and table
CREATE DATABASE test;

CREATE TABLE test.users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    password VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
