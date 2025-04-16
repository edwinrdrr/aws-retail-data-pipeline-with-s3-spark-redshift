CREATE TABLE IF NOT EXISTS gold.dim_customer (
    customer_key VARCHAR(255) NOT NULL,
    customer_natural_key VARCHAR(255) NOT NULL,
    customer_name VARCHAR(255),
    email VARCHAR(255),
    phone VARCHAR(255),
    address VARCHAR(512),
    city VARCHAR(255),
    row_effective_date TIMESTAMP,
    row_expiration_date TIMESTAMP,
    current_row_flag BOOLEAN,
    etl_processed_date TIMESTAMP,
    PRIMARY KEY (customer_key),
    CONSTRAINT uk_dim_customer_natural_key UNIQUE (customer_natural_key)
);