CREATE TABLE IF NOT EXISTS gold.dim_product (
    product_key VARCHAR(255) NOT NULL,
    product_natural_key VARCHAR(255) NOT NULL,
    product_name VARCHAR(255),
    current_price DECIMAL(18,2),
    stock_quantity INTEGER,
    category VARCHAR(255),
    row_effective_date TIMESTAMP,
    row_expiration_date TIMESTAMP,
    current_row_flag BOOLEAN,
    etl_processed_date TIMESTAMP,
    PRIMARY KEY (product_key),
    CONSTRAINT uk_dim_product_natural_key UNIQUE (product_natural_key)
);