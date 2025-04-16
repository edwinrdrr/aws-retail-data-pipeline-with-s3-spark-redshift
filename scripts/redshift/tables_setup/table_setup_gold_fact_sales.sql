CREATE TABLE IF NOT EXISTS gold.fact_sales (
    sales_key BIGINT NOT NULL,
    order_natural_key VARCHAR(255) NOT NULL,
    order_item_natural_key VARCHAR(255) NOT NULL,
    customer_natural_key VARCHAR(255),
    product_natural_key VARCHAR(255),
    quantity INTEGER,
    unit_price DECIMAL(18,2),
    discount_pct DECIMAL(5,2),
    subtotal DECIMAL(18,2),
    net_amount DECIMAL(18,2),
    shipping_cost DECIMAL(18,2),
    total_amount DECIMAL(18,2),
    order_status VARCHAR(50),
    payment_method VARCHAR(50),
    etl_processed_date TIMESTAMP,
    order_date_key INTEGER NOT NULL,
    PRIMARY KEY (sales_key)
)
DISTKEY (product_natural_key)
SORTKEY (order_date_key, customer_natural_key);