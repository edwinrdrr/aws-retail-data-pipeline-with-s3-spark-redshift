CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key INTEGER NOT NULL,
    full_date DATE NOT NULL,
    day_of_week INTEGER,
    day_name VARCHAR(9),
    day_of_month INTEGER,
    day_of_year INTEGER,
    week_of_year INTEGER,
    month INTEGER,
    month_name VARCHAR(9),
    quarter INTEGER,
    year INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN,
    etl_processed_date TIMESTAMP,
    PRIMARY KEY (date_key)
);