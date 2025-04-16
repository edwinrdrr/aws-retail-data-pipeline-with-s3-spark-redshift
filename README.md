# Enterprise Retail Analytics Pipeline (SCD2 + Dimensional Modeling)

This project implements a data pipeline for retail data processing, from raw data generation to analytics-ready data in Redshift.

## Project Structure
```
├── scripts/
│   ├── data_generation/       # Scripts to generate sample data
│   ├── spark/                 # Spark transformation scripts
│   │   ├── bronze_to_silver/  # ETL from Bronze to Silver layer
│   │   └── silver_to_gold/    # ETL from Silver to Gold layer
│   └── redshift/
|       ├── data_loading/
│       ├── database_setup/
|       ├── schema_setup/
│       └── tables_setup/
├── .gitignore
└── README.md                
```

## Pipeline Architecture

1. **Data Generation (Faker Library) (Local)** → 2. **S3 (Data Lake) Bronze Layer** → 3. **Spark (Bronze to Silver)** → 4. **S3 (Data Lake) Silver Layer** → 5. **Spark (Silver to Gold)** → 6. **S3 (Data Lake) Gold Layer** → 7. **Redshift Serverless (Data Warehouse / Analytics)**

In this project, I use Databricks to run the Spark jobs.

## Setup Instructions

### Prerequisites
- AWS Account with appropriate permissions
- Python 3.8+ with pandas and faker installed
- Spark environment (Databricks/EMR Serverless)
- Redshift Serverless access

### 1. Generating Sample Data
Run the data generation script in `scripts/data_generation/faker_script_retail_data.py`

### 2. Bronze to Silver Processing
Run the following Databricks notebooks in the `scripts/spark/bronze_to_silver/` directory:
- `bronze_to_silver_customers.py`
- `bronze_to_silver_products.py`
- `bronze_to_silver_orders.py`
- `bronze_to_silver_order_items.py`

### 3. Silver to Gold Processing
Run the following Databricks notebooks in the `scripts/spark/silver_to_gold/` directory:
- `silver_to_gold_dim_customer.py`
- `silver_to_gold_dim_product.py`
- `silver_to_gold_dim_date.py`
- `silver_to_gold_fact_sales.py`

Note: If using Databricks, you may need to manually delete committed files in the target layer between runs so that the files can be loaded to Redshift Serverless.

### 4. Data Warehousing in Redshift Serverless

#### 4.1. Provision Infrastructure
Create a Redshift Serverless namespace and workgroup using AWS CLI or console.

#### 4.2. Setup Database and Schema
Run the database setup script in `scripts/redshift/database_setup/` and `scripts/redshift/schema_setup/` 

#### 4.3. Create Tables
Run the table creation scripts in the `scripts/redshift/tables_setup/` directory:
- `table_setup_gold_dim_customer.sql`
- `table_setup_gold_dim_product.sql`
- `table_setup_gold_dim_date.sql`
- `table_setup_gold_fact_sales.sql`

#### 4.4. Load Data
Use Redshift COPY commands to load data from S3 Gold Layer to the tables created in the previous step.

Run the load data scripts in the `scripts/redshift/data_loading/` directory:
- `gold_data_loading_dim_customer.sql`
- `gold_data_loading_dim_product.sql`
- `gold_data_loading_dim_date.sql`
- `gold_data_loading_fact_sales.sql`

## Data Model (Star Schema)

### Dimension Tables
- **dim_customer**: SCD Type 2 customer dimension with historical tracking
- **dim_product**: SCD Type 2 product dimension with historical tracking
- **dim_date**: Date dimension with calendar attributes
### Fact Table
- **fact_sales**: Sales transactions with foreign keys to dimensions






