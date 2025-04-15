# AWS Retail Data Pipeline (End-to-End)

This project implements a data pipeline for retail data processing, from raw data generation to analytics-ready data in Redshift.


## Project Structure

/project-root/
│
├── /scripts/
│   ├── /data_generation/            # Contains data generation scripts (formerly faker_scripts)
│   │   └── faker_script_retail_data.py
│   │
│   ├── /spark/                       # Spark processing scripts
│   │   ├── bronze_to_silver/
│   │   ├── silver_to_gold/
│   │
│   └── /redshift/                    # Redshift SQL scripts
│       ├── schema_setup/
│       ├── data_loading/
│       └── analytics/
│
│
├── README.md                         # Main project documentation
└── .gitignore

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

```bash
python scripts/data_generation/faker_script_retail_data.py --output ./data/raw/
```

### 2. Bronze to Silver Processing

fsdfs

### 3. Silver to Gold Processing

Databricks Note: If you use databricks, you may need to manually delete committed files in the target layer between runs. So that the files can be loaded to the Redshift Serverless.

### 4. Data Warehousing in Redshift Serverless
#### 4.0. Prerequisite: Provision Infrastructure
##### 4.0.1. Create namespace (data warehouse instance)


##### 4.0.2. Create workgroup (compute resources)


#### 4.1. Database Setup

#### 4.2. Schema Setup

#### 4.3. Tables Setup

#### 4.4. Data Loading




