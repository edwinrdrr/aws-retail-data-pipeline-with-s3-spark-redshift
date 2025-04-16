COPY gold.fact_sales
FROM 's3://edwinraws-de-retail-project-bucket/gold/dev/fact_sales/'
IAM_ROLE 'arn:aws:iam::443370716564:role/edwinraws-de-retail-project-redshift-service-load-role'
FORMAT AS PARQUET;