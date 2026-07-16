# Step-by-Step Execution Guide

This guide walks you through running the Olist Data Pipeline locally.

## Prerequisites
1. **Docker Desktop**: Installed and running (with WSL 2 if on Windows).
2. **Make**: Installed for using the Makefile commands.
3. **Python 3.10+**: Optional, for running local tests.
4. **Olist Dataset**: Download the [Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) from Kaggle.
   Extract the CSV files and place them in the `data/raw/` directory.

## 1. Setup Infrastructure
Start the Docker containers (Airflow, MinIO, PostgreSQL, Redis).
```bash
make up
```

Wait a minute for the Airflow webserver to initialize, then check the logs if needed:
```bash
docker compose logs -f airflow-webserver
```

## 2. Configure Connections
Go to the Airflow UI: `http://localhost:8080` (Default login: `airflow` / `airflow`)

The pipeline uses local connections via Docker, so no external cloud credentials are required. MinIO acts as local S3.

## 3. Run the Pipeline (DAGs)
Enable and trigger the DAGs in the following order:

1. **`01_validate_raw_files`**: Ensures all required CSV files are present in `data/raw/`.
2. **`02_upload_to_minio`**: Uploads CSV files to the `olist-raw` bucket in MinIO.
3. **`03_csv_to_parquet`**: Converts CSV files to Parquet in the `olist-staging` bucket.
4. **`04_build_duckdb_warehouse`**: Loads Parquet files from MinIO into the DuckDB Bronze layer (`raw` schema).
5. **`05_run_dbt_transforms`**: Executes dbt models to build Silver (staging/intermediate) and Gold (marts) layers.
6. **`06_data_quality_checks`**: Runs dbt tests to ensure data integrity.
7. **`07_export_marts`**: Exports the final mart tables to CSV/Parquet in the `exports/` folder.

## 4. Visualization
Connect your BI tool (e.g., Power BI Desktop) to the CSV or Parquet files located in the `exports/` directory to build your dashboard.

## 5. Teardown
To stop all services and remove containers (data in volumes will persist unless destroyed):
```bash
make down
```
