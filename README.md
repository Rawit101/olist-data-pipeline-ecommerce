# Olist E-Commerce End-to-End Data Pipeline

## 🎯 Overview
An end-to-end Data Engineering pipeline for the **Brazilian E-Commerce Public Dataset by Olist**. 
It ingests raw CSVs into a local data lake (MinIO), processes them into an analytical warehouse (DuckDB), and models them using dbt for BI and dashboards.

## 🏗️ Architecture

```text
Olist Raw CSV Files (Kaggle Download)
        ↓
Airflow DAG: Validate & Ingest Raw CSVs
        ↓
Upload to MinIO (Local Object Storage)
        ↓
Airflow DAG: Convert CSV to Parquet
        ↓
DuckDB Local Warehouse (Bronze Layer)
        ↓
dbt Transformations (Silver → Gold)
        ↓
Airflow DAG: Export Marts to CSV/Parquet for Dashboards
```

## 🛠️ Tech Stack

- **Orchestration**: Apache Airflow
- **Containerization**: Docker Compose
- **Object Storage**: MinIO
- **Data Warehouse**: DuckDB
- **Transformation**: dbt (dbt-core & dbt-duckdb)
- **Data Quality**: dbt tests
- **Visualization**: Power BI (via Parquet/CSV exports)

## 🚀 Getting Started

Please see the [Step-by-Step Guide](docs/STEP_BY_STEP_GUIDE.md) for detailed instructions on running the pipeline locally.

## 📁 Repository Structure

- `dags/`: Airflow DAGs for orchestrating the pipeline
- `dbt_olist/`: dbt project with staging, intermediate, and mart models
- `src/`: Python utilities for MinIO, DuckDB, and generic pipeline tasks
- `docs/`: Comprehensive project documentation
- `config/`: Configuration examples (env files)
- `docker-compose.yml`: Local infrastructure setup

## 📊 Outputs

The pipeline produces the following business-ready marts:
- `mart_sales_overview`: Monthly revenue and order trends
- `mart_delivery_performance`: Delivery SLA analysis
- `mart_product_analytics`: Top selling categories and products
- `mart_customer_rfm`: RFM segmentation of customers
- `mart_seller_ranking`: Seller performance metrics
- `mart_geo_analysis`: Geographic distribution of sales
