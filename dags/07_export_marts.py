"""
DAG 07: Export Marts to CSV
==============================
Export DuckDB mart tables → CSV files สำหรับ Power BI
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, "/opt/airflow")

from src.config import DUCKDB_PATH, EXPORTS_DIR
from src.duckdb_utils import get_duckdb_connection


default_args = {
    "owner": "olist-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Mart tables to export for Power BI
MART_TABLES = [
    "olist.mart_sales_overview",
    "olist.mart_delivery_performance",
    "olist.mart_product_analytics",
    "olist.mart_customer_rfm",
    "olist.mart_seller_ranking",
    "olist.mart_geo_analysis",
]


def export_marts_to_csv(**kwargs):
    """Export all mart tables from DuckDB to CSV files."""
    from pathlib import Path

    Path(EXPORTS_DIR).mkdir(parents=True, exist_ok=True)

    with get_duckdb_connection(DUCKDB_PATH) as conn:
        exported = []
        for full_table_name in MART_TABLES:
            table_name = full_table_name.split(".")[-1]
            csv_path = str(EXPORTS_DIR / f"{table_name}.csv")

            try:
                conn.execute(f"""
                    COPY {full_table_name}
                    TO '{csv_path}'
                    (HEADER, DELIMITER ',')
                """)
                count = conn.execute(
                    f"SELECT COUNT(*) FROM {full_table_name}"
                ).fetchone()[0]
                print(f"✅ Exported: {table_name}.csv ({count:,} rows)")
                exported.append(table_name)
            except Exception as e:
                print(f"❌ Failed: {table_name} — {e}")
                raise

    kwargs["ti"].xcom_push(key="exported_tables", value=exported)
    return exported


def export_marts_to_parquet(**kwargs):
    """Export all mart tables from DuckDB to Parquet files."""
    from pathlib import Path

    Path(EXPORTS_DIR).mkdir(parents=True, exist_ok=True)

    with get_duckdb_connection(DUCKDB_PATH) as conn:
        for full_table_name in MART_TABLES:
            table_name = full_table_name.split(".")[-1]
            parquet_path = str(EXPORTS_DIR / f"{table_name}.parquet")

            try:
                conn.execute(f"""
                    COPY {full_table_name}
                    TO '{parquet_path}'
                    (FORMAT PARQUET)
                """)
                print(f"✅ Exported: {table_name}.parquet")
            except Exception as e:
                print(f"❌ Failed: {table_name} — {e}")
                raise


with DAG(
    dag_id="07_export_marts",
    default_args=default_args,
    description="Export DuckDB mart tables to CSV/Parquet for Power BI",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["olist", "export", "powerbi"],
) as dag:

    task_export_csv = PythonOperator(
        task_id="export_marts_to_csv",
        python_callable=export_marts_to_csv,
    )

    task_export_parquet = PythonOperator(
        task_id="export_marts_to_parquet",
        python_callable=export_marts_to_parquet,
    )

    task_export_csv >> task_export_parquet
