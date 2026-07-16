"""
DAG 04: Build DuckDB Warehouse
================================
โหลดข้อมูล Parquet จาก MinIO → สร้าง raw tables ใน DuckDB
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, "/opt/airflow")

from src.config import OLIST_RAW_FILES, MINIO_STAGING_BUCKET, DUCKDB_PATH
from src.duckdb_utils import get_duckdb_connection, configure_minio_access


default_args = {
    "owner": "olist-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


def load_parquet_to_duckdb(**kwargs):
    """Load all Parquet files from MinIO into DuckDB raw schema."""
    from pathlib import Path

    # Ensure warehouse directory exists
    Path(DUCKDB_PATH).parent.mkdir(parents=True, exist_ok=True)

    with get_duckdb_connection(DUCKDB_PATH) as conn:
        configure_minio_access(conn)
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")

        loaded = []
        for csv_filename in OLIST_RAW_FILES:
            table_name = csv_filename.replace(".csv", "")
            parquet_filename = csv_filename.replace(".csv", ".parquet")
            s3_path = f"s3://{MINIO_STAGING_BUCKET}/parquet/{parquet_filename}"

            try:
                conn.execute(f"""
                    CREATE OR REPLACE TABLE raw.{table_name} AS
                    SELECT * FROM read_parquet('{s3_path}')
                """)
                count = conn.execute(
                    f"SELECT COUNT(*) FROM raw.{table_name}"
                ).fetchone()[0]
                print(f"✅ raw.{table_name}: {count:,} rows")
                loaded.append({"table": table_name, "rows": count})
            except Exception as e:
                print(f"❌ Failed: {table_name} — {e}")
                raise

        print(f"\n📊 Loaded {len(loaded)} tables into DuckDB")

    kwargs["ti"].xcom_push(key="loaded_tables", value=loaded)
    return loaded


def verify_warehouse(**kwargs):
    """Verify all tables exist and have data."""
    from src.duckdb_utils import get_table_stats

    with get_duckdb_connection(DUCKDB_PATH) as conn:
        stats = get_table_stats(conn, schema="raw")

    print("📊 DuckDB Warehouse Stats:")
    total_rows = 0
    for s in stats:
        print(f"   {s['table']}: {s['rows']:,} rows")
        total_rows += s["rows"]

    print(f"\n   Total: {total_rows:,} rows across {len(stats)} tables")

    if len(stats) < len(OLIST_RAW_FILES):
        raise ValueError(
            f"Expected {len(OLIST_RAW_FILES)} tables, found {len(stats)}"
        )


with DAG(
    dag_id="04_build_duckdb_warehouse",
    default_args=default_args,
    description="Load Parquet data from MinIO into DuckDB warehouse",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["olist", "warehouse", "duckdb"],
) as dag:

    task_load = PythonOperator(
        task_id="load_parquet_to_duckdb",
        python_callable=load_parquet_to_duckdb,
    )

    task_verify = PythonOperator(
        task_id="verify_warehouse",
        python_callable=verify_warehouse,
    )

    task_load >> task_verify
