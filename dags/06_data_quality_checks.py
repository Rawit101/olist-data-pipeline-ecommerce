"""
DAG 06: Data Quality Checks
==============================
รัน data quality checks บน DuckDB warehouse
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, "/opt/airflow")

from src.config import DUCKDB_PATH
from src.duckdb_utils import get_duckdb_connection


default_args = {
    "owner": "olist-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def check_row_counts(**kwargs):
    """Verify all raw tables have data (row count > 0)."""
    with get_duckdb_connection(DUCKDB_PATH) as conn:
        tables = conn.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema IN ('raw', 'staging', 'marts')
            ORDER BY table_schema, table_name
        """).fetchall()

        issues = []
        for schema, table in tables:
            count = conn.execute(
                f"SELECT COUNT(*) FROM {schema}.{table}"
            ).fetchone()[0]
            if count == 0:
                issues.append(f"{schema}.{table} has 0 rows!")
                print(f"❌ {schema}.{table}: 0 rows")
            else:
                print(f"✅ {schema}.{table}: {count:,} rows")

        if issues:
            raise ValueError(f"Data quality issues: {issues}")


def check_null_primary_keys(**kwargs):
    """Check for NULL values in primary key columns."""
    checks = {
        "raw.olist_orders_dataset": "order_id",
        "raw.olist_customers_dataset": "customer_id",
        "raw.olist_products_dataset": "product_id",
        "raw.olist_sellers_dataset": "seller_id",
    }

    with get_duckdb_connection(DUCKDB_PATH) as conn:
        issues = []
        for table, pk_col in checks.items():
            try:
                null_count = conn.execute(f"""
                    SELECT COUNT(*) FROM {table}
                    WHERE {pk_col} IS NULL
                """).fetchone()[0]

                if null_count > 0:
                    issues.append(
                        f"{table}.{pk_col} has {null_count} NULL values!"
                    )
                    print(f"❌ {table}.{pk_col}: {null_count} NULLs")
                else:
                    print(f"✅ {table}.{pk_col}: No NULLs")
            except Exception as e:
                print(f"⚠️  Skipped {table}: {e}")

        if issues:
            raise ValueError(f"NULL primary key issues: {issues}")


def check_duplicate_keys(**kwargs):
    """Check for duplicate primary keys."""
    checks = {
        "raw.olist_orders_dataset": "order_id",
        "raw.olist_customers_dataset": "customer_id",
        "raw.olist_products_dataset": "product_id",
        "raw.olist_sellers_dataset": "seller_id",
    }

    with get_duckdb_connection(DUCKDB_PATH) as conn:
        issues = []
        for table, pk_col in checks.items():
            try:
                dup_count = conn.execute(f"""
                    SELECT COUNT(*) FROM (
                        SELECT {pk_col}, COUNT(*) as cnt
                        FROM {table}
                        GROUP BY {pk_col}
                        HAVING COUNT(*) > 1
                    )
                """).fetchone()[0]

                if dup_count > 0:
                    issues.append(
                        f"{table}.{pk_col} has {dup_count} duplicate keys!"
                    )
                    print(f"❌ {table}.{pk_col}: {dup_count} duplicates")
                else:
                    print(f"✅ {table}.{pk_col}: No duplicates")
            except Exception as e:
                print(f"⚠️  Skipped {table}: {e}")

        if issues:
            print(f"\n⚠️  Duplicate key warnings: {issues}")


with DAG(
    dag_id="06_data_quality_checks",
    default_args=default_args,
    description="Run data quality checks on DuckDB warehouse",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["olist", "quality", "validation"],
) as dag:

    task_row_counts = PythonOperator(
        task_id="check_row_counts",
        python_callable=check_row_counts,
    )

    task_null_keys = PythonOperator(
        task_id="check_null_primary_keys",
        python_callable=check_null_primary_keys,
    )

    task_dup_keys = PythonOperator(
        task_id="check_duplicate_keys",
        python_callable=check_duplicate_keys,
    )

    task_row_counts >> [task_null_keys, task_dup_keys]
