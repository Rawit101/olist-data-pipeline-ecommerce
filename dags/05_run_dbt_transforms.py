"""
DAG 05: Run dbt Transformations
=================================
รัน dbt models (staging → intermediate → marts) ใน DuckDB
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "olist-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DBT_PROJECT_DIR = "/opt/airflow/dbt_olist"

with DAG(
    dag_id="05_run_dbt_transforms",
    default_args=default_args,
    description="Run dbt staging, intermediate, and mart transformations",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["olist", "transformation", "dbt"],
) as dag:

    task_dbt_debug = BashOperator(
        task_id="dbt_debug",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt debug",
    )

    task_dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt deps",
    )

    task_dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select staging",
    )

    task_dbt_run_intermediate = BashOperator(
        task_id="dbt_run_intermediate",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select intermediate",
    )

    task_dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --select marts",
    )

    task_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {DBT_PROJECT_DIR} && dbt test",
    )

    (
        task_dbt_debug
        >> task_dbt_deps
        >> task_dbt_run_staging
        >> task_dbt_run_intermediate
        >> task_dbt_run_marts
        >> task_dbt_test
    )
