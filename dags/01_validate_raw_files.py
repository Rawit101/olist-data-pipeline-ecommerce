"""
DAG 01: Validate Raw Files
===========================
ตรวจสอบว่าไฟล์ CSV ของ Olist dataset ครบทั้ง 9 ไฟล์
พร้อมตรวจ schema (column names) ว่าถูกต้อง
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, "/opt/airflow")

from src.config import RAW_DIR, OLIST_RAW_FILES, EXPECTED_SCHEMAS
from src.file_utils import validate_csv_file, get_raw_files_summary


default_args = {
    "owner": "olist-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def check_files_exist(**kwargs):
    """Check all expected raw CSV files exist in data/raw/."""
    summary = get_raw_files_summary(str(RAW_DIR), OLIST_RAW_FILES)

    print(f"✅ Found: {summary['total_found']} files")
    for f in summary["found"]:
        print(f"   📄 {f['file']} ({f['size_mb']} MB)")

    if summary["missing"]:
        print(f"❌ Missing: {summary['total_missing']} files")
        for m in summary["missing"]:
            print(f"   ⚠️  {m}")
        raise FileNotFoundError(
            f"Missing {summary['total_missing']} files: {summary['missing']}"
        )

    kwargs["ti"].xcom_push(key="raw_files_summary", value=summary)
    return summary


def validate_schemas(**kwargs):
    """Validate column schemas for each CSV file."""
    results = {}
    all_valid = True

    for filename in OLIST_RAW_FILES:
        filepath = str(RAW_DIR / filename)
        table_name = filename.replace(".csv", "")
        expected_cols = EXPECTED_SCHEMAS.get(table_name, [])

        validation = validate_csv_file(filepath, expected_cols)
        results[filename] = validation

        if validation.get("schema_valid") is False:
            all_valid = False
            print(f"❌ Schema invalid: {filename}")
            for err in validation["errors"]:
                print(f"   ⚠️  {err}")
        else:
            print(
                f"✅ {filename}: {validation['row_count']:,} rows, "
                f"{len(validation['columns'])} columns"
            )

    if not all_valid:
        raise ValueError("Some files have invalid schemas!")

    kwargs["ti"].xcom_push(key="validation_results", value=results)
    return results


with DAG(
    dag_id="01_validate_raw_files",
    default_args=default_args,
    description="Validate Olist raw CSV files (existence + schema)",
    schedule_interval=None,  # Manual trigger
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["olist", "ingestion", "validation"],
) as dag:

    task_check_files = PythonOperator(
        task_id="check_files_exist",
        python_callable=check_files_exist,
    )

    task_validate_schemas = PythonOperator(
        task_id="validate_schemas",
        python_callable=validate_schemas,
    )

    task_check_files >> task_validate_schemas
