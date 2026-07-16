"""
DAG 03: Convert CSV to Parquet
===============================
ดาวน์โหลด CSV จาก MinIO → แปลงเป็น Parquet ด้วย PyArrow → อัปโหลดกลับ MinIO
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, "/opt/airflow")

from src.config import (
    OLIST_RAW_FILES, PROCESSED_DIR,
    MINIO_RAW_BUCKET, MINIO_STAGING_BUCKET,
)
from src.minio_utils import get_minio_client, ensure_bucket_exists, upload_file


default_args = {
    "owner": "olist-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


def convert_csv_to_parquet(**kwargs):
    """Download CSVs from MinIO, convert to Parquet, upload back."""
    import pandas as pd
    from pathlib import Path

    client = get_minio_client()
    ensure_bucket_exists(client, MINIO_STAGING_BUCKET)

    # Ensure processed directory exists
    Path(PROCESSED_DIR).mkdir(parents=True, exist_ok=True)

    converted = []
    for csv_filename in OLIST_RAW_FILES:
        parquet_filename = csv_filename.replace(".csv", ".parquet")
        csv_local = str(PROCESSED_DIR / csv_filename)
        parquet_local = str(PROCESSED_DIR / parquet_filename)

        try:
            # Download CSV from MinIO
            client.fget_object(
                bucket_name=MINIO_RAW_BUCKET,
                object_name=f"csv/{csv_filename}",
                file_path=csv_local,
            )

            # Convert to Parquet
            df = pd.read_csv(csv_local)
            df.to_parquet(parquet_local, engine="pyarrow", index=False)

            # Upload Parquet to MinIO staging
            upload_file(
                client=client,
                bucket_name=MINIO_STAGING_BUCKET,
                object_name=f"parquet/{parquet_filename}",
                file_path=parquet_local,
                content_type="application/octet-stream",
            )

            print(
                f"✅ {csv_filename} → {parquet_filename} "
                f"({len(df):,} rows)"
            )
            converted.append(parquet_filename)

        except Exception as e:
            print(f"❌ Failed: {csv_filename} — {e}")
            raise

    kwargs["ti"].xcom_push(key="converted_files", value=converted)
    return converted


with DAG(
    dag_id="03_csv_to_parquet",
    default_args=default_args,
    description="Convert Olist CSVs to Parquet format in MinIO staging",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["olist", "ingestion", "parquet"],
) as dag:

    task_convert = PythonOperator(
        task_id="convert_csv_to_parquet",
        python_callable=convert_csv_to_parquet,
    )
