"""
DAG 02: Upload Raw Files to MinIO
==================================
อัปโหลดไฟล์ CSV จาก data/raw/ ไปยัง MinIO bucket "olist-raw"
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

import sys
sys.path.insert(0, "/opt/airflow")

from src.config import RAW_DIR, OLIST_RAW_FILES, MINIO_RAW_BUCKET
from src.minio_utils import get_minio_client, ensure_bucket_exists, upload_file


default_args = {
    "owner": "olist-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
}


def upload_all_csv_to_minio(**kwargs):
    """Upload all raw CSV files to MinIO olist-raw bucket."""
    client = get_minio_client()
    ensure_bucket_exists(client, MINIO_RAW_BUCKET)

    uploaded = []
    failed = []

    for filename in OLIST_RAW_FILES:
        filepath = str(RAW_DIR / filename)
        try:
            upload_file(
                client=client,
                bucket_name=MINIO_RAW_BUCKET,
                object_name=f"csv/{filename}",
                file_path=filepath,
                content_type="text/csv",
            )
            uploaded.append(filename)
            print(f"✅ Uploaded: {filename}")
        except Exception as e:
            failed.append({"file": filename, "error": str(e)})
            print(f"❌ Failed: {filename} — {e}")

    print(f"\n📊 Summary: {len(uploaded)} uploaded, {len(failed)} failed")

    if failed:
        raise RuntimeError(f"Failed to upload {len(failed)} files: {failed}")

    kwargs["ti"].xcom_push(key="uploaded_files", value=uploaded)
    return uploaded


def verify_minio_objects(**kwargs):
    """Verify all expected objects exist in MinIO after upload."""
    from src.minio_utils import verify_objects_exist

    client = get_minio_client()
    expected = [f"csv/{f}" for f in OLIST_RAW_FILES]
    result = verify_objects_exist(client, MINIO_RAW_BUCKET, expected)

    print(f"✅ Found: {len(result['found'])} objects")
    for obj in result["found"]:
        print(f"   📦 {obj}")

    if result["missing"]:
        print(f"❌ Missing: {len(result['missing'])} objects")
        for obj in result["missing"]:
            print(f"   ⚠️  {obj}")
        raise FileNotFoundError(f"Missing objects: {result['missing']}")

    return result


with DAG(
    dag_id="02_upload_to_minio",
    default_args=default_args,
    description="Upload Olist raw CSV files to MinIO object storage",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["olist", "ingestion", "minio"],
) as dag:

    task_upload = PythonOperator(
        task_id="upload_csv_to_minio",
        python_callable=upload_all_csv_to_minio,
    )

    task_verify = PythonOperator(
        task_id="verify_minio_objects",
        python_callable=verify_minio_objects,
    )

    task_upload >> task_verify
