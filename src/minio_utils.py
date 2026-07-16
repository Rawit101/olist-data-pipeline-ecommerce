"""
MinIO Utility Functions — Olist Data Pipeline
"""
import os
import logging
from pathlib import Path
from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)


def get_minio_client() -> Minio:
    """Create and return a MinIO client instance."""
    return Minio(
        endpoint=os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
    )


def ensure_bucket_exists(client: Minio, bucket_name: str) -> None:
    """Create a bucket if it doesn't already exist."""
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
        logger.info(f"Created bucket: {bucket_name}")
    else:
        logger.info(f"Bucket already exists: {bucket_name}")


def upload_file(
    client: Minio,
    bucket_name: str,
    object_name: str,
    file_path: str,
    content_type: str = "application/octet-stream",
) -> None:
    """Upload a file to MinIO bucket."""
    client.fput_object(
        bucket_name=bucket_name,
        object_name=object_name,
        file_path=file_path,
        content_type=content_type,
    )
    logger.info(f"Uploaded: {file_path} → {bucket_name}/{object_name}")


def list_objects(client: Minio, bucket_name: str, prefix: str = "") -> list:
    """List all objects in a MinIO bucket with optional prefix filter."""
    objects = []
    try:
        for obj in client.list_objects(bucket_name, prefix=prefix, recursive=True):
            objects.append({
                "name": obj.object_name,
                "size": obj.size,
                "last_modified": obj.last_modified,
            })
    except S3Error as e:
        logger.error(f"Error listing objects in {bucket_name}: {e}")
        raise
    return objects


def download_file(
    client: Minio,
    bucket_name: str,
    object_name: str,
    file_path: str,
) -> None:
    """Download a file from MinIO bucket."""
    client.fget_object(
        bucket_name=bucket_name,
        object_name=object_name,
        file_path=file_path,
    )
    logger.info(f"Downloaded: {bucket_name}/{object_name} → {file_path}")


def verify_objects_exist(
    client: Minio, bucket_name: str, expected_objects: list
) -> dict:
    """Verify that expected objects exist in the bucket.

    Returns dict with 'found', 'missing', and 'all_present' keys.
    """
    existing = {obj["name"] for obj in list_objects(client, bucket_name)}
    found = [name for name in expected_objects if name in existing]
    missing = [name for name in expected_objects if name not in existing]

    return {
        "found": found,
        "missing": missing,
        "all_present": len(missing) == 0,
    }
