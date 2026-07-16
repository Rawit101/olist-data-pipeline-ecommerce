"""
Centralized Configuration — Olist Data Pipeline
"""
import os
from pathlib import Path


# ============================================================
# Paths
# ============================================================
PROJECT_ROOT = Path(os.getenv("AIRFLOW_HOME", "/opt/airflow"))
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
WAREHOUSE_DIR = DATA_DIR / "warehouse"
REJECTED_DIR = DATA_DIR / "rejected"
EXPORTS_DIR = PROJECT_ROOT / "exports"
SQL_DIR = PROJECT_ROOT / "sql"

# ============================================================
# MinIO Configuration
# ============================================================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

MINIO_RAW_BUCKET = os.getenv("MINIO_RAW_BUCKET", "olist-raw")
MINIO_STAGING_BUCKET = os.getenv("MINIO_STAGING_BUCKET", "olist-staging")
MINIO_EXPORTS_BUCKET = os.getenv("MINIO_EXPORTS_BUCKET", "olist-exports")

# ============================================================
# DuckDB Configuration
# ============================================================
DUCKDB_PATH = os.getenv("DUCKDB_PATH", str(WAREHOUSE_DIR / "olist.duckdb"))

# ============================================================
# Olist Dataset Files
# ============================================================
OLIST_RAW_FILES = [
    "olist_orders_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_products_dataset.csv",
    "olist_customers_dataset.csv",
    "olist_sellers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "product_category_name_translation.csv",
]

# ============================================================
# Schema Definitions (expected columns per table)
# ============================================================
EXPECTED_SCHEMAS = {
    "olist_orders_dataset": [
        "order_id", "customer_id", "order_status",
        "order_purchase_timestamp", "order_approved_at",
        "order_delivered_carrier_date", "order_delivered_customer_date",
        "order_estimated_delivery_date",
    ],
    "olist_order_items_dataset": [
        "order_id", "order_item_id", "product_id", "seller_id",
        "shipping_limit_date", "price", "freight_value",
    ],
    "olist_order_payments_dataset": [
        "order_id", "payment_sequential", "payment_type",
        "payment_installments", "payment_value",
    ],
    "olist_order_reviews_dataset": [
        "review_id", "order_id", "review_score",
        "review_comment_title", "review_comment_message",
        "review_creation_date", "review_answer_timestamp",
    ],
    "olist_products_dataset": [
        "product_id", "product_category_name",
        "product_name_lenght", "product_description_lenght",
        "product_photos_qty", "product_weight_g",
        "product_length_cm", "product_height_cm", "product_width_cm",
    ],
    "olist_customers_dataset": [
        "customer_id", "customer_unique_id", "customer_zip_code_prefix",
        "customer_city", "customer_state",
    ],
    "olist_sellers_dataset": [
        "seller_id", "seller_zip_code_prefix",
        "seller_city", "seller_state",
    ],
    "olist_geolocation_dataset": [
        "geolocation_zip_code_prefix", "geolocation_lat",
        "geolocation_lng", "geolocation_city", "geolocation_state",
    ],
    "product_category_name_translation": [
        "product_category_name", "product_category_name_english",
    ],
}
