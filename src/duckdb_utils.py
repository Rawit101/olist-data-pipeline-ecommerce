"""
DuckDB Utility Functions — Olist Data Pipeline
"""
import os
import logging
import duckdb
from contextlib import contextmanager

logger = logging.getLogger(__name__)

DUCKDB_PATH = os.getenv("DUCKDB_PATH", "/opt/airflow/data/warehouse/olist.duckdb")


@contextmanager
def get_duckdb_connection(db_path: str = None):
    """Context manager for DuckDB connections."""
    path = db_path or DUCKDB_PATH
    conn = duckdb.connect(path)
    try:
        # Install and load httpfs for MinIO/S3 access
        conn.execute("INSTALL httpfs; LOAD httpfs;")
        yield conn
    finally:
        conn.close()


def configure_minio_access(conn: duckdb.DuckDBPyConnection) -> None:
    """Configure DuckDB to read from MinIO using S3 protocol."""
    endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

    conn.execute(f"""
        SET s3_region = 'us-east-1';
        SET s3_endpoint = '{endpoint}';
        SET s3_access_key_id = '{access_key}';
        SET s3_secret_access_key = '{secret_key}';
        SET s3_use_ssl = false;
        SET s3_url_style = 'path';
    """)
    logger.info("Configured DuckDB for MinIO S3 access")


def create_table_from_parquet(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    parquet_path: str,
    schema: str = "raw",
) -> None:
    """Create a DuckDB table from a Parquet file (local or S3)."""
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    conn.execute(f"""
        CREATE OR REPLACE TABLE {schema}.{table_name} AS
        SELECT * FROM read_parquet('{parquet_path}')
    """)
    count = conn.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}").fetchone()[0]
    logger.info(f"Created table {schema}.{table_name} with {count:,} rows")


def create_table_from_csv(
    conn: duckdb.DuckDBPyConnection,
    table_name: str,
    csv_path: str,
    schema: str = "raw",
) -> None:
    """Create a DuckDB table from a CSV file."""
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    conn.execute(f"""
        CREATE OR REPLACE TABLE {schema}.{table_name} AS
        SELECT * FROM read_csv_auto('{csv_path}', header=true)
    """)
    count = conn.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}").fetchone()[0]
    logger.info(f"Created table {schema}.{table_name} with {count:,} rows")


def execute_sql_file(conn: duckdb.DuckDBPyConnection, sql_file_path: str) -> None:
    """Execute a SQL file against DuckDB."""
    with open(sql_file_path, "r") as f:
        sql = f.read()
    conn.execute(sql)
    logger.info(f"Executed SQL file: {sql_file_path}")


def get_table_stats(conn: duckdb.DuckDBPyConnection, schema: str = "raw") -> list:
    """Get row counts for all tables in a schema."""
    tables = conn.execute(f"""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = '{schema}'
        ORDER BY table_name
    """).fetchall()

    stats = []
    for (table_name,) in tables:
        count = conn.execute(
            f"SELECT COUNT(*) FROM {schema}.{table_name}"
        ).fetchone()[0]
        stats.append({"table": f"{schema}.{table_name}", "rows": count})
    return stats
