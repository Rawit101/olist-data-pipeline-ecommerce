"""
File Utility Functions — Olist Data Pipeline
"""
import os
import csv
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


def validate_csv_file(file_path: str, expected_columns: list = None) -> dict:
    """Validate a CSV file exists, is readable, and has expected columns.

    Returns a dict with validation results.
    """
    result = {
        "file": file_path,
        "exists": False,
        "readable": False,
        "row_count": 0,
        "columns": [],
        "schema_valid": None,
        "errors": [],
    }

    path = Path(file_path)

    # Check existence
    if not path.exists():
        result["errors"].append(f"File not found: {file_path}")
        return result
    result["exists"] = True

    # Check readability and get info
    try:
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            result["columns"] = header
            result["readable"] = True

            # Count rows
            row_count = sum(1 for _ in reader)
            result["row_count"] = row_count

    except Exception as e:
        result["errors"].append(f"Error reading file: {str(e)}")
        return result

    # Validate schema if expected columns provided
    if expected_columns:
        missing_cols = set(expected_columns) - set(result["columns"])
        if missing_cols:
            result["schema_valid"] = False
            result["errors"].append(f"Missing columns: {missing_cols}")
        else:
            result["schema_valid"] = True

    return result


def get_raw_files_summary(raw_dir: str, expected_files: list) -> dict:
    """Get summary of all raw files in the directory.

    Returns dict with found/missing files and total stats.
    """
    found = []
    missing = []

    for filename in expected_files:
        filepath = os.path.join(raw_dir, filename)
        if os.path.exists(filepath):
            size_mb = os.path.getsize(filepath) / (1024 * 1024)
            found.append({"file": filename, "size_mb": round(size_mb, 2)})
        else:
            missing.append(filename)

    return {
        "found": found,
        "missing": missing,
        "total_found": len(found),
        "total_missing": len(missing),
        "all_present": len(missing) == 0,
    }
