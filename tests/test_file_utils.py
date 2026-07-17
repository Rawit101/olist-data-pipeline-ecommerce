from src.file_utils import get_raw_files_summary, validate_csv_file


def test_validate_csv_file_success_with_expected_columns(tmp_path):
    csv_file = tmp_path / "orders.csv"
    csv_file.write_text("order_id,customer_id\n1,c1\n2,c2\n", encoding="utf-8")

    result = validate_csv_file(str(csv_file), expected_columns=["order_id", "customer_id"])

    assert result["exists"] is True
    assert result["readable"] is True
    assert result["row_count"] == 2
    assert result["schema_valid"] is True
    assert result["errors"] == []


def test_validate_csv_file_missing_file():
    result = validate_csv_file("does-not-exist.csv")

    assert result["exists"] is False
    assert result["readable"] is False
    assert result["row_count"] == 0
    assert result["errors"]
    assert "File not found" in result["errors"][0]


def test_get_raw_files_summary_counts_found_and_missing(tmp_path):
    (tmp_path / "olist_orders_dataset.csv").write_text("order_id\n1\n", encoding="utf-8")
    expected_files = ["olist_orders_dataset.csv", "olist_customers_dataset.csv"]

    summary = get_raw_files_summary(str(tmp_path), expected_files)

    assert summary["total_found"] == 1
    assert summary["total_missing"] == 1
    assert summary["all_present"] is False
    assert summary["found"][0]["file"] == "olist_orders_dataset.csv"
    assert summary["missing"] == ["olist_customers_dataset.csv"]