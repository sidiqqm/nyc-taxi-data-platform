import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from conftest import parquet_bytes

from nyc_taxi.ingestion.validators import (
    ValidationError,
    schema_fingerprint,
    validate_csv,
    validate_parquet,
)


def test_valid_parquet_reports_rows_and_columns(tmp_path):
    path = tmp_path / "ok.parquet"
    path.write_bytes(parquet_bytes(rows=25))
    facts = validate_parquet(path)
    assert facts.rows == 25
    assert [c["name"] for c in facts.columns] == ["VendorID", "fare_amount"]


def test_truncated_parquet_is_rejected(tmp_path):
    path = tmp_path / "cut.parquet"
    path.write_bytes(parquet_bytes()[:-50])
    with pytest.raises(ValidationError, match="magic"):
        validate_parquet(path)


def test_garbage_is_rejected(tmp_path):
    path = tmp_path / "junk.parquet"
    path.write_bytes(b"<html>not parquet</html>" * 10)
    with pytest.raises(ValidationError):
        validate_parquet(path)


def test_zero_row_parquet_is_rejected(tmp_path):
    path = tmp_path / "empty.parquet"
    pq.write_table(pa.table({"a": pa.array([], type=pa.int64())}), path)
    with pytest.raises(ValidationError, match="zero rows"):
        validate_parquet(path)


def test_fingerprint_ignores_order_but_not_case_or_type():
    base = [{"name": "a", "type": "int64"}, {"name": "b", "type": "double"}]
    assert schema_fingerprint(base) == schema_fingerprint(list(reversed(base)))
    assert schema_fingerprint(base) != schema_fingerprint(
        [{"name": "A", "type": "int64"}, {"name": "b", "type": "double"}]
    )
    assert schema_fingerprint(base) != schema_fingerprint(
        [{"name": "a", "type": "double"}, {"name": "b", "type": "double"}]
    )


def test_csv_with_bom_and_required_columns(tmp_path):
    path = tmp_path / "zones.csv"
    bom = chr(0xFEFF)  # the byte-order mark some tools put at the start of a UTF-8 file
    path.write_text(f"{bom}LocationID,Borough\n1,EWR\n2,Queens\n", encoding="utf-8")
    facts = validate_csv(path, ("LocationID", "Borough"))
    assert facts.rows == 2


def test_csv_missing_required_column(tmp_path):
    path = tmp_path / "zones.csv"
    path.write_text("LocationID\n1\n", encoding="utf-8")
    with pytest.raises(ValidationError, match="Borough"):
        validate_csv(path, ("LocationID", "Borough"))


def test_csv_without_data_rows(tmp_path):
    path = tmp_path / "zones.csv"
    path.write_text("LocationID,Borough\n", encoding="utf-8")
    with pytest.raises(ValidationError, match="no data rows"):
        validate_csv(path, ("LocationID",))