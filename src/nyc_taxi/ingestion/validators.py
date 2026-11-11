"""Structural validation of downloaded files.

Only checks that a file is complete and readable (is it a real Parquet or CSV file, does it
contain rows). Judging the data itself (negative distances, odd dates) is not done here;
that belongs to the processing and data quality stages.
"""

from __future__ import annotations

import csv
import hashlib
import os
from dataclasses import dataclass
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

PARQUET_MAGIC = b"PAR1"


class ValidationError(Exception):
    """The file is not a usable file of the expected format."""


@dataclass(frozen=True)
class FileFacts:
    rows: int
    columns: list[dict[str, str]]  # [{"name": ..., "type": ...}] in file order
    schema_fingerprint: str


def schema_fingerprint(columns: list[dict[str, str]]) -> str:
    """Short hash of (name, type) pairs. Order-insensitive, but case- and type-sensitive."""
    ordered = sorted(columns, key=lambda c: c["name"])
    canonical = "|".join(f"{c['name']}:{c['type']}" for c in ordered)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def validate_parquet(path: Path) -> FileFacts:
    size = path.stat().st_size
    if size < 12:
        raise ValidationError(f"file too small to be Parquet ({size} bytes)")
    with open(path, "rb") as handle:
        head = handle.read(4)
        handle.seek(-4, os.SEEK_END)
        tail = handle.read(4)
    if head != PARQUET_MAGIC or tail != PARQUET_MAGIC:
        raise ValidationError("Parquet magic bytes missing: file is truncated or not Parquet")

    try:
        metadata = pq.read_metadata(path)  # reads only the footer
        schema = metadata.schema.to_arrow_schema()
    except pa.ArrowException as exc:
        raise ValidationError(f"unreadable Parquet footer: {exc}") from exc

    if metadata.num_rows == 0:
        raise ValidationError("Parquet file has zero rows")

    columns = [{"name": field.name, "type": str(field.type)} for field in schema]
    return FileFacts(metadata.num_rows, columns, schema_fingerprint(columns))


def validate_csv(path: Path, required_columns: tuple[str, ...] = ()) -> FileFacts:
    try:
        with open(path, newline="", encoding="utf-8-sig") as handle:  # utf-8-sig drops a BOM
            reader = csv.reader(handle)
            header = next(reader, None)
            if not header:
                raise ValidationError("CSV file is empty")
            missing = [name for name in required_columns if name not in header]
            if missing:
                raise ValidationError(f"CSV missing expected columns {missing}; header: {header}")
            rows = sum(1 for _ in reader)
    except UnicodeDecodeError as exc:
        raise ValidationError(f"CSV is not valid UTF-8: {exc}") from exc

    if rows == 0:
        raise ValidationError("CSV has a header but no data rows")
    columns = [{"name": name, "type": "text"} for name in header]
    return FileFacts(rows, columns, schema_fingerprint(columns))


def validate(path: Path, file_format: str, required_columns: tuple[str, ...] = ()) -> FileFacts:
    if file_format == "parquet":
        return validate_parquet(path)
    if file_format == "csv":
        return validate_csv(path, required_columns)
    raise ValueError(f"unsupported file format: {file_format}")