import argparse
import re
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

OUTPUT_DIR = Path("data/exploration")


def read_schema(path: Path) -> tuple[dict, int]:
    parquet_file = pq.ParquetFile(path)
    types = {field.name: str(field.type) for field in parquet_file.schema_arrow}
    return types, parquet_file.metadata.num_rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", default="data/raw/yellow")
    parser.add_argument("--dataset", default="yellow")
    args = parser.parse_args()

    files = sorted(Path(args.input_dir).glob(f"{args.dataset}_tripdata_*.parquet"))
    if not files:
        raise SystemExit(f"no files found in {args.input_dir}")

    schemas, row_counts = {}, {}
    for path in files:
        month = re.search(r"(\d{4}-\d{2})", path.name).group(1)
        schemas[month], row_counts[month] = read_schema(path)

    ordered_columns = list(dict.fromkeys(col for s in schemas.values() for col in s))
    matrix = pd.DataFrame(schemas).reindex(ordered_columns).fillna("MISSING")
    matrix.index.name = "column"

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"schema_matrix_{args.dataset}.csv"
    matrix.to_csv(output_path)

    print("== column x month (type, or MISSING) ==")
    print(matrix.to_string())

    print("\n== row count per file (from Parquet footer) ==")
    for month, rows in row_counts.items():
        print(f"{month}: {rows:,}")

    changed = matrix[matrix.nunique(axis=1) > 1]
    print("\n== columns whose presence or type differs between months ==")
    print(changed.to_string() if not changed.empty else "none")

    variants = {}
    for column in ordered_columns:
        variants.setdefault(column.lower(), []).append(column)
    spelling = {k: v for k, v in variants.items() if len(v) > 1}
    print("\n== names that differ only by letter case ==")
    print(spelling if spelling else "none")

    print(f"\nsaved: {output_path}")


if __name__ == "__main__":
    main()