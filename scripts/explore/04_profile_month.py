import argparse
import json
import re
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

OUTPUT_DIR = Path("data/exploration")

CATEGORICAL = ["vendorid", "ratecodeid", "payment_type", "store_and_fwd_flag", "passenger_count"]
NUMERIC = ["trip_distance", "fare_amount", "tip_amount", "tolls_amount", "total_amount"]
COMPONENTS = [
    "fare_amount", "extra", "mta_tax", "tip_amount", "tolls_amount",
    "improvement_surcharge", "congestion_surcharge", "airport_fee", "cbd_congestion_fee",
]
WANTED = set(CATEGORICAL + NUMERIC + COMPONENTS + [
    "tpep_pickup_datetime", "tpep_dropoff_datetime", "pulocationid", "dolocationid",
])


def load(path: Path) -> pd.DataFrame:
    """Read only the needed columns and lowercase their names to ignore case drift."""
    available = pq.ParquetFile(path).schema_arrow.names
    selected = [name for name in available if name.lower() in WANTED]
    df = pd.read_parquet(path, columns=selected)
    df.columns = [c.lower() for c in df.columns]
    return df


def counts(series: pd.Series, limit: int = 15) -> dict:
    return {str(k): int(v) for k, v in series.value_counts(dropna=False).head(limit).items()}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", required=True, help="path to one parquet file")
    parser.add_argument("--zones", help="path to taxi_zone_lookup.csv (enables location checks)")
    args = parser.parse_args()

    path = Path(args.file)
    month = re.search(r"(\d{4}-\d{2})", path.name).group(1)
    year, mon = map(int, month.split("-"))
    df = load(path)

    pickup, dropoff = df["tpep_pickup_datetime"], df["tpep_dropoff_datetime"]
    report: dict = {"file": path.name, "rows": len(df), "columns_loaded": list(df.columns)}

    report["null_pct"] = (df.isna().mean() * 100).round(3).to_dict()
    report["value_counts"] = {c: counts(df[c]) for c in CATEGORICAL if c in df.columns}

    report["numeric_summary"] = {
        c: {
            "min": float(df[c].min()),
            "p01": float(df[c].quantile(0.01)),
            "median": float(df[c].median()),
            "p99": float(df[c].quantile(0.99)),
            "max": float(df[c].max()),
        }
        for c in NUMERIC if c in df.columns
    }

    obs = {
        "pickup_after_dropoff": int((pickup > dropoff).sum()),
        "pickup_equals_dropoff": int((pickup == dropoff).sum()),
        "pickup_outside_file_month": int(((pickup.dt.year * 100 + pickup.dt.month) != year * 100 + mon).sum()),
        "trip_distance_negative": int((df["trip_distance"] < 0).sum()),
        "trip_distance_zero": int((df["trip_distance"] == 0).sum()),
        "fare_amount_negative": int((df["fare_amount"] < 0).sum()),
        "total_amount_negative": int((df["total_amount"] < 0).sum()),
    }

    present = [c for c in COMPONENTS if c in df.columns]
    component_sum = df[present].fillna(0).sum(axis=1)
    obs["total_differs_from_component_sum"] = int(((df["total_amount"] - component_sum).abs() > 0.01).sum())
    obs["components_used_for_sum"] = present

    if "payment_type" in df.columns:
        obs["negative_fare_by_payment_type"] = counts(df.loc[df["fare_amount"] < 0, "payment_type"])

    if args.zones:
        zones = pd.read_csv(args.zones)
        zones.columns = [c.lower() for c in zones.columns]
        zone_ids = set(zones["locationid"])
        obs["pulocationid_not_in_lookup"] = int((~df["pulocationid"].isin(zone_ids)).sum())
        obs["dolocationid_not_in_lookup"] = int((~df["dolocationid"].isin(zone_ids)).sum())

    report["observations"] = obs

    candidate_key = ["vendorid", "tpep_pickup_datetime", "tpep_dropoff_datetime", "pulocationid", "dolocationid"]
    report["duplicates"] = {
        "full_row_duplicates": int(df.duplicated().sum()),
        "candidate_key_duplicates": int(df.duplicated(subset=candidate_key).sum()),
        "candidate_key": candidate_key,
    }

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"profile_{path.stem}.json"
    output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"\nsaved: {output_path}")


if __name__ == "__main__":
    main()