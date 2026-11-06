import argparse
import csv
import time
from pathlib import Path

import requests

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
OUTPUT_DIR = Path("data/exploration")


def month_range(start: str, end: str):
    """Yield 'YYYY-MM' strings from start to end, both inclusive."""
    year, month = map(int, start.split("-"))
    end_year, end_month = map(int, end.split("-"))
    while (year, month) <= (end_year, end_month):
        yield f"{year}-{month:02d}"
        month += 1
        if month == 13:
            year, month = year + 1, 1


def head_file(session: requests.Session, dataset: str, month: str) -> dict:
    url = f"{BASE_URL}/{dataset}_tripdata_{month}.parquet"
    row = {
        "month": month,
        "url": url,
        "status": None,
        "size_mb": None,
        "last_modified": None,
        "etag": None,
        "error": None,
    }
    try:
        response = session.head(url, timeout=30, allow_redirects=True)
        row["status"] = response.status_code
        if response.ok:
            row["size_mb"] = round(int(response.headers.get("Content-Length", 0)) / 1_000_000, 1)
            row["last_modified"] = response.headers.get("Last-Modified")
            row["etag"] = response.headers.get("ETag")
    except requests.RequestException as exc:
        row["error"] = type(exc).__name__
    return row


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", default="yellow")
    parser.add_argument("--start", required=True, help="first month, e.g. 2019-01")
    parser.add_argument("--end", required=True, help="last month, e.g. 2026-09")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / f"availability_{args.dataset}.csv"

    rows = []
    with requests.Session() as session:
        for month in month_range(args.start, args.end):
            row = head_file(session, args.dataset, month)
            rows.append(row)
            print(f"{month}  status={row['status']}  size_mb={row['size_mb']}  error={row['error']}")
            time.sleep(0.2)

    with open(output_path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    available = [r for r in rows if r["status"] == 200]
    total_mb = sum(r["size_mb"] or 0 for r in available)
    print(f"\navailable: {len(available)} of {len(rows)} months, total {total_mb:.1f} MB")
    print(f"saved: {output_path}")


if __name__ == "__main__":
    main()