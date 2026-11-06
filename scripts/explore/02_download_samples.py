import argparse
from pathlib import Path

import requests

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
ZONE_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
RAW_DIR = Path("data/raw")


def download(session: requests.Session, url: str, destination: Path) -> None:
    if destination.exists():
        print(f"skip (already exists): {destination}")
        return

    destination.parent.mkdir(parents=True, exist_ok=True)
    partial = destination.with_name(destination.name + ".part")
    try:
        with session.get(url, stream=True, timeout=60) as response:
            response.raise_for_status()
            with open(partial, "wb") as handle:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    handle.write(chunk)
        partial.rename(destination)
    finally:
        partial.unlink(missing_ok=True)

    size_mb = destination.stat().st_size / 1_000_000
    print(f"downloaded: {destination} ({size_mb:.1f} MB)")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", default="yellow")
    parser.add_argument("--months", nargs="+", required=True, help="e.g. 2019-01 2024-01 2025-01")
    parser.add_argument("--zones", action="store_true", help="also download taxi_zone_lookup.csv")
    args = parser.parse_args()

    with requests.Session() as session:
        if args.zones:
            download(session, ZONE_URL, RAW_DIR / "reference" / "taxi_zone_lookup.csv")

        for month in args.months:
            file_name = f"{args.dataset}_tripdata_{month}.parquet"
            try:
                download(session, f"{BASE_URL}/{file_name}", RAW_DIR / args.dataset / file_name)
            except requests.RequestException as exc:
                print(f"FAILED: {file_name} ({type(exc).__name__}: {exc})")


if __name__ == "__main__":
    main()