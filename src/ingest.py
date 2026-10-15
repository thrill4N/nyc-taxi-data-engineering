"""
01 - Ingest
Downloads a month of NYC Yellow Taxi trip data (parquet) from the public TLC
CloudFront source, runs an initial schema/quality scan, and saves a raw-checked
snapshot plus a JSON quality report for reproducibility.

Run directly:
    python src/ingest.py
"""

import json
import sys
import requests
import pandas as pd

import config


def download(year_month: str = config.YEAR_MONTH) -> None:
    url = config.source_url(year_month)
    dest = config.raw_path(year_month)

    print(f"Downloading {url} ...")
    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()

    with open(dest, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    print(f"Downloaded {dest.stat().st_size / 1e6:.1f} MB -> {dest}")


def load_and_inspect(year_month: str = config.YEAR_MONTH) -> pd.DataFrame:
    df = pd.read_parquet(config.raw_path(year_month))
    print(df.shape)
    print(df.dtypes)
    print(df.isnull().sum())
    return df


def quality_scan(df: pd.DataFrame) -> dict:
    negative_fares = int((df["fare_amount"] < 0).sum())
    zero_passengers = int((df["passenger_count"] == 0).sum())
    zero_distance_nonzero_fare = int(
        ((df["trip_distance"] == 0) & (df["fare_amount"] > 0)).sum()
    )

    print("Negative fares:", negative_fares)
    print("Zero passengers:", zero_passengers)
    print("Zero distance, nonzero fare:", zero_distance_nonzero_fare)

    return {
        "negative_fares": negative_fares,
        "zero_passengers": zero_passengers,
        "zero_distance_nonzero_fare": zero_distance_nonzero_fare,
    }


def save_quality_report(df: pd.DataFrame, scan: dict, year_month: str = config.YEAR_MONTH) -> None:
    report = {
        "year_month": year_month,
        "row_count": len(df),
        "nulls": df.isnull().sum().to_dict(),
        **scan,
    }
    with open(config.quality_report_path(year_month), "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"Quality report saved -> {config.quality_report_path(year_month)}")


def save_raw_checked(df: pd.DataFrame, year_month: str = config.YEAR_MONTH) -> None:
    dest = config.raw_checked_path(year_month)
    df.to_parquet(dest)
    print(f"Raw-checked snapshot saved -> {dest}")


def run(year_month: str = config.YEAR_MONTH) -> None:
    if not config.raw_path(year_month).exists():
        download(year_month)
    else:
        print(f"{config.raw_path(year_month)} already exists, skipping download.")

    df = load_and_inspect(year_month)
    scan = quality_scan(df)
    save_quality_report(df, scan, year_month)
    save_raw_checked(df, year_month)


if __name__ == "__main__":
    year_month = sys.argv[1] if len(sys.argv) > 1 else config.YEAR_MONTH
    run(year_month)
