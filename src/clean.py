"""
02 - Clean
Loads the raw-checked snapshot from ingest.py, adds quality flag columns
(rather than dropping rows), fixes dtypes, and saves a cleaned dataset that
keeps the flags intact for transparency.

Also runs a Pandera schema validation (see schema.py) ALONGSIDE the flag
logic below -- the two are independent, complementary checks. The flags
drive row exclusion later, in transform.py. The schema report is purely
diagnostic: it never removes or alters rows, it just documents whether the
data matches the declared contract.

Run directly:
    python src/clean.py
"""

import sys
import pandas as pd

import config
import schema


def load(year_month: str = config.YEAR_MONTH) -> pd.DataFrame:
    df = pd.read_parquet(config.raw_checked_path(year_month))
    print(f"Loaded {df.shape[0]:,} rows, {df.shape[1]} columns")
    return df


def inspect_missing_block(df: pd.DataFrame) -> None:
    """Confirms whether the missing-optional-fields block is vendor-specific."""
    missing_block = df[df["passenger_count"].isnull()]
    print(missing_block["VendorID"].value_counts())


def add_flags(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["flag_missing_optional_fields"] = df["passenger_count"].isnull()
    df["flag_negative_fare"] = df["fare_amount"] < 0
    df["flag_zero_passenger"] = df["passenger_count"] == 0
    df["flag_zero_distance_nonzero_fare"] = (
        (df["trip_distance"] == 0) & (df["fare_amount"] > 0)
    )

    df["trip_duration_min"] = (
        df["tpep_dropoff_datetime"] - df["tpep_pickup_datetime"]
    ).dt.total_seconds() / 60
    df["flag_negative_or_zero_duration"] = df["trip_duration_min"] <= 0

    print(df[[c for c in df.columns if c.startswith("flag_")]].sum())
    return df


def fix_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["RatecodeID"] = df["RatecodeID"].astype("Int64")
    df["payment_type"] = df["payment_type"].astype("category")
    df["VendorID"] = df["VendorID"].astype("category")
    df["store_and_fwd_flag"] = df["store_and_fwd_flag"].fillna("N/A")
    return df


def run(year_month: str = config.YEAR_MONTH) -> None:
    df = load(year_month)
    original_row_count = len(df)

    # Schema validation runs on the raw-checked data, before any dtype
    # conversion below -- this checks the data as it actually arrived,
    # independent of the flag columns added next.
    print("\nRunning schema validation...")
    validation_report = schema.validate(df)
    schema.save_validation_report(validation_report, year_month)
    if validation_report["passed"]:
        print("Schema validation: PASSED")
    else:
        print(f"Schema validation: {validation_report['failure_count']} "
              f"violations found (see report -- rows are NOT dropped)")

    inspect_missing_block(df)
    df = add_flags(df)
    df = fix_dtypes(df)

    assert len(df) == original_row_count, "Row count changed - investigate before proceeding"
    print("Row count unchanged. Flags added, no rows dropped.")

    dest = config.cleaned_path(year_month)
    df.to_parquet(dest)
    print(f"Cleaned dataset saved -> {dest}")


if __name__ == "__main__":
    year_month = sys.argv[1] if len(sys.argv) > 1 else config.YEAR_MONTH
    run(year_month)
