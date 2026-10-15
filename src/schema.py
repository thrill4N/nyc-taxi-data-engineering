"""
Pandera schema for the NYC Yellow Taxi dataset.

This runs ALONGSIDE the flag-based cleaning logic in clean.py, not instead of
it. The flags in clean.py drive which rows get excluded in transform.py; this
schema is a separate, formal validation layer that checks the dataset against
a declared contract (types, ranges, nullability) and produces a structured
report. Think of it as documentation-as-code: anyone reading this file can see
exactly what "valid" means for this dataset, without reading through
clean.py's logic line by line.

Validation is run in "lazy" mode (see validate() below), meaning it collects
every violation into one report rather than stopping at the first failure.
This matches the project's overall philosophy: report and preserve, don't
silently drop.
"""

import json
import pandera.pandas as pa
from pandera.pandas import Column, Check
import pandas as pd

import config


# --------------------------------------------------------------------------
# Schema definition
# --------------------------------------------------------------------------
# nullable=True is used for columns where we already know a block of rows
# (~140,162) legitimately lacks these optional fields — see clean.py's
# inspect_missing_block(). This is NOT the same as saying "any value is fine";
# it just means "null is an accepted state for this column," which Pandera
# will still enforce (a non-null, wrong-type value would still fail).

taxi_schema = pa.DataFrameSchema(
    {
        "VendorID": Column(
            "int32",
            nullable=False,
            description="Provider that supplied the trip record.",
        ),
        "tpep_pickup_datetime": Column(
            "datetime64[us]",
            nullable=False,
            description="Trip start timestamp.",
        ),
        "tpep_dropoff_datetime": Column(
            "datetime64[us]",
            nullable=False,
            description="Trip end timestamp.",
        ),
        "passenger_count": Column(
            float,
            Check.ge(0, ignore_na=True),
            nullable=True,
            description="Number of passengers. Zero is flagged separately as "
                         "a known data quirk, not rejected here.",
        ),
        "trip_distance": Column(
            float,
            Check.ge(0),
            nullable=False,
            description="Trip distance in miles. Must be non-negative.",
        ),
        "fare_amount": Column(
            float,
            nullable=False,
            # Negative fares are a known, expected pattern (refunds/corrections)
            # so we don't fail on them here -- clean.py flags them separately.
            # We only fail on implausibly extreme values, which would indicate
            # a genuine data corruption issue rather than a refund.
            checks=Check.in_range(-1000, 5000),
            description="Metered fare amount in USD.",
        ),
        "total_amount": Column(
            float,
            nullable=False,
            description="Total charged amount in USD, including fare, tips, "
                         "tolls, and surcharges.",
        ),
        "PULocationID": Column(
            "int32",
            Check.ge(1),
            nullable=False,
            description="Pickup taxi zone ID.",
        ),
        "DOLocationID": Column(
            "int32",
            Check.ge(1),
            nullable=False,
            description="Dropoff taxi zone ID.",
        ),
    },
    strict=False,  # allow extra columns (e.g. flag_* columns added by clean.py)
    coerce=False,
)


# --------------------------------------------------------------------------
# Validation runner
# --------------------------------------------------------------------------
def validate(df: pd.DataFrame) -> dict:
    """
    Runs the schema in lazy mode: collects ALL violations instead of stopping
    at the first one. Returns a JSON-serializable report dict rather than
    raising, so this can run inside the pipeline without halting it -- in
    keeping with the project's "flag and report, don't silently fail" approach.
    """
    try:
        taxi_schema.validate(df, lazy=True)
        return {
            "passed": True,
            "failure_count": 0,
            "failures": [],
        }
    except pa.errors.SchemaErrors as err:
        failure_cases = err.failure_cases

        # Summarize by column + check, rather than dumping every single failing
        # row -- a recruiter or teammate reading this report wants "what kinds
        # of violations happened," not a 50,000-row dump.
        summary = (
            failure_cases.groupby(["column", "check"])
            .size()
            .reset_index(name="violation_count")
            .to_dict(orient="records")
        )

        return {
            "passed": False,
            "failure_count": int(len(failure_cases)),
            "failures": summary,
        }


def save_validation_report(report: dict, year_month: str = config.YEAR_MONTH) -> None:
    dest = config.REPORTS_DIR / f"schema_validation_{year_month}.json"
    with open(dest, "w") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"Schema validation report saved -> {dest}")
