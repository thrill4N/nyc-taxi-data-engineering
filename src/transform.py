"""
03 - Transform
Loads the cleaned dataset from clean.py, derives time-based fields, defines an
explicit analysis-ready subset (excluding flagged rows only for this step, not
permanently), and produces hourly, day-of-week, and route aggregations.

Run directly:
    python src/transform.py
"""

import sys
import pandas as pd

import config


def load(year_month: str = config.YEAR_MONTH) -> pd.DataFrame:
    df = pd.read_parquet(config.cleaned_path(year_month))
    print(f"Loaded {df.shape[0]:,} rows, {df.shape[1]} columns")
    return df


def add_time_fields(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df["pickup_hour"] = df["tpep_pickup_datetime"].dt.hour
    df["pickup_day_of_week"] = df["tpep_pickup_datetime"].dt.day_name()
    df["pickup_date"] = df["tpep_pickup_datetime"].dt.date
    return df


def analysis_subset(df: pd.DataFrame) -> pd.DataFrame:
    """Excludes flagged rows explicitly, only for this analysis step.
    The full flagged dataset (df) is left untouched."""
    subset = df[
        ~df["flag_negative_fare"]
        & ~df["flag_negative_or_zero_duration"]
        & ~df["flag_zero_passenger"]
    ].copy()

    print(
        f"Analysis subset: {len(subset):,} rows "
        f"({len(df) - len(subset):,} excluded, flagged and preserved in df)"
    )
    return subset


def hourly_summary(analysis_df: pd.DataFrame) -> pd.DataFrame:
    return (
        analysis_df.groupby("pickup_hour")
        .agg(
            trip_count=("VendorID", "count"),
            avg_fare=("fare_amount", "mean"),
            avg_distance=("trip_distance", "mean"),
            total_revenue=("total_amount", "sum"),
        )
        .reset_index()
    )


def dow_summary(analysis_df: pd.DataFrame) -> pd.DataFrame:
    summary = (
        analysis_df.groupby("pickup_day_of_week")
        .agg(
            trip_count=("VendorID", "count"),
            avg_fare=("fare_amount", "mean"),
        )
        .reset_index()
    )

    tip_pct = analysis_df.groupby("pickup_day_of_week").apply(
        lambda g: (g["tip_amount"] / g["fare_amount"].replace(0, pd.NA)).mean() * 100
    )
    summary["avg_tip_pct"] = summary["pickup_day_of_week"].map(tip_pct)
    return summary


def route_summary(analysis_df: pd.DataFrame, top_n: int = 20) -> pd.DataFrame:
    return (
        analysis_df.groupby(["PULocationID", "DOLocationID"])
        .size()
        .reset_index(name="trip_count")
        .sort_values("trip_count", ascending=False)
        .head(top_n)
    )


def run(year_month: str = config.YEAR_MONTH) -> None:
    df = load(year_month)
    df = add_time_fields(df)
    subset = analysis_subset(df)

    hourly = hourly_summary(subset)
    dow = dow_summary(subset)
    routes = route_summary(subset)

    hourly.to_parquet(config.hourly_summary_path(year_month))
    dow.to_parquet(config.dow_summary_path(year_month))
    routes.to_parquet(config.route_summary_path(year_month))

    print(f"Transformed summaries saved -> {config.TRANSFORMED_DIR}")


if __name__ == "__main__":
    year_month = sys.argv[1] if len(sys.argv) > 1 else config.YEAR_MONTH
    run(year_month)
