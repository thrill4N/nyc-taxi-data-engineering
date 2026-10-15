"""
04 - Analysis
Loads the transformed summaries from transform.py, produces charts, and writes
a data narrative with placeholders to fill in after reviewing the actual chart
output (no auto-generated claims).

Run directly:
    python src/analysis.py
"""

import sys
import pandas as pd
import matplotlib
matplotlib.use("Agg")  # safe default for headless / VS Code terminal runs
import matplotlib.pyplot as plt

import config

DAY_ORDER = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]


def load(year_month: str = config.YEAR_MONTH):
    hourly = pd.read_parquet(config.hourly_summary_path(year_month))
    dow = pd.read_parquet(config.dow_summary_path(year_month))
    routes = pd.read_parquet(config.route_summary_path(year_month))
    return hourly, dow, routes


def plot_hourly_trips(hourly: pd.DataFrame, year_month: str) -> None:
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(hourly["pickup_hour"], hourly["trip_count"], color="#2c7fb8")
    ax.set_xlabel("Hour of Day")
    ax.set_ylabel("Trip Count")
    ax.set_title(f"Yellow Taxi Trips by Hour - {year_month}")
    plt.tight_layout()
    out = config.TRANSFORMED_DIR / "hourly_trips_chart.png"
    plt.savefig(out, dpi=150)
    plt.close(fig)
    print(f"Saved -> {out}")


def plot_hourly_avg_fare(hourly: pd.DataFrame, year_month: str) -> None:
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(hourly["pickup_hour"], hourly["avg_fare"], marker="o", color="#d95f0e")
    ax.set_xlabel("Hour of Day")
    ax.set_ylabel("Average Fare ($)")
    ax.set_title(f"Average Fare by Hour - {year_month}")
    plt.tight_layout()
    out = config.TRANSFORMED_DIR / "hourly_avg_fare_chart.png"
    plt.savefig(out, dpi=150)
    plt.close(fig)
    print(f"Saved -> {out}")


def plot_dow_trips(dow: pd.DataFrame, year_month: str) -> pd.DataFrame:
    dow = dow.copy()
    dow["pickup_day_of_week"] = pd.Categorical(
        dow["pickup_day_of_week"], categories=DAY_ORDER, ordered=True
    )
    dow = dow.sort_values("pickup_day_of_week")

    fig, ax = plt.subplots(figsize=(10, 5))
    ax.bar(dow["pickup_day_of_week"].astype(str), dow["trip_count"], color="#31a354")
    ax.set_xlabel("Day of Week")
    ax.set_ylabel("Trip Count")
    ax.set_title(f"Trips by Day of Week - {year_month}")
    plt.tight_layout()
    out = config.TRANSFORMED_DIR / "dow_trips_chart.png"
    plt.savefig(out, dpi=150)
    plt.close(fig)
    print(f"Saved -> {out}")
    return dow


def write_narrative(hourly: pd.DataFrame, year_month: str = config.YEAR_MONTH) -> None:
    """
    NOTE: fill in the bracketed sections below only after reviewing the charts -
    don't state a pattern unless the chart actually shows it.
    """
    total_trips = int(hourly["trip_count"].sum())

    narrative = f"""# NYC Yellow Taxi Trip Analysis - {year_month}

## Scope
This analysis covers {total_trips:,} trips after excluding records flagged in
the cleaning stage (negative fares, zero/negative duration, zero passenger
count). The full flagged dataset is preserved separately - see
`src/clean.py` for exclusion criteria.

## Key findings
- Peak trip volume occurs at [FILL IN based on hourly_trips_chart.png].
- Average fare [does/does not] track directly with trip volume - see
  hourly_avg_fare_chart.png.
- [Day] sees the highest trip count; [Day] the lowest - see dow_trips_chart.png.
- Top 10 pickup-dropoff pairs account for [X]% of analyzed trips.

## Data quality notes
- {total_trips:,} trips included in this analysis.
- Rows excluded: see `data/reports/quality_report_{year_month}.json` for
  the full breakdown.
- [VendorID missing-field finding from clean.py, once confirmed]
"""

    dest = config.narrative_path(year_month)
    with open(dest, "w") as f:
        f.write(narrative)
    print(f"Narrative saved -> {dest} (fill in bracketed sections after reviewing charts)")


def run(year_month: str = config.YEAR_MONTH) -> None:
    hourly, dow, routes = load(year_month)

    plot_hourly_trips(hourly, year_month)
    plot_hourly_avg_fare(hourly, year_month)
    plot_dow_trips(dow, year_month)

    print("\nTop 10 routes:")
    print(routes.head(10))

    write_narrative(hourly, year_month)


if __name__ == "__main__":
    year_month = sys.argv[1] if len(sys.argv) > 1 else config.YEAR_MONTH
    run(year_month)
