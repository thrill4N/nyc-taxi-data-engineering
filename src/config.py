"""
Shared configuration for the NYC Taxi ETL pipeline.
Change YEAR_MONTH here once and every stage picks it up.
"""

from pathlib import Path

# --- Change this to pick your month (format: "YYYY-MM") ---
YEAR_MONTH = "2024-01"

# --- Project root (this file lives in <root>/src/config.py) ---
ROOT_DIR = Path(__file__).resolve().parent.parent

# --- Data source ---
TLC_BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

def source_url(year_month: str = YEAR_MONTH) -> str:
    return f"{TLC_BASE_URL}/yellow_tripdata_{year_month}.parquet"

# --- Directory layout ---
RAW_DIR = ROOT_DIR / "data" / "raw"
PROCESSED_DIR = ROOT_DIR / "data" / "processed"
CLEANED_DIR = ROOT_DIR / "data" / "cleaned"
TRANSFORMED_DIR = ROOT_DIR / "data" / "transformed"
REPORTS_DIR = ROOT_DIR / "data" / "reports"

for d in (RAW_DIR, PROCESSED_DIR, CLEANED_DIR, TRANSFORMED_DIR, REPORTS_DIR):
    d.mkdir(parents=True, exist_ok=True)

# --- File path helpers ---
def raw_path(year_month: str = YEAR_MONTH) -> Path:
    return RAW_DIR / f"yellow_tripdata_{year_month}.parquet"

def raw_checked_path(year_month: str = YEAR_MONTH) -> Path:
    return PROCESSED_DIR / f"yellow_tripdata_{year_month}_raw_checked.parquet"

def cleaned_path(year_month: str = YEAR_MONTH) -> Path:
    return CLEANED_DIR / f"yellow_tripdata_{year_month}_cleaned.parquet"

def quality_report_path(year_month: str = YEAR_MONTH) -> Path:
    return REPORTS_DIR / f"quality_report_{year_month}.json"

def hourly_summary_path(year_month: str = YEAR_MONTH) -> Path:
    return TRANSFORMED_DIR / f"hourly_summary_{year_month}.parquet"

def dow_summary_path(year_month: str = YEAR_MONTH) -> Path:
    return TRANSFORMED_DIR / f"dow_summary_{year_month}.parquet"

def route_summary_path(year_month: str = YEAR_MONTH) -> Path:
    return TRANSFORMED_DIR / f"route_summary_{year_month}.parquet"

def narrative_path(year_month: str = YEAR_MONTH) -> Path:
    return TRANSFORMED_DIR / f"narrative_{year_month}.md"
