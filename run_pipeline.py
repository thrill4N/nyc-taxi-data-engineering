"""
Runs the full NYC Taxi ETL pipeline end to end: ingest -> clean -> transform -> analysis.

Usage:
    python run_pipeline.py            # uses YEAR_MONTH from src/config.py
    python run_pipeline.py 2024-02    # overrides the month
"""

import sys
from pathlib import Path

# Use an absolute path so this works regardless of the current working
# directory the script is launched from (fixes Pylance "could not be
# resolved" warnings too, since the path is now unambiguous).
SRC_DIR = Path(__file__).resolve().parent / "src"
sys.path.insert(0, str(SRC_DIR))

import config
import ingest
import clean
import transform
import analysis


def main():
    year_month = sys.argv[1] if len(sys.argv) > 1 else config.YEAR_MONTH
    print(f"=== Running pipeline for {year_month} ===\n")

    print("--- Stage 1: Ingest ---")
    ingest.run(year_month)

    print("\n--- Stage 2: Clean ---")
    clean.run(year_month)

    print("\n--- Stage 3: Transform ---")
    transform.run(year_month)

    print("\n--- Stage 4: Analysis ---")
    analysis.run(year_month)

    print("\n=== Pipeline complete ===")


if __name__ == "__main__":
    main()
