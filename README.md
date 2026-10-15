# NYC Taxi ETL Pipeline

End-to-end data engineering pipeline for NYC Yellow Taxi trip data (2.9M+ records),
with two implementations: a local Python/Pandas pipeline that runs from the
command line without notebooks, and a Databricks/PySpark pipeline using a
bronze-silver-gold architecture with Delta Lake and Unity Catalog. Both ingest
public monthly Parquet data, validate and report data quality, and produce
analysis-ready summaries.

## Project structure

```
nyc-taxi-etl-pipeline/
├── run_pipeline.py         # runs all four stages end to end
├── databricks/             # Databricks / PySpark medallion pipeline
│   ├── 00_setup.py
│   ├── 01_bronze_ingest.py
│   ├── 02_silver_clean.py
│   ├── 03_gold_transform.py
│   ├── 04_gold_analysis.py
│   ├── run_all.py
│   └── README.md
├── src/
│   ├── config.py           # shared paths, YEAR_MONTH, file naming
│   ├── schema.py           # Pandera validation contract (runs alongside clean.py)
│   ├── ingest.py           # stage 1: download + initial quality scan
│   ├── clean.py            # stage 2: flag-based cleaning + schema validation
│   ├── transform.py        # stage 3: derived fields + aggregations
│   └── analysis.py         # stage 4: charts + data narrative
├── data/
│   ├── raw/                # downloaded parquet (gitignored)
│   ├── processed/          # raw-checked snapshot (gitignored)
│   ├── cleaned/             # flagged, dtype-fixed dataset (gitignored)
│   ├── transformed/        # summaries, charts, narrative (tracked)
│   └── reports/            # quality_report + schema_validation JSON (tracked)
├── requirements.txt
└── README.md
```

## Setup

```bash
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

## Running the pipeline

Run everything end to end:

```bash
python run_pipeline.py
```

Or override the month:

```bash
python run_pipeline.py 2024-02
```

Or run a single stage while iterating:

```bash
python src/ingest.py
python src/clean.py
python src/transform.py
python src/analysis.py
```

Each stage reads its input from the previous stage's output directory under
`data/`, so they can be re-run independently once the earlier stages have
produced their files.

## Databricks integration

The Databricks implementation complements the local Pandas pipeline, applying
the same dataset and data-quality principles with PySpark, Delta Lake, and Unity
Catalog across bronze, silver, and gold layers. Use it to run the pipeline in a
Databricks workspace and explore its managed tables and quality reports. Start with
[`databricks/README.md`](databricks/README.md) for workspace setup and the
sample-first run instructions.

## Data source

Trip data is published monthly by the NYC Taxi & Limousine Commission and
hosted as public parquet files:

```
https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_YYYY-MM.parquet
```

No API key or AWS account required. Data is published with roughly a
two-month delay, so the most recent month will not be the current month.

## Design decisions

- **Flag, don't drop.** Data quality issues (negative fares, zero-passenger
  trips, zero-distance trips with nonzero fares) are flagged as boolean
  columns in `clean.py` rather than silently removed, so the full dataset
  stays auditable. Exclusions are only applied explicitly, in `transform.py`,
  and are documented in code.
- **Schema validation runs alongside, not instead of, the flags.**
  `schema.py` defines a declarative Pandera contract (types, ranges,
  nullability) and validates the raw-checked data independently of the flag
  logic. It never drops or alters rows — it produces a diagnostic report
  (`data/reports/schema_validation_<YEAR_MONTH>.json`) documenting whether
  the data matches the declared contract. The two systems answer different
  questions: flags say "should this row be excluded from analysis," the
  schema says "does this data match the format we expect."
- **Reproducible quality reporting.** Every ingestion run produces a
  `data/reports/quality_report_<YEAR_MONTH>.json` file capturing null counts
  and flagged-row counts at that point in time.
- **No inflated claims.** The narrative written by `analysis.py` ships with
  bracketed placeholders — findings are only filled in after actually
  reviewing the chart output, not auto-generated from the numbers.

## Data quality findings

See `data/reports/quality_report_<YEAR_MONTH>.json` and
`data/transformed/narrative_<YEAR_MONTH>.md` for the specific findings from
the month analyzed, including the VendorID breakdown for the
missing-optional-fields block identified in `clean.py`.
