# Eversys Gold Layer V3

This package combines the best of both previous Gold approaches:
- the old Gold business enrichment logic from the PostgreSQL scripts
- the new SQL Server OLAP star-schema loading approach from the current Python loaders

## What is improved

- Loads cleaned Silver CSV files into an OLAP-friendly Gold schema
- Keeps surrogate keys and dimensions for analytics
- Adds SQL schema hardening and reporting views
- Adds row-level idempotency with `source_row_hash`
- Adds lineage columns: `source_file_path`, `source_row_number`
- Preserves business enrichment for product, cleaning status, rinse status, stop reason, and alert metadata
- Handles sentinel rinse values like `65535` as null
- Creates enriched reporting views for BI use

## Main files

- `gold_utils_v3.py`: shared helpers, schema bootstrap, dimension enrichment, hashing
- `load_production_gold_v3.py`
- `load_cleaning_gold_v3.py`
- `load_rinse_gold_v3.py`
- `load_alerts_gold_v3.py`
- `gold_loading_flow_v3.py`
- `gold_loading_flow_test_batch_v3.py`
- `sql/001_gold_schema_v3.sql`
- `sql/002_gold_seed_dimensions_v3.sql`
- `sql/003_gold_reporting_views_v3.sql`

## Environment variables

Required:
- `AZURE_SQL_SERVER`
- `AZURE_SQL_DATABASE`
- `AZURE_SQL_USERNAME`
- `AZURE_SQL_PASSWORD`

Optional:
- `AZURE_SQL_DRIVER` (default: `ODBC Driver 18 for SQL Server`)
- `EVERSYS_SILVER_ROOT` (default: `C:\RawData\Eversys_Cleaned`)
- `EVERSYS_GOLD_STATE_DIR`
- `EVERSYS_GOLD_LOG_DIR`

## Usage

Run a single loader:

```powershell
python .\load_production_gold_v3.py
```

Run all loaders with Prefect:

```powershell
python .\gold_loading_flow_v3.py
```

Run against the test silver batch:

```powershell
python .\gold_loading_flow_test_batch_v3.py
```

## BI-ready views

- `gold.vw_fact_production_enriched`
- `gold.vw_fact_cleaning_enriched`
- `gold.vw_fact_rinse_enriched`
- `gold.vw_fact_alerts_enriched`

These views expose readable business labels similar to the first Gold analysis while keeping the warehouse normalized underneath.
