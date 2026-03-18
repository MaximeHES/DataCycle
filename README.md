🚧 Data Cycle Project – Development Branch
📌 Overview

This branch (dev) is the active development environment for the Data Cycle project.

It contains all experimental features, ongoing improvements, and testing pipelines before promotion to production (main branch).

The goal of this project is to build a complete end-to-end data pipeline, from raw ingestion to analytics and visualization, following a Medallion Architecture (Bronze → Silver → Gold) .

🏗️ Architecture

The project follows a simplified data engineering architecture:

SOURCE (Eversys Share)
        ↓
BRONZE (Raw - incremental ingestion)
        ↓
SILVER (Cleaned & structured data)
        ↓
GOLD (Analytics / BI - future)
🔹 Bronze Layer (Raw Data)

Incremental ingestion using PowerShell

Source: network share (\\10.130.25.152\Eversys)

Destination: local VM storage (C:\RawData\Eversys)

No transformation applied

Historical data preserved

🔹 Silver Layer (Transformation)

Data cleaning using Python scripts

Handles:

Data formatting

Deduplication

Error handling

Standardization

🔹 Orchestration

Managed using Prefect

Two main flows:

bronze-ingestion-flow

silver-transformation-flow

⚙️ Development Workflow
🌱 Branch Strategy
Branch	Purpose
main	Production-ready pipelines
dev	Development & testing
🔁 Workflow

Develop features in dev

Test pipelines locally and in Prefect

Validate data quality

Merge into main when stable
