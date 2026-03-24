# 🚀 Data Cycle Project – Production Branch

![Status](https://img.shields.io/badge/status-production-brightgreen)
![Python](https://img.shields.io/badge/python-3.x-blue)
![Prefect](https://img.shields.io/badge/orchestration-prefect-6f42c1)
![Platform](https://img.shields.io/badge/platform-windows-lightgrey)

---

## 📌 Overview

Welcome to the **production branch (`main`)** of the Data Cycle project.

This branch contains the **stable and validated version** of the data pipelines.  
It is deployed on the **production VM** and used for scheduled data processing.

> ✅ Only tested and approved code is merged here  
> ⚠️ Do NOT develop directly in this branch

---

## 🏗️ Architecture

We follow a **Medallion Architecture**:
SOURCE (Eversys Share)
↓
🥉 Bronze Layer (Raw Data)
↓
🥈 Silver Layer (Cleaned Data)
↓
🥇 Gold Layer (Analytics - future)


---

## 🥉 Bronze Layer – Raw Ingestion

- 📂 Source: `\\10.130.25.152\Eversys`
- 💾 Destination: `C:\RawData\Eversys`
- ⚙️ Tool: PowerShell (`.ps1`)
- 🔁 Mode: Incremental ingestion

### ✅ Characteristics
- No transformation applied
- Full historical data preserved
- Watermark-based ingestion
- Reliable and idempotent

---

## 🥈 Silver Layer – Data Transformation

- 🐍 Tool: Python
- 📦 Scripts:
  - `clean_product_history.py`
  - `clean_rinse_history.py`
  - `clean_info_message_history.py`
  - `clean_cleaning_history.py`

### 🧹 Processing Includes
- Data cleaning and normalization
- Schema standardization
- Deduplication
- Error handling and logging

---

## 🎯 Orchestration

Managed using **Prefect**

### 🔄 Production Flows

| Flow | Description |
|------|------------|
| `bronze-ingestion-flow` | Incremental ingestion from source |
| `silver-transformation-flow` | Data cleaning and structuring |

### ⏱️ Scheduling

- Bronze ingestion: runs periodically (e.g. hourly)
- Silver transformation: triggered after ingestion

---

## ⚙️ Branch Strategy

| Branch | Purpose |
|--------|--------|
| `main` | Production 🚀 |
| `dev` | Development 🧪 |

### 🔁 Workflow

All development is done in `dev`.  
Once validated, changes are merged into `main` for production deployment.

---

## 🖥️ Deployment

### 📍 Environment

- 🖥️ Production VM (Windows Server)
- 📦 Code pulled from `main` branch
- ⚙️ Pipelines executed via Prefect + Task Scheduler

---

### 🔁 Deployment Process

```mermaid
flowchart LR
    A[Dev validated] --> B[Merge dev → main]
    B --> C[VM pulls latest code]
    C --> D[Production pipelines run]
