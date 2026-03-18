# 🚧 Data Cycle Project – Dev Branch

![Status](https://img.shields.io/badge/status-development-orange)
![Python](https://img.shields.io/badge/python-3.x-blue)
![Prefect](https://img.shields.io/badge/orchestration-prefect-6f42c1)
![Platform](https://img.shields.io/badge/platform-windows-lightgrey)

---

## 📌 Overview

Welcome to the **development branch (`dev`)** of the Data Cycle project.

This branch is where all the magic happens ✨  
New features, pipeline improvements, and experiments are built and tested here before going to production.

> ⚠️ This branch may be unstable. Use `main` for production-ready pipelines.

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
- 🔁 Mode: Incremental (only new files)

### ✅ Features
- No data transformation
- Full history preserved
- Duplicate-safe ingestion
- Ready for large volumes

---

## 🥈 Silver Layer – Data Cleaning

- 🐍 Tool: Python
- 📦 Scripts:
  - `clean_product_history.py`
  - `clean_rinse_history.py`
  - `clean_info_message_history.py`
  - `clean_cleaning_history.py`

### 🧹 Processing Includes
- Data normalization
- Error handling
- Deduplication
- Format alignment

---

## 🎯 Orchestration

Handled with **Prefect**

### 🔄 Flows

| Flow | Description |
|------|------------|
| `bronze-ingestion-flow` | Ingest raw data |
| `silver-transformation-flow` | Clean & transform data |

---

## ⚙️ Development Workflow

### 🌱 Branch Strategy

| Branch | Purpose |
|--------|--------|
| `main` | Production 🚀 |
| `dev` | Development 🧪 |

---

### 🔁 Workflow

```mermaid
flowchart LR
    A[Develop Feature] --> B[Test Locally]
    B --> C[Run in Prefect]
    C --> D[Validate Data]
    D --> E[Merge to main]
