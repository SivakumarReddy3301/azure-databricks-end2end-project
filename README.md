# azure-databricks-end2end-project

End-to-end data engineering pipeline on **Azure Databricks**. It includes **incremental data ingestion(Spark Structured Streaming)**, **Dimensional modeling(STAR)**, **Data transformations**, **Slowly Changing Dimensions**,**Delta Live Tables**, **Unity Catalog** and **Workflow orchestration**.

---

## 🧱 Architecture

- **Bronze Layer**: Raw incremental ingestion of `customers`, `orders`, `products` from ADLS
- **Silver Layer**: Cleansed and transformed datasets stored as Delta tables
- **Gold Layer**:
  - `customers` → SCD Type 1
  - `orders` → SCD Type 1 (Fact table)
  - `products` → SCD Type 2 via **Delta Live Tables (DLT)**

---

## 🔄 Pipeline Workflow

- Orchestrated using **Databricks Workflows**
- Dependencies between notebooks defined in a json file `pipeline folder`
- Triggers ingest → transform → dimensional modeling in sequence

---

## ☁️ Cloud Setup

- **Azure Data Lake Storage Gen2 (ADLS)** containers: `source`, `bronze`, `silver`, `gold`
- **Unity Catalog** configured with:
  - External locations
  - Storage credentials
  - Managed via access connectors

---

## 🧠 Slowly Changing Dimensions

| Table     | Type | Logic                         |
|-----------|------|-------------------------------|
| customers | SCD1 | Upserts using Delta MERGE     |
| orders    | SCD1 | Overwrite with unique ID      |
| products  | SCD2 | Using Delta Live Tables       |

---

## 📂 Notebooks Overview

- `bronze/` → Raw ingest from source to ADLS
- `silver/` → Data cleansing, joins, validations
- `gold/` → SCD logic, surrogate keys, historical tracking
- `pipeline/` → Master orchestration pipeline

---

## 🔧 Setup

1. Configure Unity Catalog (run `unity_catalog_setup.sql`)
2. Update ADLS paths in `configs/paths_config.py`
3. Import notebooks into Databricks
4. Import `end2end_PipelineJob.json` into Databricks Jobs UI
