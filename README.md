# 🔐 Security Data Pipeline – Cloud-Native Architecture for Vulnerability Log Processing

This project implements a robust, end-to-end data pipeline for ingesting, transforming, and analyzing security-related data, such as vulnerability and threat intelligence logs. Built using **Azure Data Factory**, **Apache Spark on Databricks**, **Delta Lake**, and **Apache Airflow**, this solution supports scalable, modular, and cross-platform data processing to enable security teams to derive actionable insights from raw logs.

---

## 📊 Key Use Case

> Efficiently process system vulnerability logs from various Linux environments (Ubuntu, AlmaLinux, Bitnami) into an analytics-ready data lake format.  
> Enable real-time and batch processing, streamline security operations, and support compliance reporting.

---

## 🏗️ Architecture Overview

```
graph TD
    A[Blob Storage (JSON logs)] --> B[Azure Data Factory]
    B --> C[Databricks - Spark Jobs]
    C --> D[Delta Lake (Raw → Staging → Curated)]
    D --> E[Business Intelligence / Dashboards]
    C --> F[Airflow DAGs (Orchestration)]
```

---

## ⚙️ Tech Stack

| Component | Technology |
|----------|-------------|
| Cloud Platform | Azure |
| Orchestration | Azure Data Factory, Apache Airflow |
| Processing | Apache Spark (Databricks) |
| Storage | Azure Blob, Delta Lake |
| Programming | Python, PySpark |
| Visualization | (Pluggable – Power BI, Tableau, etc.) |

---

## ✅ Core Features

- 🔄 **Full & Delta Load Support** for ingesting JSON log files.
- 🧠 **Data Transformation**: Flattening nested JSON, deduplication, normalization.
- 📂 **Multi-Layered Storage**: Raw → Staging → Curated zones for clean data flow.
- 📅 **Workflow Automation**: Airflow DAGs enable modular ingestion per OS.
- ⚡ **Performance Optimizations**: Partitioning, Z-ordering, broadcast joins, caching.
- 🔍 **Extensibility**: Supports additional log formats and systems easily.

---

## 📁 Project Structure

```
├── Azure Data Factory/
│   └── ADF pipelines (.zip) for full and delta load ingestion
├── Databricks Notebook Files/
│   └── JSON parsing, data transformation, writing to Delta
├── Airflow_DAGs/
│   └── Platform-specific ingestion DAGs (Ubuntu, AlmaLinux, Bitnami)
├── Documentation PDFs/
│   ├── Data Architecture.pdf
│   ├── Data Ingestion Process.pdf
│   ├── Spark Jobs for Transformations.pdf
│   ├── Techniques_of_Optimization_used.pdf
│   └── Data Pipeline Architecture.pdf
├── Diagrams/
│   └── Dimensional model & architectural diagrams
```

---

## 🔁 End-to-End Workflow

1. **Data Source**: JSON logs containing OS, timestamp, CVE data, etc., are uploaded to Azure Blob Storage.
2. **Ingestion (ADF)**: ADF pipelines load this data into the raw zone of Delta Lake.
3. **Transformation (Databricks)**: Spark-based transformations cleanse and flatten the JSON, apply business rules, and move it to staging/curated zones.
4. **Automation (Airflow)**: Platform-specific DAGs ensure ingestion consistency and monitoring.
5. **Output**: Data is ready for analytics, dashboards, or machine learning applications.

---

## 🚀 Optimization Techniques

- ✅ **Z-Ordering** for fast filtering and indexing in Delta Lake.
- ✅ **Partitioning by OS / Timestamp** to speed up queries.
- ✅ **Caching / Persisting Spark DataFrames** for efficiency.
- ✅ **Broadcast Joins** for small dimension tables.

---

## 📦 How to Deploy

### Prerequisites:
- Azure Subscription with Blob Storage and ADF setup
- Databricks Workspace
- Airflow Environment (on VM or MWAA)

### Steps:

1. Upload pipeline templates (`Azure Data Factory/`) to ADF.
2. Import notebooks into Databricks and configure cluster.
3. Upload sample JSON files to Azure Blob.
4. Run ADF pipelines to ingest data into Delta Lake.
5. Trigger DAGs from Airflow or schedule cron jobs.
6. Validate data in Delta Lake (via Databricks or notebooks).

---

## 🙋‍♀️ Author

**Arundhati Pathrikar**  
MS in Information Systems – Northeastern University  
Feel free to connect with me on [LinkedIn](#) or reach out for collaboration!

---
