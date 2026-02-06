# Data Engineering Zoomcamp 2026 - Module 3: BigQuery & Data Warehousing

This repository contains the solution for the Module 3 homework. The focus was on **BigQuery** best practices, cost optimization, and understanding the underlying storage architecture.

## 🚀 Project Overview
The goal was to ingest NYC Yellow Taxi data (January - June 2024) into Google Cloud Storage and perform analytical queries in BigQuery using different table strategies: **External Tables**, **Materialized (Native) Tables**, and **Partitioned/Clustered Tables**.

### 📊 Dataset Statistics
- **Total Records:** 20,332,093
- **Source Format:** Parquet (GCS)
- **Data Size:** ~2.72 GB (Logical)

## 📁 Files
- `load_yellow_taxi_data.py`: Python script to automate the upload of 6 months of taxi data to GCS.
- `hw_3_big_query.sql`: Complete SQL script containing all setups and homework queries.

## 📥 Data Ingestion Script (`load_yellow_taxi_data.py`)
The ingestion process utilizes a Python script that automates the transfer of Parquet files from the NYC Taxi CloudFront distribution to Google Cloud Storage.

**Technical Features:**
- **Parallelism:** Uses `ThreadPoolExecutor` for concurrent downloads and uploads to reduce total runtime.
- **Robustness:** Implemented retries and post-upload verification to ensure data integrity.
- **GCS Integration:** Uses the `google-cloud-storage` SDK for direct bucket interaction.

## 🛠️ BigQuery Optimization Strategies



### 1. External vs. Materialized Tables
I created an **External Table** to link directly to the Parquet files in GCS and a **Materialized (Native) Table** within BigQuery to compare performance and metadata behavior.

* **Key Insight:** Querying an External Table estimates **0 MB** because BigQuery hasn't scanned the remote files yet. The Native Table provides an accurate estimate because BigQuery manages its own metadata.

### 2. Columnar Storage Efficiency
Verified BigQuery's **Columnar Storage** architecture by comparing column selection costs:
* `SELECT PULocationID`: Scanned **~155.12 MB**.
* `SELECT PULocationID, DOLocationID`: Scanned **~310.24 MB**.

### 3. Partitioning & Clustering
To optimize for queries filtering by date and ordering by Vendor ID, I implemented:
- **Partitioning:** By `tpep_dropoff_datetime` (Daily).
- **Clustering:** By `VendorID`.



**Performance Comparison:**
- **Non-Partitioned Table:** Scanned **310.24 MB**.
- **Partitioned Table:** Scanned **26.84 MB**.
- **Efficiency Gain:** ~91.3% reduction in data scanned.

## ❓ Homework Answers Summary

| Question | Topic | Result |
| :--- | :--- | :--- |
| **Q1** | Record Count | 20,332,093 |
| **Q2** | Est. Bytes (Ext vs Nat) | 0 MB (External) / 155.12 MB (Native) |
| **Q3** | Columnar Logic | BigQuery only scans requested columns |
| **Q4** | Fare amount = 0 | 8,333 |
| **Q5** | Strategy | Partition by `tpep_dropoff_datetime`, Cluster on `VendorID` |
| **Q6** | Partitioning Proof | 310.24 MB (Non-part) / 26.84 MB (Part) |
| **Q7** | Data Location | GCP Bucket (Cloud Storage) |
| **Q8** | Clustering Best Practice | False |
| **Q9** | Metadata Trick | 0 Bytes (Count is stored in Table Metadata) |

## 🏁 Conclusion
This module demonstrated how significant cost savings are achieved through proper table design. Moving from a raw external table to a partitioned/clustered native table reduced query costs by over 90% for standard analytical patterns.
