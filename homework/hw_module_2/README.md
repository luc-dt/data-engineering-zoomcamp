# Data Engineering Zoomcamp 2026 – Module 2: Workflow Orchestration

This repository contains the solution for **Module 2 Homework**, focusing on workflow orchestration using **Kestra**, **PostgreSQL**, and **Docker**.

---

## 🚀 Project Overview

The objective of this module is to design and orchestrate an **ETL pipeline** that:

1. **Extracts** NYC Taxi data from a public GitHub repository
2. **Transforms** the data by:

   * Uncompressing source files
   * Adding relevant metadata
3. **Loads** the processed data into a **local PostgreSQL** database

The pipeline is fully orchestrated using **Kestra**, ensuring repeatability, reliability, and observability.

---

## 🛠️ Infrastructure & Troubleshooting

During development, several environment-level challenges were encountered and resolved:

### 1. Storage Optimization

* Migrated the Docker / WSL2 virtual disk (`ext4.vhdx`) from the default **C:** drive to a high-capacity **D:** drive.
* This prevented disk exhaustion and recurring system crashes during large data processing.

### 2. Disk Extension

* Reconfigured **WSL2 storage limits** to handle large uncompressed datasets.
* Successfully processed **6.4+ million rows** from the Yellow Taxi January 2020 dataset.

### 3. Resource Management

* Implemented `PurgeCurrentExecutionFiles` in Kestra flows.
* Ensured all temporary execution files are cleaned up after each pipeline run to maintain system stability.

---

## 📝 Homework Answers

### 1. Uncompressed File Size (Yellow 2020-12)

**Question:** What is the uncompressed file size of the extract task output for Yellow Taxi data (2020-12)?

**Answer:** `128.3 MB`

---

### 2. Rendered Variable Value

**Question:** What is the value of the variable `file` when the inputs are:

* `taxi = green`
* `year = 2020`
* `month = 04`

**Answer:**

```text
green_tripdata_2020-04.csv
```

---

### 3. Yellow Taxi Rows (2020 Total)

**Question:** How many rows are there for the Yellow Taxi data for all CSV files in the year 2020?

**Answer:** `24,648,499`

---

### 4. Green Taxi Rows (2020 Total)

**Question:** How many rows are there for the Green Taxi data for all CSV files in the year 2020?

**Answer:** `1,734,051`

---

### 5. Yellow Taxi Rows (March 2021)

**Question:** How many rows are there for the Yellow Taxi data for the March 2021 CSV file?

**Answer:** `1,925,152`

---

### 6. Schedule Timezone Configuration

**Question:** How would you configure the timezone to New York in a `Schedule` trigger?

**Answer:**
Add a `timezone` property set to:

```yaml
timezone: America/New_York
```

within the `Schedule` trigger configuration.

---

## ✅ Summary

This module demonstrates practical workflow orchestration at scale, covering not only ETL logic but also **real-world infrastructure constraints** such as disk management, resource cleanup, and environment stability—critical skills for production-grade Data Engineering systems.

