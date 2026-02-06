# NYC Taxi Data Engineering Project: Module 3 (BigQuery & MLOps)

🚀 **Week 3 of Data Engineering Zoomcamp by @DataTalksClub complete!**

I have just finished Module 3 - Data Warehousing with BigQuery. Through this project, I learned how to bridge the gap between Data Engineering and Machine Learning Operations (MLOps).

### ✅ Key Learning Milestones:
* **Cloud Storage & Warehousing:** Managed 20M+ records by creating external tables from GCS bucket data.
* **Performance Optimization:** Built materialized tables and implemented **Partitioning** and **Clustering** to reduce query costs and increase speed.
* **Columnar Storage:** Gained a deep understanding of how BigQuery's storage architecture impacts billing and query optimization.
* **BigQuery ML (BQML):** Developed an end-to-end taxi tip prediction model directly within the warehouse using SQL.
* **Model Deployment:** Exported trained models to **Docker** using **TensorFlow Serving** for real-time REST API inference.

---

## 🛠️ The "A to Z" Technical Workflow

This project follows a decoupled architecture, separating storage from compute:

1.  **Ingestion:** Python scripts (`web_to_gcs.py`) automate data flow from the web to a GCS Data Lake.
2.  **Transformation:** SQL-based feature engineering in BigQuery (Casting types, cleaning outliers).
3.  **Modeling:** Used the **Core Four** strategy (Prep, Create, Evaluate, Predict) to build a Linear Regression model.
4.  **Deployment:** Orchestrated the model export into a Dockerized environment for production-ready serving.



---

## 💰 FinOps & Optimization
Working with large datasets (20M+ records) requires a "cost-first" mindset. I practiced:
* **Dry Run Estimates:** Using the BigQuery validator to predict costs before execution.
* **Metadata Utilization:** Fetching row counts and schema info for 0 VND.
* **Partitioning & Clustering:** Drastically reducing the bytes scanned per query.

---

## End-to-End Taxi Tip Prediction Pipeline

This document is the definitive guide to the Data Engineering and MLOps lifecycle practiced in the NYC Taxi project. It covers the full flow from raw ingestion to production-grade serving.

---

## 🏗️ 1. Infrastructure & Orchestration
Before any code runs, the environment must be established.
* **GCP Project:** `your-project-id-here`
* **Storage:** Google Cloud Storage (GCS) acts as the Data Lake.
* **Compute:** BigQuery for warehouse/ML and Docker for local serving.

---

## 📁 2. The File-System Breakdown

### 🔹 Ingestion Layer: `web_to_gcs.py`
* **Goal:** Automated Data Lake population.
* **Logic:** Downloads NYC Taxi Parquet files from the web and uploads them directly to your GCS bucket.
* **Value:** Decouples the project from external dependencies and ensures you have a "Source of Truth."



### 🔹 Storage Layer: `big_query.sql`
* **External Tables:** Created links to GCS files to allow querying without storage costs.
* **Native Tables:** Materialized data into native BigQuery storage for high-performance ML training.

### 🔹 Machine Learning Layer: `big_query_ml.sql`
This file implements a **Three-Tier Workflow Strategy**:

#### **Tier 1: The "Core Four" (Essential for every run)**
1. **Step 2 (Data Prep):** Casting features (e.g., `PULocationID` as STRING) and filtering outliers (`trip_distance > 0`).
2. **Step 3 (Create Model):** Training the `linear_reg` model using BQML.
3. **Step 5 (Evaluate):** Checking MAE and $R^2$ to measure how "smart" the model is.
4. **Step 6 (Predict):** Generating predictions for new data.

#### **Tier 2: The "Debug & Trust" (When results look weird)**
* **Step 4 (Feature Info):** Used to inspect min/max values and catch "poisoned" data (like negative distances).
* **Step 7 (Explain Predict):** Provides transparency by showing which top 3 features impacted each specific prediction.



#### **Tier 3: The "Pro" Step (Optimization)**
* **Step 8 (Hyperparameter Tuning):** Running multiple trials with L1/L2 regularization to maximize model performance and prevent overfitting.

---

## 🐳 3. Deployment & MLOps (Terminal Steps)

This phase moves the model from the Warehouse into a live API.

1. **`gcloud auth login`**: Establishes the connection to GCP.
2. **`bq extract -m ...`**: Exports the model from BigQuery back to GCS as a TensorFlow bundle.
3. **`gsutil cp -r ...`**: Downloads the model files from the Cloud to your local machine.
4. **`mkdir -p .../1`**: **The Mandatory Step:** TensorFlow Serving requires a versioned subfolder.
5. **`docker run ...`**: Hosts the model on port 8501 using TensorFlow Serving.
6. **`curl -d ...`**: Pings the local API with JSON data to get a real-time tip prediction.



---

## 🚀 4. Automation Script (`deploy_model.sh`)

Save this script to automate the deployment lifecycle every time you update your model:

### 1. Model Deployment
To spin up the prediction API on your local machine:

```bash
#!/bin/bash
PROJECT_ID="your-project-id-here"
BUCKET_NAME="your-bucket-id-here"
MODEL_NAME="tip_model"
LOCAL_DIR="./serving_dir/$MODEL_NAME/1"

echo "🚀 Starting Full Deployment..."
gcloud config set project $PROJECT_ID

# Export and Download
bq extract -m ny_taxi.$MODEL_NAME gs://$BUCKET_NAME/tip_model_export
mkdir -p $LOCAL_DIR
gsutil cp -r gs://$BUCKET_NAME/tip_model_export/* $LOCAL_DIR/

# Docker Serve
docker rm -f $(docker ps -aq) || true
docker run -d -p 8501:8501 \
  --mount type=bind,source="$(pwd)"/serving_dir/$MODEL_NAME,target=/models/$MODEL_NAME \
  -e MODEL_NAME=$MODEL_NAME \
  -t tensorflow/serving

echo "✅ API is live! Use 'curl' to test."
```

### 2. Testing the API

```bash
curl -d '{"instances": [{"passenger_count":1, "trip_distance":12.2, "PULocationID":"193", "DOLocationID":"264", "payment_type":"2","fare_amount":20.4,"tolls_amount":0.0}]}' \
-X POST http://localhost:8501/v1/models/tip_model:predict
```