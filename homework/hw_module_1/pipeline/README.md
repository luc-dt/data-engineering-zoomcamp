# Module 1 Homework: Docker, SQL & Terraform

This repository contains the solutions for **Module 1** of the **Data Engineering Zoomcamp 2026**.  
The project demonstrates the use of Docker for local data infrastructure, SQL for analytical queries, and Terraform for provisioning cloud resources on Google Cloud Platform (GCP).

---

## 📂 Project Structure

```text
.
├── pipeline/      # Docker configuration and data ingestion scripts
└── terraform/     # Infrastructure-as-Code (IaC) definitions for GCP
```

---

## 🐳 Part A: Docker & SQL

### Environment Setup

The environment is orchestrated using `docker-compose` and includes:

- **PostgreSQL** as the analytical database
- **pgAdmin** for database management and query execution

The NYC Green Taxi dataset for **November 2025** is loaded into PostgreSQL prior to running the queries below.

---

### Question 3: Counting Short Trips

**Task:**  
Count the number of trips where the pickup date is in **November 2025** and the trip distance is **less than or equal to 1 mile**.

```sql
SELECT 
    COUNT(*) AS number_trip_nov_2025
FROM 
    public."green_tripdata_2025-11"
WHERE 
    lpep_pickup_datetime >= '2025-11-01 00:00:00'
    AND lpep_pickup_datetime < '2025-12-01 00:00:00'
    AND trip_distance <= 1;
```

---

### Question 4: Longest Trip for Each Day

**Task:**  
Find the day with the **longest trip distance**, excluding obvious data errors where the trip distance exceeds **100 miles**.

```sql
SELECT 
    CAST(lpep_pickup_datetime AS DATE) AS pickup_day,
    MAX(trip_distance) AS max_dist
FROM 
    public."green_tripdata_2025-11"
WHERE 
    trip_distance < 100
GROUP BY 
    1
ORDER BY 
    2 DESC
LIMIT 1;
```

---

### Question 5: Biggest Pickup Zone

**Task:**  
Determine which **pickup zone** collected the **highest total amount** on **2025-11-18**.

```sql
SELECT 
    z."Zone",
    SUM(t.total_amount) AS total_amount_sum
FROM 
    public."green_tripdata_2025-11" t
JOIN 
    public.zones z
    ON t."PULocationID" = z."LocationID"
WHERE 
    CAST(t.lpep_pickup_datetime AS DATE) = '2025-11-18'
GROUP BY 
    z."Zone"
ORDER BY 
    total_amount_sum DESC
LIMIT 1;
```

---

### Question 6: Largest Tip

**Task:**  
For trips picked up in **East Harlem North**, identify the **drop-off zone** that received the **largest tip amount**.

```sql
SELECT 
    dropoff."Zone",
    t.tip_amount
FROM 
    public."green_tripdata_2025-11" AS t
JOIN 
    public.zones AS pickup
    ON t."PULocationID" = pickup."LocationID"
JOIN 
    public.zones AS dropoff
    ON t."DOLocationID" = dropoff."LocationID"
WHERE 
    pickup."Zone" = 'East Harlem North'
ORDER BY 
    t.tip_amount DESC
LIMIT 1;
```

---

## ☁️ Part B: Terraform

All Terraform configuration files are located in the `terraform/` directory.  
The infrastructure managed by Terraform includes:

- **Google Cloud Storage Bucket**  
  - `kestra-sandbox-485104-taxi-bucket`
- **BigQuery Dataset**  
  - `trips_data_all`

---

### Question 7: Terraform Workflow

**Task:**  
Reproduce the cloud infrastructure in a repeatable and automated manner.

#### Initialize Terraform

```bash
terraform init
```

#### Apply the Configuration

```bash
terraform apply -auto-approve
```

#### Destroy the Infrastructure

```bash
terraform destroy -auto-approve
```

---

## ✅ Notes

- All SQL queries were executed against a PostgreSQL instance running in Docker.
- Terraform state is assumed to be managed locally.
- Resource naming follows the conventions used in the Data Engineering Zoomcamp assignments.
