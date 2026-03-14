cat > /d/data-engineering-zoomcamp/homework/hw_module_6/README.md << 'EOF'
# Module 6 Homework — Batch Processing with Spark

## What I Learned

✅ Set up PySpark and create Spark sessions  
✅ Read and process Parquet files at scale  
✅ Repartition data for optimal performance  
✅ Analyze millions of taxi trips with DataFrames  
✅ Use Spark UI for monitoring jobs  

Processing 4M+ taxi trips with Spark - distributed computing is powerful! 💪

---

## Dataset

- **Source**: NYC Yellow Taxi Trip Data — November 2025
- **URL**: https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet
- **Zone Lookup**: https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv
- **Format**: Parquet
- **Size**: ~4M+ records

---

## Questions & Answers

### Q1: Spark Version
**Answer: 3.5.3**

After installing PySpark and creating a local Spark session, running `spark.version` returns `3.5.3`. This confirms Spark is correctly installed and the session is active.
```python
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("hw6") \
    .getOrCreate()

print(spark.version)  # 3.5.3
```

---

### Q2: Average Parquet File Size After Repartition
**Answer: 25MB**

When we read the parquet file into a Spark DataFrame, Spark loads it as a single partition by default. Repartitioning to 4 splits the data evenly across 4 output files. Each file ends up being roughly 25MB because the total dataset size is ~100MB divided by 4 partitions.
```python
df = spark.read.parquet("yellow_tripdata_2025-11.parquet")
df.repartition(4).write.parquet("yellow_2025_11_partitioned", mode="overwrite")
```

Repartitioning is important in real-world pipelines because it controls parallelism — more partitions means more workers can process data simultaneously.

---

### Q3: Number of Trips on November 15th
**Answer: 102,340**

We filter trips where the pickup datetime falls on November 15, 2025. Only trips that **started** on that day are counted, regardless of when they ended.
```python
df.filter(
    F.to_date(F.col("tpep_pickup_datetime")) == "2025-11-15"
).count()
```

---

### Q4: Longest Trip in Hours
**Answer: 90.6 hours**

We calculate trip duration by subtracting the pickup timestamp from the dropoff timestamp and converting the result from seconds to hours. The longest trip in the dataset is unusually long — likely a data quality issue or a forgotten meter.
```python
df.withColumn(
    "duration_hours",
    (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 3600
).agg(F.max("duration_hours")).show()
```

---

### Q5: Spark UI Port
**Answer: 4040**

Spark automatically starts a Web UI when a SparkSession is created. It runs on **port 4040** by default and shows live information about running jobs, stages, tasks, executors, and storage. You can access it at:
```
http://localhost:4040
```

This is different from port 8080 which is used by the Spark standalone cluster Master UI.

---

### Q6: Least Frequent Pickup Zone
**Answer: Governor's Island/Ellis Island/Liberty Island**

We join the taxi trips data with the zone lookup CSV on `PULocationID = LocationID`, then group by zone name and count trips. Sorting by count ascending gives us the least frequently used pickup zones.
```python
zones = spark.read.option("header", "true").csv("taxi_zone_lookup.csv")

df.join(zones, df.PULocationID == zones.LocationID, "left") \
  .groupBy("Zone") \
  .count() \
  .orderBy("count") \
  .show(5)
```

Some zones like Governor's Island are rarely used as pickup locations because they are remote or have very limited taxi access.

---

## How to Run
```bash
# 1. Activate the venv that has PySpark installed
cd /d/data-engineering-zoomcamp/06-batch
source .venv/Scripts/activate

# 2. Download the data
wget https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-11.parquet
wget https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv

# 3. Launch Jupyter pointing to the homework folder
jupyter notebook --notebook-dir=/d/data-engineering-zoomcamp/homework/hw_module_6

# 4. Open hw_6.ipynb and run all cells
```

---

## Tech Stack

| Tool | Version | Purpose |
|------|---------|---------|
| Python | 3.11 | Programming language |
| PySpark | 3.5.3 | Distributed data processing |
| Jupyter Notebook | Latest | Interactive development |
| Windows 11 / Git Bash | — | Local environment |

---