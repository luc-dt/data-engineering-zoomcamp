-- Query public available table
-- SELECT station_id, name FROM
--     bigquery-public-data.new_york_citibike.citibike_stations
-- LIMIT 100;


-- =================================================================================
-- GOOGLE CLOUD ZOOMCAMP 2026 - MODULE 3: BIGQUERY HOMEWORK SCRIPT
-- =================================================================================
-- Project ID: de-zoomcamp-2026-486008
-- Dataset: ny_taxi
-- Region: us-central1
-- =================================================================================

-- Query public available table
-- SELECT station_id, name FROM
--     bigquery-public-data.new_york_citibike.citibike_stations
-- LIMIT 100;

-- 1. Create External Table referring to your GCS path
-- This doesn't move data; it just points to your .csv.gz files

CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-2026-486008.ny_taxi.external_yellow_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://de-zoomcamp-2026-raw-data/yellow/yellow_tripdata_2019-*.csv.gz', 
          'gs://de-zoomcamp-2026-raw-data/yellow/yellow_tripdata_2020-*.csv.gz']
);

-- 2. Check yellow trip data
SELECT * 
FROM `de-zoomcamp-2026-486008.ny_taxi.external_yellow_tripdata` 
LIMIT 10;

-- 3. Create a non-partitioned table from external table
CREATE OR REPLACE TABLE `de-zoomcamp-2026-486008.ny_taxi.yellow_tripdata_non_partitioned` AS
SELECT * FROM `de-zoomcamp-2026-486008.ny_taxi.external_yellow_tripdata`;

-- 4. Create a partitioned table from external table
-- Partitioning by DATE(tpep_pickup_datetime)
CREATE OR REPLACE TABLE `de-zoomcamp-2026-486008.ny_taxi.yellow_tripdata_partitioned`  
PARTITION BY
  DATE(tpep_pickup_datetime) AS
SELECT * FROM `de-zoomcamp-2026-486008.ny_taxi.external_yellow_tripdata`;


-- 5. Impact of partition
-- Scanning full table (high cost)
SELECT DISTINCT(VendorID)
FROM `de-zoomcamp-2026-486008.ny_taxi.yellow_tripdata_non_partitioned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- Scanning only June 2019 partition (low cost)
SELECT DISTINCT(VendorID)
FROM `de-zoomcamp-2026-486008.ny_taxi.yellow_tripdata_partitioned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2019-06-30';

-- 6. Inspect the partitions
SELECT table_name, partition_id, total_rows
FROM `ny_taxi.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'yellow_tripdata_partitioned'
ORDER BY total_rows DESC;

-- 7. Creating a partition and cluster table
-- Partitioned by date, clustered by VendorID
CREATE OR REPLACE TABLE `de-zoomcamp-2026-486008.ny_taxi.yellow_tripdata_partitioned_clustered`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `de-zoomcamp-2026-486008.ny_taxi.external_yellow_tripdata`;

-- 8. Compare performance with Clustering
-- Query partitioned table
SELECT count(*) as trips
FROM `de-zoomcamp-2026-486008.ny_taxi.yellow_tripdata_partitioned`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;

-- Query partitioned and clustered table (even more efficient)
SELECT count(*) as trips
FROM `de-zoomcamp-2026-486008.ny_taxi.yellow_tripdata_partitioned_clustered`
WHERE DATE(tpep_pickup_datetime) BETWEEN '2019-06-01' AND '2020-12-31'
  AND VendorID=1;


-- This deletes the dataset and every table we created today
-- DROP SCHEMA IF EXISTS `de-zoomcamp-2026-486008.ny_taxi` CASCADE;

