-- 1. Create External Table from FHV raw data in GCS
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-2026-486008.ny_taxi.external_fhv_tripdata`
OPTIONS(
  format = 'CSV',
  uris = ['gs://de-zoomcamp-2026-raw-data/fhv/fhv_tripdata_2019-*.csv.gz']
);

SELECT COUNT(*) FROM `de-zoomcamp-2026-486008.ny_taxi.external_fhv_tripdata`;

SELECT COUNT(DISTINCT(dispatching_base_num)) FROM `de-zoomcamp-2026-486008.ny_taxi.external_fhv_tripdata`;


-- 2. Create a standart Native Table (No optimization)
CREATE OR REPLACE TABLE `de-zoomcamp-2026-486008.ny_taxi.fhv_nonpartitioned_tripdata` AS
SELECT * FROM `de-zoomcamp-2026-486008.ny_taxi.external_fhv_tripdata`;

-- 3. Create Optimized Table (Partition by Date, Clustered by Base ID)
-- This is where the BiqQuery Internals (Dremel & Colossus) work best!
CREATE OR REPLACE TABLE `de-zoomcamp-2026-486008.ny_taxi.fhv_partitioned_tripdata`
PARTITION BY DATE(dropoff_datetime)
CLUSTER BY dispatching_base_num AS (
  SELECT * FROM `de-zoomcamp-2026-486008.ny_taxi.external_fhv_tripdata`
);

-- 4. Compare Performance 
-- Non-Partitionned
SELECT COUNT(*) FROM `de-zoomcamp-2026-486008.ny_taxi.fhv_nonpartitioned_tripdata`
WHERE DATE(dropOff_datetime) BETWEEN '2019-01-01' AND '2019-03-31'
  AND dispatching_base_num IN ('B00987', 'B02279', 'B02060');

-- This query uses Partitioning to prune the 4000 limit partitions
SELECT COUNT(*) FROM `de-zoomcamp-2026-486008.ny_taxi.fhv_partitioned_tripdata`
WHERE DATE(dropOff_datetime) BETWEEN '2019-01-01' AND '2019-03-31'
  AND dispatching_base_num IN ('B00987', 'B02279', 'B02060');



-- This deletes the dataset and every table we created today
--DROP SCHEMA IF EXISTS `de-zoomcamp-2026-486008.ny_taxi` CASCADE;

