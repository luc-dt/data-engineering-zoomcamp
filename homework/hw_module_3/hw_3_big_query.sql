-- BIG QUERY SETUP:
-- Create an external table using the Yellow Taxi Trip Records.
CREATE OR REPLACE EXTERNAL TABLE `de-zoomcamp-2026-486008.ny_taxi.external_yellow_taxi_2024`
OPTIONS (
  format='PARQUET',
  uris=['gs://de-zoomcamp-2026-raw-data/yellow_tripdata_2024/yellow_tripdata_2024-*.parquet']
);

-- Create a (regular/materialized) table in BQ using the Yellow Taxi Trip Records (do not partition or cluster this table).
CREATE OR REPLACE TABLE `de-zoomcamp-2026-486008.ny_taxi.yellow_taxi_2024_non_partitioned` AS
SELECT * FROM `de-zoomcamp-2026-486008.ny_taxi.external_yellow_taxi_2024`;

-- Q1. What is count of records for the 2024 Yellow Taxi Data?
----  Number of rows: 20,332,093

-- Q2.Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
-- What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
----- PULocationIDs on external_yellow_taxi_2024
SELECT 
  DISTINCT(PULocationID)
FROM 
  `de-zoomcamp-2026-486008.ny_taxi.external_yellow_taxi_2024`;

----- PULocationIDs on yellow_taxi_2024_non_partitioned
SELECT 
  DISTINCT(PULocationID)
FROM 
  `de-zoomcamp-2026-486008.ny_taxi.yellow_taxi_2024_non_partitioned`;

-- Q3. Write a query to retrieve the PULocationID from the table (not the external table) in BigQuery. 
-- Now write a query to retrieve the PULocationID and DOLocationID on the same table. Why are the estimated number of Bytes different?
--- Query A: a query to retrieve the PULocationID
SELECT 
  PULocationID 
FROM 
  `de-zoomcamp-2026-486008.ny_taxi.yellow_taxi_2024_non_partitioned`;

--- Query B: a query to retrieve the PULocationID and DOLocationID

SELECT 
  PULocationID, DOLocationID 
FROM 
  `de-zoomcamp-2026-486008.ny_taxi.yellow_taxi_2024_non_partitioned`;

-- Q4. How many records have a fare_amount of 0?
SELECT 
  COUNT(*)
FROM 
  `de-zoomcamp-2026-486008.ny_taxi.yellow_taxi_2024_non_partitioned`
WHERE 
  fare_amount = 0;

-- Q5: What is the best strategy to make an optimized table in Big Query 
-- if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID (Create a new table with this strategy)
CREATE OR REPLACE TABLE `de-zoomcamp-2026-486008.ny_taxi.yellow_taxi_2024_partitioned_and_clustered`
PARTITION BY DATE(tpep_dropoff_datetime)
CLUSTER BY VendorID AS
SELECT * FROM `de-zoomcamp-2026-486008.ny_taxi.external_yellow_taxi_2024`;


-- Q6: Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)
-- Use the materialized table you created earlier in your from clause and note the estimated bytes. 
-- Now change the table in the from clause to the partitioned table you created for question 5 and note the estimated bytes processed. 
-- What are these values?

--- Query 1: non-partitioned table
SELECT 
  DISTINCT(VendorID)
FROM 
  `de-zoomcamp-2026-486008.ny_taxi.yellow_taxi_2024_non_partitioned`
WHERE 
  tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

--- Query 2: new optimized table
SELECT 
  DISTINCT(VendorID)
FROM 
  `de-zoomcamp-2026-486008.ny_taxi.yellow_taxi_2024_partitioned_and_clustered`
WHERE 
  tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15';

-- Q7: Where is the data stored in the External Table you created?
--- GCP Bucket

-- Q8: It is best practice in Big Query to always cluster your data:
--- False

-- Q9. Write a SELECT count(*) query FROM the materialized table you created. How many bytes does it estimate will be read? Why?
SELECT 
  COUNT(*)
FROM 
  `de-zoomcamp-2026-486008.ny_taxi.yellow_taxi_2024_non_partitioned`;

---- answer is 0 bytes because BigQuery doesn't own the files in or GCS bucket.






