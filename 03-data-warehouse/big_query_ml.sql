-- 1. SELECT THE COLUMNS OF INTEREST
-- Filter out zero fares to ensure data quality,
SELECT 
  passenger_count, trip_distance, PULocationID, DOLocationID, payment_type, fare_amount,         tolls_amount, tip_amount
FROM 
  `de-zoomcamp-2026-486008.ny_taxi.yellow_tripdata_partitioned`
WHERE 
  fare_amount != 0;

-- 2. CREATE A ML TABLE WITH APPROPRIATE TYPES
-- Casting PULocationID, DOLocationID, and payment_type to STRING is critical.
-- BigQuery ML treats Strings as 'Categorical' features automatically.
CREATE OR REPLACE TABLE `de-zoomcamp-2026-486008.ny_taxi.yellow_tripdata_ml` (
  `passenger_count` INTEGER,
  `trip_distance` FLOAT64,
  `PULocationID` STRING,
  `DOLocationID` STRING,
  `payment_type` STRING,
  `fare_amount` FLOAT64,
  `tolls_amount` FLOAT64,
  `tip_amount` FLOAT64
) AS (
  SELECT passenger_count, trip_distance, CAST(PULocationID AS STRING), CAST(DOLocationID AS STRING),
  CAST(payment_type AS STRING), fare_amount, tolls_amount, tip_amount
  FROM `de-zoomcamp-2026-486008.ny_taxi.yellow_tripdata_partitioned` 
  WHERE fare_amount != 0
);

-- 3. CREATE MODEL WITH DEFAULT SETTING
-- Training a linear regression to predict 'tip_amount'.
CREATE OR REPLACE MODEL `de-zoomcamp-2026-486008.ny_taxi.tip_model`
OPTIONS (
  model_type='linear_reg',
  input_label_cols=['tip_amount'],
  DATA_SPLIT_METHOD='AUTO_SPLIT'
) AS 
SELECT *
FROM `de-zoomcamp-2026-486008.ny_taxi.yellow_tripdata_ml`
WHERE tip_amount IS NOT NULL;

-- 4. CHECK FEATURES
-- Provides statistics about the features used in the model.
SELECT *
FROM ML.FEATURE_INFO(MODEL `de-zoomcamp-2026-486008.ny_taxi.tip_model`);

-- 5. EVALUATE THE MODEL 
-- Returns metrics like mean_absolute_error and r2_score.
SELECT *
FROM ML.EVALUATE(MODEL `de-zoomcamp-2026-486008.ny_taxi.tip_model`,(
  SELECT * FROM `de-zoomcamp-2026-486008.ny_taxi.yellow_tripdata_ml` 
  WHERE tip_amount IS NOT NULL
));

-- 6. PREDICT USING THE MODEL
SELECT *
FROM ML.PREDICT(MODEL `de-zoomcamp-2026-486008.ny_taxi.tip_model`,(
  SELECT * FROM `de-zoomcamp-2026-486008.ny_taxi.yellow_tripdata_ml`
  WHERE tip_amount IS NOT NULL
));

-- 7. PREDICT AND EXPLAIN
-- Shows which top 3 features had the most impact on each specific prediction.
SELECT *
FROM ML.EXPLAIN_PREDICT(MODEL `de-zoomcamp-2026-486008.ny_taxi.tip_model`,(
  SELECT * FROM `de-zoomcamp-2026-486008.ny_taxi.yellow_tripdata_ml`
  WHERE tip_amount IS NOT NULL
), STRUCT(3 as top_k_features));

-- 8. HYPERPARAMETER TUNING
-- Run 5 trials to find the best regularization (L1/L2) for the model.
CREATE OR REPLACE MODEL `de-zoomcamp-2026-486008.ny_taxi.tip_hyperparam_model`
OPTIONS (
  model_type='linear_reg',
  input_label_cols=['tip_amount'],
  DATA_SPLIT_METHOD='AUTO_SPLIT',
  num_trials=5,
  max_parallel_trials=2,
  l1_reg=hparam_range(0, 20),
  l2_reg=hparam_candidates([0, 0.1, 1, 10])
) AS
SELECT *
FROM `de-zoomcamp-2026-486008.ny_taxi.yellow_tripdata_ml`
WHERE tip_amount IS NOT NULL;
