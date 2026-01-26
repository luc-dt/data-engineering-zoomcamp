-- QUESTION 3: Counting Short Trips
SELECT 
	COUNT(*) AS number_trip_Novem_2025
FROM 
	public."green_tripdata_2025-11"
WHERE 
	lpep_pickup_datetime >= '2025-11-01 00:00:00' 
		AND lpep_pickup_datetime < '2025-12-01 00:00:00' 
		AND trip_distance <= 1;
	
-- Question 4. Longest trip for each day
SELECT 
	CAST(lpep_pickup_datetime AS DATE) AS pickup_day,
	MAX(trip_distance) AS max_dist
FROM 
	public."green_tripdata_2025-11"
WHERE 
	trip_distance < 100 
GROUP BY 1
ORDER BY 2 DESC
LIMIT 1;

-- Question 5. Biggest pickup zone
SELECT 
    z."Zone", 
    SUM(t.total_amount) AS total_amount_sum
FROM 
    public."green_tripdata_2025-11" t
JOIN 
    public.zones z ON t."PULocationID" = z."LocationID"
WHERE 
    CAST(t.lpep_pickup_datetime AS DATE) = '2025-11-18'
GROUP BY 
    z."Zone"
ORDER BY 
    total_amount_sum DESC
LIMIT 1;

-- Question 6. Largest tip
SELECT 
	dropoff."Zone",
	t.tip_amount
FROM 
	public."green_tripdata_2025-11" AS t
	INNER JOIN public.zones AS pickup ON t."PULocationID" = pickup."LocationID"
	INNER JOIN public.zones AS dropoff ON t."DOLocationID" = dropoff."LocationID"
WHERE 
	pickup."Zone" = 'East Harlem North'
ORDER BY 
	t.tip_amount DESC
LIMIT 1



