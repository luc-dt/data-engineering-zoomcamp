-- Q3: What is the rendered value of the variable file when the input taxi is set to green, year is set to 2020,
----- and the month is set to 04 during execution?
SELECT 
	COUNT(*) 
FROM public.yellow_tripdata

-- Q4: How many rows are there for the Green Taxi data for all CSV files in the year 2020?
SELECT 
	COUNT(*) 
FROM public.green_tripdata

-- Q5: How many rows are there for the Yellow Taxi data for the March 2021 CSV file?
SELECT 
	COUNT(*) 
FROM 
	public.yellow_tripdata 
WHERE 
	filename = 'yellow_tripdata_2021-03.csv';



