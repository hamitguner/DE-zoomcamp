-- Creating external table referring to gcs path
CREATE OR REPLACE EXTERNAL TABLE `ny_taxi.green_taxi_2022_external`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://de_zoomcamp_hw3/ny_green/69a20b2337354ec2a515e52cf890f23a-0.parquet']
);

-- Create a normal tabel
CREATE OR REPLACE TABLE ny_taxi.green_taxi_2022 AS
SELECT * FROM ny_taxi.green_taxi_2022_external;

SELECT * FROM ny_taxi.green_taxi_2022_external LIMIT 10;

SELECT * FROM ny_taxi.green_taxi_2022 LIMIT 10;


-- Question 2
-- Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.
-- What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?
SELECT COUNT(DISTINCT PULocationID) AS distinct_PULocationIDs
FROM ny_taxi.green_taxi_2022_external;
-- 0B for External Table.

SELECT COUNT(DISTINCT PULocationID) AS distinct_PULocationIDs
FROM ny_taxi.green_taxi_2022;
-- 6.41 MB for Materialized Table


-- Question 3 
--  How many records have a fare_amount of 0?

SELECT COUNT(*)
FROM ny_taxi.green_taxi_2022_external
WHERE fare_amount = 0;

-- Question 4 
/*
What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on lpep_pickup_datetime? (Create a new table with this strategy)
*/

-- Create a partitioned table from external table
CREATE OR REPLACE TABLE ny_taxi.green_taxi_2022_partitoned
PARTITION BY
  DATE(lpep_pickup_datetime) AS
SELECT * FROM ny_taxi.green_taxi_2022_external;

-- Creating a partition and cluster table
CREATE OR REPLACE TABLE ny_taxi.green_taxi_2022_partitoned_clustered
PARTITION BY DATE(lpep_pickup_datetime)
CLUSTER BY PUlocationID AS
SELECT * FROM ny_taxi.green_taxi_2022_external;


-- Quesiton 5 
/*
Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)

Use the materialized table you created earlier in your from clause and note the estimated bytes. Now change the table in the from clause to the partitioned table you created for question 4 and note the estimated bytes processed. What are these values? 
*/


-- materialized table
SELECT COUNT(DISTINCT PULocationID) AS distinct_PULocationIDs
FROM ny_taxi.green_taxi_2022
WHERE lpep_pickup_datetime BETWEEN TIMESTAMP('2022-06-01') AND TIMESTAMP('2022-06-30');


-- partitioned table
SELECT COUNT(DISTINCT PULocationID) AS distinct_PULocationIDs
FROM ny_taxi.green_taxi_2022_partitoned
WHERE lpep_pickup_datetime BETWEEN TIMESTAMP('2022-06-01') AND TIMESTAMP('2022-06-30');


