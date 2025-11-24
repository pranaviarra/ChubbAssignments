-- Databricks notebook source

-- BRONZE: Raw streaming taxi data

CREATE OR REFRESH STREAMING TABLE bronze_trips
(
  CONSTRAINT positive_distance EXPECT (trip_distance > 0)
    ON VIOLATION DROP ROW
)
AS SELECT *
FROM STREAM(samples.nyctaxi.trips);


-- SILVER 1: Flag suspicious rides

CREATE OR REFRESH STREAMING TABLE silver_flagged_rides
AS
SELECT
  tpep_pickup_datetime,
  fare_amount,
  trip_distance,
  pickup_zip,
  dropoff_zip
FROM STREAM(LIVE.bronze_trips)
WHERE
      (trip_distance < 3 AND fare_amount > 30)
   OR (pickup_zip = dropoff_zip AND fare_amount > 40);


-- SILVER 2: Weekly aggregates

CREATE OR REFRESH MATERIALIZED VIEW silver_weekly_stats
AS
SELECT
  date_trunc('week', tpep_pickup_datetime) AS week,
  AVG(fare_amount) AS avg_fare,
  AVG(trip_distance) AS avg_distance
FROM LIVE.bronze_trips
GROUP BY week;

-- GOLD: Top 3 highest fare suspicious rides

CREATE OR REPLACE MATERIALIZED VIEW gold_top3_rides
AS
SELECT
  f.*,
  w.avg_fare,
  w.avg_distance
FROM LIVE.silver_flagged_rides f
LEFT JOIN LIVE.silver_weekly_stats w
  ON w.week = date_trunc('week', f.tpep_pickup_datetime)
ORDER BY f.fare_amount DESC
LIMIT 3;
