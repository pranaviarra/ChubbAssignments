-- Databricks notebook source
-- MAGIC %md
-- MAGIC VALIDATIONS

-- COMMAND ----------

--all records of bronze_table that has raw data ingested into it
select * from bronze_trips

-- COMMAND ----------

--all records of silver level with flagged ride details
select * from silver_flagged_rides

-- COMMAND ----------

--all records from silver level with weekly status data(agregations)
select * from silver_weekly_stats

-- COMMAND ----------

--the top 3 records in gold level
select * from gold_top3_rides

-- COMMAND ----------

--top 10 records from weekly stats
SELECT * FROM silver_weekly_stats ORDER BY week LIMIT 10;

-- COMMAND ----------

