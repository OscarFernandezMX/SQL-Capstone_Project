-- Databricks notebook source
-- MAGIC %md
-- MAGIC #SportsStats
-- MAGIC
-- MAGIC SportsStats is a sports analysis firm partnering with local news and elite personal trainers to provide “interesting” insights to help their partners.  Insights could be patterns/trends highlighting certain groups/events/countries, etc. for the purpose of developing a news story or discovering key health insights.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Writing raw data into Delta Bronze

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Removing files stored at this database. Just to start over every time we create a new cluster.
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/project_db.db/eventsbronze/", True)
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/project_db.db/regionsbronze/", True)
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/project_db.db/eventssilver/", True)
-- MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/project_db.db/regionssilver/", True)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### athlete_events

-- COMMAND ----------

-- Create a temporary view of the uploaded table.

CREATE OR REPLACE TEMPORARY VIEW events (
  ID INT,
  Name STRING,
  Sex STRING,
  Age INT,
  Height INT,
  Weight INT,
  Team STRING,
  NOC STRING,
  Games STRING,
  Year INT,
  Season STRING,
  City STRING,
  Sport STRING,
  Event STRING,
  Medal STRING
)
USING CSV
OPTIONS (
  path "/FileStore/tables/athlete_events.csv",
  header "true",
  inferSchema "true"
);

SELECT * FROM events LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Removing duplicates

-- COMMAND ----------

-- Idenfitying duplicates.

SELECT ID, Year, Event, COUNT(*) AS count
FROM events
GROUP BY ID, Year, Event
HAVING COUNT(*) > 1

-- COMMAND ----------

-- Removing duplicates.

CREATE OR REPLACE TEMPORARY VIEW events_nd
AS
SELECT *
FROM (
  SELECT *, row_number() OVER (PARTITION BY ID, Year, Event ORDER BY ID DESC) AS order_num
  FROM events
)
WHERE order_num = 1;

SELECT * FROM events_nd LIMIT 10

-- COMMAND ----------

-- Total records (including duplicates).

SELECT
  COUNT(*) AS Records,
  COUNT(DISTINCT ID) AS IDs,
  COUNT(DISTINCT Name) AS Names
FROM events

-- COMMAND ----------

-- Total records (without duplicates).

SELECT
  COUNT(*) AS Records,
  COUNT(DISTINCT ID) AS IDs,
  COUNT(DISTINCT Name) AS Names
FROM events_nd

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Creating bronze table

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/project_db.db/

-- COMMAND ----------

-- Create the project database and write the raw data.

CREATE DATABASE IF NOT EXISTS Project_DB;
USE Project_DB;
DROP TABLE IF EXISTS eventsBronze;

CREATE TABLE eventsBronze
USING DELTA
AS
  SELECT * FROM events_nd;

SELECT * FROM eventsBronze LIMIT 10;

-- COMMAND ----------

-- Check the location of the table.

DESCRIBE EXTENDED eventsBronze

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/project_db.db/eventsbronze

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### NOC_regions

-- COMMAND ----------

-- Create a temporary view of the uploaded table.

CREATE OR REPLACE TEMPORARY VIEW regions (
  NOC STRING,
  region STRING,
  notes STRING
)
USING CSV
OPTIONS (
  path "/FileStore/tables/noc_regions.csv",
  header "true"
);

SELECT * FROM regions LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Removing duplicates

-- COMMAND ----------

SELECT
  COUNT(*) AS Records,
  COUNT(DISTINCT NOC) AS NOCs,
  COUNT(DISTINCT region) AS Regions
FROM
  regions

-- COMMAND ----------

-- Idenfitying duplicates.

SELECT NOC, region, COUNT(*) AS count
FROM regions
GROUP BY NOC, region
HAVING COUNT(*) > 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Creating bronze table

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS Project_DB;
USE Project_DB;
DROP TABLE IF EXISTS regionsBronze;

CREATE TABLE regionsBronze
USING DELTA
AS
  SELECT * FROM regions;

SELECT * FROM regionsBronze LIMIT 10;

-- COMMAND ----------

-- Check the location of the table.

DESCRIBE EXTENDED regionsBronze

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/project_db.db/regionsbronze

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Refining bronze tables: write to Delta Silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### athlete_events

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Selecting attributes of interest

-- COMMAND ----------

DROP TABLE IF EXISTS eventsSilver;

CREATE TABLE eventsSilver
USING DELTA
AS
  SELECT
    ID, Sex, Age, Height, Weight, NOC, Year, Sport, Medal
  FROM eventsbronze
  WHERE Season = 'Summer';

  SELECT * FROM eventsSilver LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ####Cleaning data

-- COMMAND ----------

-- Number of null values.

SELECT COUNT(*) AS Nulls
FROM eventsSilver
WHERE
    ID IS NULL OR
    Sex IS NULL OR
    Age IS NULL OR
    Height IS NULL OR
    Weight IS NULL OR
    NOC IS NULL OR
    Year IS NULL OR
    Sport IS NULL

-- COMMAND ----------

-- Update table removing duplicates.

DELETE FROM eventsSilver
WHERE
    ID IS NULL OR
    Sex IS NULL OR
    Age IS NULL OR
    Height IS NULL OR
    Weight IS NULL OR
    NOC IS NULL OR
    Year IS NULL OR
    Sport IS NULL

-- COMMAND ----------

-- There are no null values after modification.

SELECT COUNT(*) AS Nulls
FROM eventsSilver
WHERE
    ID IS NULL OR
    Sex IS NULL OR
    Age IS NULL OR
    Height IS NULL OR
    Weight IS NULL OR
    NOC IS NULL OR
    Year IS NULL OR
    Sport IS NULL

-- COMMAND ----------

-- Removing records where NOC has no region (identified in regions table).

DELETE FROM eventsSilver
WHERE NOC IN ('ROT', 'TUV', 'UNK')

-- COMMAND ----------

-- Check changes made to silver data.

DESCRIBE HISTORY eventsSilver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### NOC_regions

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Selecting attributes of interest

-- COMMAND ----------

-- This is the only time when notes are useful. We'll no longer need it.

SELECT
  *
FROM
  regionsBronze
WHERE 
  notes IS NOT NULL

-- COMMAND ----------

-- In order to analyze by country, we can't consider athletes with unknown country information.
-- We need to remove records from eventsBronze that contain NA region.

SELECT
  *
FROM
  regionsBronze
WHERE 
  region = 'NA'

-- COMMAND ----------

DROP TABLE IF EXISTS regionsSilver;

CREATE TABLE regionsSilver
USING DELTA
AS
  SELECT
    NOC, region
  FROM regionsBronze;

  SELECT * FROM regionsSilver LIMIT 10;

-- COMMAND ----------

DESCRIBE EXTENDED regionssilver

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/project_db.db/regionssilver

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/project_db.db/

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ## Delta Gold

-- COMMAND ----------

-- Adding region column from NOC_regions to events as Country to handle only one table.
-- Select 3 sports and 10 countries (top 10 countties with more data).

SELECT *
FROM (
  SELECT e.ID, e.Sex, e.Age, e.Height, e.Weight, r.region AS Country, e.Year, e.Sport, e.Medal
  FROM eventssilver e
  LEFT JOIN regionssilver r
  ON e.NOC = r.NOC
)
WHERE
    Sport IN ('Athletics', 'Swimming', 'Cycling') AND
    Country IN ('USA', 'Germany', 'Russia', 'UK', 'France', 'Australia', 'Italy', 'Canada', 'Japan', 'China')

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS Project_DB;
USE Project_DB;
DROP TABLE IF EXISTS eventsGold;

CREATE TABLE eventsGold
USING DELTA
AS
  SELECT *
  FROM (
    SELECT e.ID, e.Sex, e.Age, e.Height, e.Weight, r.region AS Country, e.Year, e.Sport, e.Medal
    FROM eventssilver e
    LEFT JOIN regionssilver r
    ON e.NOC = r.NOC
  )
  WHERE
      Sport IN ('Athletics', 'Swimming', 'Cycling') AND
      Country IN ('USA', 'Germany', 'Russia', 'UK', 'France', 'Australia', 'Italy', 'Canada', 'Japan', 'China');

SELECT * FROM eventsGold LIMIT 10;

-- COMMAND ----------

SELECT
  COUNT(DISTINCT Sport),
  COUNT(DISTINCT Country)
FROM eventsGold

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/user/hive/warehouse/project_db.db/eventsgold

-- COMMAND ----------


