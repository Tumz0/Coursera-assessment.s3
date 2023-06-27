# Coursera-assessment.s3
Tarea de calificada 3
Create a new Delta table called fireCallsPartitioned partitioned by Priority.

CREATE OR REPLACE TEMPORARY VIEW fireCallsParquet
USING parquet
OPTIONS (path "/mnt/davis/fire-calls/fire-calls-clean.parquet/")

CREATE DATABASE IF NOT EXISTS Databricks;
USE Databricks;
DROP TABLE IF EXISTS fireCallsPartitioned;
CREATE TABLE fireCallsPartitioned


Question 1
How many folders were created? Enter the number of records you see from the output below (include the _delta_log in your count).

             
path                                                                                                            name                     size                     modificationTime

dbfs:/user/hive/warehouse/databricks.db/firecallspartitioned/_delta_log/_delta_log/               0                            0

1 ROW

Question 2
Delete all the records where City is null. How many records are left in the delta table?

DELETE FROM 
  fireCallsPartitioned
WHERE 
  city 
IS 
  NULL;
SELECT 
  count(*) AS TotalRecords 
FROM 
  firecallspartitioned

Question 3
After you deleted all records where the City is null, how many files were removed? Hint: Look at operationsMetrics in the transaction log using the DESCRIBE HISTORY command.

DESCRIBE HISTORY firecallspartitioned


Question 4
There are quite a few missing Call_Type_Group values. Use the UPDATE command to replace any null values with Non Life-threatening.
After you replace the null values, how many Non Life-threatening call types are there?

SELECT 
  COUNT(*)
FROM 
  firecallspartitioned
WHERE
  Call_Type_Group = "Non Life-threatening"
 

Question 5
Travel back in time to the earliest version of the Delta table (version 0). How many records were there?

SELECT 
  COUNT(*) 
FROM 
  firecallspartitioned
VERSION AS OF 0
 



