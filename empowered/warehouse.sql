--------------------
-- This script is written in TSQL for SQL Server,
-- it also builds data warehouses for Census tables built in ingest.py
--------------------

USE empowered;
GO

-------------------------
-- Create staging tables
-------------------------
SELECT *
INTO StagingCensusDataset
FROM CensusDataset;

