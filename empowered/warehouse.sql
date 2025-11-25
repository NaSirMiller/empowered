--------------------
-- This script is written in TSQL for SQL Server,
-- it also builds data warehouses for Census tables built in ingest.py
--------------------

USE empowered;
GO

---------------------------
-- Drop fact tables
---------------------------
DROP TABLE IF EXISTS fullCensusEstimate;
DROP TABLE IF EXISTS factCensusEstimate;
GO

---------------------------
-- Drop dimension tables
---------------------------
DROP TABLE IF EXISTS dimCensusPlace;
DROP TABLE IF EXISTS dimCensusCounty;
DROP TABLE IF EXISTS dimCensusState;
DROP TABLE IF EXISTS dimCensusVariable;
DROP TABLE IF EXISTS dimCensusGroup;
DROP TABLE IF EXISTS dimCensusAvailableYear;
DROP TABLE IF EXISTS dimCensusDataset;
GO

---------------------------
-- Drop staging tables
---------------------------
DROP TABLE IF EXISTS StagingCensusDataset;
DROP TABLE IF EXISTS StagingCensusAvailableYear;
DROP TABLE IF EXISTS StagingCensusGroup;
DROP TABLE IF EXISTS StagingCensusVariable;
DROP TABLE IF EXISTS StagingCensusState;
DROP TABLE IF EXISTS StagingCensusCounty;
DROP TABLE IF EXISTS StagingCensusPlace;
DROP TABLE IF EXISTS StagingCensusEstimate;
DROP TABLE IF EXISTS StagingCensusMock;
GO

-------------------------
-- Create staging tables
-------------------------

SELECT *
INTO StagingCensusDataset
FROM CensusDataset;

SELECT *
INTO StagingCensusAvailableYear
FROM CensusAvailableYear;

SELECT *
INTO StagingCensusGroup
FROM CensusGroup;

SELECT *
INTO StagingCensusVariable
FROM CensusVariable;

SELECT *
INTO StagingCensusState
FROM CensusState;

SELECT *
INTO StagingCensusCounty
FROM CensusCounty;

SELECT *
INTO StagingCensusPlace
FROM CensusPlace;

SELECT *
INTO StagingCensusEstimate
FROM CensusEstimate;

SELECT *
INTO StagingCensusMock
FROM CensusMock;


---------------------------
-- Create dimension tables
----------------------------

-- dimCensusDataset
CREATE TABLE dimCensusDataset (
    dataset_id VARCHAR(255) PRIMARY KEY,
    code VARCHAR(255),
    frequency VARCHAR(255)
);

INSERT INTO dimCensusDataset (dataset_id, code, frequency)
SELECT s.id, s.code, s.frequency
FROM StagingCensusDataset s
WHERE NOT EXISTS (
    SELECT 1
    FROM dimCensusDataset d
    WHERE d.dataset_id = s.id
);


-- dimCensusAvailableYear
CREATE TABLE dimCensusAvailableYear (
    year_id INT PRIMARY KEY,
    dataset_id VARCHAR(255),
    year INT,
    UNIQUE(dataset_id, year)
);

INSERT INTO dimCensusAvailableYear (year_id, dataset_id, year)
SELECT s.id, s.dataset_id, s.year
FROM StagingCensusAvailableYear s
WHERE NOT EXISTS (
    SELECT 1
    FROM dimCensusAvailableYear d
    WHERE d.year_id = s.id
);


-- dimCensusGroup
CREATE TABLE dimCensusGroup (
    group_id VARCHAR(255),
    [description] VARCHAR(255),
    dataset_id VARCHAR(255),
    year_id INT,
    variables_count INT,
    PRIMARY KEY (group_id, dataset_id, year_id)
);

INSERT INTO dimCensusGroup (group_id, [description], dataset_id, year_id, variables_count)
SELECT s.id, s.[description], s.dataset_id, s.year_id, s.variables_count
FROM StagingCensusGroup s
WHERE NOT EXISTS (
    SELECT 1
    FROM dimCensusGroup d
    WHERE d.group_id = s.id
      AND d.dataset_id = s.dataset_id
      AND d.year_id = s.year_id
);


-- dimCensusVariable
CREATE TABLE dimCensusVariable (
    variable_id VARCHAR(255),
    [description] VARCHAR(255),
    group_id VARCHAR(255),
    dataset_id VARCHAR(255),
    year_id INT,
    PRIMARY KEY (variable_id, dataset_id, year_id)
);

INSERT INTO dimCensusVariable (variable_id, [description], group_id, dataset_id, year_id)
SELECT s.id, s.[description], s.group_id, s.dataset_id, s.year_id
FROM StagingCensusVariable s
WHERE NOT EXISTS (
    SELECT 1
    FROM dimCensusVariable d
    WHERE d.variable_id = s.id
      AND d.dataset_id = s.dataset_id
      AND d.year_id = s.year_id
);


-- dimCensusState
CREATE TABLE dimCensusState (
    state_fips INT,
    state_name VARCHAR(255),
    dataset_id VARCHAR(255),
    year_id INT,
    PRIMARY KEY (state_fips, dataset_id, year_id)
);

INSERT INTO dimCensusState (state_fips, state_name, dataset_id, year_id)
SELECT s.state_fips, s.state_name, s.dataset_id, s.year_id
FROM StagingCensusState s
WHERE NOT EXISTS (
    SELECT 1
    FROM dimCensusState d
    WHERE d.state_fips = s.state_fips
      AND d.dataset_id = s.dataset_id
      AND d.year_id = s.year_id
);


-- dimCensusCounty
CREATE TABLE dimCensusCounty (
    county_fips INT,
    county_name VARCHAR(255),
    state_fips INT,
    dataset_id VARCHAR(255),
    year_id INT,
    PRIMARY KEY (county_fips, state_fips, dataset_id, year_id)
);

INSERT INTO dimCensusCounty (county_fips, county_name, state_fips, dataset_id, year_id)
SELECT s.county_fips, s.county_name, s.state_fips, s.dataset_id, s.year_id
FROM StagingCensusCounty s
WHERE NOT EXISTS (
    SELECT 1
    FROM dimCensusCounty d
    WHERE d.county_fips = s.county_fips
      AND d.state_fips = s.state_fips
      AND d.dataset_id = s.dataset_id
      AND d.year_id = s.year_id
);

-- dimCensusPlace
CREATE TABLE dimCensusPlace (
    place_fips INT,
    place_name VARCHAR(255),
    state_fips INT,
    dataset_id VARCHAR(255),
    year_id INT,
    PRIMARY KEY (place_fips, state_fips, dataset_id, year_id)
);
INSERT INTO dimCensusPlace (place_fips, place_name, state_fips, dataset_id, year_id)
SELECT s.place_fips, s.place_name, s.state_fips, s.dataset_id, s.year_id
FROM StagingCensusPlace s
WHERE NOT EXISTS (
    SELECT 1
    FROM dimCensusPlace d
    WHERE d.place_fips = s.place_fips
      AND d.state_fips = s.state_fips
      AND d.dataset_id = s.dataset_id
      AND d.year_id = s.year_id
);

---------------------------
-- Create fact table
----------------------------

CREATE TABLE factCensusEstimate(
    fact_id INT IDENTITY(1,1) PRIMARY KEY,
    place_fips INT NULL,
    county_fips INT NULL,
    state_fips INT NOT NULL,
    dataset_id VARCHAR(255) NOT NULL,
    year_id INT NOT NULL,
    group_id VARCHAR(255) NOT NULL,
    variable_id VARCHAR(255) NOT NULL,
    estimate FLOAT NOT NULL,
    margin_of_error FLOAT NULL,
    UNIQUE(place_fips, county_fips, state_fips, dataset_id, year_id, group_id, variable_id)
);

INSERT INTO factCensusEstimate
(
    place_fips,
    county_fips,
    state_fips,
    dataset_id,
    year_id,
    group_id,
    variable_id,
    estimate,
    margin_of_error
)
SELECT 
    s.place_fips,
    s.county_fips,
    s.state_fips,
    s.dataset_id,
    s.year_id,
    s.group_id,
    s.variable_id,
    s.estimate,
    s.margin_of_error
FROM StagingCensusEstimate s
WHERE NOT EXISTS (
    SELECT 1
    FROM factCensusEstimate f
    WHERE f.place_fips = s.place_fips
      AND f.county_fips = s.county_fips
      AND f.state_fips = s.state_fips
      AND f.dataset_id = s.dataset_id
      AND f.year_id = s.year_id
      AND f.group_id = s.group_id
      AND f.variable_id = s.variable_id
);

CREATE TABLE fullCensusEstimate (
    fact_id INT,
    dataset_id VARCHAR(255),
    dataset_code VARCHAR(255),
    year_id INT,
    [year] INT,
    group_id VARCHAR(255),
    group_description VARCHAR(255),
    variable_id VARCHAR(255),
    variable_description VARCHAR(255),
    state_fips INT,
    state_name VARCHAR(255),
    county_fips INT NULL,
    county_name VARCHAR(255) NULL,
    place_fips INT NULL,
    place_name VARCHAR(255) NULL,
    estimate FLOAT,
    margin_of_error FLOAT NULL
);

INSERT INTO fullCensusEstimate
(
    fact_id,
    dataset_id,
    dataset_code,
    year_id,
    [year],
    group_id,
    group_description,
    variable_id,
    variable_description,
    state_fips,
    state_name,
    county_fips,
    county_name,
    place_fips,
    place_name,
    estimate,
    margin_of_error
)
SELECT 
    f.fact_id,
    f.dataset_id,
    ds.code AS dataset_code,
    f.year_id,
    dy.year AS [year],
    f.group_id,
    dg.[description] AS group_description,
    f.variable_id,
    dv.[description] AS variable_description,
    f.state_fips,
    ds2.state_name,
    f.county_fips,
    dc.county_name,
    f.place_fips,
    dp.place_name,
    f.estimate,
    f.margin_of_error
FROM factCensusEstimate f
JOIN dimCensusDataset ds ON f.dataset_id = ds.dataset_id
JOIN dimCensusAvailableYear dy ON f.year_id = dy.year_id
JOIN dimCensusGroup dg ON f.group_id = dg.group_id AND f.dataset_id = dg.dataset_id AND f.year_id = dg.year_id
JOIN dimCensusVariable dv ON f.variable_id = dv.variable_id AND f.dataset_id = dv.dataset_id AND f.year_id = dv.year_id
JOIN dimCensusState ds2 ON f.state_fips = ds2.state_fips AND f.dataset_id = ds2.dataset_id AND f.year_id = ds2.year_id
LEFT JOIN dimCensusCounty dc ON f.county_fips = dc.county_fips AND f.state_fips = dc.state_fips AND f.dataset_id = dc.dataset_id AND f.year_id = dc.year_id
LEFT JOIN dimCensusPlace dp ON f.place_fips = dp.place_fips AND f.state_fips = dp.state_fips AND f.dataset_id = dp.dataset_id AND f.year_id = dp.year_id;
