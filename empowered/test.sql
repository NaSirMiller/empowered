USE empowered;
GO

SELECT COUNT(*)
FROM fullCensusEstimate;

SELECT DISTINCT state_name FROM fullCensusEstimate;

-- SELECT COUNT(*) AS [Num Estimates], COUNT(DISTINCT variable_id) AS [Num Variables], COUNT(DISTINCT state_fips) AS [Num States]  FROM CensusEstimate;

-- SELECT DISTINCT ce.state_fips, cs.state_name FROM CensusEstimate ce INNER JOIN CensusState cs ON ce.state_fips=cs.state_fips;