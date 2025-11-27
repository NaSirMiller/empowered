USE empowered;
GO

--------------------------------------------------------------------------------------------------
-- Questions
-- 1. How many counties are in each state?
-- 2. What is the total population for each state?
-- 3. What states have the highest proportion of people with a bachelor's degree or higher?
-- 4. Which counties have the highest median gross rent and home value?
-- 5. Which states have the lowest high school attainment rates?
---------------------------------------------------------------------------------------------------

-- 1. How many counties are in each state?
SELECT state_name,
        COUNT( DISTINCT place_fips) AS [Num Cities]
FROM fullCensusEstimate
GROUP BY state_name
ORDER BY state_name ASC;

-- 2. What is the total population for each state?
SELECT state_name,
        SUM(estimate) AS [Total Population]
FROM fullCensusEstimate
WHERE variable_id = 'B01003_001E'
GROUP BY state_name
ORDER BY state_name ASC;

-- 3. What states have the highest proportion of people with a bachelor's degree or higher?
SELECT
    state_name,
    SUM(
        CASE 
            WHEN variable_id LIKE 'B15003_02%' THEN estimate 
            ELSE 0 
        END
    ) AS bachelors_or_higher,

    SUM(
        CASE 
            WHEN variable_id = 'B01003_001E' THEN estimate 
            ELSE 0 
        END
    ) AS total_population,

    CAST(
        SUM(
            CASE 
                WHEN variable_id LIKE 'B15003_02%' THEN estimate 
                ELSE 0 
            END
        ) AS FLOAT
    )
    / NULLIF(
        SUM(
            CASE 
                WHEN variable_id = 'B01003_001E' THEN estimate 
                ELSE 0 
            END
        ),
        0
    ) AS proportion_bachelors
FROM fullCensusEstimate
WHERE variable_id = 'B01003_001E' OR variable_id LIKE 'B15003_02%'
GROUP BY state_name
ORDER BY proportion_bachelors DESC;

-- 4. Which counties have the highest median gross rent and home value?
SELECT TOP 25 rent.state_name,
    rent.place_name,
    rent.estimate AS [Median Gross Rent],
    home.estimate AS [Median Home Value]
FROM fullCensusEstimate AS rent
JOIN fullCensusEstimate AS home
    ON  rent.state_name = home.state_name
    AND rent.place_name = home.place_name
WHERE rent.variable_id = 'B25064_001E' AND home.variable_id = 'B25077_001E'
ORDER BY rent.estimate, home.estimate, rent.place_name, rent.state_name ASC;  

-- 5. Which places have the lowest high school attainment rates?
SELECT 
    pop.variable_id AS [vid (pop)],
    hs.variable_id AS [vid (HS)],
    hs.estimate AS [HS Attainment],
    pop.estimate AS [Total Population],
    (CASE 
        WHEN pop.estimate = 0 THEN 0
        ELSE CAST(hs.estimate AS FLOAT) / pop.estimate 
    END) AS [HS Attainment Proportion]
FROM fullCensusEstimate as pop
INNER JOIN fullCensusEstimate as hs
    ON pop.place_name = hs.place_name AND pop.state_name = hs.state_name
WHERE pop.variable_id = 'B01003_001E' AND hs.variable_id = 'B15003_017E'
ORDER BY [HS Attainment Proportion] DESC;

SELECT
    hs.state_name,
    SUM(hs.estimate) AS hs_attainment_total,
    SUM(pop.estimate) AS population_total,
    CAST(SUM(hs.estimate) AS FLOAT) / NULLIF(SUM(pop.estimate), 0)
        AS hs_attainment_proportion
FROM fullCensusEstimate AS pop
JOIN fullCensusEstimate AS hs
    ON pop.state_name = hs.state_name
   AND pop.place_name = hs.place_name

WHERE pop.variable_id = 'B01003_001E'
  AND hs.variable_id = 'B15003_017E'

GROUP BY hs.state_name 
ORDER BY hs_attainment_proportion ASC; 
