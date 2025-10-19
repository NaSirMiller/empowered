# ETL Design

## Notes

1. How to get tables dynamically?: To get a list of availible tables & variables for a given census, use `groups.json`
   Example endpoint: `https://api.census.gov/data/2021/acs/acs5/groups.json`
2. How to get available years dynamically?: Scrape the <h1> tag and take vlaues in `()` and not `-` [ACS 5 years](https://www.census.gov/data/developers/data-sets/acs-5year.html) & [ACS 1 year](https://www.census.gov/data/developers/data-sets/acs-1year.html)
3. Use SQLServer for database for interoptability with applied data science and data resource management
4. Store original responses and cleaned data
5. Include endpoint used to get the data
6. 500 requests per day limit

## Pipeline

### ACS-1

1. Get set of dates available
2. Get set of groups (tables) available for given date
3. for every group, get their variables
4. for every variable make a request for their estimate and margin of error

## ACS-5

1. Get set of dates available
2. Get set of groups (tables) available for given date
3. for every group, get their variables
4. for every variable make a request for their estimate and margin of error

Run **ACS-1** and **ACS-5** concurrently and construct e
