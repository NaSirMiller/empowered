# 📊 Census API Cheat Sheet

## Common ACS Variable Codes

These are from **ACS 5-Year Detailed Tables** (most widely used).
| Code | Concept / Meaning |
| ------------------------------------------------ | --------------------------------------------------------------------------- |
| **Population & Households** |
| `B01003_001E` | Total population (estimate) |
| `B25001_001E` | Total housing units |
| `B25002_001E` | Total occupied/vacant housing units |
| `B25010_001E` | Average household size |
| **Income** |
| `B19013_001E` | Median household income |
| `B19301_001E` | Per capita income |
| `B19001_001E` | Household income distribution |
| **B19001 – Household Income Distribution** |
| `B19001_001E` | Total households |
| `B19001_002E` | Less than $10,000 |
| `B19001_003E` | $10,000 to $14,999 |
| `B19001_004E` | $15,000 to $19,999 |
| `B19001_005E` | $20,000 to $24,999 |
| `B19001_006E` | $25,000 to $29,999 |
| `B19001_007E` | $30,000 to $34,999 |
| `B19001_008E` | $35,000 to $39,999 |
| `B19001_009E` | $40,000 to $44,999 |
| `B19001_010E` | $45,000 to $49,999 |
| `B19001_011E` | $50,000 to $59,999 |
| `B19001_012E` | $60,000 to $74,999 |
| `B19001_013E` | $75,000 to $99,999 |
| `B19001_014E` | $100,000 to $124,999 |
| `B19001_015E` | $125,000 to $149,999 |
| `B19001_016E` | $150,000 to $199,999 |
| `B19001_017E` | $200,000 or more |
| **Housing Costs** |
| `B25064_001E` | Median gross rent |
| `B25077_001E` | Median home value |
| `B25070_001E` | Gross rent as % of household income |
| **Education** |
| `B15003_017E` | High school graduate (educational attainment) |
| `B15003_022E` | Bachelor’s degree |
| `B15003_023E` | Master’s degree |
| `B15003_025E` | Doctorate degree |
| **B14007 – School Enrollment** |
| `B14007_001E` | Total population 3 years and over |
| `B14007_002E` | Enrolled in nursery school / preschool |
| `B14007_003E` | Enrolled in kindergarten |
| `B14007_004E` | Enrolled in grade 1 |
| `B14007_005E` | Enrolled in grade 2 |
| `B14007_006E` | Enrolled in grade 3 |
| `B14007_007E` | Enrolled in grade 4 |
| `B14007_008E` | Enrolled in grade 5 |
| `B14007_009E` | Enrolled in grade 6 |
| `B14007_010E` | Enrolled in grade 7 |
| `B14007_011E` | Enrolled in grade 8 |
| `B14007_012E` | Enrolled in grade 9 |
| `B14007_013E` | Enrolled in grade 10 |
| `B14007_014E` | Enrolled in grade 11 |
| `B14007_015E` | Enrolled in grade 12 |
| `B14007_016E` | Enrolled in college, undergraduate (less than 1 year) |
| `B14007_017E` | Enrolled in college, undergraduate (1 or more years, no degree) |
| `B14007_018E` | Enrolled in college, bachelor’s degree program |
| `B14007_019E` | Enrolled in graduate school (master’s, professional, doctorate) |
| **B14006 – Poverty Status by School Enrollment** |
| `B14006_001E` | Total population 3 years and over for whom poverty status is determined |
| `B14006_002E` | Total population with income below the poverty level |
| `B14006_003E` | Enrolled in school (below poverty) |
| `B14006_004E` | Enrolled in nursery school / preschool (below poverty) |
| `B14006_005E` | Enrolled in kindergarten (below poverty) |
| `B14006_006E` | Enrolled in grade 1 to grade 4 (below poverty) |
| `B14006_007E` | Enrolled in grade 5 to grade 8 (below poverty) |
| `B14006_008E` | Enrolled in grade 9 to grade 12 (below poverty) |
| `B14006_009E` | Enrolled in college, undergraduate years (below poverty) |
| `B14006_010E` | Enrolled in graduate or professional school (below poverty) |
| `B14006_011E` | Not enrolled in school (below poverty) |
| `B14006_012E` | Total population with income at or above the poverty level |
| `B14006_013E` | Enrolled in school (at or above poverty) |
| `B14006_014E` | Enrolled in nursery school / preschool (at or above poverty) |
| `B14006_015E` | Enrolled in kindergarten (at or above poverty) |
| `B14006_016E` | Enrolled in grade 1 to grade 4 (at or above poverty) |
| `B14006_017E` | Enrolled in grade 5 to grade 8 (at or above poverty) |
| `B14006_018E` | Enrolled in grade 9 to grade 12 (at or above poverty) |
| `B14006_019E` | Enrolled in college, undergraduate years (at or above poverty) |
| `B14006_020E` | Enrolled in graduate or professional school (at or above poverty) |
| `B14006_021E` | Not enrolled in school (at or above poverty) |
| **B09010 – Children & Public Assistance** |
| `B09010_001E` | Total population under 18 years in households |
| `B09010_002E` | Living in household with SSI, cash public assistance, or SNAP |
| `B09010_003E` | In family households, living with SSI/cash/SNAP |
| `B09010_004E` | In married-couple family, living with SSI/cash/SNAP |
| `B09010_005E` | In male householder, no spouse present, family, living with SSI/cash/SNAP |
| `B09010_006E` | In female householder, no spouse present, family, living with SSI/cash/SNAP |
| `B09010_007E` | In nonfamily households, living with SSI/cash/SNAP |
| `B09010_008E` | Living in household with no SSI, cash public assistance, or SNAP |
| `B09010_009E` | In family households, no SSI/cash/SNAP |
| `B09010_010E` | In married-couple family, no SSI/cash/SNAP |
| `B09010_011E` | In male householder, no spouse present, family, no SSI/cash/SNAP |
| `B09010_012E` | In female householder, no spouse present, family, no SSI/cash/SNAP |
| `B09010_013E` | In nonfamily households, no SSI/cash/SNAP |
| **B17001 – Poverty Status by Sex and Age** |
| `B17001_001E` | Total population for whom poverty status is determined |
| `B17001_002E` | Male, total below poverty |
| `B17001_003E` | Male, 15 years |
| `B17001_004E` | Male, 16 and 17 years |
| `B17001_005E` | Male, 18 to 24 years |
| `B17001_006E` | Male, 25 to 34 years |
| `B17001_007E` | Male, 35 to 44 years |
| `B17001_008E` | Male, 45 to 54 years |
| `B17001_009E` | Male, 55 to 64 years |
| `B17001_010E` | Male, 65 to 74 years |
| `B17001_011E` | Male, 75 years and over |
| `B17001_012E` | Female, total below poverty |
| `B17001_013E` | Female, 15 years |
| `B17001_014E` | Female, 16 and 17 years |
| `B17001_015E` | Female, 18 to 24 years |
| `B17001_016E` | Female, 25 to 34 years |
| `B17001_017E` | Female, 35 to 44 years |
| `B17001_018E` | Female, 45 to 54 years |
| `B17001_019E` | Female, 55 to 64 years |
| `B17001_020E` | Female, 65 to 74 years |
| `B17001_021E` | Female, 75 years and over |
| `B17001_022E` | Male, total at or above poverty |
| `B17001_023E` | Male, 15 years |
| `B17001_024E` | Male, 16 and 17 years |
| `B17001_025E` | Male, 18 to 24 years |
| `B17001_026E` | Male, 25 to 34 years |
| `B17001_027E` | Male, 35 to 44 years |
| `B17001_028E` | Male, 45 to 54 years |

_Note_: All B-series table can use the racial codes for estimates.

### Racial Codes

| Code | Description                                      |
| ---- | ------------------------------------------------ |
| A    | White alone                                      |
| B    | Black or African American alone                  |
| C    | American Indian and Alaska Native alone          |
| D    | Asian alone                                      |
| E    | Native Hawaiian and Other Pacific Islander alone |
| F    | Some Other Race alone                            |
| G    | Two or More Races                                |
| H    | White alone, Not Hispanic or Latino              |
| I    | Hispanic or Latino                               |

> 🔑 **Tip:**
>
> - `E` = Estimate
> - `M` = Margin of Error (e.g., `B01003_001M`)

---

## 🗺 State FIPS Codes

Use these with `for=state:` in Census API queries.

| State                | FIPS | State          | FIPS |
| -------------------- | ---- | -------------- | ---- |
| Alabama              | 01   | Montana        | 30   |
| Alaska               | 02   | Nebraska       | 31   |
| Arizona              | 04   | Nevada         | 32   |
| Arkansas             | 05   | New Hampshire  | 33   |
| California           | 06   | New Jersey     | 34   |
| Colorado             | 08   | New Mexico     | 35   |
| Connecticut          | 09   | New York       | 36   |
| Delaware             | 10   | North Carolina | 37   |
| District of Columbia | 11   | North Dakota   | 38   |
| Florida              | 12   | Ohio           | 39   |
| Georgia              | 13   | Oklahoma       | 40   |
| Hawaii               | 15   | Oregon         | 41   |
| Idaho                | 16   | Pennsylvania   | 42   |
| Illinois             | 17   | Rhode Island   | 44   |
| Indiana              | 18   | South Carolina | 45   |
| Iowa                 | 19   | South Dakota   | 46   |
| Kansas               | 20   | Tennessee      | 47   |
| Kentucky             | 21   | Texas          | 48   |
| Louisiana            | 22   | Utah           | 49   |
| Maine                | 23   | Vermont        | 50   |
| Maryland             | 24   | Virginia       | 51   |
| Massachusetts        | 25   | Washington     | 53   |
| Michigan             | 26   | West Virginia  | 54   |
| Minnesota            | 27   | Wisconsin      | 55   |
| Mississippi          | 28   | Wyoming        | 56   |
| Missouri             | 29   | Puerto Rico    | 72   |

---

## 📅 Dataset Years

Each dataset has specific years you can query by replacing `YEAR` in the URL.

| Dataset                                     | Coverage                                   | Available Years                        |
| ------------------------------------------- | ------------------------------------------ | -------------------------------------- |
| **ACS 1-Year (`acs1`)**                     | Large geographies (pop. 65,000+)           | 2005 → most recent year                |
| **ACS 5-Year (`acs5`)**                     | All geographies down to block group        | 2009–2013 → most recent 5-year release |
| **ACS PUMS (Public Use Microdata Sample)**  | Person- and housing-level microdata        | 2005 → most recent year                |
| **Decennial Census (`dec/sf1`)**            | 100% count data (demographics, households) | 2000, 2010                             |
| **Decennial Census (`dec/pl`)**             | Redistricting data                         | 2020                                   |
| **Population Estimates (`pep/population`)** | Annual population estimates                | Varies (2000 → most recent)            |
| **Economic Census**                         | Businesses, economy by industry            | Every 5 years (e.g., 2012, 2017, 2022) |
| **CPS (Current Population Survey)**         | Labor force, employment                    | Varies by survey supplement            |

---

---

### ACS-1 vs ACS-5

- ACS-1 for more recent data
- ACS-1 only samples from populations >65,000
- ACS-5 for more precise measurements
- ACS-5 does **not** have a population minimum

---

## ✅ Example Queries

### ACS 5-Year (2021) – Median Household Income in New York

```http
https://api.census.gov/data/2021/acs/acs5?get=B19013_001E&for=state:36&key=YOUR_KEY

```
