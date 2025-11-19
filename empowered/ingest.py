from dotenv import load_dotenv
from getpass import getpass
from logger_setup import set_logger, get_logger
from empowered.models.sql.sql_client import SQLClient
from empowered.models.pydantic.census import VariableCreate
import os
import requests
import fire

ACS_VARIABLES = [
    # Population & Households
    "B01003_001E",
    "B25001_001E",
    "B25002_001E",
    "B25010_001E",
    # Income
    "B19013_001E",
    "B19301_001E",
    "B19001_001E",
    "B19001_002E",
    "B19001_003E",
    "B19001_004E",
    "B19001_005E",
    "B19001_006E",
    "B19001_007E",
    "B19001_008E",
    "B19001_009E",
    "B19001_010E",
    "B19001_011E",
    "B19001_012E",
    "B19001_013E",
    "B19001_014E",
    "B19001_015E",
    "B19001_016E",
    "B19001_017E",
    # Housing Costs
    "B25064_001E",
    "B25077_001E",
    "B25070_001E",
    # Education
    "B15003_017E",
    "B15003_022E",
    "B15003_023E",
    "B15003_025E",
    # School Enrollment (B14007)
    "B14007_001E",
    "B14007_002E",
    "B14007_003E",
    "B14007_004E",
    "B14007_005E",
    "B14007_006E",
    "B14007_007E",
    "B14007_008E",
    "B14007_009E",
    "B14007_010E",
    "B14007_011E",
    "B14007_012E",
    "B14007_013E",
    "B14007_014E",
    "B14007_015E",
    "B14007_016E",
    "B14007_017E",
    "B14007_018E",
    "B14007_019E",
    # Poverty Status by School Enrollment (B14006)
    "B14006_001E",
    "B14006_002E",
    "B14006_003E",
    "B14006_004E",
    "B14006_005E",
    "B14006_006E",
    "B14006_007E",
    "B14006_008E",
    "B14006_009E",
    "B14006_010E",
    "B14006_011E",
    "B14006_012E",
    "B14006_013E",
    "B14006_014E",
    "B14006_015E",
    "B14006_016E",
    "B14006_017E",
    "B14006_018E",
    "B14006_019E",
    "B14006_020E",
    "B14006_021E",
    # Children & Public Assistance (B09010)
    "B09010_001E",
    "B09010_002E",
    "B09010_003E",
    "B09010_004E",
    "B09010_005E",
    "B09010_006E",
    "B09010_007E",
    "B09010_008E",
    "B09010_009E",
    "B09010_010E",
    "B09010_011E",
    "B09010_012E",
    "B09010_013E",
    # Poverty Status by Sex and Age (B17001)
    "B17001_001E",
    "B17001_002E",
    "B17001_003E",
    "B17001_004E",
    "B17001_005E",
    "B17001_006E",
    "B17001_007E",
    "B17001_008E",
    "B17001_009E",
    "B17001_010E",
    "B17001_011E",
    "B17001_012E",
    "B17001_013E",
    "B17001_014E",
    "B17001_015E",
    "B17001_016E",
    "B17001_017E",
    "B17001_018E",
    "B17001_019E",
    "B17001_020E",
    "B17001_021E",
    "B17001_022E",
    "B17001_023E",
    "B17001_024E",
    "B17001_025E",
    "B17001_026E",
    "B17001_027E",
    "B17001_028E",
]


def main():
    set_logger()
    load_dotenv()

    datasets = [
        {"code": "acs1", "frequency": "annual"},
        {"code": "acs5", "frequency": "quinquennial"},
    ]

    for dataset in datasets:
        requests.post("http://localhost:8000/census/dataset", json=dataset)
        years_response = requests.get(
            f"http://localhost:8000/census/{dataset["code"]}/years"
        )
        years = years_response["years_available"]
        for variable in ACS_VARIABLES:
            requests.post("http://localhost:8000/census/variable", json={})


if __name__ == "__main__":
    main()
