import asyncio
import aiohttp
from concurrent.futures import ThreadPoolExecutor
from dotenv import load_dotenv
from typing import Callable, Dict, List

from empowered.utils.logger_setup import set_logger, get_logger
from empowered.repositories.census.datasets_repo import DatasetRepository
from empowered.repositories.census.estimates_repo import CensusEstimateRepository
from empowered.repositories.census.geography_repo import GeographyRepository
from empowered.repositories.census.groups_repo import GroupsRepository
from empowered.repositories.census.variables_repo import VariablesRepository
from empowered.repositories.census.years_available_repo import YearsAvailableRepository
from empowered.services.census_service import (
    get_counties,
    get_estimates,
    get_groups,
    get_places,
    get_states,
    get_variables,
)

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
]  # Up to 50 at once
ACS_GROUPS = list(set([var.split("_")[0] for var in ACS_VARIABLES]))

BASE_URL = "http://localhost:8000/census/"

ACS_DATASETS = [
    # {"code": "acs", "frequency": 1, "id": "acs1"},
    {"code": "acs", "frequency": 5, "id": "acs5"},
]
YEARS = [2019, 2024]
DEFAULT_EXECUTOR = ThreadPoolExecutor(max_workers=10)
VARIABLE_EXECUTOR = ThreadPoolExecutor(max_workers=50)


set_logger()
load_dotenv()
logger = get_logger()
logger.info("Completed logging setup.")


class IngestError(Exception):
    pass


async def main():
    pass


async def arun(
    func: Callable,
    executor: ThreadPoolExecutor,
    *args,
    **kwargs,
):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, lambda: func(*args, **kwargs))


async def load_geography(dataset, year) -> List[Dict]:
    acs_id = dataset["frequency"]
    states_response = await arun(
        func=get_states,
        executor=DEFAULT_EXECUTOR,
        acs_id=acs_id,
        year=year,
    )
    assert isinstance(states_response, dict)
    states = states_response["states"]

    async def fetch_state_geo(state: dict) -> dict:
        state_fips = state["state_fips"]
        state_name = state["state_name"]

        # Run counties and places concurrently for this state
        acounties_task = arun(
            func=get_counties,
            executor=DEFAULT_EXECUTOR,
            acs_id=acs_id,
            year=year,
            state_fips=state_fips,
        )
        aplaces_task = arun(
            func=get_places,
            executor=DEFAULT_EXECUTOR,
            acs_id=acs_id,
            year=year,
            state_fips=state_fips,
        )

        acounties, aplaces = await asyncio.gather(acounties_task, aplaces_task)

        return {
            "state_fips": state_fips,
            "state_name": state_name,
            "counties": acounties,
            "places": aplaces,
        }

    # Run all states concurrently
    geography = await asyncio.gather(*(fetch_state_geo(state) for state in states))

    return geography


async def ingest_geography(
    dataset, year, geo_repo: GeographyRepository, year_repo: YearsAvailableRepository
) -> None:
    geography = await ingest_geography(dataset=dataset, year=year)
    dataset_id = dataset["id"]
    year_id = year_repo.get_years(dataset_id=dataset_id, year=year)

    states = [
        {"state_fips": state["state_fips"], "state_name": state["state_name"]}
        for state in geography
    ]
    logger.info("Inserting states into database..")
    geo_repo.insert_states(states=states, dataset_id=dataset_id, year_id=year_id)

    atasks = []
    logger.info("Creating place and county routines...")
    for state in geography:
        counties = state["counties"]
        places = state["places"]
        atasks.append(
            arun(
                func=geo_repo.insert_counties,
                executor=DEFAULT_EXECUTOR,
                counties=counties,
                dataset_id=dataset_id,
                year_id=year_id,
            )
        )
        atasks.append(
            arun(
                func=geo_repo.insert_places,
                executor=DEFAULT_EXECUTOR,
                places=places,
                dataset_id=dataset_id,
                year_id=year_id,
            )
        )

    logger.info("Inserting places and counties into database...")
    await asyncio.gather(*atasks)

    logger.info("Geography data ingested.")


async def ingest(dataset, year) -> None:
    acs_id = dataset["frequency"]
    agroups = await arun(
        func=get_groups, executor=DEFAULT_EXECUTOR, acs_id=acs_id, year=year
    )


if __name__ == "__main__":
    asyncio.run(main())
