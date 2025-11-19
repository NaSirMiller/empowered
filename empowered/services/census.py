from aiohttp import ClientSession
from bs4 import BeautifulSoup
from functools import lru_cache
import re
import requests
from requests import Response
from typing import List, Dict
from empowered.logger_setup import get_logger
from empowered.utils import get_census_api_key

logger = get_logger(name=__name__)

ACS5_URL = "https://www.census.gov/data/developers/data-sets/acs-5year.html"
ACS1_URL = "https://www.census.gov/data/developers/data-sets/acs-1year.html"


class CensusAPIError(Exception):
    pass


def get_estimate(
    acs_id: int,
    year: int,
    variables: List[str],
    batch_size: int = 50,
    state_ids: List[int] | None = None,
    county_ids: List[int] = None,
    place_ids: List[int] = None,
):
    if state_ids is None and county_ids is None and place_ids is None:
        raise CensusAPIError(
            f"No geography was specified. Please provide one of the list of state fips, county fips, and place fips: {e}"
        )
    variables_stringified: List[str] = ",".join(variables)
    api_key = get_census_api_key()

    # Census API only allows one level of geography, so select the lowest level first (place <- county <- state)
    geography_ids: List[str] = []
    geography_level: str = None
    if place_ids is not None:
        geography_ids = place_ids
        geography_level = "place"
    elif county_ids is not None:
        county_ids = geography_ids
        geography_level = "county"
    else:
        geography_ids = state_ids
        geography_level = "state"

    url = f"https://api.census.gov/data/{year}/acs/acs{acs_id}?get={variables_stringified}&for={geography_level}:{{gid}}&key={api_key}"

    response: Response = None
    if "*" in geography_ids:
        # Every available item in the geography level is fetched
        response = requests.get(url)
    else:
        current_batch: List[int]
        for current_index in range(0, len(geography_ids), batch_size):
            current_batch = geography_ids[current_index : current_index + batch_size]


# ------------------ HTML Parsing ------------------
def get_html(url: str) -> str:
    response = requests.get(url)
    if response.status_code != 200:
        raise CensusAPIError(
            f"Request at {url} failed with status code={response.status_code}"
        )
    return response.text


def parse_years_from_html(html: str) -> List[int]:
    soup = BeautifulSoup(html, "html.parser")
    headers = soup.find_all("h1")
    if not headers:
        raise CensusAPIError("No h1 headers found in HTML")

    for header in headers:
        matches = re.findall(r"\((.*?)\)", header.get_text())
        for match in matches:
            try:
                start, end = (int(y.strip()) for y in match.split("-"))
                return list(range(start, end + 1))
            except Exception:
                logger.info(f"Cannot parse years from match: {match}")
    raise CensusAPIError("Could not find valid year range in HTML")


# ------------------ Years ------------------
def get_years(acs_id: int) -> List[int]:
    if acs_id == 1:
        html = get_html(ACS1_URL)
    elif acs_id == 5:
        html = get_html(ACS5_URL)
    else:
        raise ValueError("acs_id must be 1 or 5")
    return parse_years_from_html(html)


# ------------------ Groups ------------------
@lru_cache(maxsize=32)
def get_groups(acs_id: int, year: int) -> List[Dict]:
    url = f"https://api.census.gov/data/{year}/acs/acs{acs_id}/groups/"
    try:
        response = requests.get(url)
        response.raise_for_status()
        groups = response.json()["groups"]
        return [{"id": g["name"], "purpose": g["description"]} for g in groups]
    except Exception as e:
        raise CensusAPIError(f"Failed to fetch groups: {e}")


@lru_cache(maxsize=32)
def get_group_ids(acs_id: int, year: int) -> set:
    groups = get_groups(acs_id, year)
    return {g["id"] for g in groups}


def validate_group_id(acs_id: int, year: int, group_id: str) -> bool:
    return group_id in get_group_ids(acs_id, year)


# ------------------ Variables ------------------
@lru_cache(maxsize=64)
def get_variables(acs_id: int, year: int, group_id: str) -> List[Dict]:
    url = f"https://api.census.gov/data/{year}/acs/acs{acs_id}/groups/{group_id}.json"
    try:
        response = requests.get(url)
        response.raise_for_status()
        variables = response.json()["variables"]
        return [{"id": vid, "purpose": v["label"]} for vid, v in variables.items()]
    except Exception as e:
        raise CensusAPIError(f"Failed to fetch variables for group {group_id}: {e}")


# -------------------- Geography ----------------
@lru_cache(maxsize=128)
def get_states(acs_id: int, year: int, state_name: str | None) -> Dict:
    url = f"https://api.census.gov/data/{year}/acs/acs{acs_id}?get=NAME&for=state:*"
    try:
        response = requests.get(url)
        response.raise_for_status()
        states = {}
        for state in response.json()[1:]:
            name = state[0]
            fips_code = state[1]
            states[name] = {"fips": fips_code}
        if state_name is not None and state_name not in states:
            raise CensusAPIError(
                f"Failed to fetch the states-FIPS code mapping for {state_name}"
            )
        if state_name is not None:
            return [{"states": {state_name: states[state_name]}}]

        return [{"states": states}]
    except Exception as e:
        raise CensusAPIError(f"Failed to fetch the states FIPS code: {e}")


@lru_cache(maxsize=256)
def get_counties(
    acs_id: int,
    year: int,
    fips_code: int,
    county_name: str | None,
):
    url = f"https://api.census.gov/data/{year}/acs/acs{acs_id}?get=NAME&for=county:*&in=state:{fips_code}"
    try:
        # Convert state requested to fips code
        response = requests.get(url)
        response.raise_for_status()
        counties = {}
        for county in response.json()[1:]:
            name = county[0]
            county_code = county[-1]
            counties[name] = {"fips_code": county_code}
        if county_name is not None and county_name not in counties:
            raise CensusAPIError(
                f"Failed to fetch the counties-FIPS code mapping for {county_name} in "
            )
        if county_name is not None:
            return [
                {"state": fips_code, "counties": {county_name: counties[county_name]}}
            ]
        return [{"state": fips_code, "counties": counties}]
    except Exception as e:
        raise CensusAPIError(f"Failed to fetch the counties: {e}")


@lru_cache(maxsize=128)
def get_places(
    acs_id: int,
    year: int,
    state_fips_code: int,
    place_name: str | None = None,
):
    url = (
        f"https://api.census.gov/data/{year}/acs/acs{acs_id}"
        f"?get=NAME&for=place:*&in=state:{state_fips_code}"
    )

    try:
        response = requests.get(url)
        response.raise_for_status()

        data = response.json()
        header = data[0]
        rows = data[1:]

        places = {}

        for row in rows:
            name = row[0]
            state_fips = row[1]
            place_fips = row[2]

            places[name] = {
                "state_fips": state_fips,
                "place_fips": place_fips,
            }

        if place_name:
            if place_name not in places:
                raise CensusAPIError(
                    f"Place '{place_name}' not found in state {state_fips_code}"
                )
            return [
                {"state": state_fips_code, "places": {place_name: places[place_name]}}
            ]

        return [{"state": state_fips_code, "places": places}]

    except Exception as e:
        raise CensusAPIError(f"Failed to fetch place list: {e}")


# -------------------- Estimates ----------------


async def get_estimate(
    session: ClientSession,
    url: str,
    gid: int,
    geography_level: str,
):
    async with session.get(url.format(gid, geography_level)) as resp:
        resp.raise_for_status()
        return await resp.json()
