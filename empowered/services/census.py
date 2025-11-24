from bs4 import BeautifulSoup
from functools import lru_cache
import re
import requests
from requests import Response
from typing import List, Dict, Optional
from empowered.logger_setup import get_logger
from empowered.utils import get_census_api_key

logger = get_logger(name=__name__)

ACS5_URL = "https://www.census.gov/data/developers/data-sets/acs-5year.html"
ACS1_URL = "https://www.census.gov/data/developers/data-sets/acs-1year.html"


class CensusAPIError(Exception):
    pass


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
def get_groups(
    acs_id: int,
    year: int,
    api_key: str = get_census_api_key(),
) -> List[Dict]:
    url = f"https://api.census.gov/data/{year}/acs/acs{acs_id}/groups/&key={api_key}"
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
def get_variables(
    acs_id: int,
    year: int,
    group_id: str,
    api_key: str = get_census_api_key(),
) -> List[Dict]:
    url = f"https://api.census.gov/data/{year}/acs/acs{acs_id}/groups/{group_id}.json&key={api_key}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        variables = response.json()["variables"]
        return [{"id": vid, "purpose": v["label"]} for vid, v in variables.items()]
    except Exception as e:
        raise CensusAPIError(f"Failed to fetch variables for group {group_id}: {e}")


# -------------------- Geography ----------------
@lru_cache(maxsize=128)
def get_states(
    acs_id: int,
    year: int,
    state_name: str | None,
):
    api_key: str = (get_census_api_key(),)
    url = f"https://api.census.gov/data/{year}/acs/acs{acs_id}?get=NAME&for=state:*&key={api_key}"
    try:
        response = requests.get(url)
        response.raise_for_status()

        rows = response.json()[1:]

        states_list = [{"state_name": row[0], "state_fips": row[1]} for row in rows]

        if state_name:
            match = [s for s in states_list if s["state_name"] == state_name]
            if not match:
                raise CensusAPIError(f"State '{state_name}' not found.")
            return {"states": match}

        return {"states": states_list}

    except Exception as e:
        raise CensusAPIError(f"Failed to fetch states: {e}")


@lru_cache(maxsize=256)
def get_counties(
    acs_id: int,
    year: int,
    fips_code: int,
    county_name: str | None,
    api_key: str = get_census_api_key(),
):
    url = (
        f"https://api.census.gov/data/{year}/acs/acs{acs_id}"
        f"?get=NAME&for=county:*&in=state:{fips_code}&key={api_key}"
    )
    try:
        response = requests.get(url)
        response.raise_for_status()

        rows = response.json()[1:]

        counties_list = [
            {
                "county_name": row[0],
                "county_fips": row[-1],
                "state_fips": str(fips_code),
            }
            for row in rows
        ]

        if county_name:
            match = [c for c in counties_list if c["county_name"] == county_name]
            if not match:
                raise CensusAPIError(
                    f"County '{county_name}' not found in state {fips_code}."
                )
            return {"state_fips": str(fips_code), "counties": match}

        return {"state_fips": str(fips_code), "counties": counties_list}

    except Exception as e:
        raise CensusAPIError(f"Failed to fetch counties: {e}")


@lru_cache(maxsize=128)
def get_places(
    acs_id: int,
    year: int,
    state_fips_code: int,
    place_name: str | None = None,
    api_key: str = get_census_api_key(),
):
    url = (
        f"https://api.census.gov/data/{year}/acs/acs{acs_id}"
        f"?get=NAME&for=place:*&in=state:{state_fips_code}&key={api_key}"
    )

    try:
        response = requests.get(url)
        response.raise_for_status()

        rows = response.json()[1:]

        places_list = [
            {
                "place_name": row[0],
                "state_fips": row[1],
                "place_fips": row[2],
            }
            for row in rows
        ]

        if place_name:
            match = [p for p in places_list if p["place_name"] == place_name]
            if not match:
                raise CensusAPIError(
                    f"Place '{place_name}' not found in state {state_fips_code}"
                )
            return {"state_fips": str(state_fips_code), "places": match}

        return {"state_fips": str(state_fips_code), "places": places_list}

    except Exception as e:
        raise CensusAPIError(f"Failed to fetch places: {e}")


# -------------------- Estimates ----------------


def get_estimate(
    acs_id: int,
    year: int,
    variables: List[str],
    state_fips: Optional[int] = None,
    place_fips: Optional[int] = None,
    county_fips: Optional[int] = None,
    api_key: str = get_census_api_key(),
):
    if state_fips is None and place_fips is None and county_fips is None:
        raise CensusAPIError(
            "One of the state fips, place fips, or county fips must be provided.",
        )
    variables_stringified = ",".join(variables)
    base_url = (
        f"https://api.census.gov/data/{year}/acs/acs{acs_id}"
        f"?get={variables_stringified}"
    )

    if place_fips is not None:
        url = (
            f"{base_url}"
            f"&for=place:{place_fips}"
            f"&in=state:{state_fips}"
            f"&key={api_key}"
        )
    elif state_fips is not None:
        url = f"{base_url}&for=state:{state_fips}&key={api_key}"
    else:
        url = f"{base_url}&for=county:{county_fips}&key={api_key}"

    estimates = []
    try:
        response = requests.get(url)
        response.raise_for_status()
        rows = response.json()[1:]

        for row in rows:
            row_estimates = [
                {"variable": var, "estimate": row[i]} for i, var in enumerate(variables)
            ]
            estimates.append(row_estimates)
        return {"estimates": estimates}

    except Exception as e:
        raise CensusAPIError(f"Failed to fetch estimates: {e}")
