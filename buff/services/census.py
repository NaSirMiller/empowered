from bs4 import BeautifulSoup
import re
import requests
from functools import lru_cache
from typing import List, Dict
from buff.logger_setup import get_logger

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
