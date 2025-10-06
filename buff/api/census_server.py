from bs4 import BeautifulSoup
import json
from fastapi import FastAPI, HTTPException, Response
import re
import requests
from typing import Any

from buff.logger_setup import get_logger

logger = get_logger(name=__name__)


ACS5_URL: str = "https://www.census.gov/data/developers/data-sets/acs-5year.html"
ACS1_URL: str = "https://www.census.gov/data/developers/data-sets/acs-1year.html"

app = FastAPI(title="Census API server")


def get_html(url: str) -> str:
    response = requests.get(url)
    status_code: int = response.status_code
    if status_code != 200:
        raise HTTPException(
            status_code=status_code,
            detail=f"Request at {url} failed with status code={status_code}.",
        )
    return response.text


@app.get("/years_available/{acs_id}")
def read_years_available(acs_id: int = 1) -> dict:
    """
    Provides the list of years available for a given survey type. Gets the list from the documentation for the ACS1 and ACS5 surveys such that whenever the years surveyed are changed, this can be utilized for a dynamic retrieval of the years you may use.

    Args:
        acs_id (int, optional): The survey identifier, either 1 or 5. Defaults to 1.

    HTTPException: 404 error if the acs id provided is not a 1 or 5.

    Returns:
        dict: Years the specific acs survey can be retrieved for. Has form {"years_available":[...]}
    """
    acs_html: str
    match acs_id:
        case 1:
            acs_html = get_html(url=ACS1_URL)
        case 5:
            acs_html = get_html(url=ACS5_URL)
        case _:
            raise HTTPException(
                status_code=404,
                detail=f"Invalid acs_id provided. Expected one of 1 or 5, got {acs_id}",
            )
    soup = BeautifulSoup(acs_html, "html.parser")
    headers: list = soup.find_all(["h1"])
    if headers is None:
        raise HTTPException(
            status_code=404,
            detail="There are no h1-headers with found on requested page.",
        )
    matches: list[Any]
    lower_bound: int
    upper_bound: int
    for header in headers:
        matches = re.findall(r"\((.*?)\)", header.get_text())
        for match in matches:
            try:
                years = match.split("-")
                lower_bound, upper_bound = int(years[0].strip()), int(years[1].strip())
                break  # Only one header with necessary values as of 10/03/2025
            except Exception as e:
                logger.info(
                    f"For matches={matches}, cannot convert to starting and ending year."
                )
    available_years: list[str] = list(range(lower_bound, upper_bound + 1))
    return {"years_available": available_years}


@app.get("/groups_available/{acs_id}/{year}")
def read_groups_available(acs_id: int, year: int) -> dict:
    """
    Provides the list of groups that the Census provides for a specific acs survey and year. We simply reduce the provided information from the Census to only the name and the purpose of the group.

    Args:
        acs_id (int): _description_
        year (int): _description_

    Raises:
        HTTPException: 404 error if the acs id provided is not a 1 or 5.
        HTTPException: 404 error if provided year is not in the list of available years for the given acs id
        HTTPException: 500 error if we cannot retrieve the groups attribute of the response from the Census
        HTTPException: 500 error if we cannot retrieve the name or description attribute of a group in the set of groups received

    Returns:
        dict: The groups available with form {"groups": [{
            "id":...,
            "purpose":...,
        },...], "number_of_groups":...
    """
    if acs_id not in [1, 5]:
        logger.error(f"Invalid acs_id provided. Expected one of 1 or 5, got {acs_id}")
        raise HTTPException(
            status_code=404,
            detail=f"Invalid acs_id provided. Expected one of 1 or 5, got {acs_id}",
        )
    years_available_url: str = f"http://127.0.0.1:8000/years_available/{acs_id}"
    years_available_json: dict
    try:
        years_available_response: dict = requests.get(years_available_url)
        years_available_json = years_available_response.json()
    except HTTPException as e:
        logger.error(
            "There was an issue retrieving the years available in the groups_available endpoint."
        )
        raise e
    years_available: list[int] = years_available_json["years_available"]
    if year not in years_available:
        logger.error(
            f"The year provided is not a valid year offered ({years_available}"
        )
        raise HTTPException(
            status_code=404,
            detail=f"The year provided is not a valid year offered ({years_available}",
        )
    groups_url: str = f"https://api.census.gov/data/{year}/acs/acs{acs_id}/groups/"
    groups_json: dict
    try:
        groups_response: Response = requests.get(groups_url)
        groups_json = groups_response.json()["groups"]
    except Exception as e:
        logger.error(str(e))
        raise HTTPException(status_code=500, detail=str(e))

    # Reducing information provided to user
    results: dict = {"groups_available": [], "number_of_groups": 0}
    try:
        for group in groups_json:
            results["groups_available"].append(
                {"id": group["name"], "purpose": group["description"]}
            )
            results["number_of_groups"] += 1
        return results
    except:
        raise HTTPException(
            status_code=500, detail="Groups JSON is not in expected format"
        )
