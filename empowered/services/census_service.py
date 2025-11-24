# empowered/services/census_service.py

from typing import List, Dict, Optional
from empowered.api_clients.census import (
    get_groups as api_get_groups,
    get_variables as api_get_variables,
    get_states as api_get_states,
    get_counties as api_get_counties,
    get_places as api_get_places,
    get_estimate as api_get_estimate,
    CensusAPIError,
    validate_group_id as api_validate_group_id,
)

# ------------------ Groups ------------------


def get_groups(acs_id: int, year: int) -> List[Dict]:
    """
    Transform raw API groups into:
    [
        {"group_id": "DP05", "description": "...", "variables_count": 34},
        ...
    ]
    """
    try:
        raw_groups = api_get_groups(acs_id, year)
        # Transform raw API format into standardized service format
        return [
            {
                "group_id": g.get("id") or g.get("name"),
                "description": g.get("purpose") or g.get("description"),
                "variables_count": g.get("variables_count", 0),
            }
            for g in raw_groups
        ]
    except CensusAPIError as e:
        raise RuntimeError(f"Failed to fetch groups for ACS{acs_id} {year}: {e}")


def validate_group_id(acs_id: int, year: int, group_id: str) -> bool:
    return api_validate_group_id(acs_id, year, group_id)


# ------------------ Variables ------------------


def get_variables(acs_id: int, year: int, group_id: str) -> List[Dict]:
    """
    Transform raw API variables into:
    [
        {"variable_id": "DP05_0001E", "description": "Total population"},
        ...
    ]
    """
    if not validate_group_id(acs_id, year, group_id):
        raise ValueError(f"Invalid group_id {group_id} for ACS{acs_id} {year}")
    try:
        raw_vars = api_get_variables(acs_id, year, group_id)
        return [
            {"variable_id": v.get("id"), "description": v.get("purpose")}
            for v in raw_vars
        ]
    except CensusAPIError as e:
        raise RuntimeError(f"Failed to fetch variables for group {group_id}: {e}")


# ------------------ Geography ------------------


def get_states(acs_id: int, year: int, state_name: Optional[str] = None) -> List[Dict]:
    try:
        raw = api_get_states(acs_id, year, state_name)
        return raw.get("states", [])
    except CensusAPIError as e:
        raise RuntimeError(f"Failed to fetch states for ACS{acs_id} {year}: {e}")


def get_counties(
    acs_id: int, year: int, state_fips: int, county_name: Optional[str] = None
) -> List[Dict]:
    try:
        raw = api_get_counties(
            acs_id, year, fips_code=state_fips, county_name=county_name
        )
        return raw.get("counties", [])
    except CensusAPIError as e:
        raise RuntimeError(
            f"Failed to fetch counties for ACS{acs_id} {year} state {state_fips}: {e}"
        )


def get_places(
    acs_id: int, year: int, state_fips: int, place_name: Optional[str] = None
) -> List[Dict]:
    try:
        raw = api_get_places(
            acs_id, year, state_fips_code=state_fips, place_name=place_name
        )
        return raw.get("places", [])
    except CensusAPIError as e:
        raise RuntimeError(
            f"Failed to fetch places for ACS{acs_id} {year} state {state_fips}: {e}"
        )


# ------------------ Estimates ------------------


def get_estimates(
    acs_id: int,
    year: int,
    variables: List[str],
    state_fips: Optional[int] = None,
    county_fips: Optional[int] = None,
    place_fips: Optional[int] = None,
) -> Dict[str, List[Dict]]:
    """
    Transform raw API estimates into a dict mapping:
    {
        "variable_id": [{"variable": var, "estimate": value}, ...],
        ...
    }
    """
    try:
        raw = api_get_estimate(
            acs_id=acs_id,
            year=year,
            variables=variables,
            state_fips=state_fips,
            county_fips=county_fips,
            place_fips=place_fips,
        )
        # Raw format: {"estimates": [[{"variable": var, "estimate": value}, ...], ...]}
        estimates_dict = {}
        for row in raw.get("estimates", []):
            for entry in row:
                var = entry["variable"]
                if var not in estimates_dict:
                    estimates_dict[var] = []
                estimates_dict[var].append(entry)
        return estimates_dict
    except CensusAPIError as e:
        raise RuntimeError(f"Failed to fetch estimates for ACS{acs_id} {year}: {e}")
