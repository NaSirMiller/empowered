from fastapi import FastAPI, HTTPException
from buff.services.census import (
    get_years,
    get_groups,
    get_variables,
    get_counties,
    get_states,
    validate_group_id,
    CensusAPIError,
)

app = FastAPI(title="Census API server")


def is_valid_year(acs_id: int, year: int) -> bool:
    years = get_years(acs_id)
    return year in years


# ------------------ Years Endpoint ------------------
@app.get("/years_available/{acs_id}")
def read_years_available(acs_id: int):
    try:
        years = get_years(acs_id)
        return {"years_available": years}
    except (ValueError, CensusAPIError) as e:
        raise HTTPException(status_code=404, detail=str(e))


# ------------------ Groups Endpoint ------------------
@app.get("/groups_available/{acs_id}/{year}")
def read_groups_available(acs_id: int, year: int):
    try:
        if not is_valid_year(acs_id=acs_id, year=year):
            raise HTTPException(
                status_code=404, detail=f"Year {year} not available for ACS{acs_id}"
            )
        groups = get_groups(acs_id, year)
        return {"groups_available": groups, "number_of_groups": len(groups)}
    except CensusAPIError as e:
        raise HTTPException(status_code=500, detail=str(e))


# ------------------ Variables Endpoint ------------------
@app.get("/variables_available/{acs_id}/{year}/{group_id}")
def read_variables_available(acs_id: int, year: int, group_id: str):
    try:
        if not is_valid_year(acs_id=acs_id, year=year):
            raise HTTPException(
                status_code=404, detail=f"Year {year} not available for ACS{acs_id}"
            )
        if not validate_group_id(acs_id, year, group_id):
            raise HTTPException(
                status_code=404,
                detail=f"Group {group_id} not found for ACS{acs_id} {year}",
            )
        variables = get_variables(acs_id, year, group_id)
        return {"variables_available": variables, "number_of_variables": len(variables)}
    except CensusAPIError as e:
        raise HTTPException(status_code=500, detail=str(e))


# ---------------------- Geography Endpoint ------------------
@app.get("/states_available/{acs_id}/{year}")
def read_available_states(acs_id: int, year: int):
    try:
        if not is_valid_year(acs_id=acs_id, year=year):
            raise HTTPException(
                status_code=404, detail=f"Year {year} not available for ACS{acs_id}"
            )
        states: list[dict[str, dict[str, dict[str, int]]]] = get_states(
            acs_id=acs_id, year=year
        )
        return states[0]
    except CensusAPIError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/counties_available/{acs_id}/{year}}")
def read_available_states(acs_id: int, year: int):
    pass
