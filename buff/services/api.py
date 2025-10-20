from fastapi import FastAPI, HTTPException

from buff.models.pydantic.dataset import DatasetCreate
from buff.models.pydantic.years import YearCreate
from buff.models.sql.schemas import (
    CensusAvailableYear,
    CensusDataset,
    CensusEstimate,
    CensusGeography,
    CensusGroup,
    CensusVariable,
)
from buff.repositories.dataset_repo import DatasetRepository
from buff.repositories.years_available_repo import YearsAvailableRepository
from buff.services.census import (
    get_years,
    get_groups,
    get_variables,
    get_counties,
    get_states,
    validate_group_id,
    CensusAPIError,
)
from buff.utils import get_db_client

app = FastAPI(title="Census API server")


# --------------------- Dataset Endpoint --------------
@app.post("/dataset/")
def create_dataset(data: DatasetCreate):
    DatasetRepository().insert_code(data.code, data.frequency.value)
    return {"message": "Inserted successfully"}


# ------------------ Years Endpoint ------------------
def is_valid_year(acs_id: int, year: int) -> bool:
    years = get_years(acs_id)
    return year in years


@app.post("/year/")
def create_year(data: YearCreate):
    YearsAvailableRepository().insert_year(data.dataset_id, data.year)
    return {"message": "Inserted successfully"}


@app.get("/years_available/{acs_id}")
def read_years_available(acs_id: int):
    db_client = get_db_client()
    dataset_repo = DatasetRepository(db_client=db_client)
    dataset_results = dataset_repo.get_by_code(code=f"acs{acs_id}")
    if not dataset_results:
        raise HTTPException(
            status_code=404,
            detail=f"The provided acs_id ({acs_id}) is not valid. Must be 1 or 5.",
        )
    dataset_id: int = dataset_results[0].id
    years_available_repo = YearsAvailableRepository(db_client=db_client)
    years = years_available_repo.get_years(dataset_id=dataset_id)
    if not years:
        # No data was found in the database. Request new data.
        try:
            years = get_years(acs_id)
        except (ValueError, CensusAPIError) as e:
            raise HTTPException(status_code=404, detail=str(e))
    else:
        # Convert SQL data to return format
        years = [record.year for record in years]
    return {"years_available": years}


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
def read_available_states(acs_id: int, year: int, state_name: str = None):
    try:
        if not is_valid_year(acs_id=acs_id, year=year):
            raise HTTPException(
                status_code=404, detail=f"Year {year} not available for ACS{acs_id}"
            )
        states: list[dict[str, dict[str, dict[str, int]]]] = get_states(
            acs_id=acs_id, year=year, state_name=state_name
        )
        return states[0]
    except CensusAPIError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/counties_available/{acs_id}/{year}")
def read_available_counties(
    acs_id: int,
    year: int,
    state: str | int,
    county_name: str = None,
):
    try:
        if not is_valid_year(acs_id=acs_id, year=year):
            raise HTTPException(
                status_code=404, detail=f"Year {year} not available for ACS{acs_id}"
            )
        if state is None:
            raise HTTPException(
                status_code=400,
                detail="Did not provide a fips code or state name with the state query parameter.",
            )
        if state.isdigit():
            fips_code = state
        else:
            states = get_states(acs_id=acs_id, year=year, state_name=state)
            fips_code = states[0]["states"][state]["fips"]

        counties: list[dict[str, dict[str, dict[str, int]]]] = get_counties(
            acs_id=acs_id,
            year=year,
            county_name=county_name,
            fips_code=fips_code,
        )
        return counties[0]
    except CensusAPIError as e:
        raise HTTPException(status_code=500, detail=str(e))
