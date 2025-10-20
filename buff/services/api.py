from fastapi import FastAPI, HTTPException

from buff.models.pydantic.census import (
    DatasetCreate,
    YearCreate,
    GroupCreate,
    VariableCreate,
    StateCreate,
    CountyCreate,
    CityCreate,
)
from buff.repositories.census.datasets_repo import DatasetRepository
from buff.repositories.census.years_available_repo import YearsAvailableRepository
from buff.repositories.census.groups_repo import CensusGroupRepository
from buff.repositories.census.variables_repo import CensusVariableRepository
from buff.repositories.census.geography_repo import CensusGeographyRepository
from buff.repositories.census.datasets_repo import DatasetRepository
from buff.repositories.census.years_available_repo import YearsAvailableRepository
from buff.repositories.census.groups_repo import CensusGroupRepository
from buff.repositories.census.variables_repo import CensusVariableRepository
from buff.repositories.census.geography_repo import CensusGeographyRepository
from buff.services.census import (
    get_years,
    get_groups,
    get_variables,
    get_counties,
    get_cities,
    get_states,
    validate_group_id,
    CensusAPIError,
)
from buff.utils import get_db_client

app = FastAPI(title="Census API server")


def is_valid_year(
    year: int,
    acs_id: int | None = None,
    db_client=get_db_client(),
    dataset_repo: DatasetRepository | None = None,
    dataset_id: int | None = None,
) -> bool:
    """Check if year exists in SQL for the given ACS dataset."""
    if dataset_id is None:
        dataset_repo = (
            DatasetRepository(db_client=db_client)
            if dataset_repo is None
            else dataset_repo
        )
        dataset_results = dataset_repo.get_by_code(code=f"acs{acs_id}")
        if not dataset_results:
            return False
        dataset_id: int = dataset_results[0].id
    years_repo = YearsAvailableRepository(db_client=db_client)
    years = years_repo.get_years(dataset_id=dataset_id)
    years_list = [record.year for record in years] if years else []
    return year in years_list


# --------------------- Dataset Endpoint --------------
@app.post("/census/dataset/")
def create_dataset(data: DatasetCreate):
    DatasetRepository().insert_code(data.code, data.frequency.value)
    return {"message": "Inserted successfully"}


# ------------------ Years Endpoint ------------------


@app.post("/year/")
def create_year(data: YearCreate):
    YearsAvailableRepository().insert_year(data.dataset_id, data.year)
    return {"message": "Inserted successfully"}


@app.get("/census/years_available/{acs_id}")
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
        # No data was found in the database. Request new data from API
        try:
            years_data = get_years(acs_id)
            years = years_data
        except (ValueError, CensusAPIError) as e:
            raise HTTPException(status_code=404, detail=str(e))
    else:
        # Convert SQL data to return format
        years = [record.year for record in years]
    return {"years_available": years}


# ------------------ Groups Endpoint ------------------


@app.post("/census/group/")
def create_group(data: GroupCreate):
    group_repo = CensusGroupRepository()
    group_repo.insert_group(
        dataset_id=data.dataset_id,
        year_id=data.year_id,
        group_id=data.group_id,
        description=data.description,
        variables_count=data.variables_count,
    )
    return {"message": "Inserted successfully"}


@app.get("/census/groups_available/{acs_id}/{year}")
def read_groups_available(acs_id: int, year: int):
    db_client = get_db_client()
    dataset_repo = DatasetRepository(db_client=db_client)
    dataset_results = dataset_repo.get_by_code(code=f"acs{acs_id}")
    if not dataset_results:
        raise HTTPException(
            status_code=404,
            detail=f"The provided acs_id ({acs_id}) is not valid.",
        )
    dataset_id: int = dataset_results[0].id
    if not is_valid_year(dataset_id=dataset_id, year=year):
        raise HTTPException(
            status_code=404, detail=f"Year {year} not available for ACS{acs_id}"
        )
    group_repo = CensusGroupRepository(db_client=db_client)
    # Check SQL first
    groups = group_repo.get_groups(dataset_id=dataset_id, year=year)
    if not groups:
        # Fall back to API if year exists in API
        try:

            groups_data = get_groups(acs_id, year)
            groups = groups_data
        except CensusAPIError as e:
            raise HTTPException(status_code=500, detail=str(e))
    else:
        groups = [record.id for record in groups]

    return {"groups_available": groups, "number_of_groups": len(groups)}


# ------------------ Variables Endpoint ------------------
@app.post("/census/variable/")
def create_variable(data: VariableCreate):
    variable_repo = CensusVariableRepository()
    variable_repo.insert_variable(
        dataset_id=data.dataset_id,
        year_id=data.year_id,
        group_id=data.group_id,
        variable_id=data.variable_id,
        description=data.description,
    )
    return {"message": "Inserted successfully"}


@app.get("/census/variables_available/{acs_id}/{year}/{group_id}")
def read_variables_available(acs_id: int, year: int, group_id: str):
    db_client = get_db_client()
    dataset_repo = DatasetRepository(db_client=db_client)
    dataset_results = dataset_repo.get_by_code(code=f"acs{acs_id}")
    dataset_repo = DatasetRepository(db_client=db_client)
    dataset_results = dataset_repo.get_by_code(code=f"acs{acs_id}")
    if not dataset_results:
        raise HTTPException(
            status_code=404,
            detail=f"The provided acs_id ({acs_id}) is not valid.",
        )
    dataset_id: int = dataset_results[0].id
    if not is_valid_year(dataset_id=dataset_id, year=year):
        raise HTTPException(
            status_code=404, detail=f"Year {year} not available for ACS{acs_id}"
        )
    variable_repo = CensusVariableRepository(db_client=db_client)

    # Check SQL first
    variables = variable_repo.get_variables(
        dataset_id=dataset_id, year=year, group_id=group_id
    )

    if not variables:
        try:
            if not validate_group_id(acs_id, year, group_id):
                raise HTTPException(
                    status_code=404,
                    detail=f"Group {group_id} not found for ACS{acs_id} {year}",
                )
            variables_data = get_variables(acs_id, year, group_id)
            variables = variables_data
        except CensusAPIError as e:
            raise HTTPException(status_code=500, detail=str(e))
    else:
        variables = [record.id for record in variables]

    return {"variables_available": variables, "number_of_variables": len(variables)}


# ---------------------- Geography Endpoint ------------------
@app.post("/census/state/")
def create_state(data: StateCreate):
    geo_repo = CensusGeographyRepository()
    geo_repo.insert_state(
        dataset_id=data.dataset_id,
        year_id=data.year_id,
        state_fips=data.state_fips,
        state_name=data.state_name,
    )
    return {"message": "Inserted successfully"}


@app.get("/census/states_available/{acs_id}/{year}")
def read_available_states(acs_id: int, year: int, state_name: str = None):
    db_client = get_db_client()
    geo_repo = CensusGeographyRepository(db_client=db_client)
    dataset_repo = DatasetRepository(db_client=db_client)
    dataset_results = dataset_repo.get_by_code(code=f"acs{acs_id}")
    if not dataset_results:
        raise HTTPException(
            status_code=404,
            detail=f"The provided acs_id ({acs_id}) is not valid.",
        )
    dataset_id: int = dataset_results[0].id
    if not is_valid_year(dataset_id=dataset_id, year=year):
        raise HTTPException(
            status_code=404, detail=f"Year {year} not available for ACS{acs_id}"
        )

    # Check SQL first
    states = geo_repo.get_states(acs_id=acs_id, year=year, state_name=state_name)
    if not states:
        try:
            states_data = get_states(acs_id=acs_id, year=year, state_name=state_name)
            states = states_data
        except CensusAPIError as e:
            raise HTTPException(status_code=500, detail=str(e))

    return states[0]


@app.post("/census/county/")
def create_county(data: CountyCreate):
    geo_repo = CensusGeographyRepository()
    geo_repo.insert_county(
        dataset_id=data.dataset_id,
        year_id=data.year_id,
        state_fips=data.state_fips,
        county_fips=data.county_fips,
        county_name=data.county_name,
    )
    return {"message": "Inserted successfully"}


@app.get("/census/counties_available/{acs_id}/{year}")
def read_available_counties(
    acs_id: int,
    year: int,
    state: str | int,
    county_name: str = None,
):
    db_client = get_db_client()
    geo_repo = CensusGeographyRepository(db_client=db_client)
    dataset_repo = DatasetRepository(db_client=db_client)
    dataset_results = dataset_repo.get_by_code(code=f"acs{acs_id}")
    if not dataset_results:
        raise HTTPException(
            status_code=404,
            detail=f"The provided acs_id ({acs_id}) is not valid.",
        )
    dataset_id: int = dataset_results[0].id
    if not is_valid_year(dataset_id=dataset_id, year=year):
        raise HTTPException(
            status_code=404, detail=f"Year {year} not available for ACS{acs_id}"
        )

    if state is None:
        raise HTTPException(
            status_code=400,
            detail="Did not provide a fips code or state name with the state query parameter.",
        )

    # Resolve state fips code
    if isinstance(state, str) and not state.isdigit():
        states = geo_repo.get_states(acs_id=acs_id, year=year, state_name=state)
        if not states:
            try:
                states_data = get_states(acs_id=acs_id, year=year, state_name=state)
                states = states_data
            except CensusAPIError as e:
                raise HTTPException(status_code=500, detail=str(e))
        fips_code = states[0]["states"][state]["fips"]
    else:
        fips_code = int(state)

    # Check SQL first
    counties = geo_repo.get_counties(
        acs_id=acs_id, year=year, fips_code=fips_code, county_name=county_name
    )
    if not counties:
        try:
            counties_data = get_counties(
                acs_id=acs_id,
                year=year,
                county_name=county_name,
                fips_code=fips_code,
            )
            counties = counties_data
        except CensusAPIError as e:
            raise HTTPException(status_code=500, detail=str(e))

    return counties[0]


@app.post("/census/city/")
def create_city(data: CityCreate):
    geo_repo = CensusGeographyRepository()
    geo_repo.insert_city(
        dataset_id=data.dataset_id,
        year_id=data.year_id,
        state_fips=data.state_fips,
        county_fips=data.county_fips,
        city_fips=data.city_fips,
        city_name=data.city_name,
    )
    return {"message": "Inserted successfully"}


@app.get("/census/cities_available/{acs_id}/{year}")
def read_available_cities(
    acs_id: int,
    year: int,
    state: str | int,
    county: str | int = None,
    city_name: str | None = None,
):
    db_client = get_db_client()
    dataset_repo = DatasetRepository(db_client=db_client)
    geo_repo = CensusGeographyRepository(db_client=db_client)
    dataset_results = dataset_repo.get_by_code(code=f"acs{acs_id}")
    if not dataset_results:
        raise HTTPException(
            status_code=404,
            detail=f"The provided acs_id ({acs_id}) is not valid.",
        )
    dataset_id: int = dataset_results[0].id
    if not is_valid_year(dataset_id=dataset_id, year=year):
        raise HTTPException(
            status_code=404, detail=f"Year {year} not available for ACS{acs_id}"
        )
    if state is None:
        raise HTTPException(
            status_code=400,
            detail="Did not provide a fips code or state name with the state query parameter.",
        )

    state_name: str = state if isinstance(state, str) else None
    state_fips_code: int = state if isinstance(state, int) else None
    county_name: str = county if isinstance(county, str) else None
    county_fips_code: int = county if isinstance(county, int) else None

    cities = geo_repo.get_geography(
        state_name=state_name,
        state_fips_code=state_fips_code,
        county_name=county_name,
        county_fips_code=county_fips_code,
    )
    try:
        if state_fips_code is None:
            states_data = get_states(acs_id=acs_id, year=year, state_name=state)
            states = states_data
            state_fips_code = states[0]["states"][state]["fips"]

        if county_fips_code is None and county_name is not None:
            counties_data = get_counties(
                acs_id=acs_id,
                year=year,
                county_name=county_name,
                fips_code=fips_code,
            )
            counties = counties_data
            county_fips_code = counties[0]["counties"][county_name]
        if not cities:
            cities = get_cities(
                city_name=city_name,
                acs_id=acs_id,
                year=year,
                state_fips_code=state_fips_code,
                county_fips_code=county_fips_code,
            )
    except CensusAPIError as e:
        raise HTTPException(status_code=500, detail=str(e))


# -------------------------- IPUMS ------------------------------
base_ipums_url = "https://api.ipums.org/metadata/usa/variables"
