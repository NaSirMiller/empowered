import asyncio
from fastapi import FastAPI, HTTPException, Depends
from typing import List

from empowered.models.pydantic.census import (
    DatasetCreate,
    PlaceCreate,
    StateCreate,
    CountyCreate,
    YearCreate,
    GroupCreate,
    VariableCreate,
)

from empowered.repositories.census.datasets_repo import DatasetRepository
from empowered.repositories.census.years_available_repo import YearsAvailableRepository
from empowered.repositories.census.groups_repo import CensusGroupRepository
from empowered.repositories.census.variables_repo import CensusVariableRepository
from empowered.repositories.census.geography_repo import (
    GeographyRepository,
)

from empowered.services.census import (
    get_places,
    get_counties,
    get_states,
    get_variables,
    get_years,
    get_groups,
    CensusAPIError,
    validate_group_id,
)

from empowered.utils import get_census_api_key, get_db_client


app = FastAPI(title="Census API Server")


def get_dataset_repo():
    return DatasetRepository(db_client=get_db_client())


def get_years_repo():
    return YearsAvailableRepository(db_client=get_db_client())


def get_group_repo():
    return CensusGroupRepository(db_client=get_db_client())


def get_variable_repo():
    return CensusVariableRepository(db_client=get_db_client())


def get_geography_repo():
    return GeographyRepository(db_client=get_db_client())


def is_valid_year(
    year: int,
    dataset_id: int,
    years_repo: YearsAvailableRepository,
) -> bool:
    years = years_repo.get_years(dataset_id=dataset_id)
    return year in {y.year for y in years}


@app.post("/census/datasets", status_code=201)
def create_dataset(
    data: DatasetCreate,
    repo: DatasetRepository = Depends(get_dataset_repo),
):
    repo.insert_code(data.code, data.frequency.value)
    return {"message": "Dataset created"}


@app.post("/census/years", status_code=201)
def create_year(
    data: YearCreate,
    repo: YearsAvailableRepository = Depends(get_years_repo),
):
    repo.insert_year(data.dataset_id, data.year)
    return {"message": "Year created"}


@app.get("/census/acs/{dataset_id}/years")
def read_years_available(
    dataset_id: str,
    years_repo: YearsAvailableRepository = Depends(get_years_repo),
):
    found_years = years_repo.get_years(dataset_id=dataset_id)

    if not found_years:
        try:
            api_years = get_years(dataset_id[-1])
            for y in api_years:
                years_repo.insert_year(dataset_id, y)
            return {"years_available": api_years}
        except CensusAPIError as e:
            raise HTTPException(500, str(e))

    years_out = [y.year for y in found_years]
    return {"years_available": years_out}


@app.post("/census/groups", status_code=201)
def create_group(
    data: GroupCreate,
    repo: CensusGroupRepository = Depends(get_group_repo),
):
    repo.insert_group(
        dataset_id=data.dataset_id,
        year_id=data.year_id,
        group_id=data.group_id,
        description=data.description,
        variables_count=data.variables_count,
    )
    return {"message": "Group created"}


@app.get("/census/groups/{acs_id}/{year}")
def read_groups_available(
    acs_id: int,
    year: int,
    dataset_repo: DatasetRepository = Depends(get_dataset_repo),
    group_repo: CensusGroupRepository = Depends(get_group_repo),
):
    dataset = dataset_repo.get_by_code(f"acs{acs_id}")
    if not dataset:
        raise HTTPException(404, "Invalid ACS dataset")

    dataset_id = dataset[0].id
    if not is_valid_year(dataset_id=dataset_id, year=year):
        raise HTTPException(404, f"Year {year} not available for ACS {acs_id}")

    groups = group_repo.get_groups(dataset_id, year)

    if groups:
        group_ids = [g.id for g in groups]
        return {"groups_available": group_ids, "number_of_groups": len(group_ids)}

    try:
        groups_api = get_groups(acs_id, year)
        return {"groups_available": groups_api, "number_of_groups": len(groups_api)}
    except CensusAPIError as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/census/variables", status_code=201)
def create_variable(
    data: VariableCreate,
    repo: CensusVariableRepository = Depends(get_variable_repo),
):
    repo.insert_variable(
        dataset_id=data.dataset_id,
        year_id=data.year_id,
        group_id=data.group_id,
        variable_id=data.variable_id,
        description=data.description,
    )
    return {"message": "Variable created"}


@app.get("/census/acs/{acs_id}/{year}/{group_id}/variables")
def read_variables_available(
    acs_id: int,
    year: int,
    group_id: str,
    dataset_repo: DatasetRepository = Depends(get_dataset_repo),
    years_repo: YearsAvailableRepository = Depends(get_years_repo),
    variable_repo: CensusVariableRepository = Depends(get_variable_repo),
):
    dataset = dataset_repo.get_by_code(f"acs{acs_id}")
    if not dataset:
        raise HTTPException(404, f"Invalid ACS dataset: {acs_id}")

    dataset_id = dataset[0].id
    if not is_valid_year(year, acs_id, dataset_repo, years_repo):
        raise HTTPException(404, f"Year {year} not available for ACS{acs_id}")

    stored_vars = variable_repo.get_variables(dataset_id, year, group_id)

    if stored_vars:
        variables = [v.id for v in stored_vars]
        return {"variables_available": variables, "number_of_variables": len(variables)}

    try:
        if not validate_group_id(acs_id, year, group_id):
            raise HTTPException(
                404, f"Group {group_id} not found for ACS{acs_id} {year}"
            )

        variables_api = get_variables(acs_id, year, group_id)
        return {
            "variables_available": variables_api,
            "number_of_variables": len(variables_api),
        }
    except CensusAPIError as e:
        raise HTTPException(500, str(e))


@app.get("/census/variables_available/{acs_id}/{year}/{group_id}")
def read_variables_available(
    acs_id: int,
    year: int,
    group_id: str,
    dataset_repo: DatasetRepository = Depends(get_dataset_repo),
):
    db_client = get_db_client()
    dataset = dataset_repo.get_by_code(f"acs{acs_id}")
    if not dataset:
        raise HTTPException(
            status_code=404,
            detail=f"Dataset with ACS ID {acs_id} not found",
        )
    dataset_id = dataset[0].id

    if not is_valid_year(dataset_id=dataset_id, year=year):
        raise HTTPException(
            status_code=404,
            detail=f"Year {year} not available for ACS {acs_id}",
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
                    detail=f"Group {group_id} not found for ACS {acs_id} {year}",
                )
            variables_data = get_variables(acs_id, year, group_id)
            variables = variables_data
        except CensusAPIError as e:
            raise HTTPException(status_code=500, detail=str(e))
    else:
        variables = [record.id for record in variables]

    return {"variables_available": variables, "number_of_variables": len(variables)}


# ---------------------- Geography Endpoint ------------------


@app.post("census/geography/states/", status_code=201)
def create_state(
    data: StateCreate, geo_repo: GeographyRepository = Depends(get_geography_repo)
):
    geo_repo.insert_state(
        state_fips_code=data.state_fips,
        state_name=data.state_name,
        dataset_id=data.dataset_id,
        year_id=data.year_id,
    )
    return {"message": "State created"}


@app.post("census/geography/countys/", status_code=201)
def create_county(
    data: CountyCreate,
    geo_repo: GeographyRepository = Depends(get_geography_repo),
):
    geo_repo.insert_county(
        county_fips_code=data.county_fips,
        county_name=data.county_name,
        dataset_id=data.dataset_id,
        year_id=data.year_id,
    )
    return {"message": "County created"}


@app.post("/census/geography/places", status_code=201)
def create_place(
    data: PlaceCreate, geo_repo: GeographyRepository = Depends(get_geography_repo)
):
    geo_repo.insert_place(
        dataset_id=data.dataset_id,
        year_id=data.year_id,
        state_fips=data.state_fips,
        place_fips_code=data.place_fips,
        place_name=data.place_name,
    )
    return {"message": "Place created"}


@app.get("/census/acs/{acs_id}/{year}/states")
def read_available_states(
    acs_id: int,
    year: int,
    state_name: str | None = None,
    dataset_repo: DatasetRepository = Depends(get_dataset_repo),
    years_repo: YearsAvailableRepository = Depends(get_years_repo),
    geo_repo: GeographyRepository = Depends(get_geography_repo),
):
    dataset = dataset_repo.get_by_code(f"acs{acs_id}")
    if not dataset:
        raise HTTPException(404, f"Invalid ACS dataset: {acs_id}")

    if not is_valid_year(year, acs_id, dataset_repo, years_repo):
        raise HTTPException(404, f"Year {year} not available for ACS{acs_id}")

    stored_states = geo_repo.get_states(acs_id=acs_id, year=year, state_name=state_name)

    if stored_states:
        return stored_states[0]

    try:
        return get_states(acs_id=acs_id, year=year, state_name=state_name)[0]
    except CensusAPIError as e:
        raise HTTPException(500, str(e))


@app.get("/census/acs/{acs_id}/{year}/counties")
def read_available_counties(
    acs_id: int,
    year: int,
    state: str | int,
    county_name: str | None = None,
    dataset_repo: DatasetRepository = Depends(get_dataset_repo),
    years_repo: YearsAvailableRepository = Depends(get_years_repo),
    geo_repo: GeographyRepository = Depends(get_geography_repo),
):
    dataset = dataset_repo.get_by_code(f"acs{acs_id}")
    if not dataset:
        raise HTTPException(404, f"Invalid ACS dataset: {acs_id}")

    if not is_valid_year(year, acs_id, dataset_repo, years_repo):
        raise HTTPException(404, f"Year {year} not available for ACS{acs_id}")

    if state is None:
        raise HTTPException(400, "Missing state parameter (name or FIPS).")

    if isinstance(state, str) and not state.isdigit():
        stored_states = geo_repo.get_states(acs_id, year, state_name=state)
        if not stored_states:
            try:
                stored_states = get_states(acs_id, year, state_name=state)
            except CensusAPIError as e:
                raise HTTPException(500, str(e))
        fips_code = stored_states[0]["states"][state]["fips"]
    else:
        fips_code = int(state)

    stored_counties = geo_repo.get_counties(
        acs_id=acs_id,
        year=year,
        fips_code=fips_code,
        county_name=county_name,
    )

    if stored_counties:
        return stored_counties[0]

    try:
        return get_counties(
            acs_id=acs_id,
            year=year,
            county_name=county_name,
            fips_code=fips_code,
        )[0]
    except CensusAPIError as e:
        raise HTTPException(500, str(e))


@app.get("/census/acs/{acs_id}/{year}/places")
def read_available_places(
    acs_id: int,
    year: int,
    state: str | int,
    county: str | int | None = None,
    city_name: str | None = None,
    dataset_repo: DatasetRepository = Depends(get_dataset_repo),
    years_repo: YearsAvailableRepository = Depends(get_years_repo),
    geo_repo: GeographyRepository = Depends(get_geography_repo),
):
    dataset = dataset_repo.get_by_code(f"acs{acs_id}")
    if not dataset:
        raise HTTPException(404, f"Invalid ACS dataset: {acs_id}")

    if not is_valid_year(year, acs_id, dataset_repo, years_repo):
        raise HTTPException(404, f"Year {year} not available for ACS{acs_id}")

    if state is None:
        raise HTTPException(400, "Missing state parameter (name or FIPS).")

    state_name = state if isinstance(state, str) else None
    state_fips = state if isinstance(state, int) else None
    county_name_param = county if isinstance(county, str) else None
    county_fips = county if isinstance(county, int) else None

    try:
        if state_fips is None:
            states = get_states(acs_id=acs_id, year=year, state_name=state)
            state_fips = states[0]["states"][state]["fips"]

        if county_fips is None and county_name_param is not None:
            counties = get_counties(
                acs_id=acs_id,
                year=year,
                county_name=county_name_param,
                fips_code=state_fips,
            )
            county_fips = counties[0]["counties"][county_name_param]

        stored_places = geo_repo.get_place(
            state_name=state_name,
            state_fips_code=state_fips,
            county_name=county_name_param,
            county_fips_code=county_fips,
        )

        if stored_places:
            return stored_places

        return get_places(
            city_name=city_name,
            acs_id=acs_id,
            year=year,
            state_fips_code=state_fips,
            county_fips_code=county_fips,
        )
    except CensusAPIError as e:
        raise HTTPException(500, str(e))


@app.get("/census/acs/{acs_id}/{year}/estimates")
async def read_estimates(
    acs_id: int,
    year: int,
    variables: List[str],
    state: int | None = None,
    city: int | None = None,
    county: int | None = None,
):
    if not any([state, city, county]):
        raise HTTPException(401, "Specify a state, county, or city FIPS.")

    if city is not None and state is None:
        raise HTTPException(401, "City FIPS requires a state FIPS.")

    if not variables:
        raise HTTPException(401, "At least one variable must be provided.")

    variables_stringified = ",".join(variables)
    api_key = get_census_api_key()

    base_url = (
        f"https://api.census.gov/data/{year}/acs/acs{acs_id}"
        f"?get={variables_stringified}"
    )

    if city:
        url = f"{base_url}&for=place:{city}&in=state:{state}&key={api_key}"
    elif state:
        url = f"{base_url}&for=state:{state}&key={api_key}"
    else:
        url = f"{base_url}&for=county:{county}&key={api_key}"

    semaphore = asyncio.Semaphore(50)
    tasks = []

    async with ClientSession() as session:
        for gid in geography_list:
            tasks.append(
                asyncio.ensure_future(
                    bound_fetch(
                        semaphore=semaphore,
                        url=url.format(gid),
                        session=session,
                        fetch_method=get_estimate,
                        gid=gid,
                        geography_level=geography_level,
                    )
                )
            )

        responses = await asyncio.gather(*tasks)

    processed_data = []
    for resp in responses:
        header, row = resp[0], resp[1]
        processed_data.append(dict(zip(header, row)))

    return processed_data
