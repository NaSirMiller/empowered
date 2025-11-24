from fastapi import FastAPI, HTTPException, Depends

from empowered.models.pydantic.census import (
    DatasetCreate,
    EstimateRequest,
)

from empowered.repositories.census.datasets_repo import DatasetRepository
from empowered.repositories.census.factory import RepositoryFactory
from empowered.repositories.census.years_available_repo import YearsAvailableRepository
from empowered.repositories.census.groups_repo import CensusGroupRepository
from empowered.repositories.census.variables_repo import CensusVariableRepository
from empowered.repositories.census.estimates_repo import CensusEstimateRepository
from empowered.repositories.census.geography_repo import GeographyRepository

from empowered.services.census import (
    get_places,
    get_counties,
    get_estimate,
    get_states,
    get_variables,
    get_years,
    get_groups,
    CensusAPIError,
    validate_group_id,
)

from empowered.utils import get_sql_client


app = FastAPI(title="Census API Server")


def get_repo_factory():
    return RepositoryFactory(get_sql_client())


def get_dataset_repo(factory: RepositoryFactory = Depends(get_repo_factory)):
    return factory.dataset()


def get_years_repo(factory: RepositoryFactory = Depends(get_repo_factory)):
    return factory.years()


def get_group_repo(factory: RepositoryFactory = Depends(get_repo_factory)):
    return factory.group()


def get_variable_repo(factory: RepositoryFactory = Depends(get_repo_factory)):
    return factory.variable()


def get_geography_repo(factory: RepositoryFactory = Depends(get_repo_factory)):
    return factory.geography()


def get_estimate_repo(factory: RepositoryFactory = Depends(get_repo_factory)):
    return factory.estimate()


def is_valid_year(
    year: int, dataset_id: int, years_repo: YearsAvailableRepository
) -> bool:
    years = years_repo.get_years(dataset_id=dataset_id)
    return year in {y.year for y in years}


@app.post("/census/datasets", status_code=201)
def create_dataset(
    data: DatasetCreate,
    repo: DatasetRepository = Depends(get_dataset_repo),
):
    """
    Only explicit create endpoint allowed.
    All other inserts must be done via ingestion.
    """
    repo.insert_code(data.code, data.frequency.value)
    return {"message": "Dataset created"}


@app.get("/census/years/acs/{dataset_id}/")
def read_years_available(
    dataset_id: str,
    years_repo: YearsAvailableRepository = Depends(get_years_repo),
):
    """
    Returns stored years. Does NOT insert missing years.
    """
    found_years = years_repo.get_years(dataset_id=dataset_id)

    if not found_years:
        # fetch from Census API but do NOT insert
        try:
            api_years = get_years(dataset_id[-1])
            return {"years_available": api_years}
        except CensusAPIError as e:
            raise HTTPException(500, str(e))

    years_out = [y.year for y in found_years]
    return {"years_available": years_out}


@app.get("/census/groups/acs/{acs_id}/{year}")
def read_groups_available(
    acs_id: int,
    year: int,
    dataset_repo: DatasetRepository = Depends(get_dataset_repo),
    years_repo: YearsAvailableRepository = Depends(get_years_repo),
    group_repo: CensusGroupRepository = Depends(get_group_repo),
):
    dataset = dataset_repo.get_by_code(f"acs{acs_id}")
    if not dataset:
        raise HTTPException(404, "Invalid ACS dataset")

    dataset_id = dataset[0].id
    if not is_valid_year(year, dataset_id, years_repo):
        raise HTTPException(404, f"Year {year} not available")

    year_id = years_repo.get_years(dataset_id=dataset_id, year=year)

    stored_groups = group_repo.get_groups(dataset_id, year_id)
    if stored_groups:
        return {
            "groups_available": [g.id for g in stored_groups],
            "number_of_groups": len(stored_groups),
        }

    try:
        groups_api = get_groups(acs_id, year)
        return {
            "groups_available": [g["group_id"] for g in groups_api],
            "number_of_groups": len(groups_api),
        }
    except CensusAPIError as e:
        raise HTTPException(500, str(e))


@app.get("/census/variables/acs/{acs_id}/{year}/{group_id}")
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
    if not is_valid_year(year, dataset_id, years_repo):
        raise HTTPException(404, "Year invalid")

    year_id = years_repo.get_years(dataset_id=dataset_id, year=year)

    stored_vars = variable_repo.get_variables(dataset_id, year_id, group_id)
    if stored_vars:
        return {
            "variables_available": [v.id for v in stored_vars],
            "number_of_variables": len(stored_vars),
        }

    if not validate_group_id(acs_id, year, group_id):
        raise HTTPException(404, "Invalid group")

    try:
        variables_api = get_variables(acs_id, year, group_id)
        return {
            "variables_available": [v["variable_id"] for v in variables_api],
            "number_of_variables": len(variables_api),
        }
    except CensusAPIError as e:
        raise HTTPException(500, str(e))


@app.get("/census/geography/states/acs/{acs_id}/{year}")
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
        raise HTTPException(404, "Invalid dataset")

    if not is_valid_year(year, dataset[0].id, years_repo):
        raise HTTPException(404, "Invalid year")

    stored_states = geo_repo.get_states(acs_id=acs_id, year=year, state_name=state_name)

    if stored_states:
        return stored_states

    try:
        states = get_states(acs_id=acs_id, year=year, state_name=state_name)[0]
        return states
    except CensusAPIError as e:
        raise HTTPException(500, str(e))


@app.get("/census/geography/counties/acs/{acs_id}/{year}")
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
        raise HTTPException(404, "Invalid dataset")

    if not is_valid_year(year, dataset[0].id, years_repo):
        raise HTTPException(404, "Invalid year")

    # convert state name to fips if needed (lookup only)
    if isinstance(state, str) and not state.isdigit():
        stored_states = geo_repo.get_states(acs_id, year, state_name=state)
        if stored_states:
            fips_code = stored_states["states"][state]["state_fips"]
        else:
            fetched = get_states(acs_id, year, state_name=state)
            fips_code = fetched["states"][state]["state_fips"]
    else:
        fips_code = int(state)

    stored_counties = geo_repo.get_counties(
        acs_id=acs_id,
        year=year,
        state_fips=fips_code,
        county_name=county_name,
    )

    if stored_counties:
        return stored_counties

    try:
        counties = get_counties(
            acs_id=acs_id,
            year=year,
            county_name=county_name,
            fips_code=fips_code,
        )
        return counties
    except CensusAPIError as e:
        raise HTTPException(500, str(e))


@app.get("/census/geography/places/acs/{acs_id}/{year}")
def read_available_places(
    acs_id: int,
    year: int,
    state: str | int,
    place_name: str | None = None,
    dataset_repo: DatasetRepository = Depends(get_dataset_repo),
    years_repo: YearsAvailableRepository = Depends(get_years_repo),
    geo_repo: GeographyRepository = Depends(get_geography_repo),
):
    dataset = dataset_repo.get_by_code(f"acs{acs_id}")
    if not dataset:
        raise HTTPException(404, "Invalid dataset")

    if not is_valid_year(year, dataset[0].id, years_repo):
        raise HTTPException(404, "Invalid year")

    if isinstance(state, str) and not state.isdigit():
        stored_states = geo_repo.get_states(acs_id, year, state_name=state)
        if stored_states:
            state_fips = stored_states["states"][state]["state_fips"]
        else:
            fetched = get_states(acs_id, year, state_name=state)
            state_fips = fetched["states"][state]["state_fips"]
    else:
        state_fips = int(state)

    stored_places = geo_repo.get_places(
        acs_id=acs_id,
        year=year,
        state_fips=state_fips,
        place_name=place_name,
    )

    if stored_places:
        return stored_places

    try:
        places = get_places(
            acs_id=acs_id,
            year=year,
            state_fips_code=state_fips,
            place_name=place_name,
        )
        return places

    except CensusAPIError as e:
        raise HTTPException(500, str(e))


@app.get("/census/estimates/acs/{acs_id}/{year}/")
async def read_estimates(
    acs_id: int,
    year: int,
    geo: EstimateRequest = Depends(),
    estimate_repo: CensusEstimateRepository = Depends(get_estimate_repo),
    dataset_repo: DatasetRepository = Depends(get_dataset_repo),
    years_repo: YearsAvailableRepository = Depends(get_years_repo),
):
    variables, state, county, place = geo.variables, geo.state, geo.county, geo.place

    dataset = dataset_repo.get_by_code(f"acs{acs_id}")
    if not dataset:
        raise HTTPException(404, "Invalid dataset")

    if not is_valid_year(year, dataset[0].id, years_repo):
        raise HTTPException(404, "Invalid year")

    if place is not None:
        stored_estimates = [
            estimate_repo.get_estimates(
                place_fips=place,
                year_id=year,
                dataset_id=f"acs{acs_id}",
                variable=var,
                group_id=var.split("_")[0],
            )
            for var in variables
        ]
        if any(stored_estimates):
            return stored_estimates

    try:
        estimates = get_estimate(
            acs_id=acs_id,
            year=year,
            variables=variables,
            state_fips=state,
            county_fips=county,
            place_fips=place,
        )
        return estimates
    except Exception as e:
        raise HTTPException(500, str(e))
