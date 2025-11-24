from sqlmodel import SQLModel

from empowered.models.sql.sql_client import SQLClient
from empowered.models.sql.schemas import CensusPlace, CensusState, CensusCounty
from empowered.utils import get_sql_client


class GeographyRepository:
    def __init__(self, db_client: SQLClient = get_sql_client()) -> None:
        self.db_client = db_client

    def get_states(
        self,
        state_fips_code: int,
        dataset_id: str,
        year_id: int,
        state_name: str | None = None,
    ) -> list[SQLModel]:
        parameters = {
            "state_fips": state_fips_code,
            "dataset_id": dataset_id,
            "year_id": year_id,
        }
        if state_name is not None:
            parameters["state_name"] = state_name
        return self.db_client.select(model=CensusState, params=parameters)

    def get_counties(
        self,
        county_fips_code: int,
        dataset_id: str,
        year_id: int,
        county_name: str | None = None,
    ) -> list[SQLModel]:
        parameters = {
            "county_fips": county_fips_code,
            "dataset_id": dataset_id,
            "year_id": year_id,
        }
        if county_name is not None:
            parameters["county_name"] = county_name
        return self.db_client.select(model=CensusCounty, params=parameters)

    def get_places(
        self,
        state_fips_code: int,
        dataset_id: str,
        year_id: int,
        place_fips_code: int | None = None,
        place_name: str | None = None,
    ) -> list[SQLModel]:
        parameters = {
            "state_fips": state_fips_code,
            "dataset_id": dataset_id,
            "year_id": year_id,
        }
        if place_fips_code is not None:
            parameters["place_fips"] = place_fips_code
        if place_name is not None:
            parameters["place_name"] = place_name
        return self.db_client.select(model=CensusPlace, params=parameters)

    def insert_states(
        self,
        states: list[dict],
        dataset_id: str,
        year_id: int,
    ) -> None:

        instances = [
            CensusState(
                state_fips=s["state_fips"],
                state_name=s["state_name"],
                dataset_id=dataset_id,
                year_id=year_id,
            )
            for s in states
        ]

        return self.db_client.insert(instances=instances)

    def insert_counties(
        self,
        counties: list[dict],
        dataset_id: str,
        year_id: int,
    ) -> None:
        instances = [
            CensusCounty(
                county_fips=c["county_fips"],
                county_name=c["county_name"],
                dataset_id=dataset_id,
                year_id=year_id,
            )
            for c in counties
        ]

        return self.db_client.insert(instances=instances)

    def insert_places(
        self,
        places: list[dict],
        dataset_id: str,
        year_id: int,
    ) -> None:
        instances = [
            CensusPlace(
                place_fips=p["place_fips"],
                state_fips=p["state_fips"],
                place_name=p["place_name"],
                dataset_id=dataset_id,
                year_id=year_id,
            )
            for p in places
        ]

        return self.db_client.insert(instances=instances)
