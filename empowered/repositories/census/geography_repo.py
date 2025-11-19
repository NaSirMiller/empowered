from sqlmodel import SQLModel

from empowered.models.sql.sql_client import SQLClient
from empowered.models.sql.schemas import CensusPlace, CensusState, CensusCounty
from empowered.utils import get_db_client


class GeographyRepository:
    def __init__(self, db_client: SQLClient = get_db_client()) -> None:
        self.db_client = db_client

    def get_state(
        self,
        state_fips_code: int,
        dataset_id: str,
        year_id: int,
        state_name: str | None = None,
    ) -> list[SQLModel]:
        parameters = {"state_fips": state_fips_code}
        if state_name is not None:
            parameters["state_name"] = state_name
        return self.db_client.select(model=CensusState, params=parameters)

    def get_county(
        self,
        county_fips_code: int,
        dataset_id: str,
        year_id: int,
        county_name: str | None = None,
    ) -> list[SQLModel]:
        parameters = {"county_fips": county_fips_code}
        if county_name is not None:
            parameters["county_name"] = county_name
        return self.db_client.select(model=CensusCounty, params=parameters)

    def get_place(
        self,
        state_fips_code: int,
        dataset_id: str,
        year_id: int,
        place_fips_code: int | None = None,
        place_name: str | None = None,
    ) -> list[SQLModel]:
        parameters = {"state_fips": state_fips_code}
        if place_fips_code is not None:
            parameters["place_fips"] = place_fips_code
        if place_name is not None:
            parameters["place_name"] = place_name
        return self.db_client.select(model=CensusPlace, params=parameters)

    def insert_state(
        self,
        state_fips_code: int,
        state_name: str,
        dataset_id: str,
        year_id: int,
    ) -> None:
        return self.db_client.insert(
            instances=[
                CensusState(
                    state_fips=state_fips_code,
                    state_name=state_name,
                    dataset_id=dataset_id,
                    year_id=year_id,
                )
            ]
        )

    def insert_county(
        self,
        county_fips_code: int,
        county_name: str,
        dataset_id: str,
        year_id: int,
    ) -> None:
        return self.db_client.insert(
            instances=[
                CensusCounty(
                    county_fips=county_fips_code,
                    county_name=county_name,
                    dataset_id=dataset_id,
                    year_id=year_id,
                )
            ]
        )

    def insert_place(
        self,
        state_fips_code: int,
        place_fips_code: int,
        place_name: str,
        dataset_id: str,
        year_id: int,
    ) -> None:
        return self.db_client.insert(
            instances=[
                CensusPlace(
                    place_fips=place_fips_code,
                    state_fips=state_fips_code,
                    place_name=place_name,
                    dataset_id=dataset_id,
                    year_id=year_id,
                )
            ]
        )
