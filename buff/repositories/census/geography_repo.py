from sqlmodel import SQLModel

from buff.models.sql.sql_client import SQLClient
from buff.models.sql.schemas import CensusGeography
from buff.utils import get_db_client


class GeographyRepository:
    def __init__(self, db_client: SQLClient = get_db_client()) -> None:
        self.db_client = db_client

    def get_geography(
        self,
        state_fips_code: int,
        county_fips_code: int | None = None,
        county_name: str | None = None,
        city_fips_code: int | None = None,
        city_name: str | None = None,
        state_name: str | None = None,
    ) -> list[SQLModel]:
        parameters = {"state_fips": state_fips_code}
        if county_fips_code is not None:
            parameters["county_fips"] = county_fips_code
        if county_name is not None:
            parameters["county_name"] = county_name
        if city_fips_code is not None:
            parameters["city_fips"] = city_fips_code
        if city_name is not None:
            parameters["city_name"] = city_name
        if state_name is not None:
            parameters["state_name"] = state_name
        return self.db_client.select(model=CensusGeography, params=parameters)

    def insert_geography(
        self,
        state_fips_code: int,
        county_fips_code: int,
        county_name: str,
        city_fips_code: int,
        city_name: str,
        state_name: str,
    ) -> None:
        return self.db_client.insert(
            instances=[
                CensusGeography(
                    geo_id=None,
                    state_fips=state_fips_code,
                    state_name=state_name,
                    county_fips=county_fips_code,
                    county_name=county_name,
                    city_fips=city_fips_code,
                    city_name=city_name,
                )
            ]
        )
