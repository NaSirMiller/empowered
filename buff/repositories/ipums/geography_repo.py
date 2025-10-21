from sqlmodel import SQLModel

from buff.models.sql.sql_client import SQLClient
from buff.models.sql.schemas import IpumsCity, CensusGeography
from buff.utils import get_db_client
from typing import List


class GeographyRepository:
    def __init__(self, db_client: SQLClient = get_db_client()) -> None:
        self.db_client = db_client

    def get_state(self, state_fips_code: int) -> List[SQLModel]:
        return self.db_client.select(
            model=CensusGeography, params={"state_fips": state_fips_code}
        )

    def insert_city(self, city_id: int, city_name: str, state_fips_code: int) -> None:
        self.db_client.insert(
            IpumsCity(name=city_name, city_id=city_id, state_fips_id=state_fips_code)
        )

    def get_city(
        self, city_id: int | None = None, city_name: str | None = None
    ) -> List[SQLModel]:
        parameters = {}
        if city_id is not None:
            parameters["city_id"] = city_id
        if city_name is not None:
            parameters["name"] = city_name
        if city_id is None and city_name is None:
            return []
        return self.db_client.select(model=IpumsCity, params=parameters)
